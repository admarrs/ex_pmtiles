defmodule ExPmtiles.Cache.Operations do
  @moduledoc """
  Pure business logic for PMTiles cache operations.

  This module contains all the tile fetching, caching, and coordination logic
  without any GenServer-specific code. All functions are pure and easily testable.
  """

  require Logger
  alias ExPmtiles.Cache.FileHandler

  @env Mix.env()

  @doc """
  Handles a tile request, checking cache and fetching if needed.
  """
  def handle_tile_request(name, tile_id, z, x, y, request_id, config, enable_tile_cache) do
    if enable_tile_cache do
      handle_cached_tile_request(name, tile_id, z, x, y, request_id, config, enable_tile_cache)
    else
      handle_uncached_tile_request(name, tile_id, z, x, y, request_id, config, enable_tile_cache)
    end
  end

  @doc """
  Handles a tile request when tile caching is enabled.
  """
  def handle_cached_tile_request(
        name,
        tile_id,
        z,
        x,
        y,
        request_id,
        %{stats: _stats_table, cache_path: nil} = config,
        enable_tile_cache
      ),
      # Caching disabled
      do: handle_cache_miss(name, tile_id, z, x, y, request_id, config, enable_tile_cache)

  def handle_cached_tile_request(
        name,
        tile_id,
        z,
        x,
        y,
        request_id,
        %{stats: stats_table, cache_path: cache_path} = config,
        enable_tile_cache
      ) do
    tile_file_path = FileHandler.tile_cache_file_path(cache_path, tile_id)

    if File.exists?(tile_file_path) do
      # Cache hit - read from file
      case FileHandler.read_tile_from_cache(tile_file_path) do
        {:ok, tile_data} ->
          :ets.update_counter(stats_table, :hits, 1)
          {:ok, tile_data}

        :error ->
          # Failed to read, treat as cache miss
          handle_cache_miss(name, tile_id, z, x, y, request_id, config, enable_tile_cache)
      end
    else
      # Cache miss
      handle_cache_miss(name, tile_id, z, x, y, request_id, config, enable_tile_cache)
    end
  end

  @doc """
  Handles a tile request when tile caching is disabled.
  """
  def handle_uncached_tile_request(
        name,
        tile_id,
        z,
        x,
        y,
        _request_id,
        config,
        enable_tile_cache
      ) do
    %{pending: pending_table} = config

    case check_pending_request(tile_id, pending_table, enable_tile_cache) do
      {:pending, result} ->
        result

      :not_pending ->
        fetch_new_tile(name, tile_id, z, x, y, config, enable_tile_cache)
    end
  end

  @doc """
  Handles a cache miss by checking if request is pending or fetching new tile.
  """
  def handle_cache_miss(name, tile_id, z, x, y, _request_id, config, enable_tile_cache) do
    %{pending: pending_table, stats: stats_table, cache_path: cache_path} = config

    case check_pending_request(tile_id, pending_table, enable_tile_cache) do
      {:pending, :retry_cache_lookup} ->
        retry_cache_lookup(tile_id, cache_path, stats_table)

      {:pending, result} ->
        result

      :not_pending ->
        fetch_new_tile(name, tile_id, z, x, y, config, enable_tile_cache)
    end
  end

  @doc """
  Retries looking up a tile in the cache after another process fetched it.
  """
  def retry_cache_lookup(_tile_id, nil, _stats_table),
    do: {:error, :tile_not_found}

  def retry_cache_lookup(tile_id, cache_path, stats_table) do
    tile_file_path = FileHandler.tile_cache_file_path(cache_path, tile_id)

    if File.exists?(tile_file_path) do
      case FileHandler.read_tile_from_cache(tile_file_path) do
        {:ok, tile_data} ->
          :ets.update_counter(stats_table, :hits, 1)
          {:ok, tile_data}

        :error ->
          {:error, :tile_not_found}
      end
    else
      {:error, :tile_not_found}
    end
  end

  @doc """
  Checks if a tile request is already pending.
  Returns {:pending, result} if pending, :not_pending otherwise.
  """
  def check_pending_request(tile_id, pending_table, enable_tile_cache) do
    case :ets.lookup(pending_table, tile_id) do
      [{^tile_id, waiting_pids}] ->
        handle_pending_tile_request(tile_id, pending_table, waiting_pids, enable_tile_cache)

      [] ->
        :not_pending
    end
  end

  defp handle_pending_tile_request(tile_id, pending_table, waiting_pids, enable_tile_cache) do
    # Add ourselves to the waiting list
    :ets.insert(pending_table, {tile_id, [self() | waiting_pids]})

    # Double-check the entry still exists after we added ourselves
    # This handles the race where notification happened between our lookup and insert
    case :ets.lookup(pending_table, tile_id) do
      [] ->
        handle_race_condition(tile_id, enable_tile_cache)

      [{^tile_id, _}] ->
        {:pending, wait_for_tile_result(tile_id)}
    end
  end

  defp handle_race_condition(tile_id, enable_tile_cache) do
    # Entry was deleted - fetch completed while we were adding ourselves
    # Check if we got the notification
    receive do
      {:tile_ready, ^tile_id, result} ->
        {:pending, result}
    after
      0 ->
        # No notification received - we added ourselves after notification was sent
        if enable_tile_cache do
          # For cached tiles, the result is in cache
          {:pending, :retry_cache_lookup}
        else
          # For uncached tiles, return not_pending so a new fetch happens
          :not_pending
        end
    end
  end

  defp wait_for_tile_result(tile_id) do
    receive do
      {:tile_ready, ^tile_id, result} ->
        result
    after
      30_000 ->
        Logger.error(
          "Timeout waiting for tile #{tile_id} after 30s (waiting for another process)"
        )

        {:error, :timeout}
    end
  end

  @doc """
  Fetches a new tile from the PMTiles file and caches it.
  """
  def fetch_new_tile(name, tile_id, z, x, y, config, enable_tile_cache) do
    %{
      pending: pending_table,
      stats: stats_table,
      table: table_name,
      cache_path: cache_path
    } = config

    # Don't add the first caller to the pending list - it will use Task.await
    :ets.insert(pending_table, {tile_id, []})
    :ets.update_counter(stats_table, :misses, 1)

    # Get table names from config map
    pending_directories_table = :"#{table_name}_pending_directories"
    max_entries_table = :"#{table_name}_max_entries"

    start_time = System.monotonic_time(:millisecond)

    # Spawn task to fetch tile without blocking
    task =
      Task.async(fn ->
        try do
          # Get PMTiles instance from GenServer (lightweight call, just returns struct)
          {pmtiles, _table_refs} =
            GenServer.call(
              name,
              :get_pmtiles,
              5000
            )

          # Fetch tile using file-based caching
          cache_params = %{
            cache_path: cache_path,
            pending_directories_table: pending_directories_table,
            max_entries_table: max_entries_table,
            enable_tile_cache: enable_tile_cache
          }

          # Return result WITHOUT calling notify - let the await handler do it
          # This prevents double-notification race condition on timeout
          fetch_and_cache_tile(pmtiles, tile_id, z, x, y, cache_params)
        rescue
          e ->
            Logger.error("Error fetching tile #{tile_id} (#{z}/#{x}/#{y}): #{inspect(e)}")
            Logger.error(Exception.format_stacktrace(__STACKTRACE__))
            {:error, :fetch_failed}
        end
      end)

    try do
      # Wait up to 30 seconds - with proper connection pooling, fetches should be much faster
      result = Task.await(task, 30_000)
      # Notify on successful completion
      notify_waiting_processes(pending_table, tile_id, result)
      result
    catch
      :exit, {:timeout, _} ->
        elapsed = System.monotonic_time(:millisecond) - start_time

        Logger.error(
          "Tile fetch for #{z}/#{x}/#{y} (tile_id: #{tile_id}) timed out after #{elapsed}ms (exceeded 30s limit). " <>
            "This suggests slow S3/storage response or network issues."
        )

        Task.shutdown(task, :brutal_kill)
        notify_waiting_processes(pending_table, tile_id, {:error, :timeout})
        {:error, :timeout}

      :exit, reason ->
        # Handle other exit reasons (e.g., GenServer timeout, task crash, etc.)
        elapsed = System.monotonic_time(:millisecond) - start_time

        Logger.error(
          "Tile fetch for #{z}/#{x}/#{y} (tile_id: #{tile_id}) failed after #{elapsed}ms with exit reason: #{inspect(reason)}"
        )

        Task.shutdown(task, :brutal_kill)
        notify_waiting_processes(pending_table, tile_id, {:error, :fetch_failed})
        {:error, :fetch_failed}
    end
  end

  @doc """
  Notifies all processes waiting for a tile and cleans up the pending entry.

  This is primarily used internally but can also be used for testing and diagnostics.
  """
  def notify_waiting_processes(pending_table, tile_id, result) do
    case :ets.lookup(pending_table, tile_id) do
      [{^tile_id, waiting_pids}] ->
        Enum.each(waiting_pids, fn pid ->
          send(pid, {:tile_ready, tile_id, result})
        end)

      [] ->
        :ok
    end

    :ets.delete(pending_table, tile_id)
  end

  defp fetch_and_cache_tile(
         pmtiles,
         tile_id,
         z,
         x,
         y,
         cache_params
       ) do
    %{
      cache_path: cache_path,
      pending_directories_table: pending_directories_table,
      max_entries_table: max_entries_table,
      enable_tile_cache: enable_tile_cache
    } = cache_params

    # Use file-based directory caching in production, regular caching in tests
    result = fetch_tile_from_pmtiles(pmtiles, z, x, y, cache_path, pending_directories_table)

    case result do
      {nil, _updated_pmtiles} ->
        Logger.warning("Tile not found: #{z}/#{x}/#{y} (tile_id: #{tile_id})")
        {:error, :tile_not_found}

      {{_offset, _length, tile_data}, _updated_pmtiles} ->
        maybe_cache_tile(
          tile_data,
          tile_id,
          cache_path,
          max_entries_table,
          enable_tile_cache
        )

        {:ok, tile_data}
    end
  end

  defp fetch_tile_from_pmtiles(pmtiles, z, x, y, cache_path, pending_directories_table) do
    if @env == :test do
      pmtiles_module().get_zxy(pmtiles, z, x, y)
    else
      pmtiles_module().get_zxy(pmtiles, z, x, y, cache_path, pending_directories_table)
    end
  end

  defp maybe_cache_tile(tile_data, tile_id, cache_dir, _max_entries_table, enable_tile_cache) do
    if enable_tile_cache and not is_nil(cache_dir) do
      tile_file_path = FileHandler.tile_cache_file_path(cache_dir, tile_id)
      FileHandler.write_tile_to_cache(tile_file_path, tile_data)
    end
  end

  # Used to get the pmtiles module from the application environment so it can be mocked in tests
  defp pmtiles_module do
    Application.get_env(:ex_pmtiles, :pmtiles_module, ExPmtiles)
  end
end
