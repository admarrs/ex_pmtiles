defmodule ExPmtiles.Cache do
  @moduledoc """
  A GenServer implementation of a multi-level cache for PMTiles file data.

  This GenServer provides efficient access to PMTiles map data through several caching mechanisms:

  ## Cache Levels

  ### 1. Directory Cache
  - Caches deserialized directory structures from the PMTiles file
  - Pre-populates cache for zoom levels 0-4 on startup
  - Persists cached directories to disk for faster restarts
  - Uses zlib compression for efficient storage
  - Shared across all requests to prevent duplicate S3 fetches

  ### 2. Tile Cache
  - Uses ETS tables to cache individual tile data
  - Implements LRU (Least Recently Used) eviction
  - Configurable maximum entries (default: 100,000)
  - Maintains hit/miss statistics
  - Automatic cleanup of idle entries (1 hour timeout)

  ### 3. Concurrent Request Handling
  - Prevents duplicate requests for the same tile
  - Coordinates multiple requests through a pending requests table
  - Broadcasts results to all waiting processes
  - 30-second timeout for waiting processes

  ## Cache Files
  Directory cache files are stored in `priv/pmtiles_cache` with the format:
  `{bucket}_{filename}_directories.cache`

  ## Configuration
  - `:max_entries` - Maximum number of tiles to cache (default: 100,000)
  - `:pmtiles_module` - Module to use for PMTiles operations (configurable for testing)

  ## Usage

  ```elixir
  # Start the cache for an S3 PMTiles file
  {:ok, pid} = ExPmtiles.Cache.start_link(bucket: "maps", path: "map.pmtiles")

  # Start the cache for a local PMTiles file
  {:ok, pid} = ExPmtiles.Cache.start_link(bucket: nil, path: "/path/to/map.pmtiles")

  # Get a tile by coordinates
  case ExPmtiles.Cache.get_tile(pid, 10, 512, 256) do
    {:ok, tile_data} ->
      # Handle tile data
      tile_data
    {:error, reason} ->
      # Handle error
      nil
  end

  # Get cache statistics
  stats = ExPmtiles.Cache.get_stats(pid)
  # Returns: %{hits: 150, misses: 25}
  ```

  ## Implementation Details
  The GenServer maintains several ETS tables:
  - **Main table**: Caches tile data with timestamps for LRU eviction
  - **Pending table**: Tracks in-progress requests to prevent duplicates
  - **Stats table**: Maintains hit/miss statistics

  The directory cache is maintained in the GenServer state and is progressively
  updated as new directories are accessed. This prevents duplicate S3 requests
  and directory deserialization operations.

  ## Performance Features
  - **Background population**: Pre-loads directories for zoom levels 0-4
  - **Persistent cache**: Saves directories to disk for faster restarts
  - **Compression**: Uses zlib compression for cache files
  - **Concurrent access**: Multiple processes can safely access the same cache
  - **Automatic cleanup**: Removes idle entries every 5 minutes
  """
  use GenServer
  require Logger

  @env Mix.env()
  @table_prefix :pmtiles_cache
  @max_idle_time :timer.hours(1)
  @default_max_entries 100_000
  @cache_dir "priv/pmtiles_cache"

  # Client API
  @doc """
  Starts a new PMTiles cache GenServer.

  Creates a named GenServer process that manages caching for a specific PMTiles file.
  The process name is derived from the bucket and path to ensure uniqueness.

  ## Parameters

  - `opts` - Keyword list of options:
    - `:bucket` - S3 bucket name (or `nil` for local files)
    - `:path` - Path to the PMTiles file
    - `:max_entries` - Maximum number of tiles to cache (default: 100,000)

  ## Returns

  - `{:ok, pid}` - Successfully started cache process
  - `{:error, reason}` - Failed to start cache

  ## Examples

      iex> ExPmtiles.Cache.start_link(region: nil, bucket: "maps", path: "world.pmtiles")
      {:ok, #PID<0.123.0>}

      iex> ExPmtiles.Cache.start_link(bucket: nil, path: "/data/local.pmtiles")
      {:ok, #PID<0.124.0>}

      iex> ExPmtiles.Cache.start_link(bucket: "maps", path: "world.pmtiles", max_entries: 50_000)
      {:ok, #PID<0.125.0>}
  """
  def start_link(opts \\ []) do
    region = Keyword.get(opts, :region)
    bucket = Keyword.get(opts, :bucket)
    path = Keyword.get(opts, :path)
    storage = Keyword.get(opts, :storage, :s3)
    GenServer.start_link(__MODULE__, {region, bucket, path, storage, opts}, name: name_for(bucket, path))
  end

  @doc """
  Retrieves a tile from the cache by coordinates.

  This function handles both cache hits and misses. If the tile is not in the cache,
  it will be fetched from the PMTiles file and cached for future requests.

  ## Parameters

  - `{bucket, path}` - Tuple identifying the PMTiles file or `pid` identifying the cache process
  - `z` - Zoom level (integer)
  - `x` - X coordinate (integer)
  - `y` - Y coordinate (integer)

  ## Returns

  - `{:ok, tile_data}` - Tile data as binary
  - `{:error, reason}` - Error retrieving tile (e.g., `:tile_not_found`, `:timeout`)
  - `nil` - If the cache process is not available

  ## Examples

      iex> ExPmtiles.Cache.get_tile({"maps", "world.pmtiles"}, 10, 512, 256)
      {:ok, <<...>>}

      iex> ExPmtiles.Cache.get_tile({"maps", "world.pmtiles"}, 25, 0, 0)
      {:error, :tile_not_found}

      iex> ExPmtiles.Cache.get_tile(pid, 10, 512, 256)
      {:ok, <<...>>}
  """
  def get_tile({bucket, path}, z, x, y) do
    request_id = System.unique_integer([:positive])
    Logger.debug("Starting request #{request_id} for tile #{z}/#{x}/#{y}")

    name = name_for(bucket, path)
    table_name = table_name_for(bucket, path)

    tables = %{
      table: table_name,
      pending: :"#{table_name}_pending",
      stats: :"#{table_name}_stats"
    }

    tile_id = pmtiles_module().zxy_to_tile_id(z, x, y)

    case :ets.whereis(table_name) do
      :undefined -> nil
      _ -> handle_tile_request(name, tile_id, z, x, y, request_id, tables)
    end
  end

  def get_tile(pid, z, x, y) do
    case GenServer.call(pid, {:get_tile, z, x, y}, :infinity) do
      {:ok, tile_data} ->
        tile_data

      {:error, _reason} ->
        nil
    end
  end

  @doc """
  Retrieves cache statistics.

  Returns hit and miss counts for the cache, useful for monitoring cache
  performance and effectiveness.

  ## Parameters

  - `pid` - The cache process PID

  ## Returns

  - `%{hits: integer(), misses: integer()}` - Cache statistics

  ## Examples

      iex> {:ok, pid} = ExPmtiles.Cache.start_link(bucket: "maps", path: "world.pmtiles")
      iex> ExPmtiles.Cache.get_stats(pid)
      %{hits: 150, misses: 25}

      iex> # After some cache operations
      iex> ExPmtiles.Cache.get_stats(pid)
      %{hits: 175, misses: 30}
  """
  def get_stats(pid) do
    GenServer.call(pid, :get_stats)
  end

  # Server callbacks
  @impl true
  def init({region, bucket, path, storage, opts}) do
    case initialize_pmtiles(region, bucket, path, storage) do
      nil -> {:stop, :pmtiles_not_found}
      pmtiles -> setup_cache_server(pmtiles, bucket, path, opts)
    end
  end

  @impl true
  def handle_call(
        {:get_tile, z, x, y},
        _from,
        %{pmtiles: pmtiles, table: table, stats_table: stats_table, max_entries: max_entries} =
          state
      ) do
    tile_id = pmtiles_module().zxy_to_tile_id(z, x, y)
    now = System.system_time(:second)

    case :ets.lookup(table, tile_id) do
      [{^tile_id, tile_data, _last_access}] ->
        # Update last access time
        :ets.insert(table, {tile_id, tile_data, now})
        # Cache hit
        update_stats(stats_table, :hit)
        {:reply, {:ok, tile_data}, state}

      [] ->
        # Cache miss - fetch tile and cache the result
        update_stats(stats_table, :miss)

        case fetch_and_cache_tile(pmtiles, tile_id, table, max_entries) do
          {:ok, tile_data} ->
            {:reply, {:ok, tile_data}, state}

          {:error, reason} ->
            {:reply, {:error, reason}, state}
        end
    end
  end

  @impl true
  def handle_call({:fetch_and_cache_tile, z, x, y, request_id, tile_id}, _from, state) do
    Logger.debug("Starting fetch for request #{request_id}")
    handle_tile_fetch(z, x, y, request_id, tile_id, state)
  end

  @impl true
  def handle_call(:get_stats, _from, %{stats_table: stats_table} = state) do
    [{:hits, hits}] = :ets.lookup(stats_table, :hits)
    [{:misses, misses}] = :ets.lookup(stats_table, :misses)
    {:reply, %{hits: hits, misses: misses}, state}
  end

  @impl true
  def handle_cast({:update_pmtiles, updated_pmtiles}, state) do
    Logger.debug(
      "Updating pmtiles instance with cached directories: #{inspect(Map.keys(updated_pmtiles.directories))}"
    )

    {:noreply, %{state | pmtiles: updated_pmtiles}}
  end

  @impl true
  def handle_info(:cleanup, state) do
    cleanup_idle_entries(state.table)
    Process.send_after(self(), :cleanup, :timer.minutes(5))
    {:noreply, state}
  end

  @impl true
  def terminate(_reason, %{table: table}) do
    :ets.delete(table)
    :ok
  end

  # Private functions
  defp name_for(bucket, path) do
    :"#{@table_prefix}_process_#{bucket}_#{Path.basename(path, ".pmtiles")}"
  end

  defp table_name_for(bucket, path) do
    :"#{@table_prefix}_table_#{bucket}_#{Path.basename(path, ".pmtiles")}"
  end

  defp fetch_and_cache_tile(pmtiles, tile_id, table, max_entries) do
    {z, x, y} = pmtiles_module().tile_id_to_zxy(tile_id)

    case pmtiles_module().get_zxy(pmtiles, z, x, y) do
      nil ->
        {:error, :tile_not_found}

      {_offset, _length, tile_data} ->
        now = System.system_time(:second)
        :ets.insert(table, {tile_id, tile_data, now})
        check_table_size(table, max_entries)
        {:ok, tile_data}
    end
  end

  defp update_stats(stats_table, :hit) do
    :ets.update_counter(stats_table, :hits, 1)
  end

  defp update_stats(stats_table, :miss) do
    :ets.update_counter(stats_table, :misses, 1)
  end

  defp cleanup_idle_entries(table) do
    now = System.system_time(:second)
    cutoff_time = now - @max_idle_time
    :ets.select_delete(table, [{{:_, :_, :"$1"}, [{:<, :"$1", cutoff_time}], [true]}])
  end

  defp check_table_size(table, max_entries) do
    case :ets.info(table, :size) do
      size when size > max_entries ->
        # Get all entries sorted by timestamp (oldest first)
        entries =
          :ets.match_object(table, {:_, :_, :_})
          |> Enum.sort_by(fn {_, _, timestamp} -> timestamp end)

        # Remove enough entries to get below max_entries
        # -1 to ensure we get under the limit
        num_to_remove = size - (max_entries - 1)
        entries_to_remove = Enum.take(entries, num_to_remove)

        Enum.each(entries_to_remove, fn {key, _, _} ->
          :ets.delete(table, key)
        end)

      _ ->
        :ok
    end
  end

  defp populate_initial_cache(server_pid, pmtiles, bucket, path) do
    # Populate cache for zoom levels 0-4
    result =
      for z <- 0..4,
          x <- 0..(trunc(:math.pow(2, z)) - 1),
          y <- 0..(trunc(:math.pow(2, z)) - 1),
          reduce: pmtiles do
        current_pmtiles ->
          _tile_id = pmtiles_module().zxy_to_tile_id(z, x, y)

          case pmtiles_module().get_zxy(current_pmtiles, z, x, y) do
            {{_offset, _length, _tile_data}, updated_pmtiles} ->
              GenServer.cast(server_pid, {:update_pmtiles, updated_pmtiles})
              updated_pmtiles

            {_nil, updated_pmtiles} ->
              GenServer.cast(server_pid, {:update_pmtiles, updated_pmtiles})
              updated_pmtiles
          end
      end

    # Save the cached directories
    save_cached_directories(bucket, path, result.directories)

    Logger.info("Completed background population of directory cache for zoom levels 0-4")
    result
  end

  defp cache_file_path(bucket, path) do
    filename = "#{bucket}_#{Path.basename(path, ".pmtiles")}_directories.cache"
    Path.join(@cache_dir, filename)
  end

  defp save_cached_directories(bucket, path, directories) do
    cache_path = cache_file_path(bucket, path)

    with :ok <- File.mkdir_p(Path.dirname(cache_path)),
         binary = :erlang.term_to_binary(directories),
         compressed = :zlib.compress(binary),
         :ok <- File.write(cache_path, compressed) do
      Logger.info("Saved compressed directory cache for #{bucket}/#{path}")
      :ok
    else
      error -> Logger.error("Failed to save cached directories: #{inspect(error)}")
    end
  end

  defp load_cached_directories(bucket, path) do
    cache_path = cache_file_path(bucket, path)

    with :ok <- File.mkdir_p(Path.dirname(cache_path)),
         {:ok, compressed} <- File.read(cache_path),
         binary = :zlib.uncompress(compressed),
         {:ok, term} <- safe_binary_to_term(binary) do
      Logger.info("Loaded compressed directory cache for #{bucket}/#{path}")
      {:ok, term}
    else
      error -> {:error, error}
    end
  end

  # Safe deserialization of terms
  defp safe_binary_to_term(binary) do
    try do
      term = :erlang.binary_to_term(binary, [:safe])
      {:ok, term}
    rescue
      error ->
        Logger.error("Failed to deserialize cached directories: #{inspect(error)}")
        {:error, :invalid_cache_data}
    end
  end

  # Used to get the pmtiles module from the application environment so it can be mocked in tests
  defp pmtiles_module do
    case @env do
      :test ->
        ExPmtiles.CacheMock

      _ ->
        ExPmtiles
    end
  end

  defp handle_tile_request(name, tile_id, z, x, y, request_id, tables) do
    %{table: _table_name, pending: pending_table, stats: _stats_table} = tables

    case check_pending_request(tile_id, pending_table, request_id) do
      {:pending, result} -> result
      :not_pending -> fetch_new_tile(name, tile_id, z, x, y, request_id, pending_table)
    end
  end

  defp check_pending_request(tile_id, pending_table, request_id) do
    case :ets.lookup(pending_table, tile_id) do
      [{^tile_id, waiting_pids}] ->
        Logger.debug("Request #{request_id} waiting for pending fetch")
        :ets.insert(pending_table, {tile_id, [self() | waiting_pids]})
        {:pending, wait_for_tile_result(tile_id)}

      [] ->
        :not_pending
    end
  end

  defp wait_for_tile_result(tile_id) do
    receive do
      {:tile_ready, ^tile_id, result} ->
        Logger.debug("Received tile_ready for tile #{tile_id}")
        result
    after
      30_000 -> {:error, :timeout}
    end
  end

  defp fetch_new_tile(name, tile_id, z, x, y, request_id, pending_table) do
    Logger.debug("No pending request found for tile #{tile_id}, starting new request")
    :ets.insert(pending_table, {tile_id, [self()]})

    result =
      GenServer.call(name, {:fetch_and_cache_tile, z, x, y, request_id, tile_id}, :infinity)

    notify_waiting_processes(pending_table, tile_id, result)
    :ets.delete(pending_table, tile_id)
    result
  end

  defp notify_waiting_processes(pending_table, tile_id, result) do
    case :ets.lookup(pending_table, tile_id) do
      [{^tile_id, waiting_pids}] ->
        Enum.each(waiting_pids, fn pid ->
          send(pid, {:tile_ready, tile_id, result})
        end)

      [] ->
        :ok
    end
  end

  defp handle_tile_fetch(z, x, y, request_id, tile_id, state) do
    result = fetch_tile_data(state.pmtiles, z, x, y)
    handle_fetch_result(result, tile_id, request_id, state)
  end

  defp fetch_tile_data(pmtiles, z, x, y) do
    pmtiles_module().get_zxy(pmtiles, z, x, y)
  end

  defp handle_fetch_result({nil, updated_pmtiles}, tile_id, request_id, state) do
    Logger.debug("Tile not found for request #{request_id}")
    # Notify waiting processes
    case :ets.lookup(state.pending_table, tile_id) do
      [{^tile_id, waiting_pids}] ->
        Enum.each(waiting_pids, fn pid ->
          send(pid, {:tile_ready, tile_id, {:error, :tile_not_found}})
        end)

      [] ->
        :ok
    end

    :ets.delete(state.pending_table, tile_id)

    {:reply, {:error, :tile_not_found}, %{state | pmtiles: updated_pmtiles}}
  end

  defp handle_fetch_result(
         {{_offset, _length, tile_data}, updated_pmtiles},
         tile_id,
         request_id,
         state
       ) do
    Logger.debug("Got tile data for request #{request_id}, caching...")
    now = System.system_time(:second)
    :ets.insert(state.table, {tile_id, tile_data, now})
    check_table_size(state.table, state.max_entries)

    # Notify waiting processes
    case :ets.lookup(state.pending_table, tile_id) do
      [{^tile_id, waiting_pids}] ->
        Enum.each(waiting_pids, fn pid ->
          send(pid, {:tile_ready, tile_id, {:ok, tile_data}})
        end)

      [] ->
        :ok
    end

    :ets.delete(state.pending_table, tile_id)

    {:reply, {:ok, tile_data}, %{state | pmtiles: updated_pmtiles}}
  end

  defp initialize_pmtiles(region, bucket, path, storage) do
    pmtiles_module().new(region, bucket, path, storage)
  end

  defp setup_cache_server(pmtiles, bucket, path, opts) do
    table_name = table_name_for(bucket, path)
    max_entries = Keyword.get(opts, :max_entries, @default_max_entries)
    Logger.info("Initializing PMTiles cache for #{bucket}/#{path}")

    tables = create_ets_tables(table_name)

    pmtiles = start_background_cache_population(pmtiles, bucket, path)

    {:ok,
     %{
       pmtiles: pmtiles,
       table: table_name,
       pending_table: tables.pending,
       stats_table: tables.stats,
       max_entries: max_entries
     }}
  end

  defp create_ets_tables(table_name) do
    :ets.new(table_name, [:set, :public, :named_table, read_concurrency: true])
    pending_table = :"#{table_name}_pending"
    :ets.new(pending_table, [:set, :public, :named_table])
    stats_table = :"#{table_name}_stats"
    :ets.new(stats_table, [:set, :public, :named_table])
    :ets.insert(stats_table, {:hits, 0})
    :ets.insert(stats_table, {:misses, 0})

    %{table: table_name, pending: pending_table, stats: stats_table}
  end

  defp start_background_cache_population(pmtiles, bucket, path) do
    if @env != :test do
      Logger.info("Starting background population of directory cache for zoom levels 0-4")

      try_load_cached_directories(pmtiles, bucket, path)
    else
      pmtiles
    end
  end

  defp try_load_cached_directories(pmtiles, bucket, path) do
    # Try to load cached directories
    case load_cached_directories(bucket, path) do
      {:ok, directories} ->
        Logger.info("Loaded cached directories for #{bucket}/#{path}")
        %{pmtiles | directories: directories}

      {:error, reason} ->
        Logger.info("No cached directories found for #{bucket}/#{path}: #{inspect(reason)}")
        # Start background cache population
        Logger.info("Starting background population of directory cache for zoom levels 0-4")

        Task.start(fn ->
          populate_initial_cache(self(), pmtiles, bucket, path)
        end)

        pmtiles
    end
  end
end
