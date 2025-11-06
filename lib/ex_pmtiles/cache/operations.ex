defmodule ExPmtiles.Cache.Operations do
  @moduledoc """
  Pure business logic for PMTiles cache operations.

  This module contains all the tile fetching and caching logic without any
  GenServer-specific code. All functions are pure and easily testable.

  The architecture is simplified: each request process does its own work,
  checking file-based caches and fetching from storage as needed. No inter-process
  coordination is needed because the filesystem handles concurrent access naturally.
  """

  require Logger
  alias ExPmtiles.Cache.FileHandler

  @env Mix.env()

  @doc """
  Handles a tile request, checking cache and fetching if needed.

  This function is called in the context of the requesting process (e.g., Phoenix request).
  It does all the work synchronously in the caller's process.
  """
  def handle_tile_request(pmtiles, tile_id, z, x, y, config, enable_tile_cache) do
    if enable_tile_cache do
      handle_cached_tile_request(pmtiles, tile_id, z, x, y, config)
    else
      handle_uncached_tile_request(pmtiles, tile_id, z, x, y, config)
    end
  end

  @doc """
  Handles a tile request when tile caching is enabled.
  """
  def handle_cached_tile_request(
        pmtiles,
        tile_id,
        z,
        x,
        y,
        %{stats: _stats_table, cache_path: nil} = config
      ) do
    # Caching disabled, fetch directly
    fetch_tile(pmtiles, tile_id, z, x, y, config, false)
  end

  def handle_cached_tile_request(
        pmtiles,
        tile_id,
        z,
        x,
        y,
        %{stats: stats_table, cache_path: cache_path} = config
      ) do
    tile_file_path = FileHandler.tile_cache_file_path(cache_path, tile_id)

    if File.exists?(tile_file_path) do
      # Cache hit - read from file
      case FileHandler.read_tile_from_cache(tile_file_path) do
        {:ok, tile_data} ->
          :ets.update_counter(stats_table, :hits, 1)
          {:ok, tile_data}

        :error ->
          # Failed to read, fetch fresh
          fetch_tile(pmtiles, tile_id, z, x, y, config, true)
      end
    else
      # Cache miss - fetch and cache
      fetch_tile(pmtiles, tile_id, z, x, y, config, true)
    end
  end

  @doc """
  Handles a tile request when tile caching is disabled.
  """
  def handle_uncached_tile_request(pmtiles, tile_id, z, x, y, config) do
    fetch_tile(pmtiles, tile_id, z, x, y, config, false)
  end

  @doc """
  Fetches a tile from the PMTiles file and optionally caches it.

  This does all the work synchronously in the calling process.
  """
  def fetch_tile(pmtiles, tile_id, z, x, y, config, enable_tile_cache) do
    %{
      stats: stats_table,
      cache_path: cache_path,
      pending_directories: pending_directories_table
    } = config

    :ets.update_counter(stats_table, :misses, 1)

    try do
      # Fetch tile using file-based directory caching
      result = fetch_tile_from_pmtiles(pmtiles, z, x, y, cache_path, pending_directories_table)

      case result do
        {nil, _updated_pmtiles} ->
          # Log at debug level since missing tiles are normal (sparse coverage)
          Logger.debug("Tile not found: #{z}/#{x}/#{y} (tile_id: #{tile_id})")
          {:error, :tile_not_found}

        {{_offset, _length, tile_data}, _updated_pmtiles} ->
          # Cache the tile if enabled
          if enable_tile_cache and not is_nil(cache_path) do
            tile_file_path = FileHandler.tile_cache_file_path(cache_path, tile_id)
            FileHandler.write_tile_to_cache(tile_file_path, tile_data)
          end

          {:ok, tile_data}
      end
    rescue
      e ->
        Logger.error("Error fetching tile #{tile_id} (#{z}/#{x}/#{y}): #{inspect(e)}")
        Logger.error(Exception.format_stacktrace(__STACKTRACE__))
        {:error, :fetch_failed}
    end
  end

  defp fetch_tile_from_pmtiles(pmtiles, z, x, y, cache_path, pending_directories_table) do
    if @env == :test do
      pmtiles_module().get_zxy(pmtiles, z, x, y)
    else
      pmtiles_module().get_zxy(pmtiles, z, x, y, cache_path, pending_directories_table)
    end
  end

  # Used to get the pmtiles module from the application environment so it can be mocked in tests
  defp pmtiles_module do
    Application.get_env(:ex_pmtiles, :pmtiles_module, ExPmtiles)
  end
end
