defmodule ExPmtiles.Cache.FileHandler do
  @moduledoc """
  Pure functions for file-based cache operations.

  This module handles all file system operations for the cache,
  including reading, writing, and clearing cache files.
  """

  require Logger

  @doc """
  Generates the file path for a cached tile.
  """
  def tile_cache_file_path(cache_dir, tile_id) do
    Path.join([cache_dir, "tiles", "#{tile_id}.bin"])
  end

  @doc """
  Reads a tile from the cache file.
  Returns {:ok, tile_data} on success, :error on failure.
  """
  def read_tile_from_cache(cache_file_path) do
    try do
      tile_data = File.read!(cache_file_path)
      {:ok, tile_data}
    rescue
      e ->
        Logger.warning("Failed to read tile cache file #{cache_file_path}: #{inspect(e)}")
        :error
    end
  end

  @doc """
  Writes tile data to a cache file.
  Returns :ok on success, :error on failure.
  """
  def write_tile_to_cache(cache_file_path, tile_data) do
    try do
      # Ensure directory exists (handle race conditions gracefully)
      dir_path = Path.dirname(cache_file_path)

      case File.mkdir_p(dir_path) do
        :ok ->
          :ok

        # Another process created it, that's fine
        {:error, :eexist} ->
          :ok

        {:error, reason} ->
          Logger.warning("Failed to create directory #{dir_path}: #{inspect(reason)}")
          throw({:mkdir_failed, reason})
      end

      # Write tile data directly (no serialization needed, it's already binary)
      File.write!(cache_file_path, tile_data)
      :ok
    rescue
      e ->
        Logger.warning("Failed to write tile cache file #{cache_file_path}: #{inspect(e)}")
        :error
    catch
      {:mkdir_failed, _reason} ->
        :error
    end
  end

  @doc """
  Clears all cache files (both tiles and directories) for a given cache path.
  """
  def clear_cache_files(cache_path) do
    if cache_path do
      clear_tile_cache(cache_path)
      clear_directory_cache(cache_path)
    end
  end

  @doc """
  Clears the tile cache directory.
  """
  def clear_tile_cache(cache_path) do
    tile_cache_dir = Path.join(cache_path, "tiles")

    case File.rm_rf(tile_cache_dir) do
      {:ok, _} ->
        File.mkdir_p!(tile_cache_dir)
        Logger.info("Cleared tile cache at #{tile_cache_dir}")

      {:error, reason, _} ->
        Logger.warning("Failed to clear tile cache at #{tile_cache_dir}: #{inspect(reason)}")
    end
  end

  @doc """
  Clears the directory cache directory.
  """
  def clear_directory_cache(cache_path) do
    dir_cache_dir = Path.join(cache_path, "directories")

    case File.rm_rf(dir_cache_dir) do
      {:ok, _} ->
        File.mkdir_p!(dir_cache_dir)
        Logger.info("Cleared directory cache at #{dir_cache_dir}")

      {:error, reason, _} ->
        Logger.warning("Failed to clear directory cache at #{dir_cache_dir}: #{inspect(reason)}")
    end
  end

  @doc """
  Initializes cache directories for a given cache path.
  """
  def init_cache_directories(cache_path, enable_tile_cache, enable_dir_cache) do
    if enable_tile_cache or enable_dir_cache do
      File.mkdir_p!(cache_path)

      if enable_tile_cache do
        File.mkdir_p!(Path.join(cache_path, "tiles"))
      end

      if enable_dir_cache do
        File.mkdir_p!(Path.join(cache_path, "directories"))
      end
    end
  end
end
