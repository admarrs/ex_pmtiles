defmodule ExPmtiles do
  @moduledoc """
  A module for working with PMTiles files.

  PMTiles is a single-file format for storing tiled map data. This module provides
  functionality to read and access tiles from PMTiles files stored either locally
  or on S3.

  ## Features

  - Read PMTiles files from local storage or S3
  - Access tiles by zoom level and coordinates (z/x/y)
  - Automatic directory caching and decompression
  - Support for various compression types (gzip, none)
  - Tile ID calculations and conversions

  ## Usage

  ```elixir
  # Open a local PMTiles file
  instance = ExPmtiles.new("path/to/file.pmtiles", :local)

  # Open a PMTiles file from S3
  instance = ExPmtiles.new("region", "my-bucket", "path/to/file.pmtiles", :s3)

  # Get a tile by coordinates
  case ExPmtiles.get_zxy(instance, 10, 512, 256) do
    {{offset, length, data}, updated_instance} ->
      # Use the tile data
      data
    {nil, updated_instance} ->
      # Tile not found
      nil
  end

  # Convert coordinates to tile ID
  tile_id = ExPmtiles.zxy_to_tile_id(10, 512, 256)

  # Convert tile ID back to coordinates
  {z, x, y} = ExPmtiles.tile_id_to_zxy(tile_id)
  ```

  ## Compression Support

  The module supports the following compression types:
  - `:none` - No compression
  - `:gzip` - Gzip compression
  - `:unknown` - Unknown compression type

  Note: Brotli and Zstd compression are not currently supported.

  ## Tile Types

  Supported tile types include:
  - `:mvt` - Mapbox Vector Tiles
  - `:png` - PNG images
  - `:jpg` - JPEG images
  - `:webp` - WebP images
  - `:avif` - AVIF images
  - `:unknown` - Unknown tile type

  ## HTTP Connection Pooling

  For optimal S3 performance, this module uses a dedicated Hackney connection pool.
  The pool is automatically started when the application starts. You can configure
  the pool size via application configuration:

  ```elixir
  config :ex_pmtiles,
    http_pool_size: 100,  # Maximum number of connections (default: 100)
    http_timeout: 15_000  # Request timeout in milliseconds (default: 15_000)
  ```
  """
  @behaviour ExPmtiles.Behaviour
  import Bitwise

  alias ExPmtiles.Storage

  alias __MODULE__

  require Logger

  @doc false
  def start(_type, _args) do
    # Configure Hackney connection pool for optimal S3 performance
    pool_size = Application.get_env(:ex_pmtiles, :http_pool_size, 100)

    pool_opts = [
      # Connection idle timeout (2.5 minutes)
      timeout: 150_000,
      max_connections: pool_size
    ]

    :hackney_pool.start_pool(:s3_pool, pool_opts)

    # Start a supervisor (even though we don't have children yet, this allows future expansion)
    children = []
    opts = [strategy: :one_for_one, name: ExPmtiles.Supervisor]
    Supervisor.start_link(children, opts)
  end

  @compression_types %{
    0 => :unknown,
    1 => :none,
    2 => :gzip,
    3 => :brotli,
    4 => :zstd
  }

  @tile_types %{
    0 => :unknown,
    1 => :mvt,
    2 => :png,
    3 => :jpg,
    4 => :webp,
    5 => :avif
  }

  defstruct [
    :header,
    :region,
    :bucket,
    :path,
    :source,
    directories: %{},
    pending_directories: %{}
  ]

  @tz_values %{
    0 => 0,
    1 => 1,
    2 => 5,
    3 => 21,
    4 => 85,
    5 => 341,
    6 => 1365,
    7 => 5461,
    8 => 21_845,
    9 => 87_381,
    10 => 349_525,
    11 => 1_398_101,
    12 => 5_592_405,
    13 => 22_369_621,
    14 => 89_478_485,
    15 => 357_913_941,
    16 => 1_431_655_765,
    17 => 5_726_623_061,
    18 => 22_906_492_245,
    19 => 91_625_968_981,
    20 => 366_503_875_925,
    21 => 1_466_015_503_701,
    22 => 5_864_062_014_805,
    23 => 23_456_248_059_221,
    24 => 93_824_992_236_885,
    25 => 375_299_968_947_541,
    26 => 1_501_199_875_790_165
  }

  @doc """
  Creates a new PMTiles instance for an S3-stored file.

  ## Parameters

  - `bucket` - The S3 bucket name
  - `path` - The path to the PMTiles file within the bucket
  - `:s3` - Source type identifier

  ## Returns

  - `%ExPmtiles{}` - A configured PMTiles instance with parsed header
  - `nil` - If the file cannot be accessed or is invalid

  ## Examples

      iex> ExPmtiles.new("my-bucket", "maps/world.pmtiles", :s3)
      %ExPmtiles{bucket: "my-bucket", path: "maps/world.pmtiles", source: :s3, header: %{...}}
  """
  def new(region, bucket, path, :s3) do
    instance = %ExPmtiles{region: region, bucket: bucket, path: path, source: :s3}

    case get_bytes(instance, 0, 16_384) do
      nil ->
        nil

      data ->
        header = parse_header(data)
        %{instance | header: header}
    end
  end

  @doc """
  Creates a new PMTiles instance for a locally-stored file.

  ## Parameters

  - `path` - The local file path to the PMTiles file
  - `:local` - Source type identifier

  ## Returns

  - `%ExPmtiles{}` - A configured PMTiles instance with parsed header
  - `nil` - If the file cannot be accessed or is invalid

  ## Examples

      iex> ExPmtiles.new("data/world.pmtiles", :local)
      %ExPmtiles{bucket: nil, path: "data/world.pmtiles", source: :local, header: %{...}}
  """
  def new(path, :local) do
    instance = %ExPmtiles{bucket: nil, path: path, source: :local}

    case get_bytes(instance, 0, 16_384) do
      nil ->
        nil

      data ->
        header = parse_header(data)
        %{instance | header: header}
    end
  end

  @doc """
  Gets raw tile data from the PMTiles file at the specified offset and length.

  ## Parameters

  - `instance` - The PMTiles instance
  - `offset` - Byte offset in the file
  - `length` - Number of bytes to read

  ## Returns

  - `binary()` - The raw tile data
  - `nil` - If the data cannot be read

  ## Examples

      iex> ExPmtiles.get_tile(instance, 1024, 512)
      <<...>>
  """
  def get_tile(instance, offset, length) do
    get_bytes(instance, offset, length)
  end

  @doc """
  Gets a tile by its zoom level and coordinates.

  This function converts the z/x/y coordinates to a tile ID and searches for the
  tile in the PMTiles directory structure. It handles both leaf and internal
  directories automatically.

  ## Parameters

  - `instance` - The PMTiles instance
  - `z` - Zoom level (integer)
  - `x` - X coordinate (integer)
  - `y` - Y coordinate (integer)

  ## Returns

  - `{{offset, length, data}, updated_instance}` - Tuple containing tile information and updated instance
  - `{nil, updated_instance}` - If tile is not found or outside zoom bounds

  ## Examples

      iex> ExPmtiles.get_zxy(instance, 10, 512, 256)
      {{1024, 512, <<...>>}, updated_instance}

      iex> ExPmtiles.get_zxy(instance, 25, 0, 0)
      {nil, instance}  # Zoom level out of bounds
  """
  def get_zxy(instance, z, x, y, dir_cache_path \\ nil, pending_directories_table \\ nil) do
    tile_id = zxy_to_tile_id(z, x, y)
    header = instance.header

    if z < header.min_zoom or z > header.max_zoom do
      {nil, instance}
    else
      try_get_tile(
        instance,
        tile_id,
        header.root_offset,
        header.root_length,
        0,
        dir_cache_path,
        pending_directories_table
      )
    end
  end

  defp try_get_tile(
         _instance,
         _tile_id,
         _d_offset,
         _d_length,
         depth,
         _dir_cache_path,
         _pending_directories_table
       )
       when depth > 3 do
    Logger.warning("ExPmtiles try_get_tile, max directory depth exceeded #{depth}")
    raise ArgumentError, "Maximum directory depth exceeded"
  end

  defp try_get_tile(
         instance,
         tile_id,
         d_offset,
         d_length,
         depth,
         dir_cache_path,
         pending_directories_table
       ) do
    # Use file-based caching if cache paths provided, otherwise use in-memory caching
    {directory, updated_instance} =
      if is_nil(dir_cache_path) or is_nil(pending_directories_table) do
        # In-memory caching for simple API
        get_cached_directory(instance, d_offset, d_length)
      else
        # File-based caching for Cache module
        directory =
          get_cached_directory_file(
            instance,
            d_offset,
            d_length,
            dir_cache_path,
            pending_directories_table
          )

        {directory, instance}
      end

    case directory do
      nil ->
        require Logger

        Logger.warning(
          "Directory fetch failed at offset #{d_offset}, length #{d_length}, depth #{depth} for tile_id #{tile_id}"
        )

        {nil, updated_instance}

      directory ->
        if is_nil(dir_cache_path) or is_nil(pending_directories_table) do
          # In-memory path
          process_directory_entry(directory, tile_id, updated_instance, d_offset, d_length, depth)
        else
          # File-based path
          process_directory_entry_file(
            directory,
            tile_id,
            updated_instance,
            d_offset,
            d_length,
            depth,
            dir_cache_path,
            pending_directories_table
          )
        end
    end
  end

  defp process_directory_entry(directory, tile_id, instance, d_offset, d_length, depth) do
    case find_tile(directory, tile_id) do
      nil ->
        require Logger

        Logger.warning(
          "Tile #{tile_id} not found in directory at offset #{d_offset}, length #{d_length}, depth #{depth}. Directory has #{length(directory)} entries"
        )

        {nil, instance}

      entry ->
        handle_directory_entry(entry, directory, instance, tile_id, d_offset, d_length, depth)
    end
  end

  defp handle_directory_entry(
         %{run_length: run_length} = entry,
         _directory,
         instance,
         tile_id,
         _d_offset,
         _d_length,
         _depth
       )
       when run_length > 0 do
    case get_bytes(instance, instance.header.tile_data_offset + entry.offset, entry.length) do
      nil ->
        require Logger

        Logger.warning(
          "Failed to read tile data at offset #{instance.header.tile_data_offset + entry.offset}, length #{entry.length} for tile_id #{tile_id}"
        )

        {nil, instance}

      response ->
        {{instance.header.tile_data_offset + entry.offset, entry.length, response}, instance}
    end
  end

  defp handle_directory_entry(entry, directory, instance, tile_id, d_offset, d_length, depth) do
    # When starting recursive search, notify waiting processes about the directory
    cache_key = "#{d_offset}:#{d_length}"
    notify_directory_waiters(cache_key, directory, instance)

    # Continue searching in leaf directory (use in-memory caching)
    try_get_tile(
      instance,
      tile_id,
      instance.header.leaf_dir_offset + entry.offset,
      entry.length,
      depth + 1,
      nil,
      nil
    )
  end

  defp get_cached_directory(instance, offset, length) do
    cache_key = "#{offset}:#{length}"

    case Map.get(instance.directories, cache_key) do
      nil ->
        # Check if there's a pending deserialization
        case Map.get(instance.pending_directories, cache_key) do
          nil ->
            # No pending request, start a new one

            # Mark this directory as being processed
            instance = %{
              instance
              | pending_directories: Map.put(instance.pending_directories, cache_key, self())
            }

            directory =
              get_bytes(instance, offset, length)
              |> decompress(instance.header.internal_compression)
              |> deserialize_directory()

            # Update instance with the new directory and remove from pending
            updated_instance = %{
              instance
              | directories: Map.put(instance.directories, cache_key, directory),
                pending_directories: Map.delete(instance.pending_directories, cache_key)
            }

            {directory, updated_instance}

          pid when pid == self() ->
            # We're already processing this directory (recursive case)
            {nil, instance}

          _other_pid ->
            # Another request is processing this directory, wait for it

            receive do
              {:directory_ready, ^cache_key, directory, updated_instance} ->
                {directory, updated_instance}
            after
              100_000 ->
                Logger.error(
                  "Timeout waiting for directory #{cache_key} after 100s (in-memory cache, waiting for another process)"
                )

                {nil, instance}
            end
        end

      directory ->
        {directory, instance}
    end
  end

  defp notify_directory_waiters(cache_key, directory, instance) do
    case Map.get(instance.pending_directories, cache_key) do
      nil ->
        :ok

      pid when pid == self() ->
        :ok

      other_pid ->
        # Notify any waiting processes about the completed directory
        send(other_pid, {:directory_ready, cache_key, directory, instance})
    end
  end

  # File-based directory caching functions
  @doc false
  def get_cached_directory_file(
        instance,
        offset,
        length,
        dir_cache_path,
        pending_directories_table
      ) do
    cache_key = "#{offset}:#{length}"

    # Use file-based caching for persistent storage
    # dir_cache_path contains the cache directory path
    if is_nil(dir_cache_path) do
      # Caching not enabled, fetch directly
      fetch_directory_without_cache(instance, offset, length)
    else
      # Check if cached file exists
      cache_file_path = directory_cache_file_path(dir_cache_path, cache_key)

      if File.exists?(cache_file_path) do
        # Cache hit - read from file
        read_directory_from_cache(cache_file_path)
      else
        # Cache miss - fetch and cache
        handle_directory_cache_miss(
          instance,
          cache_key,
          length,
          dir_cache_path,
          pending_directories_table
        )
      end
    end
  end

  defp fetch_directory_without_cache(instance, offset, length) do
    # Fetch and deserialize directly without caching
    bytes = get_bytes(instance, offset, length)

    if is_nil(bytes) do
      nil
    else
      bytes
      |> decompress(instance.header.internal_compression)
      |> deserialize_directory()
    end
  end

  # Generate file path for a cached directory
  defp directory_cache_file_path(cache_dir, cache_key) do
    # Sanitize cache_key to be filesystem-safe
    safe_key = String.replace(cache_key, ":", "_")
    Path.join([cache_dir, "#{safe_key}.bin"])
  end

  # Read directory from cache file
  defp read_directory_from_cache(cache_file_path) do
    cache_file_path
    |> File.read!()
    |> :erlang.binary_to_term()
  rescue
    e ->
      Logger.warning("Failed to read directory cache file #{cache_file_path}: #{inspect(e)}")
      nil
  end

  # Write directory to cache file
  defp write_directory_to_cache(cache_file_path, directory) do
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

    # Write serialized directory
    cache_file_path
    |> File.write!(:erlang.term_to_binary(directory))

    :ok
  rescue
    e ->
      Logger.warning("Failed to write directory cache file #{cache_file_path}: #{inspect(e)}")
      :error
  catch
    {:mkdir_failed, _reason} ->
      :error
  end

  defp handle_directory_cache_miss(
         instance,
         cache_key,
         length,
         dir_cache_path,
         pending_directories_table
       ) do
    case :ets.insert_new(pending_directories_table, {cache_key, self()}) do
      true ->
        fetch_and_cache_directory(
          instance,
          cache_key,
          length,
          dir_cache_path,
          pending_directories_table
        )

      false ->
        wait_for_directory_fetch(
          instance,
          cache_key,
          length,
          dir_cache_path,
          pending_directories_table
        )
    end
  end

  defp fetch_and_cache_directory(
         instance,
         cache_key,
         length,
         dir_cache_path,
         pending_directories_table
       ) do
    start_time = System.monotonic_time(:millisecond)

    try do
      result = fetch_and_process_directory(instance, cache_key, length, start_time)

      case result do
        nil ->
          cleanup_failed_directory_fetch(pending_directories_table, cache_key)
          nil

        directory ->
          cache_and_notify_directory(
            directory,
            cache_key,
            dir_cache_path,
            pending_directories_table
          )

          directory
      end
    rescue
      e ->
        handle_directory_fetch_error(e, cache_key, pending_directories_table, __STACKTRACE__)
        nil
    end
  end

  defp fetch_and_process_directory(instance, cache_key, length, start_time) do
    fetch_start = System.monotonic_time(:millisecond)
    bytes = get_bytes(instance, get_offset_from_cache_key(cache_key), length)
    fetch_elapsed = System.monotonic_time(:millisecond) - fetch_start

    if is_nil(bytes) do
      nil
    else
      process_directory_bytes(bytes, instance, cache_key, start_time, fetch_elapsed)
    end
  end

  defp process_directory_bytes(bytes, instance, _cache_key, _start_time, _fetch_elapsed) do
    decompressed = decompress(bytes, instance.header.internal_compression)
    directory = deserialize_directory(decompressed)

    directory
  end

  defp get_offset_from_cache_key(cache_key) do
    [offset_str, _length_str] = String.split(cache_key, ":")
    String.to_integer(offset_str)
  end

  defp cache_and_notify_directory(
         directory,
         cache_key,
         dir_cache_path,
         pending_directories_table
       ) do
    # Write directory to individual file (no size limits)
    # dir_cache_path contains the cache directory path
    if not is_nil(dir_cache_path) do
      cache_file_path = directory_cache_file_path(dir_cache_path, cache_key)
      write_directory_to_cache(cache_file_path, directory)
    end

    # Notify all waiting processes BEFORE deleting from pending table
    notify_directory_waiters_file(pending_directories_table, cache_key, directory)

    # Now clean up the pending entry
    :ets.delete(pending_directories_table, cache_key)
  end

  defp cleanup_failed_directory_fetch(pending_directories_table, cache_key) do
    :ets.delete(pending_directories_table, cache_key)
  end

  defp handle_directory_fetch_error(e, cache_key, pending_directories_table, stacktrace) do
    Logger.error("Failed to process directory #{cache_key}: #{inspect(e)}")
    Logger.error(Exception.format_stacktrace(stacktrace))

    # Notify waiting processes of failure before cleaning up
    notify_directory_waiters_of_failure(pending_directories_table, cache_key)

    :ets.delete(pending_directories_table, cache_key)
  end

  defp wait_for_directory_fetch(
         instance,
         cache_key,
         length,
         dir_cache_path,
         pending_directories_table
       ) do
    result =
      wait_for_directory_completion(
        cache_key,
        dir_cache_path,
        pending_directories_table,
        instance,
        get_offset_from_cache_key(cache_key),
        length
      )

    result
  end

  defp wait_for_directory_completion(
         cache_key,
         dir_cache_path,
         pending_directories_table,
         instance,
         offset,
         length
       ) do
    caller_pid = self()

    case :ets.lookup(pending_directories_table, cache_key) do
      [{^cache_key, waiting_pids}] when is_list(waiting_pids) ->
        handle_waiting_with_list(
          cache_key,
          waiting_pids,
          caller_pid,
          dir_cache_path,
          pending_directories_table,
          instance,
          offset,
          length
        )

      [{^cache_key, _single_pid}] ->
        handle_waiting_with_single_pid(
          cache_key,
          caller_pid,
          dir_cache_path,
          pending_directories_table,
          instance,
          offset,
          length
        )

      [] ->
        handle_missing_pending_entry(
          cache_key,
          dir_cache_path,
          pending_directories_table,
          instance,
          offset,
          length
        )
    end
  end

  defp handle_waiting_with_list(
         cache_key,
         waiting_pids,
         caller_pid,
         dir_cache_path,
         pending_directories_table,
         instance,
         offset,
         length
       ) do
    # Add ourselves to the waiting list
    :ets.insert(pending_directories_table, {cache_key, [caller_pid | waiting_pids]})

    # Wait for notification via message passing (event-driven, not polling)
    receive do
      {:directory_ready, ^cache_key, directory} ->
        Logger.debug("Directory #{cache_key} received via notification")
        directory
    after
      20_000 ->
        handle_wait_timeout(
          cache_key,
          caller_pid,
          dir_cache_path,
          pending_directories_table,
          instance,
          offset,
          length,
          "file-based cache"
        )
    end
  end

  defp handle_waiting_with_single_pid(
         cache_key,
         caller_pid,
         dir_cache_path,
         pending_directories_table,
         instance,
         offset,
         length
       ) do
    # Old format with single PID - convert to list format and add ourselves
    :ets.insert(pending_directories_table, {cache_key, [caller_pid]})

    receive do
      {:directory_ready, ^cache_key, directory} ->
        directory
    after
      20_000 ->
        handle_wait_timeout(
          cache_key,
          caller_pid,
          dir_cache_path,
          pending_directories_table,
          instance,
          offset,
          length,
          "file-based cache, old format"
        )
    end
  end

  defp handle_missing_pending_entry(
         cache_key,
         dir_cache_path,
         pending_directories_table,
         instance,
         offset,
         length
       ) do
    # Pending entry was deleted - directory should be in file cache now
    if is_nil(dir_cache_path) do
      # Cache not available, fetch without cache
      fetch_directory_without_cache(instance, offset, length)
    else
      cache_file_path = directory_cache_file_path(dir_cache_path, cache_key)

      if File.exists?(cache_file_path) do
        read_directory_from_cache(cache_file_path)
      else
        # Not in cache either - fetch failed, retry
        get_cached_directory_file(
          instance,
          offset,
          length,
          dir_cache_path,
          pending_directories_table
        )
      end
    end
  end

  defp handle_wait_timeout(
         cache_key,
         caller_pid,
         dir_cache_path,
         pending_directories_table,
         instance,
         offset,
         length,
         context
       ) do
    # Log timeout with context
    Logger.error(
      "Timeout waiting for directory #{cache_key} after 20s (#{context}). " <>
        "Attempting to fetch ourselves. This suggests slow storage or network issues."
    )

    # Remove ourselves from waiting list
    remove_from_waiting_list(pending_directories_table, cache_key, caller_pid)

    # Try to fetch it ourselves
    fetch_directory_with_takeover(
      cache_key,
      dir_cache_path,
      pending_directories_table,
      instance,
      offset,
      length
    )
  end

  defp remove_from_waiting_list(pending_directories_table, cache_key, caller_pid) do
    case :ets.lookup(pending_directories_table, cache_key) do
      [{^cache_key, pids}] when is_list(pids) ->
        :ets.insert(pending_directories_table, {cache_key, List.delete(pids, caller_pid)})

      [{^cache_key, _single_pid}] ->
        # Old format, just delete the entry
        :ets.delete(pending_directories_table, cache_key)

      [] ->
        :ok
    end
  end

  defp fetch_directory_with_takeover(
         cache_key,
         dir_cache_path,
         pending_directories_table,
         instance,
         offset,
         length
       ) do
    # Try to take over the fetch by deleting the old pending entry
    :ets.delete(pending_directories_table, cache_key)

    # Check if it appeared in file cache while we were setting up
    if is_nil(dir_cache_path) do
      # Cache not available, fetch without cache
      fetch_directory_without_cache(instance, offset, length)
    else
      cache_file_path = directory_cache_file_path(dir_cache_path, cache_key)

      if File.exists?(cache_file_path) do
        read_directory_from_cache(cache_file_path)
      else
        # Do the fetch ourselves
        get_cached_directory_file(
          instance,
          offset,
          length,
          dir_cache_path,
          pending_directories_table
        )
      end
    end
  end

  # Helper function to notify all processes waiting for a directory (file-based)
  defp notify_directory_waiters_file(pending_directories_table, cache_key, directory) do
    case :ets.lookup(pending_directories_table, cache_key) do
      [{^cache_key, waiting_pids}] when is_list(waiting_pids) ->
        Enum.each(waiting_pids, fn pid ->
          send(pid, {:directory_ready, cache_key, directory})
        end)

      [{^cache_key, _single_pid}] ->
        # Old format - just single PID, no list
        :ok

      [] ->
        # No waiters
        :ok
    end
  end

  defp notify_directory_waiters_of_failure(pending_directories_table, cache_key) do
    case :ets.lookup(pending_directories_table, cache_key) do
      [{^cache_key, waiting_pids}] when is_list(waiting_pids) ->
        # Don't send failure message - let them timeout and retry
        :ok

      _ ->
        :ok
    end
  end

  defp process_directory_entry_file(
         directory,
         tile_id,
         instance,
         d_offset,
         d_length,
         depth,
         dir_cache_path,
         pending_directories_table
       ) do
    case find_tile(directory, tile_id) do
      nil ->
        require Logger

        Logger.warning(
          "Tile #{tile_id} not found in directory at offset #{d_offset}, length #{d_length}, depth #{depth}. Directory has #{length(directory)} entries"
        )

        {nil, instance}

      entry ->
        handle_directory_entry_file(
          entry,
          instance,
          tile_id,
          dir_cache_path,
          pending_directories_table,
          depth
        )
    end
  end

  defp handle_directory_entry_file(
         %{run_length: run_length} = entry,
         instance,
         tile_id,
         _dir_cache_path,
         _pending_directories_table,
         _depth
       )
       when run_length > 0 do
    case get_bytes(instance, instance.header.tile_data_offset + entry.offset, entry.length) do
      nil ->
        require Logger

        Logger.warning(
          "Failed to read tile data at offset #{instance.header.tile_data_offset + entry.offset}, length #{entry.length} for tile_id #{tile_id}"
        )

        {nil, instance}

      response ->
        {{instance.header.tile_data_offset + entry.offset, entry.length, response}, instance}
    end
  end

  defp handle_directory_entry_file(
         entry,
         instance,
         tile_id,
         dir_cache_path,
         pending_directories_table,
         depth
       ) do
    # Continue searching in leaf directory
    leaf_offset = instance.header.leaf_dir_offset + entry.offset

    try_get_tile(
      instance,
      tile_id,
      leaf_offset,
      entry.length,
      depth + 1,
      dir_cache_path,
      pending_directories_table
    )
  end

  @doc """
  Retrieves bytes from the PMTiles file using the configured storage backend.

  ## Parameters

  - `instance` - The PMTiles instance
  - `offset` - Byte offset in the file
  - `length` - Number of bytes to read

  ## Returns

  - `binary()` - The requested bytes
  - `nil` - If the bytes cannot be read

  ## Examples

      iex> ExPmtiles.get_bytes(instance, 0, 1024)
      <<...>>
  """
  def get_bytes(instance, offset, length) do
    Storage.get_bytes(instance, offset, length)
  end

  @doc """
  Deserializes a PMTiles directory from binary data.

  PMTiles directories contain entries with tile IDs, offsets, lengths, and run lengths.
  This function parses the binary format into a list of directory entries.

  ## Parameters

  - `buf` - Binary data containing the directory

  ## Returns

  - `list()` - List of directory entries with keys: `:tile_id`, `:offset`, `:length`, `:run_length`

  ## Examples

      iex> ExPmtiles.deserialize_directory(<<...>>)
      [
        %{tile_id: 0, offset: 0, length: 1024, run_length: 1},
        %{tile_id: 1, offset: 1024, length: 512, run_length: 0}
      ]
  """
  def deserialize_directory(buf) do
    # First get number of entries
    {num_entries, rest} = read_varint(buf)

    # First pass: Create entries with tile_ids
    {entries, rest} =
      Enum.reduce(1..num_entries, {[], rest, 0}, fn _i, {entries, curr_data, last_id} ->
        {tmp, new_rest} = read_varint(curr_data)
        new_id = last_id + tmp
        entry = %{tile_id: new_id, offset: 0, length: 0, run_length: 1}
        {[entry | entries], new_rest, new_id}
      end)
      |> then(fn {entries, rest, _} -> {Enum.reverse(entries), rest} end)

    # Second pass: Add run_lengths
    {entries, rest} =
      Enum.reduce(entries, {[], rest}, fn entry, {acc, curr_data} ->
        {run_length, new_rest} = read_varint(curr_data)
        {[%{entry | run_length: run_length} | acc], new_rest}
      end)
      |> then(fn {entries, rest} -> {Enum.reverse(entries), rest} end)

    # Third pass: Add lengths
    {entries, rest} =
      Enum.reduce(entries, {[], rest}, fn entry, {acc, curr_data} ->
        {length, new_rest} = read_varint(curr_data)
        {[%{entry | length: length} | acc], new_rest}
      end)
      |> then(fn {entries, rest} -> {Enum.reverse(entries), rest} end)

    # Fourth pass: Add offsets
    {entries, _rest} =
      Enum.reduce(Enum.with_index(entries), {[], rest, nil}, fn {entry, i},
                                                                {acc, curr_data, prev_entry} ->
        {tmp, new_rest} = read_varint(curr_data)

        offset =
          if i > 0 and tmp == 0 and prev_entry != nil do
            prev_entry.offset + prev_entry.length
          else
            tmp - 1
          end

        updated_entry = %{entry | offset: offset}
        {[updated_entry | acc], new_rest, updated_entry}
      end)
      |> then(fn {entries, rest, _prev} -> {Enum.reverse(entries), rest} end)

    entries
  end

  @doc """
  Finds a tile entry in a directory by tile ID.

  Uses binary search to efficiently locate the tile entry. Handles run-length
  encoding where multiple consecutive tiles may share the same entry.

  ## Parameters

  - `entries` - List of directory entries
  - `tile_id` - The tile ID to search for

  ## Returns

  - `map()` - The matching directory entry
  - `nil` - If no matching entry is found

  ## Examples

      iex> ExPmtiles.find_tile(entries, 1024)
      %{tile_id: 1024, offset: 2048, length: 512, run_length: 1}
  """
  def find_tile([], _tile_id), do: nil

  def find_tile(entries, tile_id) do
    last_index = length(entries) - 1
    do_find_tile(entries, tile_id, 0, last_index)
  end

  defp do_find_tile(entries, tile_id, m, n) when m <= n do
    # equivalent to (n + m) >> 1
    k = div(m + n, 2)
    entry = Enum.at(entries, k)
    c = tile_id - entry.tile_id

    cond do
      c > 0 -> do_find_tile(entries, tile_id, k + 1, n)
      c < 0 -> do_find_tile(entries, tile_id, m, k - 1)
      true -> entry
    end
  end

  defp do_find_tile(entries, tile_id, _m, n) when n >= 0 do
    entry = Enum.at(entries, n)

    cond do
      entry.run_length == 0 -> entry
      tile_id - entry.tile_id < entry.run_length -> entry
      true -> nil
    end
  end

  defp do_find_tile(_entries, _tile_id, _m, _n), do: nil

  @doc """
  Converts zoom level and coordinates to a PMTiles tile ID.

  Uses the Hilbert curve mapping to convert z/x/y coordinates to a unique tile ID.
  This is the standard method used by PMTiles for tile identification.

  ## Parameters

  - `z` - Zoom level (integer, 0-26)
  - `x` - X coordinate (integer, 0 to 2^z - 1)
  - `y` - Y coordinate (integer, 0 to 2^z - 1)

  ## Returns

  - `integer()` - The tile ID

  ## Raises

  - `ArgumentError` - If zoom level exceeds 26 or coordinates are out of bounds

  ## Examples

      iex> ExPmtiles.zxy_to_tile_id(10, 512, 256)
      1048576

      iex> ExPmtiles.zxy_to_tile_id(0, 0, 0)
      0
  """
  def zxy_to_tile_id(z, x, y) when is_integer(z) and is_integer(x) and is_integer(y) do
    if z > 26 do
      raise ArgumentError, "Tile zoom level exceeds max safe number limit (26)"
    end

    max_coord = trunc(:math.pow(2, z)) - 1

    if x > max_coord or y > max_coord do
      raise ArgumentError, "tile x/y outside zoom level bounds"
    end

    # You'll need to define @tz_values module attribute
    acc = Map.get(@tz_values, z)
    n = trunc(:math.pow(2, z))

    calculate_tile_id(x, y, n, acc)
  end

  @doc """
  Converts a PMTiles tile ID back to zoom level and coordinates.

  Performs the inverse operation of `zxy_to_tile_id/3`, converting a tile ID
  back to its corresponding z/x/y coordinates.

  ## Parameters

  - `tile_id` - The tile ID to convert

  ## Returns

  - `{z, x, y}` - Tuple of zoom level and coordinates

  ## Raises

  - `ArgumentError` - If tile_id is invalid or exceeds 64-bit limit

  ## Examples

      iex> ExPmtiles.tile_id_to_zxy(1048576)
      {10, 512, 256}

      iex> ExPmtiles.tile_id_to_zxy(0)
      {0, 0, 0}
  """
  def tile_id_to_zxy(tile_id) when is_integer(tile_id) and tile_id >= 0 do
    do_tile_id_to_zxy(tile_id, 0, 0)
  end

  def tile_id_to_zxy(tile_id), do: raise(ArgumentError, "invalid tile_id: #{inspect(tile_id)}")

  defp do_tile_id_to_zxy(tile_id, z, acc) when z < 32 do
    # Calculate 2^z using bit shift
    side_length = 1 <<< z
    num_tiles = side_length * side_length

    if acc + num_tiles > tile_id do
      t_on_level(z, tile_id - acc)
    else
      do_tile_id_to_zxy(tile_id, z + 1, acc + num_tiles)
    end
  end

  defp do_tile_id_to_zxy(_tile_id, _z, _acc) do
    raise ArgumentError, "tile zoom exceeds 64-bit limit"
  end

  defp calculate_tile_id(x, y, n, acc) do
    do_calculate_tile_id([x, y], n, div(n, 2), 0, acc)
  end

  defp do_calculate_tile_id(_xy, _n, s, d, acc) when s <= 0, do: acc + d

  defp do_calculate_tile_id(xy, n, s, d, acc) do
    rx = if Bitwise.band(Enum.at(xy, 0), s) > 0, do: 1, else: 0
    ry = if Bitwise.band(Enum.at(xy, 1), s) > 0, do: 1, else: 0

    new_d = d + s * s * Bitwise.bxor(3 * rx, ry)
    new_xy = rotate(s, xy, rx, ry)

    do_calculate_tile_id(new_xy, n, div(s, 2), new_d, acc)
  end

  defp rotate(n, [x, y], rx, ry) do
    if ry == 0 do
      if rx == 1 do
        # Rotate 90 degrees clockwise
        [n - 1 - y, n - 1 - x]
      else
        # Rotate 90 degrees counter-clockwise
        [y, x]
      end
    else
      [x, y]
    end
  end

  defp t_on_level(z, pos) do
    n = 1 <<< z
    do_t_on_level(pos, n, 1, [0, 0], z)
  end

  defp do_t_on_level(_pos, n, s, xy, z) when s >= n do
    {z, Enum.at(xy, 0), Enum.at(xy, 1)}
  end

  defp do_t_on_level(t, n, s, xy, z) do
    rx = Bitwise.band(div(t, 2), 1)
    ry = Bitwise.band(Bitwise.bxor(t, rx), 1)

    new_xy = rotate(s, xy, rx, ry)

    # Update coordinates
    new_xy = [
      Enum.at(new_xy, 0) + s * rx,
      Enum.at(new_xy, 1) + s * ry
    ]

    # Recurse with updated values
    do_t_on_level(div(t, 4), n, s * 2, new_xy, z)
  end

  @doc """
  Reads a variable-length integer from binary data.

  PMTiles uses variable-length integers (varints) for efficient encoding of
  small numbers. This function reads one varint from the beginning of the binary.

  ## Parameters

  - `binary` - Binary data containing the varint
  - `shift` - Internal parameter for bit shifting (default: 0)
  - `result` - Internal parameter for accumulating result (default: 0)

  ## Returns

  - `{integer(), binary()}` - Tuple of the decoded integer and remaining binary
  - Raises error if binary is empty or malformed

  ## Examples

      iex> ExPmtiles.read_varint(<<1>>)
      {1, ""}

      iex> ExPmtiles.read_varint(<<128, 1>>)
      {128, ""}
  """
  def read_varint(binary, shift \\ 0, result \\ 0)

  def read_varint(<<>>, _shift, _result) do
    raise "unexpectedly reached end of varint stream"
  end

  def read_varint(<<byte::8, rest::binary>>, shift, result) do
    new_result = Bitwise.bor(result, Bitwise.bsl(Bitwise.band(byte, 0x7F), shift))

    if Bitwise.band(byte, 0x80) == 0 do
      {new_result, rest}
    else
      read_varint(rest, shift + 7, new_result)
    end
  end

  @doc """
  Decompresses data based on the specified compression type.

  ## Parameters

  - `data` - The compressed binary data
  - `compression_type` - The compression type (`:gzip`, `:none`, etc.)

  ## Returns

  - `binary()` - The decompressed data

  ## Raises

  - Error for unsupported compression types (brotli, zstd)

  ## Examples

      iex> ExPmtiles.decompress(compressed_data, :gzip)
      <<...>>

      iex> ExPmtiles.decompress(data, :none)
      data
  """
  def decompress(data, compression_type) do
    case compression_type do
      :gzip -> :zlib.gunzip(data)
      :brotli -> raise "Brotli compression not supported"
      :zstd -> raise "Zstd compression not supported"
      _ -> data
    end
  end

  @doc """
  Parses the PMTiles file header from binary data.

  The PMTiles header contains metadata about the file including offsets,
  compression settings, zoom levels, and geographic bounds.

  ## Parameters

  - `binary` - Binary data containing the PMTiles header (first 16,384 bytes)

  ## Returns

  - `map()` - Header information with keys:
    - `:magic_number` - File magic number ("PMTiles")
    - `:spec_version` - PMTiles specification version
    - `:root_offset`, `:root_length` - Root directory location
    - `:metadata_offset`, `:metadata_length` - Metadata location
    - `:leaf_dir_offset`, `:leaf_dir_length` - Leaf directories location
    - `:tile_data_offset`, `:tile_data_length` - Tile data location
    - `:num_addr_tiles`, `:num_tile_entries`, `:num_tile_contents` - Tile counts
    - `:clustered?` - Whether tiles are clustered
    - `:internal_compression`, `:tile_compression` - Compression types
    - `:tile_type` - Type of tiles stored
    - `:min_zoom`, `:max_zoom` - Zoom level bounds
    - `:min_position`, `:max_position` - Geographic bounds
    - `:center_zoom`, `:center_position` - Center point

  ## Examples

      iex> ExPmtiles.parse_header(header_binary)
      %{
        magic_number: "PMTiles",
        spec_version: 3,
        min_zoom: 0,
        max_zoom: 14,
        ...
      }
  """
  def parse_header(
        <<magic_number::binary-size(7), spec_version::little-size(8),
          root_offset::little-size(64), root_length::little-size(64),
          metadata_offset::little-size(64), metadata_length::little-size(64),
          leaf_dir_offset::little-size(64), leaf_dir_length::little-size(64),
          tile_data_offset::little-size(64), tile_data_length::little-size(64),
          num_addr_tiles::little-size(64), num_tile_entries::little-size(64),
          num_tile_contents::little-size(64), clustered::little-size(8),
          internal_compression::little-size(8), tile_compression::little-size(8),
          tile_type::little-size(8), min_zoom::little-size(8), max_zoom::little-size(8),
          min_lon::little-signed-size(32), min_lat::little-signed-size(32),
          max_lon::little-signed-size(32), max_lat::little-signed-size(32),
          center_zoom::little-size(8), center_lon::little-signed-size(32),
          center_lat::little-signed-size(32), _::binary>>
      ) do
    clustered = if clustered == 1, do: true, else: false

    %{
      magic_number: List.to_string(:binary.bin_to_list(magic_number)),
      spec_version: spec_version,
      root_offset: root_offset,
      root_length: root_length,
      metadata_offset: metadata_offset,
      metadata_length: metadata_length,
      leaf_dir_offset: leaf_dir_offset,
      leaf_dir_length: leaf_dir_length,
      tile_data_offset: tile_data_offset,
      tile_data_length: tile_data_length,
      num_addr_tiles: num_addr_tiles,
      num_tile_entries: num_tile_entries,
      num_tile_contents: num_tile_contents,
      clustered?: clustered,
      internal_compression: @compression_types[internal_compression],
      tile_compression: @compression_types[tile_compression],
      tile_type: @tile_types[tile_type],
      min_zoom: min_zoom,
      max_zoom: max_zoom,
      min_position: [min_lon / 10_000_000, min_lat / 10_000_000],
      max_position: [max_lon / 10_000_000, max_lat / 10_000_000],
      center_zoom: center_zoom,
      center_position: [center_lon / 10_000_000, center_lat / 10_000_000]
    }
  end
end
