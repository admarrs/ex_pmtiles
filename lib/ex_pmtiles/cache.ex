defmodule ExPmtiles.Cache do
  @moduledoc """
  A GenServer implementation of a multi-level cache for PMTiles file data.

  This GenServer provides efficient concurrent access to PMTiles map data through
  file-based caching mechanisms optimized for high-concurrency workloads.

  ## Cache Levels

  ### 1. Directory Cache (File-based)
  - Caches deserialized directory structures from the PMTiles file
  - Stored as individual binary files on disk (one file per directory)
  - Root directory pre-fetched in background on startup
  - Leaf directories fetched on-demand as tiles are requested
  - Coordinates concurrent directory fetches to prevent duplicate S3 requests
  - No size limits - cache can grow indefinitely
  - No corruption issues from concurrent writes

  ### 2. Tile Cache (Optional File-based)
  - Optional persistent tile caching using individual files (disabled by default)
  - When enabled, each tile is stored as a separate file on disk
  - No size limits - cache can grow indefinitely
  - Maintains hit/miss statistics
  - File system handles concurrent access naturally

  ### 3. Concurrent Request Handling
  - Non-blocking tile fetching using spawned tasks
  - Prevents duplicate requests for the same tile
  - Coordinates multiple requests through ETS-based pending requests tables
  - Broadcasts results to all waiting processes
  - Handles errors gracefully with proper cleanup

  ## Configuration
  - `:name` - Optional custom name for the GenServer (defaults to generated name)
  - `:storage` - Storage backend (`:s3` or `:local`, auto-detected from bucket parameter)
  - `:enable_dir_cache` - Enable file-based directory caching (default: false)
  - `:enable_tile_cache` - Enable file-based tile caching (default: false)
  - `:cache_dir` - Directory for cache files (default: System.tmp_dir!() <> "/ex_pmtiles_cache")
  - `:max_cache_age_ms` - Maximum age of cache in milliseconds before automatic clearing (default: nil, disabled).
    When set, clears cache on GenServer startup and periodically based on age.
  - `:cleanup_interval_ms` - Interval for checking cache age in milliseconds (default: 1 hour)
  - `:file_check_interval_ms` - Interval for checking if source file has changed (default: 5 minutes).
    When the file changes (detected via ETag for S3 or mtime for local), cache is automatically cleared and repopulated.

  ## Usage

  ```elixir
  # Start the cache for an S3 PMTiles file
  {:ok, pid} = ExPmtiles.Cache.start_link(bucket: "maps", path: "map.pmtiles")

  # Start the cache for a local PMTiles file
  {:ok, pid} = ExPmtiles.Cache.start_link(bucket: nil, path: "/path/to/map.pmtiles")

  # Start with automatic cache clearing after 24 hours
  {:ok, pid} = ExPmtiles.Cache.start_link(
    bucket: "maps",
    path: "map.pmtiles",
    enable_tile_cache: true,
    max_cache_age_ms: :timer.hours(24),
    cleanup_interval_ms: :timer.hours(1)
  )

  # Start with custom file change detection interval
  {:ok, pid} = ExPmtiles.Cache.start_link(
    bucket: "maps",
    path: "map.pmtiles",
    enable_dir_cache: true,
    file_check_interval_ms: :timer.minutes(1)  # Check for file changes every minute
  )

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

  # Manually clear the cache
  ExPmtiles.Cache.clear_cache(pid)
  ```

  ## Implementation Details

  The GenServer maintains several tables and file structures:
  - **Directory cache files**: Individual binary files in `cache_dir/directories/` (when `enable_dir_cache: true`)
  - **Tile cache files**: Individual binary files in `cache_dir/tiles/` (when `enable_tile_cache: true`)
  - **Pending tile table**: (ETS) Tracks in-progress tile requests to prevent duplicates
  - **Stats table**: (ETS) Maintains hit/miss statistics
  - **Pending directory table**: (ETS) Coordinates concurrent directory fetches

  ## Performance Features
  - **Non-blocking init**: GenServer starts immediately, ready to serve requests
  - **Concurrent tile fetching**: Multiple tiles can be fetched concurrently using tasks
  - **File-based caching**: Persistent storage with no size limits or corruption issues
  - **Request deduplication**: Multiple requests for the same tile/directory are coordinated
  - **Scalable**: Cache grows with your data without size constraints
  - **Race condition handling**: Safe concurrent writes to cache files
  - **Automatic cache clearing**: Optional time-based cache expiration with configurable intervals
  - **Smart repopulation**: Directory cache automatically repopulates after clearing
  - **File change detection**: Automatically detects when the source PMTiles file changes (via ETag for S3 or mtime for local files)
    and invalidates all caches, preventing stale or corrupted data from being served
  """
  use GenServer
  require Logger

  alias ExPmtiles.Cache.FileHandler
  alias ExPmtiles.Cache.Operations

  @table_prefix :pmtiles_cache

  @env Mix.env()
  # Client API

  @doc """
  Starts a new PMTiles cache GenServer.

  Creates a named GenServer process that manages caching for a specific PMTiles file.
  The process name is derived from the bucket and path to ensure uniqueness, unless
  a custom name is provided.

  ## Parameters

  - `opts` - Keyword list of options:
    - `:bucket` - S3 bucket name (or `nil` for local files)
    - `:path` - Path to the PMTiles file
    - `:name` - Custom name for the GenServer (atom). If not provided, a name will be derived from bucket and path
    - `:enable_dir_cache` - Enable file-based directory caching (default: false)
    - `:enable_tile_cache` - Enable file-based tile caching (default: false)
    - `:max_cache_age_ms` - Maximum age of cache before automatic clearing (default: nil, disabled). When set, clears cache on startup.
    - `:cleanup_interval_ms` - Interval for checking cache age (default: 1 hour)
    - `:file_check_interval_ms` - Interval for checking if source file has changed (default: 5 minutes)
    - `:exaws_config` - ExAws config for testing (default: nil, uses default config)

  ## Returns

  - `{:ok, pid}` - Successfully started cache process
  - `{:error, reason}` - Failed to start cache

  ## Examples

      iex> ExPmtiles.Cache.start_link(region: nil, bucket: "maps", path: "world.pmtiles")
      {:ok, #PID<0.123.0>}

      iex> ExPmtiles.Cache.start_link(bucket: nil, path: "/data/local.pmtiles")
      {:ok, #PID<0.124.0>}

      iex> ExPmtiles.Cache.start_link(bucket: "maps", path: "world.pmtiles", enable_tile_cache: true)
      {:ok, #PID<0.125.0>}

      iex> ExPmtiles.Cache.start_link(bucket: "maps", path: "world.pmtiles", name: :my_custom_cache, enable_dir_cache: true)
      {:ok, #PID<0.126.0>}
  """
  def start_link(opts \\ []) do
    region = Keyword.get(opts, :region)
    bucket = Keyword.get(opts, :bucket)
    path = Keyword.get(opts, :path)
    storage = Keyword.get(opts, :storage, :s3)
    name = Keyword.get(opts, :name, name_for(bucket, path))
    GenServer.start_link(__MODULE__, {region, bucket, path, storage, opts}, name: name)
  end

  @doc """
  Retrieves a tile from the cache by coordinates.

  This function handles both cache hits and misses. If the tile is not in the cache,
  it will be fetched from the PMTiles file and cached for future requests.

  ## Parameters

  - `server` - One of:
    - `{bucket, path}` - Tuple identifying the PMTiles file (uses derived name)
    - `pid` - Process identifier for the cache GenServer
    - `name` - Atom representing the registered name of the cache GenServer
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

      iex> ExPmtiles.Cache.get_tile(:my_custom_cache, 10, 512, 256)
      {:ok, <<...>>}
  """
  def get_tile({bucket, path}, z, x, y) do
    name = name_for(bucket, path)
    table_name = table_name_for(name)

    tile_id = pmtiles_module().zxy_to_tile_id(z, x, y)

    # Check if tile cache is enabled by trying to get server info
    case Process.whereis(name) do
      nil ->
        Logger.error("Pmtiles.Cache process not found: #{inspect(name)}")
        nil

      pid ->
        # Get PMTiles struct and configuration from server
        {pmtiles, enable_tile_cache, enable_dir_cache, cache_path} =
          GenServer.call(pid, :get_config, 5000)

        config = %{
          stats: :"#{table_name}_stats",
          cache_path: cache_path,
          pending_directories: :"#{table_name}_pending_directories"
        }

        # Do all the work in the calling process (Phoenix request process)
        Operations.handle_tile_request(
          pmtiles,
          tile_id,
          z,
          x,
          y,
          config,
          enable_tile_cache,
          enable_dir_cache
        )
    end
  end

  def get_tile(server, z, x, y) when is_pid(server) or is_atom(server) do
    name =
      cond do
        is_atom(server) -> server
        is_pid(server) -> GenServer.call(server, :get_name, 5000)
      end

    table_name = table_name_for(name)

    tile_id = pmtiles_module().zxy_to_tile_id(z, x, y)

    # Get PMTiles struct and configuration from server
    {pmtiles, enable_tile_cache, enable_dir_cache, cache_path} =
      GenServer.call(server, :get_config, 5000)

    config = %{
      stats: :"#{table_name}_stats",
      cache_path: cache_path,
      pending_directories: :"#{table_name}_pending_directories"
    }

    # Do all the work in the calling process (Phoenix request process)
    Operations.handle_tile_request(
      pmtiles,
      tile_id,
      z,
      x,
      y,
      config,
      enable_tile_cache,
      enable_dir_cache
    )
  end

  @doc """
  Retrieves cache statistics.

  Returns hit and miss counts for the cache, useful for monitoring cache
  performance and effectiveness.

  ## Parameters

  - `server` - The cache process PID or registered name

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
  def get_stats(server) when is_pid(server) or is_atom(server) do
    GenServer.call(server, :get_stats, :infinity)
  end

  @doc """
  Clears all cached data (tiles and directories) and resets statistics.

  This function removes all cached files from disk and resets hit/miss counters.
  It's useful for forcing a cache refresh or freeing up disk space.

  When directory caching is enabled, this function will automatically trigger
  a background task to repopulate the directory cache after clearing. This ensures
  that frequently accessed directories are quickly available again.

  ## Parameters

  - `server` - The cache process PID or registered name

  ## Returns

  - `:ok` - Cache cleared successfully

  ## Examples

      iex> {:ok, pid} = ExPmtiles.Cache.start_link(bucket: "maps", path: "world.pmtiles")
      iex> ExPmtiles.Cache.clear_cache(pid)
      :ok

      iex> ExPmtiles.Cache.clear_cache(:my_custom_cache)
      :ok
  """
  def clear_cache(server) when is_pid(server) or is_atom(server) do
    GenServer.call(server, :clear_cache, :infinity)
  end

  # Server callbacks
  @impl true
  def init({region, bucket, path, storage, opts}) do
    name = Keyword.get(opts, :name, name_for(bucket, path))

    case initialize_pmtiles(region, bucket, path, storage) do
      nil -> {:stop, :pmtiles_not_found}
      pmtiles -> setup_cache_server(pmtiles, name, bucket, path, opts)
    end
  end

  @impl true
  def handle_call(:get_identifier, _from, %{bucket: bucket, path: path} = state) do
    {:reply, {bucket, path}, state}
  end

  @impl true
  def handle_call(:get_name, _from, %{name: name} = state) do
    {:reply, name, state}
  end

  @impl true
  def handle_call(:get_tile_cache_enabled, _from, %{enable_tile_cache: enable_tile_cache} = state) do
    {:reply, enable_tile_cache, state}
  end

  @impl true
  def handle_call(:get_cache_path, _from, %{cache_path: cache_path} = state) do
    {:reply, cache_path, state}
  end

  @impl true
  def handle_call(:get_config, _from, state) do
    # Return PMTiles struct and configuration for the caller to use
    {:reply, {state.pmtiles, state.enable_tile_cache, state.enable_dir_cache, state.cache_path},
     state}
  end

  @impl true
  def handle_call(:get_stats, _from, %{stats_table: stats_table} = state) do
    [{:hits, hits}] = :ets.lookup(stats_table, :hits)
    [{:misses, misses}] = :ets.lookup(stats_table, :misses)
    {:reply, %{hits: hits, misses: misses}, state}
  end

  @impl true
  def handle_call(:clear_cache, _from, state) do
    FileHandler.clear_cache_files(state.cache_path)
    reset_stats(state.stats_table)

    # Trigger background directory cache population if conditions are met
    if state.enable_dir_cache and @env != :test and not is_nil(state.bucket) and
         not is_nil(state.path) do
      cache_config = build_cache_config(state)

      trigger_background_dir_population(
        state.pmtiles,
        state.name,
        state.bucket,
        state.path,
        cache_config
      )

      Logger.debug("Triggered background directory cache population after clearing")
    end

    state =
      if state.max_cache_age_ms do
        %{state | cache_start_time: System.monotonic_time(:millisecond)}
      else
        state
      end

    {:reply, :ok, state}
  end

  @impl true
  def handle_info(:cleanup, state) do
    # No cleanup needed for file-based cache
    # Files are managed by the OS and automatically cleaned up on restart

    Process.send_after(self(), :cleanup, :timer.minutes(5))
    {:noreply, state}
  end

  @impl true
  def handle_info(:cleanup_old_cache, state) do
    state =
      if state.max_cache_age_ms do
        cleanup_cache_if_expired(state)
      else
        state
      end

    {:noreply, state}
  end

  @impl true
  def handle_info(:check_file_changed, state) do
    state = check_and_handle_file_change(state)
    {:noreply, state}
  end

  defp cleanup_cache_if_expired(state) do
    current_time = System.monotonic_time(:millisecond)
    age = current_time - state.cache_start_time

    if age >= state.max_cache_age_ms do
      perform_cache_cleanup(state, age, current_time)
    else
      state
    end
  end

  defp perform_cache_cleanup(state, age, current_time) do
    Logger.info(
      "Cache age (#{age}ms) exceeds max age (#{state.max_cache_age_ms}ms), clearing cache for #{state.name}"
    )

    FileHandler.clear_cache_files(state.cache_path)
    reset_stats(state.stats_table)

    trigger_dir_population_if_enabled(state)

    %{state | cache_start_time: current_time}
  end

  defp trigger_dir_population_if_enabled(state) do
    if should_trigger_dir_population?(state) do
      cache_config = build_cache_config(state)

      trigger_background_dir_population(
        state.pmtiles,
        state.name,
        state.bucket,
        state.path,
        cache_config
      )

      Logger.debug("Triggered background directory cache population after automatic clearing")
    end
  end

  defp should_trigger_dir_population?(state) do
    state.enable_dir_cache and @env != :test and not is_nil(state.bucket) and
      not is_nil(state.path)
  end

  defp check_and_handle_file_change(state) do
    case ExPmtiles.Storage.get_file_metadata(state.pmtiles, state.exaws_config) do
      {:ok, current_metadata} ->
        handle_metadata_result(state, current_metadata)

      {:error, reason} ->
        Logger.debug(
          "Failed to check file metadata for #{state.name}: #{inspect(reason)}. Skipping file change check."
        )

        state
    end
  end

  defp handle_metadata_result(state, current_metadata) do
    cond do
      file_changed?(state, current_metadata) ->
        handle_file_change(state, current_metadata)

      is_nil(state.file_metadata) ->
        %{state | file_metadata: current_metadata}

      true ->
        state
    end
  end

  defp file_changed?(state, current_metadata) do
    state.file_metadata && current_metadata != state.file_metadata
  end

  defp handle_file_change(state, new_metadata) do
    Logger.warning(
      "PMTiles file changed detected for #{state.name}. Old metadata: #{state.file_metadata}, New metadata: #{new_metadata}. Clearing cache..."
    )

    # File has changed - clear cache and re-initialize
    FileHandler.clear_cache_files(state.cache_path)
    reset_stats(state.stats_table)

    # Clear in-memory directories in the pmtiles struct
    pmtiles = %{state.pmtiles | directories: %{}, pending_directories: %{}}

    # Trigger background directory cache population if conditions are met
    trigger_dir_population_on_file_change(state, pmtiles)

    %{
      state
      | file_metadata: new_metadata,
        pmtiles: pmtiles,
        cache_start_time: System.monotonic_time(:millisecond)
    }
  end

  defp trigger_dir_population_on_file_change(state, pmtiles) do
    if should_trigger_dir_population?(state) do
      cache_config = build_cache_config(state)

      trigger_background_dir_population(
        pmtiles,
        state.name,
        state.bucket,
        state.path,
        cache_config
      )

      Logger.debug("Triggered background directory cache population after file change detection")
    end
  end

  @impl true
  def terminate(_reason, _state) do
    # File-based cache, no resources to close
    :ok
  end

  # Private functions
  defp name_for(bucket, path) do
    :"#{@table_prefix}_process_#{bucket}_#{Path.basename(path, ".pmtiles")}"
  end

  defp table_name_for(server_name) do
    :"#{server_name}_table"
  end

  # Used to get the pmtiles module from the application environment so it can be mocked in tests
  defp pmtiles_module do
    Application.get_env(:ex_pmtiles, :pmtiles_module, ExPmtiles)
  end

  # Private functions

  defp initialize_pmtiles(region, bucket, path, storage) do
    pmtiles_module().new(region, bucket, path, storage)
  end

  defp reset_stats(stats_table) do
    :ets.insert(stats_table, {:hits, 0})
    :ets.insert(stats_table, {:misses, 0})
    Logger.debug("Reset cache statistics")
  end

  defp build_cache_config(state) do
    %{
      table: state.table,
      cache_path: state.cache_path,
      stats: state.stats_table,
      pending_directories: state.pending_directories_table
    }
  end

  defp setup_cache_server(pmtiles, server_name, bucket, path, opts) do
    table_name = table_name_for(server_name)
    enable_dir_cache = Keyword.get(opts, :enable_dir_cache, false)
    enable_tile_cache = Keyword.get(opts, :enable_tile_cache, false)
    max_cache_age_ms = Keyword.get(opts, :max_cache_age_ms)
    cleanup_interval_ms = Keyword.get(opts, :cleanup_interval_ms, :timer.hours(1))
    file_check_interval_ms = Keyword.get(opts, :file_check_interval_ms, :timer.minutes(5))

    Logger.info("Initializing PMTiles cache for #{bucket}/#{path} with name #{server_name}")

    cache_config =
      init_cache(table_name, server_name, enable_dir_cache, enable_tile_cache)

    # Get ExAws config for testing (only used in tests)
    exaws_config = Keyword.get(opts, :exaws_config)

    # Only get file metadata if caching is enabled (needed for change detection)
    file_metadata =
      if enable_dir_cache or enable_tile_cache do
        case ExPmtiles.Storage.get_file_metadata(pmtiles, exaws_config) do
          {:ok, metadata} ->
            Logger.debug("Initial file metadata for #{server_name}: #{metadata}")
            metadata

          {:error, reason} ->
            Logger.debug(
              "Failed to get initial file metadata for #{server_name}: #{inspect(reason)}"
            )

            nil
        end
      else
        nil
      end

    # If max_cache_age_ms is set, clear existing cache on startup
    # Run the clearing in a task and await it to ensure it completes before background population
    if max_cache_age_ms do
      task =
        Task.async(fn ->
          FileHandler.clear_cache_files(cache_config.cache_path)
        end)

      Task.await(task, :infinity)
      Logger.debug("Cleared existing cache on startup (max_cache_age_ms is configured)")
    end

    pmtiles =
      start_background_cache_population(
        pmtiles,
        server_name,
        bucket,
        path,
        cache_config,
        enable_dir_cache
      )

    # Schedule periodic cleanup if max_cache_age_ms is set
    if max_cache_age_ms do
      :timer.send_interval(cleanup_interval_ms, :cleanup_old_cache)

      Logger.debug(
        "Automatic cache clearing enabled: max age #{max_cache_age_ms}ms, check interval #{cleanup_interval_ms}ms"
      )
    end

    # Only schedule file change detection if caching is enabled
    if enable_dir_cache or enable_tile_cache do
      :timer.send_interval(file_check_interval_ms, :check_file_changed)

      Logger.debug("File change detection enabled: check interval #{file_check_interval_ms}ms")
    end

    {:ok,
     %{
       name: server_name,
       pmtiles: pmtiles,
       bucket: bucket,
       path: path,
       table: table_name,
       cache_path: cache_config.cache_path,
       stats_table: cache_config.stats,
       pending_directories_table: cache_config.pending_directories,
       enable_dir_cache: enable_dir_cache,
       enable_tile_cache: enable_tile_cache,
       max_cache_age_ms: max_cache_age_ms,
       cache_start_time: System.monotonic_time(:millisecond),
       file_metadata: file_metadata,
       exaws_config: exaws_config
     }}
  end

  defp init_cache(table_name, server_name, enable_dir_cache, enable_tile_cache) do
    # Create a unique cache directory for this cache instance using the server name
    # This ensures multiple cache instances don't share the same cache files
    base_cache_dir =
      Application.get_env(
        :ex_pmtiles,
        :cache_dir,
        Path.join(System.tmp_dir!(), "ex_pmtiles_cache")
      )

    # Only create the cache directory structure if either caching mode is enabled
    cache_path =
      if enable_tile_cache or enable_dir_cache do
        path = Path.join(base_cache_dir, Atom.to_string(server_name))
        FileHandler.init_cache_directories(path, enable_tile_cache, enable_dir_cache)

        if enable_tile_cache do
          Logger.debug("File-based tile caching enabled at #{path}")
        else
          Logger.debug("Tile caching disabled - tiles will not be cached")
        end

        if enable_dir_cache do
          Logger.debug("File-based directory caching enabled at #{path}")
        else
          Logger.debug("Directory caching disabled")
        end

        path
      else
        nil
      end

    # Stats table for tracking hits/misses
    stats_table = :"#{table_name}_stats"
    :ets.new(stats_table, [:set, :public, :named_table])
    :ets.insert(stats_table, {:hits, 0})
    :ets.insert(stats_table, {:misses, 0})

    # Pending directories table for coordinating concurrent directory fetches
    pending_directories_table = :"#{table_name}_pending_directories"
    :ets.new(pending_directories_table, [:set, :public, :named_table])

    %{
      table: table_name,
      cache_path: cache_path,
      stats: stats_table,
      pending_directories: pending_directories_table
    }
  end

  # Directory caching enabled with bucket and path (S3), not in test mode - trigger background pre-fetch
  defp start_background_cache_population(pmtiles, server_name, bucket, path, cache_config, true)
       when not is_nil(bucket) and not is_nil(path) and @env != :test do
    trigger_background_dir_population(pmtiles, server_name, bucket, path, cache_config)
    Logger.debug("Directory caching enabled (background pre-fetch + on-demand)")
    pmtiles
  end

  # Directory caching enabled with bucket and path (S3), in test mode - on-demand only
  defp start_background_cache_population(pmtiles, _server_name, bucket, path, _cache_config, true)
       when not is_nil(bucket) and not is_nil(path) and @env == :test do
    Logger.debug("Directory caching enabled (on-demand fetching only)")
    pmtiles
  end

  # Directory caching enabled but bucket or path is nil (local files) - on-demand only
  defp start_background_cache_population(
         pmtiles,
         _server_name,
         _bucket,
         _path,
         _cache_config,
         true
       ) do
    Logger.debug("Directory caching enabled (on-demand fetching)")
    pmtiles
  end

  # Directory caching disabled
  defp start_background_cache_population(
         pmtiles,
         _server_name,
         _bucket,
         _path,
         _cache_config,
         false
       ) do
    Logger.debug("Directory caching disabled")
    pmtiles
  end

  defp trigger_background_dir_population(pmtiles, server_name, bucket, path, cache_config) do
    # Start background task that doesn't block
    Task.start(fn ->
      # Add a small delay to let the app finish starting up or cache clearing to complete
      Process.sleep(1000)

      try do
        header = pmtiles.header

        Logger.debug(
          "Starting background directory pre-cache for #{bucket}/#{path} (#{server_name})"
        )

        # Pre-fetch root directory (most commonly needed)
        dir =
          pmtiles_module().get_cached_directory_file(
            pmtiles,
            header.root_offset,
            header.root_length,
            cache_config.cache_path,
            cache_config.pending_directories
          )

        Enum.filter(dir, fn d -> d.run_length == 0 end)
        |> Enum.chunk_every(4)
        |> Enum.each(fn dirs ->
          try do
            tasks =
              Enum.map(
                dirs,
                fn d ->
                  Task.async(fn ->
                    pmtiles_module().get_cached_directory_file(
                      pmtiles,
                      header.leaf_dir_offset + d.offset,
                      d.length,
                      cache_config.cache_path,
                      cache_config.pending_directories
                    )
                  end)
                end
              )

            Task.await_many(tasks, 20_000)
          catch
            :exit, _ -> nil
          end
        end)

        Logger.debug("Root directory pre-cached successfully")
      rescue
        e ->
          Logger.debug("Failed to pre-cache root directory: #{inspect(e)}")
          Logger.debug(Exception.format_stacktrace(__STACKTRACE__))
      end
    end)
  end
end
