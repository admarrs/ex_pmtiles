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
  """
  use GenServer
  require Logger

  alias ExPmtiles.Cache.Operations
  alias ExPmtiles.Cache.FileHandler

  @env Mix.env()
  @table_prefix :pmtiles_cache
  @default_max_entries 100_000

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
    request_id = System.unique_integer([:positive])

    name = name_for(bucket, path)
    table_name = table_name_for(name)

    tile_id = pmtiles_module().zxy_to_tile_id(z, x, y)

    # Check if tile cache is enabled by trying to get server info
    case Process.whereis(name) do
      nil ->
        Logger.error("Pmtiles.Cache process not found: #{inspect(name)}")
        nil

      pid ->
        # Get cache configuration from server
        enable_tile_cache = GenServer.call(pid, :get_tile_cache_enabled, 5000)
        cache_path = GenServer.call(pid, :get_cache_path, 5000)

        config = %{
          table: table_name,
          pending: :"#{table_name}_pending",
          stats: :"#{table_name}_stats",
          cache_path: cache_path
        }

        Operations.handle_tile_request(
          name,
          tile_id,
          z,
          x,
          y,
          request_id,
          config,
          enable_tile_cache
        )
    end
  end

  def get_tile(server, z, x, y) when is_pid(server) or is_atom(server) do
    # For PID/atom, we need to get the server name
    request_id = System.unique_integer([:positive])

    name =
      cond do
        is_atom(server) -> server
        is_pid(server) -> GenServer.call(server, :get_name, 5000)
      end

    table_name = table_name_for(name)

    tile_id = pmtiles_module().zxy_to_tile_id(z, x, y)

    # Get cache configuration from server
    enable_tile_cache = GenServer.call(server, :get_tile_cache_enabled, 5000)
    cache_path = GenServer.call(server, :get_cache_path, 5000)

    config = %{
      table: table_name,
      pending: :"#{table_name}_pending",
      stats: :"#{table_name}_stats",
      cache_path: cache_path
    }

    Operations.handle_tile_request(name, tile_id, z, x, y, request_id, config, enable_tile_cache)
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

      Logger.info("Triggered background directory cache population after clearing")
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
  def handle_call(:get_pmtiles, _from, state) do
    # Just return the pmtiles instance as-is
    # ETS-based directory caching is handled externally
    {:reply, {state.pmtiles, %{}}, state}
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

      Logger.info("Triggered background directory cache population after automatic clearing")
    end
  end

  defp should_trigger_dir_population?(state) do
    state.enable_dir_cache and @env != :test and not is_nil(state.bucket) and
      not is_nil(state.path)
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
    case @env do
      :test ->
        ExPmtiles.CacheMock

      _ ->
        ExPmtiles
    end
  end

  # Private functions

  defp initialize_pmtiles(region, bucket, path, storage) do
    pmtiles_module().new(region, bucket, path, storage)
  end

  defp reset_stats(stats_table) do
    :ets.insert(stats_table, {:hits, 0})
    :ets.insert(stats_table, {:misses, 0})
    Logger.info("Reset cache statistics")
  end

  defp build_cache_config(state) do
    %{
      table: state.table,
      cache_path: state.cache_path,
      pending: state.pending_table,
      stats: state.stats_table,
      pending_directories: state.pending_directories_table,
      max_entries: :"#{state.table}_max_entries"
    }
  end

  defp setup_cache_server(pmtiles, server_name, bucket, path, opts) do
    table_name = table_name_for(server_name)
    max_entries = Keyword.get(opts, :max_entries, @default_max_entries)
    enable_dir_cache = Keyword.get(opts, :enable_dir_cache, true)
    enable_tile_cache = Keyword.get(opts, :enable_tile_cache, false)
    max_cache_age_ms = Keyword.get(opts, :max_cache_age_ms)
    cleanup_interval_ms = Keyword.get(opts, :cleanup_interval_ms, :timer.hours(1))

    Logger.info("Initializing PMTiles cache for #{bucket}/#{path} with name #{server_name}")

    cache_config =
      init_cache(table_name, server_name, max_entries, enable_dir_cache, enable_tile_cache)

    # If max_cache_age_ms is set, clear existing cache on startup
    # Run the clearing in a task and await it to ensure it completes before background population
    if max_cache_age_ms do
      task =
        Task.async(fn ->
          FileHandler.clear_cache_files(cache_config.cache_path)
        end)

      Task.await(task, :infinity)
      Logger.info("Cleared existing cache on startup (max_cache_age_ms is configured)")
    end

    pmtiles = start_background_cache_population(pmtiles, server_name, bucket, path, cache_config)

    # Schedule periodic cleanup if max_cache_age_ms is set
    if max_cache_age_ms do
      :timer.send_interval(cleanup_interval_ms, :cleanup_old_cache)

      Logger.info(
        "Automatic cache clearing enabled: max age #{max_cache_age_ms}ms, check interval #{cleanup_interval_ms}ms"
      )
    end

    {:ok,
     %{
       name: server_name,
       pmtiles: pmtiles,
       bucket: bucket,
       path: path,
       table: table_name,
       cache_path: cache_config.cache_path,
       pending_table: cache_config.pending,
       stats_table: cache_config.stats,
       pending_directories_table: cache_config.pending_directories,
       max_entries: max_entries,
       enable_dir_cache: enable_dir_cache,
       enable_tile_cache: enable_tile_cache,
       max_cache_age_ms: max_cache_age_ms,
       cache_start_time: System.monotonic_time(:millisecond)
     }}
  end

  defp init_cache(table_name, server_name, max_entries, enable_dir_cache, enable_tile_cache) do
    # Create a unique cache directory for this cache instance using the server name
    # This ensures multiple cache instances don't share the same cache files
    base_cache_dir =
      Application.get_env(
        :ex_pmtiles,
        :cache_dir,
        Path.join(System.tmp_dir!(), "ex_pmtiles_cache")
      )

    cache_path = Path.join(base_cache_dir, Atom.to_string(server_name))

    # Only create the cache directory structure if either caching mode is enabled
    if enable_tile_cache or enable_dir_cache do
      FileHandler.init_cache_directories(cache_path, enable_tile_cache, enable_dir_cache)

      if enable_tile_cache do
        Logger.info("File-based tile caching enabled at #{cache_path}")
      else
        Logger.info("Tile caching disabled - tiles will not be cached")
      end

      if enable_dir_cache do
        Logger.info("File-based directory caching enabled at #{cache_path}")
      else
        Logger.info("Directory caching disabled")
      end

      cache_path
    else
      nil
    end

    pending_table = :"#{table_name}_pending"
    :ets.new(pending_table, [:set, :public, :named_table])
    stats_table = :"#{table_name}_stats"
    :ets.new(stats_table, [:set, :public, :named_table])
    :ets.insert(stats_table, {:hits, 0})
    :ets.insert(stats_table, {:misses, 0})

    # Pending directories table for coordinating concurrent directory fetches
    pending_directories_table = :"#{table_name}_pending_directories"
    :ets.new(pending_directories_table, [:set, :public, :named_table])

    # Max entries table for storing the max_entries limit
    max_entries_table = :"#{table_name}_max_entries"
    :ets.new(max_entries_table, [:set, :public, :named_table])
    :ets.insert(max_entries_table, {:max_entries, max_entries})

    %{
      table: table_name,
      cache_path: cache_path,
      pending: pending_table,
      stats: stats_table,
      pending_directories: pending_directories_table,
      max_entries: max_entries_table
    }
  end

  defp start_background_cache_population(pmtiles, server_name, bucket, path, cache_config)
       when not is_nil(bucket) and not is_nil(path) do
    # Pre-fetch root directory in background without blocking startup
    if @env != :test do
      trigger_background_dir_population(pmtiles, server_name, bucket, path, cache_config)
      Logger.info("Directory caching enabled (background pre-fetch + on-demand)")
    else
      Logger.info("Directory caching enabled (on-demand fetching only)")
    end

    pmtiles
  end

  defp start_background_cache_population(pmtiles, _server_name, _bucket, _path, _cache_configs) do
    # Bucket or path is nil (e.g., local files), skip pre-fetch
    Logger.info("Directory caching enabled (on-demand fetching)")
    pmtiles
  end

  defp trigger_background_dir_population(pmtiles, server_name, bucket, path, cache_config) do
    # Start background task that doesn't block
    Task.start(fn ->
      # Add a small delay to let the app finish starting up or cache clearing to complete
      Process.sleep(1000)

      try do
        header = pmtiles.header

        Logger.info(
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

        Logger.info("Root directory pre-cached successfully")
      rescue
        e ->
          Logger.warning("Failed to pre-cache root directory: #{inspect(e)}")
          Logger.debug(Exception.format_stacktrace(__STACKTRACE__))
      end
    end)
  end
end
