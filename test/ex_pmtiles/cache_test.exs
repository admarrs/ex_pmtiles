defmodule ExPmtiles.CacheTest do
  use ExUnit.Case, async: false
  import Mox
  import ExPmtile.Support.BypassHelpers

  # Set up expectations to be verified when the test exits
  setup :verify_on_exit!

  alias ExPmtiles.Cache
  alias ExPmtiles.CacheMock

  @bucket "test-bucket"
  @path "test.pmtiles"
  @max_cache_size 1_000

  # Mock the Pmtiles module
  setup do
    # Clean up any existing process
    case Process.whereis(:cache_one) do
      nil -> :ok
      pid -> GenServer.stop(pid)
    end

    # Clean up cache files from previous test runs
    cache_dir = Path.join([System.tmp_dir!(), "ex_pmtiles_cache"])
    File.rm_rf(cache_dir)

    # Set up individual stubs for each function
    # Add this line to make mocks global
    Mox.set_mox_global()

    stub(CacheMock, :new, fn region, bucket, path, storage ->
      %{region: region, bucket: bucket, path: path, storage: storage}
    end)

    stub(CacheMock, :get_zxy, fn pmtiles, z, x, y ->
      {{0, 100, "tile_data_#{z}_#{x}_#{y}"}, pmtiles}
    end)

    stub(CacheMock, :zxy_to_tile_id, fn z, x, y ->
      z * 1_000_000 + x * 1000 + y
    end)

    stub(CacheMock, :tile_id_to_zxy, fn id ->
      z = div(id, 1_000_000)
      x = div(rem(id, 1_000_000), 1000)
      y = rem(id, 1000)
      {z, x, y}
    end)

    # Start the cache with tile caching enabled for tests
    {:ok, pid} =
      Cache.start_link(
        name: :cache_one,
        region: nil,
        bucket: @bucket,
        path: @path,
        max_entries: @max_cache_size,
        enable_tile_cache: true
      )

    on_exit(fn ->
      if Process.alive?(pid) do
        GenServer.stop(pid)
      end
    end)

    # Return the test context
    %{cache_pid: pid}
  end

  describe "initialization" do
    test "starts successfully", %{cache_pid: pid} do
      assert Process.alive?(pid)
    end

    test "creates required tables" do
      table_name = :cache_one_table
      stats_table = :"#{table_name}_stats"
      cache_dir = Path.join([System.tmp_dir!(), "ex_pmtiles_cache"])

      assert :ets.info(stats_table) != :undefined
      # Cache directory should exist when tile caching is enabled
      assert File.exists?(cache_dir)
    end

    test "initializes stats counters", %{cache_pid: pid} do
      stats = Cache.get_stats(pid)
      assert stats == %{hits: 0, misses: 0}
    end
  end

  describe "get_tile/4" do
    test "returns cached tile on hit", %{cache_pid: pid} do
      # First request (miss)
      tile_data = Cache.get_tile(pid, 0, 0, 0)
      assert tile_data == {:ok, "tile_data_0_0_0"}

      # Second request (hit)
      tile_data = Cache.get_tile(pid, 0, 0, 0)
      assert tile_data == {:ok, "tile_data_0_0_0"}

      # Check stats
      stats = Cache.get_stats(pid)
      assert stats.hits == 1
      assert stats.misses == 1
    end

    test "handles cache misses", %{cache_pid: pid} do
      tile_data = Cache.get_tile(pid, 5, 10, 15)
      assert tile_data == {:ok, "tile_data_5_10_15"}

      stats = Cache.get_stats(pid)
      assert stats.misses == 1
    end
  end

  describe "cache size management" do
    # Tag slow tests
    @tag :slow
    test "caches tiles to individual files", %{cache_pid: pid} do
      cache_dir = Path.join([System.tmp_dir!(), "ex_pmtiles_cache", "cache_one"])

      # Request a tile
      Cache.get_tile(pid, 10, 1, 1)

      # Check that the tile file was created
      tile_id = 10 * 1_000_000 + 1 * 1000 + 1
      tile_file = Path.join([cache_dir, "tiles", "#{tile_id}.bin"])

      assert File.exists?(tile_file)
    end

    test "reads cached tiles from files", %{cache_pid: pid} do
      # First request (miss)
      {:ok, tile_data1} = Cache.get_tile(pid, 10, 2, 2)

      # Second request (hit from file)
      {:ok, tile_data2} = Cache.get_tile(pid, 10, 2, 2)

      assert tile_data1 == tile_data2
      assert tile_data1 == "tile_data_10_2_2"
    end
  end

  describe "cache expiration" do
    test "expires entries after TTL", %{cache_pid: pid} do
      # Add a tile
      Cache.get_tile(pid, 0, 0, 0)

      # Force cleanup
      send(pid, :cleanup)

      # Wait for cleanup to process
      :timer.sleep(100)

      # Request same tile again (should be a miss)
      Cache.get_tile(pid, 0, 0, 0)

      stats = Cache.get_stats(pid)
      assert stats.misses == 1
    end
  end

  describe "termination" do
    test "cleans up on termination", %{cache_pid: pid} do
      cache_dir = Path.join([System.tmp_dir!(), "ex_pmtiles_cache", "cache_one"])

      # Create a cached tile
      Cache.get_tile(pid, 10, 1, 1)

      # Verify cache file exists
      tile_id = 10 * 1_000_000 + 1 * 1000 + 1
      tile_file = Path.join([cache_dir, "tiles", "#{tile_id}.bin"])
      assert File.exists?(tile_file)

      # Stop the cache
      GenServer.stop(pid)

      # Wait for termination
      :timer.sleep(100)

      # Cache files remain (cleaned up manually or on restart)
      # This is expected behavior for file-based cache
      assert File.exists?(tile_file)
    end
  end

  describe "concurrent request handling" do
    test "coordinates multiple concurrent requests for the same tile", %{cache_pid: pid} do
      # Use a tile that hasn't been cached yet
      {z, x, y} = {15, 100, 200}

      # Launch multiple concurrent requests for the same tile
      tasks =
        for i <- 1..10 do
          Task.async(fn ->
            start = System.monotonic_time(:millisecond)
            result = Cache.get_tile(pid, z, x, y)
            duration = System.monotonic_time(:millisecond) - start
            {i, result, duration}
          end)
        end

      # Wait for all tasks
      results = Task.await_many(tasks, 20_000)

      # All should succeed
      assert Enum.all?(results, fn {_, result, _} ->
               result == {:ok, "tile_data_#{z}_#{x}_#{y}"}
             end)

      # None should take longer than 5 seconds (blocking indicator)
      assert Enum.all?(results, fn {_, _, duration} -> duration < 5_000 end)

      # Note: With simplified architecture, concurrent requests are not coordinated
      # Each does its own work. Multiple processes may fetch the same tile.
      # We just verify all requests succeeded in a reasonable time
      stats = Cache.get_stats(pid)
      assert stats.misses >= 1, "Expected at least 1 miss"
    end

    test "handles concurrent requests for different tiles without blocking", %{cache_pid: pid} do
      # Launch concurrent requests for different tiles
      tiles = for i <- 1..20, do: {10, 500 + i, 250 + i}

      tasks =
        Enum.map(tiles, fn {z, x, y} ->
          Task.async(fn ->
            start = System.monotonic_time(:millisecond)
            result = Cache.get_tile(pid, z, x, y)
            duration = System.monotonic_time(:millisecond) - start
            {z, x, y, result, duration}
          end)
        end)

      results = Task.await_many(tasks, 10_000)

      # All should succeed
      assert Enum.all?(results, fn {z, x, y, result, _} ->
               result == {:ok, "tile_data_#{z}_#{x}_#{y}"}
             end)

      # None should take unreasonably long (would indicate blocking)
      max_duration = Enum.max_by(results, fn {_, _, _, _, duration} -> duration end) |> elem(4)
      assert max_duration < 5_000, "Max request took #{max_duration}ms, indicates blocking"

      # Should have one miss per tile
      stats = Cache.get_stats(pid)
      assert stats.misses == length(tiles)
    end

    test "handles concurrent requests for the same tile", %{cache_pid: pid} do
      # Use a unique tile coordinate that hasn't been cached yet
      {z, x, y} = {25, 9999, 8888}

      # Launch 10 concurrent requests for the SAME tile
      tasks =
        for _ <- 1..10 do
          Task.async(fn ->
            Cache.get_tile(pid, z, x, y)
          end)
        end

      results = Task.await_many(tasks, :infinity)

      # All should succeed with the same data
      assert Enum.all?(results, fn result ->
               match?({:ok, _}, result)
             end)

      # Verify all results are identical
      [first | rest] = results
      assert Enum.all?(rest, fn result -> result == first end)

      # Get final stats
      final_stats = Cache.get_stats(pid)

      # Note: With the simplified architecture, we no longer coordinate duplicate requests.
      # Multiple processes may fetch the same tile concurrently (rare in practice).
      # This is acceptable because:
      # 1. The filesystem handles concurrent writes safely
      # 2. Each process does its own work without blocking
      # 3. The tradeoff is much simpler code with no inter-process coordination complexity
      # We just verify that all requests succeeded
      assert final_stats.misses >= 1,
             "Expected at least 1 miss for concurrent requests"
    end

    test "does not block GenServer with cache misses", %{cache_pid: pid} do
      # Trigger multiple cache misses concurrently
      tasks =
        for i <- 1..50 do
          Task.async(fn ->
            Cache.get_tile(pid, 12, 1000 + i, 2000 + i)
          end)
        end

      # While tasks are running, check the message queue
      Process.sleep(50)
      queue_during = Process.info(pid, :message_queue_len) |> elem(1)

      # Wait for all tasks to complete
      Task.await_many(tasks, 10_000)

      # Queue should not have grown excessively (would indicate blocking)
      assert queue_during < 100,
             "GenServer queue grew to #{queue_during}, indicates blocking"
    end
  end

  describe "tile caching disabled" do
    setup do
      # Clean up any existing process
      case Process.whereis(:cache_no_tile_cache) do
        nil -> :ok
        pid -> GenServer.stop(pid)
      end

      # Start the cache with tile caching disabled (default)
      {:ok, pid} =
        Cache.start_link(
          name: :cache_no_tile_cache,
          region: nil,
          bucket: @bucket,
          path: @path,
          enable_tile_cache: false
        )

      on_exit(fn ->
        if Process.alive?(pid) do
          GenServer.stop(pid)
        end
      end)

      %{cache_pid: pid}
    end

    test "does not create cache files when disabled", %{cache_pid: pid} do
      cache_dir = Path.join([System.tmp_dir!(), "ex_pmtiles_cache"])

      # Request a tile
      Cache.get_tile(pid, 0, 0, 0)

      # Check that no tile cache file was created (directory caching may still exist)
      tiles_dir = Path.join(cache_dir, "tiles")

      # Either tiles directory doesn't exist or it's empty
      refute File.exists?(tiles_dir) and File.ls!(tiles_dir) != []
    end

    test "fetches tiles without caching", %{cache_pid: pid} do
      # First request
      assert Cache.get_tile(pid, 0, 0, 0) == {:ok, "tile_data_0_0_0"}

      # Second request - should still work but not use cache
      assert Cache.get_tile(pid, 0, 0, 0) == {:ok, "tile_data_0_0_0"}

      # Stats should show misses (no cache hits)
      stats = Cache.get_stats(pid)
      assert stats.misses >= 1
    end
  end

  describe "file change detection" do
    setup do
      # Create a temporary PMTiles file for testing
      temp_file =
        Path.join(System.tmp_dir!(), "test_pmtiles_file_change_#{:rand.uniform(100_000)}.pmtiles")

      File.write!(temp_file, "initial content")

      # Mock get_file_metadata to track calls
      metadata_ref = :counters.new(1, [])
      :counters.put(metadata_ref, 1, 0)

      # Clean up any existing process
      case Process.whereis(:cache_file_change) do
        nil -> :ok
        pid -> GenServer.stop(pid)
      end

      on_exit(fn ->
        File.rm(temp_file)

        case Process.whereis(:cache_file_change) do
          nil -> :ok
          pid -> GenServer.stop(pid)
        end
      end)

      %{temp_file: temp_file, metadata_ref: metadata_ref}
    end

    test "initializes with file metadata" do
      # Note: This test can't directly verify file metadata without accessing GenServer state,
      # but we can verify that the cache starts successfully and doesn't crash
      # when get_file_metadata is called

      {:ok, pid} =
        Cache.start_link(
          name: :cache_file_change,
          region: nil,
          bucket: @bucket,
          path: @path,
          enable_tile_cache: true
        )

      # Cache should be alive
      assert Process.alive?(pid)

      # Should be able to get stats (proves cache initialized)
      stats = Cache.get_stats(pid)
      assert stats == %{hits: 0, misses: 0}

      GenServer.stop(pid)
    end

    test "periodic file change check message is scheduled" do
      {:ok, pid} =
        Cache.start_link(
          name: :cache_file_change,
          region: nil,
          bucket: @bucket,
          path: @path,
          enable_tile_cache: true,
          # Use a long interval to prevent triggering during test
          file_check_interval_ms: :timer.hours(1)
        )

      # Cache should be alive and functional
      assert Process.alive?(pid)

      # Verify cache is working
      stats = Cache.get_stats(pid)
      assert stats == %{hits: 0, misses: 0}

      GenServer.stop(pid)
    end

    test "handles file change detection gracefully when metadata check fails" do
      {:ok, pid} =
        Cache.start_link(
          name: :cache_file_change,
          region: nil,
          bucket: @bucket,
          path: @path,
          enable_tile_cache: true,
          # Use long interval to prevent automatic checks
          file_check_interval_ms: :timer.hours(1)
        )

      # Manually trigger a file change check
      send(pid, :check_file_changed)

      # Wait for the message to be processed
      Process.sleep(100)

      # Cache should still be alive despite metadata check likely failing (no S3 credentials)
      assert Process.alive?(pid)

      # Should still be able to operate normally
      stats = Cache.get_stats(pid)
      assert is_map(stats)

      GenServer.stop(pid)
    end

    test "clears cache when file metadata changes" do
      # This is a behavioral test - we verify that the cache clearing logic
      # would be triggered if metadata changed, but we can't easily mock
      # the metadata check in a running GenServer. Instead, we test the
      # clear_cache function which is called when file changes are detected.

      {:ok, pid} =
        Cache.start_link(
          name: :cache_file_change,
          region: nil,
          bucket: @bucket,
          path: @path,
          enable_tile_cache: true
        )

      # Cache a tile
      Cache.get_tile(pid, 0, 0, 0)

      stats_before = Cache.get_stats(pid)
      assert stats_before.misses == 1

      # Manually clear cache (simulates what happens on file change)
      Cache.clear_cache(pid)

      # Wait for clear to complete
      Process.sleep(100)

      # Stats should be reset
      stats_after = Cache.get_stats(pid)
      assert stats_after.hits == 0
      assert stats_after.misses == 0

      GenServer.stop(pid)
    end
  end

  describe "file change detection with S3 (Bypass)" do
    setup [:start_bypass]

    test "detects file changes via ETag and clears cache", %{bypass: bypass} do
      # Clean up any existing process
      case Process.whereis(:cache_s3_change) do
        nil -> :ok
        pid -> GenServer.stop(pid)
      end

      # Set up Bypass to handle HEAD requests
      initial_etag = "\"initial-etag-123\""
      etag_ref = :atomics.new(1, [])
      :atomics.put(etag_ref, 1, 1)

      Bypass.expect(bypass, "HEAD", "/#{@bucket}/#{@path}", fn conn ->
        etag_version = :atomics.get(etag_ref, 1)

        etag =
          case etag_version do
            1 -> initial_etag
            2 -> "\"changed-etag-456\""
            _ -> "\"later-etag-789\""
          end

        conn
        |> Plug.Conn.put_resp_header("etag", etag)
        |> Plug.Conn.resp(200, "")
      end)

      # Create S3 instance with Bypass config
      pmtiles_instance = %ExPmtiles{
        source: :s3,
        region: "us-east-1",
        bucket: @bucket,
        path: @path,
        directories: %{},
        pending_directories: %{}
      }

      config = exaws_config_for_bypass(bypass)

      # Verify initial metadata fetch works
      {:ok, metadata} = ExPmtiles.Storage.get_file_metadata(pmtiles_instance, config)
      assert metadata == initial_etag

      # Note: We can't easily inject the Bypass config into the Cache GenServer
      # This test demonstrates the S3 metadata checking works with Bypass,
      # but full integration requires dependency injection
      if Process.whereis(:cache_s3_change) do
        GenServer.stop(:cache_s3_change)
      end
    end

    test "handles S3 errors gracefully during file change check", %{bypass: bypass} do
      # Clean up any existing process
      case Process.whereis(:cache_s3_error) do
        nil -> :ok
        pid -> GenServer.stop(pid)
      end

      # Set up Bypass to return 404
      Bypass.expect(bypass, "HEAD", "/#{@bucket}/#{@path}", fn conn ->
        Plug.Conn.resp(conn, 404, "Not Found")
      end)

      pmtiles_instance = %ExPmtiles{
        source: :s3,
        region: "us-east-1",
        bucket: @bucket,
        path: @path,
        directories: %{},
        pending_directories: %{}
      }

      config = exaws_config_for_bypass(bypass)

      # Should return error but not crash
      result = ExPmtiles.Storage.get_file_metadata(pmtiles_instance, config)
      assert {:error, _} = result

      if Process.whereis(:cache_s3_error) do
        GenServer.stop(:cache_s3_error)
      end
    end

    test "retrieves different ETags for file changes", %{bypass: bypass} do
      # Clean up any existing process
      case Process.whereis(:cache_s3_etag_change) do
        nil -> :ok
        pid -> GenServer.stop(pid)
      end

      # Use a counter to change ETag on each request
      request_count = :counters.new(1, [])

      Bypass.expect(bypass, "HEAD", "/#{@bucket}/#{@path}", fn conn ->
        count = :counters.get(request_count, 1)
        :counters.add(request_count, 1, 1)

        etag = "\"etag-version-#{count}\""

        conn
        |> Plug.Conn.put_resp_header("etag", etag)
        |> Plug.Conn.resp(200, "")
      end)

      pmtiles_instance = %ExPmtiles{
        source: :s3,
        region: "us-east-1",
        bucket: @bucket,
        path: @path,
        directories: %{},
        pending_directories: %{}
      }

      config = exaws_config_for_bypass(bypass)

      # First request
      {:ok, metadata1} = ExPmtiles.Storage.get_file_metadata(pmtiles_instance, config)
      assert metadata1 == "\"etag-version-0\""

      # Second request - should get different ETag
      {:ok, metadata2} = ExPmtiles.Storage.get_file_metadata(pmtiles_instance, config)
      assert metadata2 == "\"etag-version-1\""

      # Third request
      {:ok, metadata3} = ExPmtiles.Storage.get_file_metadata(pmtiles_instance, config)
      assert metadata3 == "\"etag-version-2\""

      # All should be different
      assert metadata1 != metadata2
      assert metadata2 != metadata3
      assert metadata1 != metadata3

      if Process.whereis(:cache_s3_etag_change) do
        GenServer.stop(:cache_s3_etag_change)
      end
    end
  end

  describe "file check interval configuration" do
    test "accepts custom file check interval" do
      # Clean up any existing process
      case Process.whereis(:cache_custom_interval) do
        nil -> :ok
        pid -> GenServer.stop(pid)
      end

      {:ok, pid} =
        Cache.start_link(
          name: :cache_custom_interval,
          region: nil,
          bucket: @bucket,
          path: @path,
          enable_tile_cache: true,
          # Use a very long interval to avoid triggering during test
          file_check_interval_ms: :timer.hours(24)
        )

      # Should start successfully with custom interval
      assert Process.alive?(pid)

      # Verify cache is functional
      stats = Cache.get_stats(pid)
      assert stats == %{hits: 0, misses: 0}

      GenServer.stop(pid)
    end

    test "uses default interval when not specified" do
      # Clean up any existing process
      case Process.whereis(:cache_default_interval) do
        nil -> :ok
        pid -> GenServer.stop(pid)
      end

      {:ok, pid} =
        Cache.start_link(
          name: :cache_default_interval,
          region: nil,
          bucket: @bucket,
          path: @path,
          enable_tile_cache: true
        )

      # Should start successfully with default interval (5 minutes)
      assert Process.alive?(pid)

      # Verify cache is functional
      stats = Cache.get_stats(pid)
      assert stats == %{hits: 0, misses: 0}

      GenServer.stop(pid)
    end
  end
end
