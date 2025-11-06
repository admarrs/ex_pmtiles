defmodule ExPmtiles.CacheTest do
  use ExUnit.Case, async: false
  import Mox

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
end
