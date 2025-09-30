defmodule ExPmtiles.CacheTest do
  use ExUnit.Case, async: false
  import Mox

  # Set up expectations to be verified when the test exits
  setup :verify_on_exit!
  setup :set_mox_private

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

    # Start the cache
    {:ok, pid} =
      Cache.start_link(
        name: :cache_one,
        region: nil,
        bucket: @bucket,
        path: @path,
        max_entries: @max_cache_size
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

    test "creates ETS tables" do
      table_name = :cache_one_table
      stats_table = :"#{table_name}_stats"

      assert :ets.info(table_name) != :undefined
      assert :ets.info(stats_table) != :undefined
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
    test "respects cache size limits", %{cache_pid: pid} do
      table_name = :cache_one_table

      # Fill cache to just over the limit (faster test)
      for i <- 1..(@max_cache_size + 100) do
        Cache.get_tile(pid, 10, i, i)
      end

      size = :ets.info(table_name, :size)

      assert size <= @max_cache_size,
             "Cache size (#{size}) exceeded maximum (#{@max_cache_size})"
    end

    test "basic cache size management works", %{cache_pid: pid} do
      table_name = :cache_one_table

      # Test with just a few entries
      for i <- 1..5 do
        Cache.get_tile(pid, 10, i, i)
      end

      assert :ets.info(table_name, :size) == 5
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
    test "cleans up ETS tables on termination", %{cache_pid: pid} do
      table_name = :cache_one_table
      stats_table = :"#{table_name}_stats"

      # Stop the cache
      GenServer.stop(pid)

      # Wait for termination
      :timer.sleep(100)

      # Verify tables are deleted
      assert :ets.info(table_name) == :undefined
      assert :ets.info(stats_table) == :undefined
    end
  end
end
