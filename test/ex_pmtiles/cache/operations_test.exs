defmodule ExPmtiles.Cache.OperationsTest do
  use ExUnit.Case, async: false
  import Mox

  alias ExPmtiles.Cache.FileHandler
  alias ExPmtiles.Cache.Operations
  alias ExPmtiles.CacheMock

  setup :verify_on_exit!

  @test_cache_dir Path.join(System.tmp_dir!(), "ex_pmtiles_test_operations")

  setup do
    # Clean up test directory
    File.rm_rf(@test_cache_dir)
    File.mkdir_p!(@test_cache_dir)

    # Create test ETS tables
    stats_table = :test_stats_table
    pending_directories_table = :test_pending_directories

    # Clean up existing tables if they exist
    if :ets.info(stats_table) != :undefined, do: :ets.delete(stats_table)

    if :ets.info(pending_directories_table) != :undefined,
      do: :ets.delete(pending_directories_table)

    :ets.new(stats_table, [:set, :public, :named_table])
    :ets.insert(stats_table, {:hits, 0})
    :ets.insert(stats_table, {:misses, 0})

    :ets.new(pending_directories_table, [:set, :public, :named_table])

    # Set up mocks globally for this test module
    Mox.set_mox_global()

    stub(CacheMock, :new, fn region, bucket, path, storage ->
      %{region: region, bucket: bucket, path: path, storage: storage}
    end)

    stub(CacheMock, :get_zxy, fn pmtiles, z, x, y ->
      {{0, 100, "tile_data_#{z}_#{x}_#{y}"}, pmtiles}
    end)

    stub(CacheMock, :get_zxy, fn pmtiles, z, x, y, _cache_path, _pending_dirs ->
      {{0, 100, "tile_data_#{z}_#{x}_#{y}"}, pmtiles}
    end)

    stub(CacheMock, :zxy_to_tile_id, fn z, x, y ->
      z * 1_000_000 + x * 1000 + y
    end)

    # Create a mock PMTiles struct
    pmtiles = %{region: nil, bucket: "test", path: "test.pmtiles", storage: :s3}

    config = %{
      stats: stats_table,
      cache_path: @test_cache_dir,
      pending_directories: pending_directories_table
    }

    on_exit(fn ->
      File.rm_rf(@test_cache_dir)
      if :ets.info(stats_table) != :undefined, do: :ets.delete(stats_table)

      if :ets.info(pending_directories_table) != :undefined,
        do: :ets.delete(pending_directories_table)
    end)

    %{config: config, stats_table: stats_table, pmtiles: pmtiles}
  end

  describe "handle_cached_tile_request/6" do
    test "returns cached tile on cache hit", %{config: config, pmtiles: pmtiles} do
      tile_id = 10_001_001
      z = 10
      x = 1
      y = 1
      tile_data = "cached_tile_data"

      # Pre-populate cache
      tile_path = FileHandler.tile_cache_file_path(config.cache_path, tile_id)
      FileHandler.write_tile_to_cache(tile_path, tile_data)

      result =
        Operations.handle_cached_tile_request(pmtiles, tile_id, z, x, y, config)

      assert result == {:ok, tile_data}

      # Should increment hits
      [{:hits, hits}] = :ets.lookup(config.stats, :hits)
      assert hits == 1
    end

    test "fetches tile on cache miss", %{config: config, pmtiles: pmtiles} do
      tile_id = 10_002_002
      z = 10
      x = 2
      y = 2

      # Don't pre-populate cache

      result =
        Operations.handle_cached_tile_request(pmtiles, tile_id, z, x, y, config)

      # Should fetch from PMTiles and return data
      assert result == {:ok, "tile_data_#{z}_#{x}_#{y}"}

      # Should increment misses (not hits)
      [{:misses, misses}] = :ets.lookup(config.stats, :misses)
      assert misses == 1
    end
  end

  describe "handle_tile_request/7" do
    test "routes to cached handler when tile caching enabled", %{config: config, pmtiles: pmtiles} do
      tile_id = 20_005_005
      z = 20
      x = 5
      y = 5

      # Pre-populate cache
      tile_path = FileHandler.tile_cache_file_path(config.cache_path, tile_id)
      FileHandler.write_tile_to_cache(tile_path, "cached_data")

      result = Operations.handle_tile_request(pmtiles, tile_id, z, x, y, config, true)

      assert result == {:ok, "cached_data"}

      # Should use cached version (hit)
      [{:hits, hits}] = :ets.lookup(config.stats, :hits)
      assert hits == 1
    end

    test "fetches directly when tile caching disabled", %{config: config, pmtiles: pmtiles} do
      tile_id = 20_006_006
      z = 20
      x = 6
      y = 6

      result = Operations.handle_tile_request(pmtiles, tile_id, z, x, y, config, false)

      assert result == {:ok, "tile_data_#{z}_#{x}_#{y}"}

      # Should increment misses
      [{:misses, misses}] = :ets.lookup(config.stats, :misses)
      assert misses == 1
    end
  end
end
