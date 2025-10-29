defmodule ExPmtiles.Cache.FileHandlerTest do
  use ExUnit.Case, async: false

  alias ExPmtiles.Cache.FileHandler

  @test_cache_dir Path.join(System.tmp_dir!(), "ex_pmtiles_test_file_handler")

  setup do
    # Clean up test directory before each test
    File.rm_rf(@test_cache_dir)
    File.mkdir_p!(@test_cache_dir)

    on_exit(fn ->
      File.rm_rf(@test_cache_dir)
    end)

    :ok
  end

  describe "tile_cache_file_path/2" do
    test "generates correct file path for tile" do
      tile_id = 12_345
      expected = Path.join([@test_cache_dir, "tiles", "12345.bin"])

      assert FileHandler.tile_cache_file_path(@test_cache_dir, tile_id) == expected
    end

    test "handles different tile IDs" do
      assert FileHandler.tile_cache_file_path(@test_cache_dir, 0) ==
               Path.join([@test_cache_dir, "tiles", "0.bin"])

      assert FileHandler.tile_cache_file_path(@test_cache_dir, 999_999) ==
               Path.join([@test_cache_dir, "tiles", "999999.bin"])
    end
  end

  describe "write_tile_to_cache/2" do
    test "writes tile data to cache file" do
      tile_path = Path.join([@test_cache_dir, "tiles", "test.bin"])
      tile_data = "test_tile_data"

      assert FileHandler.write_tile_to_cache(tile_path, tile_data) == :ok
      assert File.exists?(tile_path)
      assert File.read!(tile_path) == tile_data
    end

    test "creates directory structure if missing" do
      tile_path = Path.join([@test_cache_dir, "tiles", "nested", "test.bin"])
      tile_data = "test_data"

      assert FileHandler.write_tile_to_cache(tile_path, tile_data) == :ok
      assert File.exists?(tile_path)
      assert File.read!(tile_path) == tile_data
    end

    test "handles binary data correctly" do
      tile_path = Path.join([@test_cache_dir, "tiles", "binary.bin"])
      tile_data = <<1, 2, 3, 4, 5>>

      assert FileHandler.write_tile_to_cache(tile_path, tile_data) == :ok
      assert File.read!(tile_path) == tile_data
    end

    test "overwrites existing file" do
      tile_path = Path.join([@test_cache_dir, "tiles", "overwrite.bin"])

      FileHandler.write_tile_to_cache(tile_path, "first")
      assert File.read!(tile_path) == "first"

      FileHandler.write_tile_to_cache(tile_path, "second")
      assert File.read!(tile_path) == "second"
    end
  end

  describe "read_tile_from_cache/1" do
    test "reads existing tile from cache" do
      tile_path = Path.join([@test_cache_dir, "tiles", "read_test.bin"])
      tile_data = "cached_tile_data"

      File.mkdir_p!(Path.dirname(tile_path))
      File.write!(tile_path, tile_data)

      assert FileHandler.read_tile_from_cache(tile_path) == {:ok, tile_data}
    end

    test "returns error for non-existent file" do
      tile_path = Path.join([@test_cache_dir, "tiles", "nonexistent.bin"])

      assert FileHandler.read_tile_from_cache(tile_path) == :error
    end

    test "handles binary data correctly" do
      tile_path = Path.join([@test_cache_dir, "tiles", "binary_read.bin"])
      tile_data = <<255, 254, 253, 252>>

      File.mkdir_p!(Path.dirname(tile_path))
      File.write!(tile_path, tile_data)

      assert FileHandler.read_tile_from_cache(tile_path) == {:ok, tile_data}
    end

    test "returns error for corrupted file" do
      tile_path = Path.join([@test_cache_dir, "tiles", "corrupted.bin"])

      # Create a directory instead of a file to simulate corruption
      File.mkdir_p!(tile_path)

      assert FileHandler.read_tile_from_cache(tile_path) == :error
    end
  end

  describe "init_cache_directories/3" do
    test "creates tile cache directory when enabled" do
      cache_path = Path.join(@test_cache_dir, "init_test_tiles")

      FileHandler.init_cache_directories(cache_path, true, false)

      assert File.exists?(Path.join(cache_path, "tiles"))
      refute File.exists?(Path.join(cache_path, "directories"))
    end

    test "creates directory cache directory when enabled" do
      cache_path = Path.join(@test_cache_dir, "init_test_dirs")

      FileHandler.init_cache_directories(cache_path, false, true)

      refute File.exists?(Path.join(cache_path, "tiles"))
      assert File.exists?(Path.join(cache_path, "directories"))
    end

    test "creates both directories when both enabled" do
      cache_path = Path.join(@test_cache_dir, "init_test_both")

      FileHandler.init_cache_directories(cache_path, true, true)

      assert File.exists?(Path.join(cache_path, "tiles"))
      assert File.exists?(Path.join(cache_path, "directories"))
    end

    test "creates nothing when both disabled" do
      cache_path = Path.join(@test_cache_dir, "init_test_none")

      FileHandler.init_cache_directories(cache_path, false, false)

      refute File.exists?(cache_path)
    end
  end

  describe "clear_tile_cache/1" do
    test "clears all tiles from cache" do
      cache_path = Path.join(@test_cache_dir, "clear_tiles_test")
      tiles_dir = Path.join(cache_path, "tiles")

      # Create some tile files
      File.mkdir_p!(tiles_dir)
      File.write!(Path.join(tiles_dir, "tile1.bin"), "data1")
      File.write!(Path.join(tiles_dir, "tile2.bin"), "data2")
      File.write!(Path.join(tiles_dir, "tile3.bin"), "data3")

      assert length(File.ls!(tiles_dir)) == 3

      FileHandler.clear_tile_cache(cache_path)

      # Directory should exist but be empty
      assert File.exists?(tiles_dir)
      assert File.ls!(tiles_dir) == []
    end

    test "recreates directory after clearing" do
      cache_path = Path.join(@test_cache_dir, "recreate_test")
      tiles_dir = Path.join(cache_path, "tiles")

      File.mkdir_p!(tiles_dir)
      File.write!(Path.join(tiles_dir, "tile.bin"), "data")

      FileHandler.clear_tile_cache(cache_path)

      # Should be able to write immediately after clearing
      assert FileHandler.write_tile_to_cache(Path.join(tiles_dir, "new_tile.bin"), "new_data") ==
               :ok
    end
  end

  describe "clear_directory_cache/1" do
    test "clears all directories from cache" do
      cache_path = Path.join(@test_cache_dir, "clear_dirs_test")
      dirs_dir = Path.join(cache_path, "directories")

      # Create some directory files
      File.mkdir_p!(dirs_dir)
      File.write!(Path.join(dirs_dir, "dir1.bin"), "data1")
      File.write!(Path.join(dirs_dir, "dir2.bin"), "data2")

      assert length(File.ls!(dirs_dir)) == 2

      FileHandler.clear_directory_cache(cache_path)

      # Directory should exist but be empty
      assert File.exists?(dirs_dir)
      assert File.ls!(dirs_dir) == []
    end

    test "recreates directory after clearing" do
      cache_path = Path.join(@test_cache_dir, "recreate_dirs_test")
      dirs_dir = Path.join(cache_path, "directories")

      File.mkdir_p!(dirs_dir)
      File.write!(Path.join(dirs_dir, "dir.bin"), "data")

      FileHandler.clear_directory_cache(cache_path)

      # Directory should be recreated
      assert File.exists?(dirs_dir)
      assert File.ls!(dirs_dir) == []
    end
  end

  describe "clear_cache_files/1" do
    test "clears both tile and directory caches" do
      cache_path = Path.join(@test_cache_dir, "clear_all_test")
      tiles_dir = Path.join(cache_path, "tiles")
      dirs_dir = Path.join(cache_path, "directories")

      # Create files in both directories
      File.mkdir_p!(tiles_dir)
      File.mkdir_p!(dirs_dir)
      File.write!(Path.join(tiles_dir, "tile.bin"), "tile_data")
      File.write!(Path.join(dirs_dir, "dir.bin"), "dir_data")

      FileHandler.clear_cache_files(cache_path)

      # Both directories should exist but be empty
      assert File.exists?(tiles_dir)
      assert File.exists?(dirs_dir)
      assert File.ls!(tiles_dir) == []
      assert File.ls!(dirs_dir) == []
    end

    test "handles nil cache path gracefully" do
      # Should not raise an error
      assert FileHandler.clear_cache_files(nil) == nil
    end
  end

  describe "concurrent operations" do
    test "handles concurrent writes to same tile gracefully" do
      cache_path = Path.join(@test_cache_dir, "concurrent_write")
      tile_path = Path.join([cache_path, "tiles", "concurrent.bin"])

      tasks =
        for i <- 1..10 do
          Task.async(fn ->
            FileHandler.write_tile_to_cache(tile_path, "data_#{i}")
          end)
        end

      results = Task.await_many(tasks, 5000)

      # All writes should succeed
      assert Enum.all?(results, fn result -> result == :ok end)

      # File should exist with one of the data values
      assert File.exists?(tile_path)
      content = File.read!(tile_path)
      assert String.starts_with?(content, "data_")
    end

    test "handles concurrent reads safely" do
      cache_path = Path.join(@test_cache_dir, "concurrent_read")
      tile_path = Path.join([cache_path, "tiles", "shared.bin"])
      expected_data = "shared_tile_data"

      # Write initial data
      FileHandler.write_tile_to_cache(tile_path, expected_data)

      # Launch concurrent reads
      tasks =
        for _ <- 1..20 do
          Task.async(fn ->
            FileHandler.read_tile_from_cache(tile_path)
          end)
        end

      results = Task.await_many(tasks, 5000)

      # All reads should succeed with correct data
      assert Enum.all?(results, fn result -> result == {:ok, expected_data} end)
    end
  end
end
