#!/usr/bin/env elixir

# Example demonstrating that local file caching now works without AWS credentials
# Usage: elixir examples/test_local_cache.exs

# Note: This example requires a local PMTiles file to test with
# You can download one from: https://pmtiles.io/

Mix.install([
  {:ex_pmtiles, path: "."}
])

defmodule LocalCacheExample do
  def run do
    # Replace this with the path to your local PMTiles file
    local_path = "path/to/your/file.pmtiles"

    IO.puts("Starting local file cache example...")
    IO.puts("Local file: #{local_path}")

    # Start the cache for a local PMTiles file
    # Note: No AWS credentials needed!
    {:ok, pid} =
      ExPmtiles.Cache.start_link(
        bucket: nil,
        path: local_path,
        enable_dir_cache: true,
        enable_tile_cache: true
      )

    IO.puts("Cache started successfully: #{inspect(pid)}")

    # Try to get a tile (adjust z/x/y to valid coordinates for your file)
    case ExPmtiles.Cache.get_tile(pid, 0, 0, 0) do
      {:ok, tile_data} ->
        IO.puts("Successfully retrieved tile (#{byte_size(tile_data)} bytes)")

      {:error, reason} ->
        IO.puts("Failed to retrieve tile: #{inspect(reason)}")

      nil ->
        IO.puts("Tile not found")
    end

    # Get cache statistics
    stats = ExPmtiles.Cache.get_stats(pid)
    IO.puts("Cache stats: #{inspect(stats)}")

    IO.puts("Done!")
  end
end

# Uncomment the following line if you have a local PMTiles file to test with
# LocalCacheExample.run()

IO.puts("""
This example demonstrates using ExPmtiles.Cache with local files.

To run this example:
1. Download a PMTiles file (e.g., from https://pmtiles.io/)
2. Update the `local_path` variable in this script
3. Uncomment the last line: LocalCacheExample.run()
4. Run: elixir examples/test_local_cache.exs

Key points:
- No AWS credentials required for local files
- Set bucket: nil for local storage
- Storage type is auto-detected based on bucket value
- File change detection works via mtime for local files
""")
