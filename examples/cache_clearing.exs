#!/usr/bin/env elixir

# Example demonstrating cache clearing functionality
# This example shows how to:
# 1. Start a cache with automatic clearing after a certain age
# 2. Manually clear the cache
# 3. Monitor cache statistics

# Start a cache with automatic clearing every 24 hours
# When max_cache_age_ms is set:
# - Cache is cleared on GenServer startup (fresh start every time)
# - Periodic cleanup runs every hour to check if cache age exceeds 24 hours
# - You can restart the GenServer to force a cache clear
{:ok, cache_pid} =
  ExPmtiles.Cache.start_link(
    bucket: "my-bucket",
    path: "map.pmtiles",
    enable_tile_cache: true,
    enable_dir_cache: true,
    max_cache_age_ms: :timer.hours(24),
    cleanup_interval_ms: :timer.hours(1)
  )

# Get some tiles (this will populate the cache)
{:ok, _tile1} = ExPmtiles.Cache.get_tile(cache_pid, 10, 512, 256)
{:ok, _tile2} = ExPmtiles.Cache.get_tile(cache_pid, 10, 513, 256)

# Check cache statistics
stats = ExPmtiles.Cache.get_stats(cache_pid)
IO.puts("Cache stats: #{inspect(stats)}")
# => %{hits: 0, misses: 2}

# Get the same tiles again (should be cache hits)
{:ok, _tile1} = ExPmtiles.Cache.get_tile(cache_pid, 10, 512, 256)
{:ok, _tile2} = ExPmtiles.Cache.get_tile(cache_pid, 10, 513, 256)

stats = ExPmtiles.Cache.get_stats(cache_pid)
IO.puts("Cache stats after repeat requests: #{inspect(stats)}")
# => %{hits: 2, misses: 2}

# Manually clear the cache
# Note: When directory caching is enabled, this will automatically trigger
# a background task to repopulate the directory cache
:ok = ExPmtiles.Cache.clear_cache(cache_pid)

# Check stats after clearing (should be reset)
stats = ExPmtiles.Cache.get_stats(cache_pid)
IO.puts("Cache stats after manual clear: #{inspect(stats)}")
# => %{hits: 0, misses: 0}

IO.puts("Background directory cache population started (runs in background)...")

# Get tiles again (will be cache misses since we cleared)
{:ok, _tile1} = ExPmtiles.Cache.get_tile(cache_pid, 10, 512, 256)

stats = ExPmtiles.Cache.get_stats(cache_pid)
IO.puts("Cache stats after re-fetching: #{inspect(stats)}")
# => %{hits: 0, misses: 1}

IO.puts("\nCache clearing functionality working correctly!")
IO.puts(
  "Note: Automatic cache clearing will occur when the cache age exceeds max_cache_age_ms (24h in this example)"
)
