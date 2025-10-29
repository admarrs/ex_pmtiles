#!/usr/bin/env elixir

# to run > export $(cat .env | xargs) && elixir test_s3.exs

Mix.install([
  {:ex_aws, "~> 2.5"},
  {:ex_aws_s3, "~> 2.0"},
  {:hackney, "~> 1.9"},
  {:sweet_xml, "~> 0.6"},
  {:ex_pmtiles, path: "."} # Add this line to include your library
])

defmodule TestS3 do
  def run(bucket, path) do
    Application.put_env(:ex_aws, :access_key_id, System.get_env("AWS_ACCESS_KEY_ID"))
    Application.put_env(:ex_aws, :secret_access_key, System.get_env("AWS_SECRET_ACCESS_KEY"))
    Application.put_env(:ex_aws, :region, System.get_env("AWS_REGION"))

    {:ok, pid} = ExPmtiles.Cache.start_link(bucket: bucket, path: path)
    IO.puts("Cache started: #{inspect(pid)}\n")

    # Test configuration
    num_concurrent_requests = 5
    num_sequential_batches = 2

    IO.puts("=== Testing Concurrent Tile Requests ===")
    IO.puts("Concurrent requests per batch: #{num_concurrent_requests}")
    IO.puts("Number of batches: #{num_sequential_batches}\n")

    for batch <- 1..num_sequential_batches do
      IO.puts("\n--- Batch #{batch} ---")
      test_concurrent_requests(pid, num_concurrent_requests)
      Process.sleep(500)
    end

    # Show cache statistics
    IO.puts("\n=== Cache Statistics ===")
    stats = ExPmtiles.Cache.get_stats(pid)
    IO.puts("Hits: #{stats.hits}")
    IO.puts("Misses: #{stats.misses}")
    hit_rate = if stats.hits + stats.misses > 0 do
      Float.round(stats.hits / (stats.hits + stats.misses) * 100, 2)
    else
      0.0
    end
    IO.puts("Hit rate: #{hit_rate}%")
  end

  defp test_concurrent_requests(pid, count) do
    # Generate random tile coordinates
    tiles = generate_random_tiles(count)

    # Start all requests concurrently and measure individual timings
    start_time = System.monotonic_time(:millisecond)

    tasks =
      Enum.map(tiles, fn {z, x, y} ->
        Task.async(fn ->
          request_start = System.monotonic_time(:millisecond)
          result = ExPmtiles.Cache.get_tile(pid, z, x, y)
          request_end = System.monotonic_time(:millisecond)
          duration = request_end - request_start

          {z, x, y, duration, result}
        end)
      end)

    # Wait for all tasks to complete (2min timeout for initial directory fetches)
    results = Task.await_many(tasks, 120_000)
    total_time = System.monotonic_time(:millisecond) - start_time

    # Analyze results
    durations = Enum.map(results, fn {_, _, _, duration, _} -> duration end)
    min_time = Enum.min(durations)
    max_time = Enum.max(durations)
    avg_time = Float.round(Enum.sum(durations) / length(durations), 2)

    successes = Enum.count(results, fn {_, _, _, _, result} -> match?({:ok, _}, result) end)
    errors = Enum.count(results, fn {_, _, _, _, result} -> match?({:error, _}, result) end)

    IO.puts("Total wall time: #{total_time}ms")
    IO.puts("Individual request times: min=#{min_time}ms, max=#{max_time}ms, avg=#{avg_time}ms")
    IO.puts("Results: #{successes} successful, #{errors} errors")

    # Show individual timings if there's significant variance (possible blocking)
    if max_time > min_time * 2 do
      IO.puts("\n⚠️  WARNING: Significant variance detected (max > 2x min)")
      IO.puts("Individual request timings:")
      results
      |> Enum.sort_by(fn {_, _, _, duration, _} -> duration end, :desc)
      |> Enum.each(fn {z, x, y, duration, result} ->
        status = case result do
          {:ok, _} -> "✓"
          {:error, reason} -> "✗ #{reason}"
        end
        IO.puts("  #{z}/#{x}/#{y}: #{duration}ms #{status}")
      end)
    end
  end

  defp generate_random_tiles(count) do
    # Generate random tiles across various zoom levels
    # Using zoom levels 0-10 for reasonable variety
    Enum.map(1..count, fn _ ->
      z = Enum.random(0..10)
      max_coord = trunc(:math.pow(2, z)) - 1
      x = Enum.random(0..max_coord)
      y = Enum.random(0..max_coord)
      {z, x, y}
    end)
  end
end

TestS3.run(System.get_env("BUCKET"), System.get_env("OBJECT"))
