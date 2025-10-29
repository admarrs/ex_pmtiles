# Run with: mix run bench/deserialize_directory_bench.exs

# Helper to create a serialized directory with N entries
defmodule BenchHelper do
  def create_test_directory(num_entries) do
    # Simulate a PMTiles directory buffer
    # Format: num_entries (varint) + tile_ids + run_lengths + lengths + offsets

    # Write num_entries as varint
    buf = write_varint(num_entries)

    # First pass: tile_ids (write deltas)
    buf =
      Enum.reduce(1..num_entries, buf, fn _i, acc ->
        # Delta from last tile_id (1 for simplicity)
        acc <> write_varint(1)
      end)

    # Second pass: run_lengths
    buf =
      Enum.reduce(1..num_entries, buf, fn _i, acc ->
        acc <> write_varint(1)
      end)

    # Third pass: lengths
    buf =
      Enum.reduce(1..num_entries, buf, fn _i, acc ->
        acc <> write_varint(256)
      end)

    # Fourth pass: offsets (write 0 to use prev_offset + prev_length pattern)
    buf =
      Enum.reduce(1..num_entries, buf, fn i, acc ->
        if i == 1 do
          # First offset is 0 (encoded as 1)
          acc <> write_varint(1)
        else
          # 0 means use previous offset + length
          acc <> write_varint(0)
        end
      end)

    buf
  end

  # Simple varint encoder for testing
  defp write_varint(n) when n < 128 do
    <<n::8>>
  end

  defp write_varint(n) do
    <<Bitwise.bor(Bitwise.band(n, 0x7F), 0x80)::8>> <>
      write_varint(Bitwise.bsr(n, 7))
  end
end

# Run benchmarks
IO.puts("Benchmarking deserialize_directory/1 performance\n")

Benchee.run(
  %{
    "100 entries" => fn -> ExPmtiles.deserialize_directory(BenchHelper.create_test_directory(100)) end,
    "500 entries" => fn -> ExPmtiles.deserialize_directory(BenchHelper.create_test_directory(500)) end,
    "1,000 entries" => fn -> ExPmtiles.deserialize_directory(BenchHelper.create_test_directory(1_000)) end,
    "5,000 entries" => fn -> ExPmtiles.deserialize_directory(BenchHelper.create_test_directory(5_000)) end,
    "10,000 entries" => fn -> ExPmtiles.deserialize_directory(BenchHelper.create_test_directory(10_000)) end,
    "25,000 entries" => fn -> ExPmtiles.deserialize_directory(BenchHelper.create_test_directory(25_000)) end
  },
  time: 5,
  memory_time: 2,
  warmup: 2,
  formatters: [
    Benchee.Formatters.Console
  ]
)

IO.puts("\n=== Complexity Analysis ===")
IO.puts("For O(n) complexity, the time should scale linearly with the number of entries.")
IO.puts("For O(n²) complexity, the time would scale quadratically (100x entries = 10,000x time).")
IO.puts("\nIf 10,000 entries takes ~10x longer than 1,000 entries, we have O(n) complexity ✓")
IO.puts("If 10,000 entries takes ~100x longer than 1,000 entries, we have O(n²) complexity ✗")
