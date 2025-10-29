defmodule ExPmtiles.PerformanceTest do
  use ExUnit.Case, async: true

  @moduletag :performance

  # Helper to create a serialized directory with N entries
  defp create_test_directory(num_entries) do
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
    <<Bitwise.bor(Bitwise.band(n, 0x7F), 0x80)::8>> <> write_varint(Bitwise.bsr(n, 7))
  end

  describe "deserialize_directory/1 performance" do
    test "scales linearly with number of entries" do
      # Test with different sizes
      sizes = [100, 500, 1000, 5000]

      results =
        Enum.map(sizes, fn size ->
          buf = create_test_directory(size)

          {time_us, _result} =
            :timer.tc(fn ->
              ExPmtiles.deserialize_directory(buf)
            end)

          {size, time_us}
        end)

      # Verify results are returned
      Enum.each(results, fn {size, time_us} ->
        IO.puts(
          "Size: #{size} entries, Time: #{time_us} μs (#{Float.round(time_us / 1000, 2)} ms)"
        )
      end)

      # Calculate time per entry for each size
      time_per_entry =
        Enum.map(results, fn {size, time_us} ->
          time_us / size
        end)

      # For O(n) complexity, time per entry should be roughly constant
      # For O(n²) complexity, time per entry would grow linearly with size

      # Check that the time per entry doesn't grow more than 3x
      # (allowing some overhead for smaller sizes)
      min_time_per_entry = Enum.min(time_per_entry)
      max_time_per_entry = Enum.max(time_per_entry)

      ratio = max_time_per_entry / min_time_per_entry

      IO.puts("\nTime per entry analysis:")
      IO.puts("Min: #{Float.round(min_time_per_entry, 2)} μs/entry")
      IO.puts("Max: #{Float.round(max_time_per_entry, 2)} μs/entry")
      IO.puts("Ratio: #{Float.round(ratio, 2)}x")

      # If this were O(n²), the largest size would take ~50x longer per entry than smallest
      # For O(n), ratio should be < 5x (allowing for some variance and overhead)
      assert ratio < 5.0, """
      Performance does not scale linearly!
      Time per entry ratio: #{Float.round(ratio, 2)}x
      This suggests O(n²) or worse complexity.
      Expected ratio < 5.0 for O(n) complexity.
      """
    end

    test "deserializes large directory in reasonable time" do
      # Test with a large directory (10,000 entries)
      # This should complete quickly if O(n), but would timeout if O(n²)
      buf = create_test_directory(10_000)

      {time_us, result} =
        :timer.tc(fn ->
          ExPmtiles.deserialize_directory(buf)
        end)

      time_ms = time_us / 1000

      IO.puts("\nLarge directory test:")
      IO.puts("Entries: 10,000")
      IO.puts("Time: #{Float.round(time_ms, 2)} ms")
      IO.puts("Time per entry: #{Float.round(time_us / 10_000, 2)} μs")

      # Verify the result is correct
      assert length(result) == 10_000

      # Should complete in under 500ms for O(n) complexity
      # With O(n²) complexity, this would take many seconds
      assert time_ms < 500, """
      Deserializing 10,000 entries took #{Float.round(time_ms, 2)} ms.
      Expected < 500 ms for O(n) complexity.
      This suggests a performance regression.
      """
    end

    test "correctly deserializes directory structure" do
      # Verify the deserialization is actually correct
      buf = create_test_directory(10)
      result = ExPmtiles.deserialize_directory(buf)

      assert length(result) == 10

      # Verify structure
      Enum.each(Enum.with_index(result), fn {entry, i} ->
        assert entry.tile_id == i + 1
        assert entry.run_length == 1
        assert entry.length == 256

        # First entry has offset 0, rest use previous offset + length
        expected_offset = i * 256
        assert entry.offset == expected_offset
      end)
    end
  end
end
