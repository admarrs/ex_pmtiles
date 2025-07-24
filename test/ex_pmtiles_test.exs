defmodule ExPmtilesTest do
  use ExUnit.Case

  alias ExPmtiles

  @test_file "test/fixtures/test_fixture_1.pmtiles"

  setup do
    # Ensure we're using local storage for tests
    instance = ExPmtiles.new(@test_file, :local)
    {:ok, instance: instance}
  end

  test "deserialize_directory/1", %{instance: instance} do
    header = instance.header

    dir =
      ExPmtiles.deserialize_directory(
        ExPmtiles.decompress(
          ExPmtiles.get_bytes(instance, header.root_offset, header.root_length),
          header.internal_compression
        )
      )

    assert Enum.at(dir, 0) == %{offset: 0, length: 69, tile_id: 0, run_length: 1}
  end

  test "get_tile/4", %{instance: instance} do
    {{offset, length, tile_data}, instance} =
      ExPmtiles.get_zxy(instance, 0, 0, 0)

    assert tile_data != nil
    assert offset != nil
    assert length != nil

    assert :binary.part(
             ExPmtiles.decompress(
               tile_data,
               instance.header.internal_compression
             ),
             0,
             15
           ) ==
             "\x1A/x\x02\n\x15test_fixt"
  end

  describe "test util functions" do
    test "read_varint/1" do
      assert ExPmtiles.read_varint(<<0xE5, 0x16>>) == {2917, <<>>}

      assert ExPmtiles.read_varint(<<0x00, 0x01, 0x7F, 0xE5, 0x8E, 0x26>>) ==
               {0, <<0x01, 0x7F, 0xE5, 0x8E, 0x26>>}

      assert ExPmtiles.read_varint(<<0x01, 0x7F, 0xE5, 0x8E, 0x26>>) ==
               {1, <<0x7F, 0xE5, 0x8E, 0x26>>}

      assert ExPmtiles.read_varint(<<0x7F, 0xE5, 0x8E, 0x26>>) == {127, <<0xE5, 0x8E, 0x26>>}
      assert ExPmtiles.read_varint(<<0xE5, 0x8E, 0x26>>) == {624_485, <<>>}
    end
  end
end
