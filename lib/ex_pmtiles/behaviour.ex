defmodule ExPmtiles.Behaviour do
  @moduledoc """
  A behaviour module for PMTiles files. Primarily to support Mox testing.
  """
  # coveralls-ignore-start
  @callback new(String.t(), String.t(), atom()) :: map()
  @callback new(String.t(), atom()) :: map()
  @callback get_tile(map(), integer(), integer()) :: binary() | nil
  @callback get_zxy(map(), integer(), integer(), integer()) ::
              {integer(), integer(), binary()} | nil
  @callback zxy_to_tile_id(integer(), integer(), integer()) :: integer()
  @callback tile_id_to_zxy(integer()) :: {integer(), integer(), integer()}
  # coveralls-ignore-end
end
