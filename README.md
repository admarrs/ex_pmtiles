# ExPmtiles

An Elixir library for working with PMTiles files - a single-file format for storing tiled map data.

## Overview

PMTiles is an efficient single-file format for storing tiled map data, designed for cloud storage and CDN delivery. This library provides a complete Elixir implementation for reading and accessing tiles from PMTiles files stored either locally or on Amazon S3.

## Features

- **Multi-storage support**: Read PMTiles files from local storage or Amazon S3
- **Efficient caching**: Multi-level caching system with directory and tile caching
- **Concurrent access**: Safe concurrent access with request deduplication
- **Compression support**: Built-in support for gzip compression
- **Tile type support**: MVT, PNG, JPEG, WebP, and AVIF tile formats
- **Performance optimized**: Background directory pre-loading and persistent cache storage
- **Production ready**: Configurable timeouts, connection pooling, and error handling

## Installation

Add `ex_pmtiles` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:ex_pmtiles, "~> 0.1.4"}
  ]
end
```

## Quick Start

### Basic Usage

```elixir
# Open a local PMTiles file
instance = ExPmtiles.new("path/to/file.pmtiles", :local)

# Open a PMTiles file from S3
instance = ExPmtiles.new("my-bucket", "path/to/file.pmtiles", :s3)

# Get a tile by coordinates
case ExPmtiles.get_zxy(instance, 10, 512, 256) do
  {{offset, length, data}, updated_instance} ->
    # Use the tile data
    data
  {nil, updated_instance} ->
    # Tile not found
    nil
end
```

### Using the Cache

For production applications, use the caching layer for better performance:

```elixir
# Start the cache for an S3 PMTiles file
{:ok, cache_pid} = ExPmtiles.Cache.start_link(
  name: :cache_one,
  bucket: "maps", 
  path: "world.pmtiles",
  max_entries: 100_000
)

# Get tiles with automatic caching
case ExPmtiles.Cache.get_tile(:cache_one, 10, 512, 256) do
  {:ok, tile_data} ->
    # Handle tile data
    tile_data
  {:error, reason} ->
    # Handle error
    nil
end

# Get cache statistics
stats = ExPmtiles.Cache.get_stats(:cache_one)
# Returns: %{hits: 150, misses: 25}
```

## API Reference

### Core Functions

#### `ExPmtiles.new/2` and `ExPmtiles.new/4`

Create a new PMTiles instance:

```elixir
# Local file
instance = ExPmtiles.new("data/world.pmtiles", :local)

# S3 file
instance = ExPmtiles.new("my-bucket", "maps/world.pmtiles", :s3)
```

#### `ExPmtiles.get_zxy/4`

Get a tile by zoom level and coordinates:

```elixir
case ExPmtiles.get_zxy(instance, 10, 512, 256) do
  {{offset, length, data}, updated_instance} ->
    # Tile found
    data
  {nil, updated_instance} ->
    # Tile not found
    nil
end
```

#### `ExPmtiles.zxy_to_tile_id/3` and `ExPmtiles.tile_id_to_zxy/1`

Convert between coordinates and tile IDs:

```elixir
# Convert coordinates to tile ID
tile_id = ExPmtiles.zxy_to_tile_id(10, 512, 256)

# Convert tile ID back to coordinates
{z, x, y} = ExPmtiles.tile_id_to_zxy(tile_id)
```

### Cache Functions

#### `ExPmtiles.Cache.start_link/1`

Start a cache process:

```elixir
{:ok, pid} = ExPmtiles.Cache.start_link(
  bucket: "maps",
  path: "world.pmtiles",
  max_entries: 100_000
)
```

#### `ExPmtiles.Cache.get_tile/4`

Get a tile with caching:

```elixir
case ExPmtiles.Cache.get_tile(pid, 10, 512, 256) do
  {:ok, data} -> data
  {:error, reason} -> nil
end
```

## Configuration

### S3 Configuration

For S3 access, ensure you have ExAws configured in your application:

```elixir
# In config/config.exs
config :ex_aws,
  access_key_id: {:system, "AWS_ACCESS_KEY_ID"},
  secret_access_key: {:system, "AWS_SECRET_ACCESS_KEY"},
  region: "us-east-1"
```

### Cache Configuration

Configure cache behavior:

```elixir
# In config/config.exs
config :ex_pmtiles,
  max_entries: 100_000,  # Maximum tiles to cache
  cache_dir: "priv/pmtiles_cache"  # Directory for persistent cache
```

## Performance Features

### Multi-Level Caching

The library implements a sophisticated caching system:

1. **Directory Cache**: Caches deserialized PMTiles directory structures
2. **Tile Cache**: Caches individual tile data with LRU eviction
3. **Persistent Cache**: Saves directories to disk for faster restarts

### Background Pre-loading

The cache automatically pre-loads directories for zoom levels 0-4 in the background, improving response times for common zoom levels.

### Concurrent Request Handling

Multiple processes can safely access the same cache without duplicate requests. The system coordinates requests and broadcasts results to all waiting processes.

## Supported Formats

### Compression Types
- `:none` - No compression
- `:gzip` - Gzip compression
- `:unknown` - Unknown compression type

### Tile Types
- `:mvt` - Mapbox Vector Tiles
- `:png` - PNG images
- `:jpg` - JPEG images
- `:webp` - WebP images
- `:avif` - AVIF images

## Testing

The library includes test support with mock implementations:

```elixir
# In test/test_helper.exs
Mox.defmock(ExPmtiles.CacheMock, for: ExPmtiles.Behaviour)

# Mock implementations are automatically configured for test environment
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass
6. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Documentation

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc) and published on [HexDocs](https://hexdocs.pm). Once published, the docs can be found at <https://hexdocs.pm/ex_pmtiles>.

