# ExPmtiles

An Elixir library for working with PMTiles files - a single-file format for storing tiled map data.

## Overview

PMTiles is an efficient single-file format for storing tiled map data, designed for cloud storage and CDN delivery. This library provides a complete Elixir implementation for reading and accessing tiles from PMTiles files stored either locally or on Amazon S3.

## Features

- **Multi-storage support**: Read PMTiles files from local storage or Amazon S3
- **Efficient caching**: File-based caching system with no size limits
- **Concurrent access**: Safe concurrent access with request deduplication
- **Compression support**: Built-in support for gzip compression
- **Tile type support**: MVT, PNG, JPEG, WebP, and AVIF tile formats
- **Performance optimized**: Background directory pre-loading and persistent file-based cache
- **Production ready**: Configurable timeouts, connection pooling, and error handling
- **Scalable**: No 2GB file size limit - cache grows with your data

## Installation

Add `ex_pmtiles` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:ex_pmtiles, "~> 0.2.4"}
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
  enable_dir_cache: true,    # Enable directory caching (default: false)
  enable_tile_cache: true    # Enable tile caching (default: false)
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
  enable_dir_cache: true,    # Optional: enable directory caching (default: false)
  enable_tile_cache: true    # Optional: enable tile caching (default: false)
  :max_cache_age_ms          # Maximum age of cache before automatic clearing (default: nil, disabled)
)
```

**Options:**
- `:bucket` - S3 bucket name (or `nil` for local files)
- `:path` - Path to the PMTiles file
- `:name` - Optional custom name for the cache process (atom)
- `:enable_dir_cache` - Enable file-based directory caching (default: false)
- `:enable_tile_cache` - Enable file-based tile caching (default: false)
- `:max_cache_age_ms` - Maximum age of cache before automatic clearing (default: nil, disabled)

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
  cache_dir: "priv/pmtiles_cache",  # Directory for file-based cache (default: System.tmp_dir!() <> "/ex_pmtiles_cache")
  http_pool_size: 100,               # Maximum HTTP connections for S3 (default: 100)
  http_timeout: 15_000               # HTTP timeout in milliseconds (default: 15_000)
```

### File-Based Cache Persistence

The cache persists directory structures and tiles to disk using individual files. This eliminates size limits and provides better scalability than traditional database approaches. The cache directory structure is:

```
cache_dir/
├── directories/    # Cached directory structures
│   └── 124044919597_120976.bin
└── tiles/          # Cached tile data
    └── 10512256.bin
```

**Enable/Disable Caching** (per-cache instance):

```elixir
# With both directory and tile caching enabled
{:ok, pid} = ExPmtiles.Cache.start_link(
  bucket: "maps",
  path: "world.pmtiles",
  enable_dir_cache: true,    # Enable directory caching
  enable_tile_cache: true    # Enable tile caching
)

# With only directory caching (recommended for most use cases)
{:ok, pid} = ExPmtiles.Cache.start_link(
  bucket: "maps",
  path: "world.pmtiles",
  enable_dir_cache: true,    # Enable directory caching
  enable_tile_cache: false   # Disable tile caching (default)
)

# Without any persistent caching - useful for ephemeral environments
{:ok, pid} = ExPmtiles.Cache.start_link(
  bucket: "maps",
  path: "world.pmtiles",
  enable_dir_cache: false,   # No directory caching
  enable_tile_cache: false   # No tile caching
)
```

**Configure cache storage location**:

```elixir
# In config/config.exs
config :ex_pmtiles,
  cache_dir: "/var/cache/pmtiles"  # Default: System.tmp_dir!() <> "/ex_pmtiles_cache"
```

**When to disable caching**:
- Ephemeral containers (Docker/Kubernetes) with no persistent volumes
- Serverless environments (AWS Lambda, etc.)
- Testing environments where you want a fresh cache each time
- When disk space is limited and you prefer to re-fetch data as needed

**Benefits of file-based caching**:
- **No size limits**: File-based cache can grow indefinitely without database size constraints
- **No corruption issues**: Individual files are atomically written, eliminating corruption problems
- **Better concurrency**: File system handles concurrent access naturally
- **Faster restarts**: Cached data persists across restarts, avoiding expensive S3 fetches

## Performance Features

### Multi-Level Caching

The library implements a sophisticated caching system:

1. **Directory Cache (File-based)**: Persistent cache - deserialized PMTiles directory structures saved as individual files on disk (optional, disabled by default)
2. **Tile Cache (File-based)**: Persistent cache - individual tile data saved as files on disk (optional, disabled by default)
3. **Request Coordination (ETS)**: In-memory coordination tables to prevent duplicate fetches for the same tile/directory

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

You can also test against a pmtiles file stored in an s3 bucket using `export $(cat .env | xargs) && elixir test_s3.exs`

This expects you to define environment variables in `.env`:
```
AWS_ACCESS_KEY_ID=<>
AWS_REGION=<>
AWS_SECRET_ACCESS_KEY=<>
BUCKET=<>
OBJECT=<>
```

### Performance Testing

The library includes performance tests to ensure O(n) linear complexity for directory deserialization:

```bash
# Run performance tests
mix test test/ex_pmtiles/performance_test.exs

# Run detailed benchmarks
mix run bench/deserialize_directory_bench.exs
```

The performance tests verify that:
- Directory deserialization scales linearly with the number of entries
- Large directories (10,000+ entries) deserialize in under 500ms
- Time per entry remains roughly constant regardless of directory size

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

