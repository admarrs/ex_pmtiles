defmodule ExPmtiles.Storage do
  @moduledoc """
  Storage adapter for PMTiles files, supporting both S3 and local file storage.

  This module provides a unified interface for reading bytes from PMTiles files
  regardless of whether they are stored locally or in Amazon S3. It handles
  the different access patterns and error conditions for each storage type.

  ## Storage Types

  ### S3 Storage
  - Uses ExAws for S3 access
  - Supports range requests for efficient partial file reading
  - Configurable timeouts and connection pooling
  - Automatic error handling and logging

  ### Local Storage
  - Direct file system access
  - Binary file reading with offset support
  - Automatic file handle management
  - Graceful error handling for missing files

  ## Usage

  ```elixir
  # The module is used internally by ExPmtiles
  # You typically don't call these functions directly

  # For S3 files
  instance = %ExPmtiles{source: :s3, bucket: "my-bucket", path: "file.pmtiles"}
  data = ExPmtiles.Storage.get_bytes(instance, 0, 1024)

  # For local files
  instance = %ExPmtiles{source: :local, path: "/path/to/file.pmtiles"}
  data = ExPmtiles.Storage.get_bytes(instance, 0, 1024)
  ```

  ## Configuration

  S3 requests use the following default configuration:
  - Receive timeout: 30 seconds
  - Connection pool: `:s3_pool`
  - Range request support for partial reads
  """

  require Logger

  @doc """
  Retrieves file metadata for change detection.

  For S3 files, this returns the ETag which changes when the file is updated.
  For local files, this returns the last modified timestamp.

  ## Parameters

  - `instance` - The PMTiles instance containing storage configuration
  - `config` - Optional ExAws config (primarily for testing with Bypass)

  ## Returns

  - `{:ok, metadata}` - A string representing the file's current state (ETag or timestamp)
  - `{:error, reason}` - If metadata cannot be retrieved

  ## Examples

      iex> instance = %ExPmtiles{source: :s3, bucket: "bucket", path: "file.pmtiles"}
      iex> ExPmtiles.Storage.get_file_metadata(instance)
      {:ok, "\"abc123def456\""}

      iex> instance = %ExPmtiles{source: :local, path: "test.pmtiles"}
      iex> ExPmtiles.Storage.get_file_metadata(instance)
      {:ok, "1702838400"}
  """
  def get_file_metadata(instance, config \\ nil)

  def get_file_metadata(%{source: source} = instance, config) do
    case source do
      :s3 -> get_s3_metadata(instance, config)
      :local -> get_local_metadata(instance)
    end
  end

  def get_file_metadata(%{storage: storage} = instance, config) when storage == :s3 do
    get_s3_metadata(instance, config)
  end

  def get_file_metadata(%{storage: storage} = instance, _config) when storage == :local do
    get_local_metadata(instance)
  end

  def get_file_metadata(_instance, _config) do
    # For test mocks or instances without source/storage field, return error
    {:error, :unsupported_instance}
  end

  defp get_s3_metadata(instance, config) do
    timeout = Application.get_env(:ex_pmtiles, :http_timeout, 15_000)

    try do
      result =
        if config do
          # Use provided config (for testing)
          ExAws.S3.head_object(instance.bucket, instance.path)
          |> ExAws.request(config)
        else
          # Use default config
          ExAws.S3.head_object(instance.bucket, instance.path)
          |> request(instance.region, timeout)
        end

      case result do
        {:ok, %{headers: headers}} ->
          # Extract ETag from headers
          etag =
            headers
            |> Enum.find_value(fn
              {"ETag", value} -> value
              {"etag", value} -> value
              _ -> nil
            end)

          if etag do
            {:ok, etag}
          else
            {:error, :etag_not_found}
          end

        {:error, error} ->
          {:error, error}
      end
    rescue
      e in ArgumentError ->
        # Handle cases where ExAws ETS tables don't exist (e.g., in tests)
        {:error, {:aws_not_available, Exception.message(e)}}

      e ->
        {:error, {:unexpected_error, Exception.message(e)}}
    catch
      # Catch exits from GenServer calls (e.g., ExAws.Config.AuthCache timeouts/crashes)
      :exit, reason ->
        {:error, {:aws_auth_failed, reason}}
    end
  end

  defp get_local_metadata(instance) do
    case File.stat(instance.path) do
      {:ok, %File.Stat{mtime: mtime}} ->
        # Convert mtime tuple to Unix timestamp for comparison
        timestamp =
          mtime
          |> :calendar.datetime_to_gregorian_seconds()
          |> Kernel.-(62_167_219_200)
          |> Integer.to_string()

        {:ok, timestamp}

      {:error, error} ->
        {:error, error}
    end
  end

  @doc """
  Retrieves a range of bytes from a PMTiles file.

  This function automatically routes to the appropriate storage backend based on
  the instance's source type. It provides a unified interface for reading file
  data regardless of storage location.

  ## Parameters

  - `instance` - The PMTiles instance containing storage configuration
  - `offset` - Byte offset in the file (0-based)
  - `length` - Number of bytes to read

  ## Returns

  - `binary()` - The requested bytes
  - `nil` - If the bytes cannot be read (file not found, network error, etc.)

  ## Examples

      iex> instance = %ExPmtiles{source: :local, path: "test.pmtiles"}
      iex> ExPmtiles.Storage.get_bytes(instance, 0, 1024)
      <<...>>

      iex> instance = %ExPmtiles{source: :s3, bucket: "bucket", path: "file.pmtiles"}
      iex> ExPmtiles.Storage.get_bytes(instance, 1024, 512)
      <<...>>
  """
  def get_bytes(instance, offset, length) do
    case instance.source do
      :s3 -> get_from_s3(instance, offset, length)
      :local -> get_from_local(instance, offset, length)
    end
  end

  defp get_from_s3(instance, offset, length) do
    # Use application config for timeout, default to 15s (reasonable for S3)
    timeout = Application.get_env(:ex_pmtiles, :http_timeout, 15_000)

    result =
      ExAws.S3.get_object(
        instance.bucket,
        instance.path,
        range: "bytes=#{offset}-#{offset + length - 1}"
      )
      |> request(instance.region, timeout)

    case result do
      {:ok, %{body: body}} ->
        body

      {:error, _error} ->
        nil
    end
  end

  defp request(op, nil, timeout) do
    ExAws.request(op,
      http_opts: [
        recv_timeout: timeout,
        connect_timeout: 5_000,
        hackney: [
          pool: :s3_pool,
          # Enable TCP keepalive for long-lived connections
          tcp_options: [:inet6, {:keepalive, true}]
        ]
      ]
    )
  end

  defp request(op, region, timeout) do
    ExAws.request(op,
      region: region,
      http_opts: [
        recv_timeout: timeout,
        connect_timeout: 5_000,
        hackney: [
          pool: :s3_pool,
          tcp_options: [:inet6, {:keepalive, true}]
        ]
      ]
    )
  end

  defp get_from_local(instance, offset, length) do
    path = instance.path

    case File.open(path, [:read, :binary]) do
      {:ok, file} ->
        try do
          :file.pread(file, offset, length)
          |> case do
            {:ok, data} -> data
            _ -> nil
          end
        after
          File.close(file)
        end

      _ ->
        nil
    end
  end
end
