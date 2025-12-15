defmodule ExPmtiles.StorageTest do
  use ExUnit.Case, async: true

  import ExPmtile.Support.BypassHelpers

  alias ExPmtiles.Storage

  setup [:start_bypass]

  describe "get_file_metadata/1 S3 with Bypass" do
    test "returns ETag for successful S3 HEAD request", %{bypass: bypass} do
      # Mock S3 HEAD response with ETag
      Bypass.expect_once(bypass, "HEAD", "/test-bucket/test.pmtiles", fn conn ->
        conn
        |> Plug.Conn.put_resp_header("etag", "\"abc123def456\"")
        |> Plug.Conn.put_resp_header("content-length", "1024")
        |> Plug.Conn.resp(200, "")
      end)

      instance = %ExPmtiles{
        source: :s3,
        region: "us-east-1",
        bucket: "test-bucket",
        path: "test.pmtiles"
      }

      config = exaws_config_for_bypass(bypass)

      assert {:ok, etag} = Storage.get_file_metadata(instance, config)
      assert etag == "\"abc123def456\""
    end

    test "returns error when S3 HEAD request fails (403 Forbidden)", %{bypass: bypass} do
      Bypass.expect_once(bypass, "HEAD", "/test-bucket/test.pmtiles", fn conn ->
        conn
        |> Plug.Conn.put_resp_header("content-type", "application/xml")
        |> Plug.Conn.resp(403, """
        <?xml version="1.0" encoding="UTF-8"?>
        <Error>
          <Code>AccessDenied</Code>
          <Message>Access Denied</Message>
        </Error>
        """)
      end)

      instance = %ExPmtiles{
        source: :s3,
        region: "us-east-1",
        bucket: "test-bucket",
        path: "test.pmtiles"
      }

      config = exaws_config_for_bypass(bypass)

      assert {:error, {:http_error, 403, _response}} = Storage.get_file_metadata(instance, config)
    end

    test "returns error when S3 file does not exist (404)", %{bypass: bypass} do
      Bypass.expect_once(bypass, "HEAD", "/test-bucket/nonexistent.pmtiles", fn conn ->
        conn
        |> Plug.Conn.put_resp_header("content-type", "application/xml")
        |> Plug.Conn.resp(404, """
        <?xml version="1.0" encoding="UTF-8"?>
        <Error>
          <Code>NoSuchKey</Code>
          <Message>The specified key does not exist.</Message>
        </Error>
        """)
      end)

      instance = %ExPmtiles{
        source: :s3,
        region: "us-east-1",
        bucket: "test-bucket",
        path: "nonexistent.pmtiles"
      }

      config = exaws_config_for_bypass(bypass)

      assert {:error, {:http_error, 404, _response}} = Storage.get_file_metadata(instance, config)
    end

    test "handles missing ETag in response", %{bypass: bypass} do
      # Mock S3 HEAD response without ETag (unlikely but possible)
      Bypass.expect_once(bypass, "HEAD", "/test-bucket/test.pmtiles", fn conn ->
        conn
        |> Plug.Conn.put_resp_header("content-length", "1024")
        |> Plug.Conn.resp(200, "")
      end)

      instance = %ExPmtiles{
        source: :s3,
        region: "us-east-1",
        bucket: "test-bucket",
        path: "test.pmtiles"
      }

      config = exaws_config_for_bypass(bypass)

      assert {:error, :etag_not_found} = Storage.get_file_metadata(instance, config)
    end

    test "handles different ETag formats", %{bypass: bypass} do
      # Test with ETag in different case
      Bypass.expect_once(bypass, "HEAD", "/test-bucket/test.pmtiles", fn conn ->
        conn
        |> Plug.Conn.put_resp_header("ETag", "\"xyz789\"")
        |> Plug.Conn.resp(200, "")
      end)

      instance = %ExPmtiles{
        source: :s3,
        region: "us-east-1",
        bucket: "test-bucket",
        path: "test.pmtiles"
      }

      config = exaws_config_for_bypass(bypass)

      assert {:ok, etag} = Storage.get_file_metadata(instance, config)
      assert etag == "\"xyz789\""
    end
  end

  describe "get_file_metadata/1 S3 without credentials" do
    test "returns error for S3 instance without proper credentials" do
      instance = %ExPmtiles{
        source: :s3,
        region: "us-east-1",
        bucket: "test-bucket",
        path: "test.pmtiles"
      }

      # Without config and without valid AWS credentials, this should return an error
      assert {:error, _reason} = Storage.get_file_metadata(instance)
    end
  end

  describe "get_file_metadata/1 local files" do
    test "returns error for non-existent local file" do
      instance = %ExPmtiles{
        source: :local,
        bucket: nil,
        path: "/nonexistent/file.pmtiles"
      }

      assert {:error, :enoent} = Storage.get_file_metadata(instance)
    end

    test "returns timestamp for existing local file" do
      # Create a temporary file
      temp_file = Path.join(System.tmp_dir!(), "test_pmtiles_#{:rand.uniform(100_000)}.pmtiles")

      try do
        File.write!(temp_file, "test content")

        instance = %ExPmtiles{
          source: :local,
          bucket: nil,
          path: temp_file
        }

        assert {:ok, metadata} = Storage.get_file_metadata(instance)
        # Should be a string representation of a Unix timestamp
        assert is_binary(metadata)
        assert String.match?(metadata, ~r/^\d+$/)
      after
        File.rm(temp_file)
      end
    end

    test "returns different timestamps when file is modified" do
      temp_file = Path.join(System.tmp_dir!(), "test_pmtiles_#{:rand.uniform(100_000)}.pmtiles")

      try do
        File.write!(temp_file, "initial content")

        instance = %ExPmtiles{
          source: :local,
          bucket: nil,
          path: temp_file
        }

        {:ok, metadata1} = Storage.get_file_metadata(instance)

        # Wait a bit and modify the file
        Process.sleep(1100)
        File.write!(temp_file, "modified content")

        {:ok, metadata2} = Storage.get_file_metadata(instance)

        # Timestamps should be different
        assert metadata1 != metadata2
      after
        File.rm(temp_file)
      end
    end
  end

  describe "get_file_metadata/1 backwards compatibility" do
    test "handles instance with storage field instead of source" do
      instance = %{
        storage: :local,
        bucket: nil,
        path: "/nonexistent/file.pmtiles"
      }

      assert {:error, :enoent} = Storage.get_file_metadata(instance)
    end

    test "returns error for unsupported instance format" do
      instance = %{some_field: "value"}

      assert {:error, :unsupported_instance} = Storage.get_file_metadata(instance)
    end
  end
end
