defmodule ExPmtile.Support.BypassHelpers do
  @moduledoc """
  Helper functions for testing S3 operations with Bypass.
  Based on ExAws.S3 test helpers.
  """

  def start_bypass(_context) do
    bypass = Bypass.open()
    [bypass: bypass]
  end

  def exaws_config_for_bypass(bypass) do
    ExAws.Config.new(:s3,
      access_key_id: "AKIAIOSFODNN7EXAMPLE",
      secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
      host: "localhost",
      port: bypass.port,
      scheme: "http://",
      region: "us-east-1"
    )
  end
end
