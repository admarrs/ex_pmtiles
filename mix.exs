defmodule ExPmtiles.MixProject do
  use Mix.Project

  @source_url "https://github.com/almarrs/ex_pmtiles"
  @version "0.1.0"

  def project do
    [
      app: :ex_pmtiles,
      version: "0.1.0",
      elixir: "~> 1.17",
      name: "ExPmtiles",
      source_url: @source_url,
      package: package(),
      deps: deps(),
      docs: docs(),
      start_permanent: Mix.env() == :prod,
      test_coverage: [tool: ExCoveralls],
      preferred_cli_env: [
        coveralls: :test,
        "coveralls.detail": :test,
        "coveralls.post": :test,
        "coveralls.html": :test,
        "coveralls.cobertura": :test
      ]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:ex_aws, "~> 2.0"},
      {:ex_aws_s3, "~> 2.0"},

      # test deps
      {:excoveralls, "~> 0.18", only: :test},
      {:mox, "~> 1.0", only: :test},

      # dev & test deps
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.34", only: :dev, runtime: false, warn_if_outdated: true}
    ]
  end

  defp package do
    [
      description: description(),
      files: ["lib", "mix.exs", "README*", "LICENSE", "CHANGELOG.md"],
      exclude_patterns: ["_build", "deps", "test", "*~"],
      maintainers: ["Alan Marrs"],
      licenses: ["MIT"],
      links: %{
        Changelog: "#{@source_url}/blob/master/CHANGELOG.md",
        GitHub: @source_url
      },
      exclude_patterns: [~r/.*~/]
    ]
  end

  defp description do
    """
    Elixir library for working with PMTiles files - a single-file format for storing tiled map data.
    """
  end

  defp docs do
    [
      main: "readme",
      source_ref: "v#{@version}",
      source_url: @source_url,
      extras: ["README.md"]
    ]
  end
end
