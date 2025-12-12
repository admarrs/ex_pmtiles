defmodule ExPmtiles.MixProject do
  use Mix.Project

  @source_url "https://github.com/admarrs/ex_pmtiles"
  @version "0.3.1"

  def project do
    [
      app: :ex_pmtiles,
      version: @version,
      elixir: "~> 1.17",
      name: "ExPmtiles",
      source_url: @source_url,
      aliases: aliases(),
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
      mod: {ExPmtiles, []},
      extra_applications: [:logger, :hackney]
    ]
  end

  def cli do
    [
      preferred_envs: [precommit: :test]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:ex_aws, "~> 2.5"},
      {:ex_aws_s3, "~> 2.5"},
      {:hackney, "~> 1.18"},

      # test deps
      {:excoveralls, "~> 0.18", only: :test},
      {:mox, "~> 1.0", only: :test},
      {:benchee, "~> 1.0", only: [:dev, :test]},

      # dev & test deps
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.34", only: :dev, runtime: false, warn_if_outdated: true}
    ]
  end

  defp aliases do
    [
      precommit: ["compile --warning-as-errors", "deps.unlock --unused", "format", "test"]
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
