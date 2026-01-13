#!/usr/bin/env elixir

# to run > export $(cat .env | xargs) && elixir test_s3.exs

Mix.install([
  {:req, "~> 0.5.16"},
  {:pythonx, "~> 0.4"},
  {:ex_pmtiles, path: "."} # Add this line to include your library
])


Req
