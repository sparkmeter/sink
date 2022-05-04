defmodule Sink.MixProject do
  use Mix.Project

  def project do
    [
      app: :sink,
      version: "0.0.1",
      elixir: "~> 1.12",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases(),
      test_coverage: [tool: ExCoveralls],
      docs: docs(),
      preferred_cli_env: [
        coveralls: :test,
        "coveralls.detail": :test,
        "coveralls.post": :test,
        "coveralls.html": :test
      ]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      mod: {Sink.Application, []},
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:dialyxir, "~> 1.0", runtime: false, only: [:dev, :test]},
      {:excoveralls, "~> 0.10", only: :test},
      {:ex_doc, "~> 0.24", only: :dev, runtime: false},
      {:mox, "~> 1.0", only: :test},
      {:ranch, "~> 1.8"},
      {:stream_data, "~> 0.5", only: :test},
      {:telemetry, "~> 0.4 or ~> 1.0"},
      # Blocked by kayrock dependency in gladys
      {:varint, "~> 1.2.0"},
      {:x509, "~> 0.7"}
    ]
  end

  defp elixirc_paths(:test), do: ["test/support", "lib"]
  defp elixirc_paths(_), do: ["lib"]

  def aliases do
    [
      check: ["format", "dialyzer"],
      docs: ["docs", &move_coverage/1]
    ]
  end

  def docs do
    [
      name: "Sink",
      main: "main",
      api_reference: false,
      extra_section: "Information",
      groups_for_modules: groups_for_modules(),
      extras: extras(),
      groups_for_extras: groups_for_extras(),
      formatters: ["html"]
    ]
  end

  def extras do
    [
      "docs/main.md",
      "CHANGELOG.md": [title: "Changelog"]
    ]
  end

  def groups_for_modules do
    [
      Connection: ~r/^Sink\.Connection/,
      Example: ~r/^ExampleApp/
    ]
  end

  def groups_for_extras do
    []
  end

  def move_coverage(_) do
    File.cp_r!("./cover", "./doc/coverage")
  end
end
