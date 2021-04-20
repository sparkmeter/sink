defmodule Sink.MixProject do
  use Mix.Project

  def project do
    [
      app: :sink,
      version: "0.8.0",
      elixir: "~> 1.10.4",
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
      # {:ecto, "2.2.11"},
      {:ecto, "2.2.11", only: [:dev, :test]},
      {:excoveralls, "~> 0.10", only: :test},
      {:ex_doc, "~> 0.24", only: :dev, runtime: false},
      {:mox, "~> 1.0", only: :test},
      {:ranch, "1.7.1"},
      # {:sqlite_ecto2, "2.4.1"}
      {:sqlite_ecto2, "2.4.1", only: :test},
      {:telemetry, "~>0.4"},
      {:varint, "1.2.0"},
      {:x509, "~> 0.7"}
    ]
  end

  defp elixirc_paths(:test), do: ["test/support", "lib"]
  defp elixirc_paths(_), do: ["lib"]

  def aliases do
    [
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
      "Event Log": [~r/^Sink\.EventLog/, ~r/^Sink\.EventSubscription/, Sink.EctoEventTypeConfig],
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
