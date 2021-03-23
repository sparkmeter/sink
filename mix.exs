defmodule Sink.MixProject do
  use Mix.Project

  def project do
    [
      app: :sink,
      version: "0.3.1",
      elixir: "~> 1.10.4",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps()
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
      {:avrora, "~> 0.18",
       git: "git@gitlab.com:sparkmeter/zappy/avrora.git", branch: "fix/incorrect-typespec"},
      # {:ecto, "2.2.11"},
      {:ecto, "2.2.11", only: :test},
      {:erlavro, "~> 2.9.0"},
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
end
