defmodule Sink.MixProject do
  use Mix.Project

  def project do
    [
      app: :sink,
      version: "0.2.0",
      elixir: "~> 1.10.4",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps()
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
      # {:ecto, "2.2.11"},
      {:ecto, "2.2.11", only: :test},
      {:ranch, "1.7.1"},
      # {:sqlite_ecto2, "2.4.1"}
      {:sqlite_ecto2, "2.4.1", only: :test},
      {:varint, "1.3.0"}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]
end
