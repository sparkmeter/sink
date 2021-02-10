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
      {:avrora,
       git: "git@gitlab.com:sparkmeter/zappy/avrora.git",
       branch: "fix/plain_binary_decoding_when_schema_is_given"},
      # {:ecto, "2.2.11"},
      {:ecto, "2.2.11", only: :test},
      {:erlavro, "~> 2.9.0"},
      {:ranch, "1.7.1"},
      # {:sqlite_ecto2, "2.4.1"}
      {:sqlite_ecto2, "2.4.1", only: :test},
      {:varint, "1.2.0"},
      {:x509, "~> 0.7"}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]
end
