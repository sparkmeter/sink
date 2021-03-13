use Mix.Config
alias Ecto.Adapters.SQL.Sandbox

config :sink, :ecto_repo, Sink.TestRepo
# Application.put_env(:sink, Sink.TestRepo, adapter: Sqlite.Ecto2, database: ":memory:")
config :sink, Sink.TestRepo,
  adapter: Sqlite.Ecto2,
  database: "_build/test/ecto_simple.sqlite3",
  pool: Sandbox

config :sink, :transport, Sink.Connection.Transport.SSLMock
