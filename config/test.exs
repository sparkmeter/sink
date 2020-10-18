alias Ecto.Adapters.SQL.Sandbox

Application.put_env(:sink, :ecto_repo, Sink.TestRepo)
# Application.put_env(:sink, Sink.TestRepo, adapter: Sqlite.Ecto2, database: ":memory:")
Application.put_env(:sink, Sink.TestRepo,
  adapter: Sqlite.Ecto2,
  database: "_build/test/ecto_simple.sqlite3",
  pool: Sandbox
)
