require Logger
alias Ecto.Adapters.SQL.Sandbox

Logger.configure(level: :warn)

ExUnit.start()

db_file = File.cwd!() <> "/_build/test/ecto_simple.sqlite3"

File.rm(db_file)
Sink.TestRepo.start_link()
Sink.TestRepo.setup_db!()
# Sandbox.mode(Sink.TestRepo, :manual)
Sandbox.mode(Sink.TestRepo, {:shared, self()})

schemas_path = Path.join([to_string(:code.priv_dir(:sink)), "schemas"])
Application.put_env(:avrora, :schemas_path, schemas_path)
Avrora.start_link()
