defmodule Sink.TestRepo do
  use Ecto.Repo,
    otp_app: :sink,
    adapter: Sqlite.Ecto2

  def setup_db! do
    setup_repo!(Sink.TestRepo)
    migrate_repo!(Sink.TestRepo)

    :ok
  end

  defp setup_repo!(repo) do
    db_file = Application.get_env(:sink, repo)[:database]

    unless File.exists?(db_file) do
      :ok = repo.__adapter__.storage_up(repo.config)
    end
  end

  defp migrate_repo!(repo) do
    opts = [all: true]
    {:ok, _pid, _apps} = Mix.Ecto.ensure_started(repo, opts)

    Ecto.Migrator.up(Sink.TestRepo, 0, Sink.TestMigrations)
  end
end
