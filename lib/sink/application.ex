defmodule Sink.Application do
  @moduledoc false

  use Application
  require Logger

  @impl Application
  def start(_type, _args) do
    children = [
      {Registry, keys: :unique, name: Sink.Connection.ServerHandler}
    ]

    opts = [strategy: :one_for_one, name: Sink.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
