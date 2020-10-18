defmodule Sink.Connection.ServerListener do
  @moduledoc """
  A simple TCP server.
  """

  use GenServer
  alias Sink.Connection.ServerHandler
  require Logger

  @doc """
  Starts the server.
  """
  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: __MODULE__)
  end

  def terminate(_, _) do
    :ranch.stop_listener(:sink)
  end

  @doc """
  Initiates the listener (pool of acceptors).
  """
  def init(port: port, ssl_opts: ssl_opts, handler: handler) do
    Process.flag(:trap_exit, true)
    opts = [{:port, port}]
    server_handler_opts = [ssl_opts: ssl_opts, handler: handler]

    {:ok, pid} =
      :ranch.start_listener(
        :sink,
        :ranch_tcp,
        %{socket_opts: opts},
        ServerHandler,
        server_handler_opts
      )

    Logger.info(fn ->
      "Listening for connections on port #{port}"
    end)

    {:ok, pid}
  end
end
