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

  @doc """
  Initiates the listener (pool of acceptors).
  """
  @impl true
  def init(init_arg) do
    {port, handler_opts} = Keyword.pop!(init_arg, :port)
    ssl_opts = Keyword.fetch!(handler_opts, :ssl_opts)
    server_handler = Keyword.get(handler_opts, :server_handler, ServerHandler)
    handler_opts = Keyword.put_new(handler_opts, :transport, Sink.Connection.Transport.SSL)
    ranch_opts = [port: port, keepalive: true] ++ ssl_opts

    Process.flag(:trap_exit, true)

    case :ranch.start_listener(:sink, :ranch_ssl, ranch_opts, server_handler, handler_opts) do
      {:ok, pid} ->
        ref = Process.monitor(pid)
        Logger.info("Listening for connections on port #{port}")
        {:ok, %{pid: pid, ref: ref}}

      {:error, _} = err ->
        {:stop, err}
    end
  end

  @impl true
  def handle_info({:DOWN, ref, :process, pid, reason}, %{ref: ref, pid: pid} = state) do
    Logger.info("Ranch listener stopped: #{reason}")
    {:stop, :normal, state}
  end

  @impl true
  def terminate(_, _) do
    :ranch.stop_listener(:sink)
  end
end
