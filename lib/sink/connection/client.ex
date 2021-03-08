defmodule Sink.Connection.Client do
  @moduledoc """
  Attempts to connect to a Sink server. Sends publishes and acks to the connection (if
  it exists).

  Client manages connection attempts, retries, and tracking disconnect reasons / error
  states. It wil backoff if it is unable to make a connection.

  Note:
  Currently if the Sink client's SSL cert is not present in the server the server will
  close the connection immediately. Need to implement CONN and CONNACK/CONNERROR
  messages.
  """
  use GenServer
  require Logger
  alias Sink.Connection.ClientConnection

  ClientConnection

  defmodule State do
    defstruct [
      :connection_pid,
      :port,
      :host,
      :peername,
      :ssl_opts,
      :handler,
      :connect_attempt_interval,
      :disconnect_reason
    ]

    @first_connect_attempt 50

    @doc """
    This was meant to mean "is the client connected?". However that may not be accurate since
    the client can lose network connection and :ssl/:gen_tcp won't know. Probably need to change
    the name to communicate this and add some code to check last msg received from Server and
    ability to send a ping.

    """

    def init(port, host, ssl_opts, handler) do
      %State{
        connection_pid: nil,
        port: port,
        host: host,
        ssl_opts: ssl_opts,
        handler: handler,
        connect_attempt_interval: @first_connect_attempt
      }
    end

    def backoff(%State{connect_attempt_interval: nil} = state) do
      %State{state | connect_attempt_interval: @first_connect_attempt}
    end

    def backoff(%State{connect_attempt_interval: @first_connect_attempt} = state) do
      %State{state | connect_attempt_interval: 1_000}
    end

    def backoff(%State{connect_attempt_interval: 1_000} = state) do
      %State{state | connect_attempt_interval: 5_000}
    end

    def backoff(%State{connect_attempt_interval: 5_000} = state) do
      %State{state | connect_attempt_interval: 10_000}
    end

    def backoff(%State{connect_attempt_interval: 10_000} = state) do
      %State{state | connect_attempt_interval: 20_000}
    end

    def backoff(%State{connect_attempt_interval: _} = state) do
      %State{state | connect_attempt_interval: 30_000}
    end

    def connected(%State{} = state, connection_pid) do
      %State{
        state
        | connection_pid: connection_pid
      }
    end

    def disconnected(%State{} = state, reason) do
      %State{
        state
        | connect_attempt_interval: nil,
          disconnect_reason: reason
      }
    end
  end

  # Client

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def connected? do
    !!Process.whereis(ClientConnection)
  end

  @doc """
  Returns the internal state of the Sink.Connection.Client process.
  """
  def info do
    :sys.get_state(Process.whereis(__MODULE__))
  end

  @doc """
  Send an ACK message to the Sink server.
  """
  def ack(message_id) do
    if connected?() do
      pid = Process.whereis(ClientConnection)
      GenServer.call(pid, {:ack, message_id})
    else
      {:error, :no_connection}
    end
  end

  @doc """
  Send a PUBLISH message to the Sink server.
  """
  def publish(binary, ack_key) do
    if connected?() do
      pid = Process.whereis(ClientConnection)
      GenServer.call(pid, {:publish, binary, ack_key})
    else
      {:error, :no_connection}
    end
  end

  # Server callbacks

  def init(port: port, host: host, ssl_opts: ssl_opts, handler: handler) do
    Process.flag(:trap_exit, true)
    state = State.init(port, host, ssl_opts, handler)

    Process.send_after(self(), :open_connection, state.connect_attempt_interval)

    {:ok, state}
  end

  def handle_info(:open_connection, %State{} = state) do
    opts =
      [:binary] ++
        Keyword.merge(state.ssl_opts,
          packet: 2,
          active: true
        )

    host = String.to_charlist(state.host)

    case :ssl.connect(host, state.port, opts, 60_000) do
      {:ok, socket} ->
        Logger.info("Connected to Sink server @ #{state.host}")
        # todo: send message to handler that we're connected

        {:ok, pid} = ClientConnection.start_link(socket: socket, handler: state.handler)
        :ok = :ssl.controlling_process(socket, pid)

        {:noreply, State.connected(state, pid)}

      {:error, reason} ->
        if reason in [:econnrefused, :closed] do
          Logger.warn("Can't find Sink server - #{inspect(reason)}")
        else
          Logger.error("Failed to connect to Sink server, #{inspect(reason)}")
        end

        new_state = State.backoff(state)
        Process.send_after(self(), :open_connection, new_state.connect_attempt_interval)
        {:noreply, new_state}
    end
  end

  def handle_info({:EXIT, pid, reason}, state) do
    new_state =
      if pid == state.connection_pid do
        Logger.info("Disconnected from Sink server")
        Process.send_after(self(), :open_connection, 5_000)
        State.disconnected(state, reason)
      else
        state
      end

    {:noreply, new_state}
  end
end
