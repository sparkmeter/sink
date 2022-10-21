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
  alias Sink.Event
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
      :transport,
      :connect_attempt_interval,
      :disconnect_reason,
      :disconnect_time,
      :connection_request_succeeded
    ]

    @first_connect_attempt 50

    @doc """
    This was meant to mean "is the client connected?". However that may not be accurate since
    the client can lose network connection and :ssl/:gen_tcp won't know. Probably need to change
    the name to communicate this and add some code to check last msg received from Server and
    ability to send a ping.

    """

    def init(port, host, ssl_opts, handler, transport) do
      %State{
        connection_pid: nil,
        port: port,
        host: host,
        ssl_opts: ssl_opts,
        handler: handler,
        transport: transport,
        connect_attempt_interval: @first_connect_attempt,
        disconnect_time: nil
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
      %State{state | connect_attempt_interval: 30_000}
    end

    def backoff(%State{connect_attempt_interval: _} = state) do
      %State{state | connect_attempt_interval: 60_000}
    end

    def connected(%State{} = state, connection_pid) do
      %State{state | connection_pid: connection_pid, connect_attempt_interval: 300_000}
    end

    def connection_request_succeeded(%State{} = state) do
      %State{state | connect_attempt_interval: nil}
    end

    def disconnected(%State{} = state, reason, now) do
      struct!(state,
        connection_pid: nil,
        disconnect_reason: reason,
        disconnect_time: now
      )
    end
  end

  # Client

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Is the client connected to the server?
  """
  def connected? do
    ClientConnection.connected?()
  end

  @doc """
  Returns the internal state of the Sink.Connection.Client process.
  """
  def info do
    :sys.get_state(Process.whereis(__MODULE__))
  end

  @doc """
  Publish an event for the Sink server.
  """
  def publish(%Event{} = event, ack_key) do
    GenServer.call(ClientConnection, {:publish, event, ack_key})
  catch
    :exit, _ ->
      {:error, :no_connection}
  end

  @doc """
  Returns details about the connection status of the client.
  """
  @spec connection_status :: {:connected | :disconnected, DateTime.t()} | :disconnected
  def connection_status do
    gen_server_name =
      if connected?() do
        ClientConnection
      else
        __MODULE__
      end

    with {status, diff_in_ms} <- GenServer.call(gen_server_name, :connection_status) do
      start_time = DateTime.add(DateTime.utc_now(), diff_in_ms * -1, :millisecond)
      {status, start_time}
    end
  catch
    # If those fail we're for sure disconnected,
    # though unsure when we disconnected.
    :exit, _ -> :disconnected
  end

  # Server callbacks

  def init(init_arg) do
    port = Keyword.fetch!(init_arg, :port)
    host = Keyword.fetch!(init_arg, :host)
    ssl_opts = Keyword.fetch!(init_arg, :ssl_opts)
    handler = Keyword.fetch!(init_arg, :handler)
    transport = Keyword.get(init_arg, :transport, Sink.Connection.Transport.SSL)
    Process.flag(:trap_exit, true)
    state = State.init(port, host, ssl_opts, handler, transport)

    Process.send_after(self(), :open_connection, state.connect_attempt_interval)

    {:ok, state}
  end

  def handle_call(:connection_status, _from, %State{disconnect_time: nil} = state) do
    {:reply, :disconnected, state}
  end

  def handle_call(:connection_status, _from, %State{disconnect_time: time} = state)
      when is_integer(time) do
    {:reply, {:disconnected, now() - time}, state}
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
        {:ok, pid} =
          ClientConnection.start_link(
            socket: socket,
            handler: state.handler,
            transport: state.transport
          )

        case :ssl.controlling_process(socket, pid) do
          :ok ->
            Logger.info("Connected to Sink server @ #{state.host}")
            # todo: send message to handler that we're connected

            Process.send_after(self(), :check_if_connection_request_succeeded, 100)
            {:noreply, State.connected(state, pid)}

          {:error, reason} ->
            _ = ClientConnection.stop(:connection_tranfer_failed)
            log_ssl_error(reason)
            {:noreply, on_connection_init_failure(state)}
        end

      {:error, reason} ->
        log_ssl_error(reason)
        {:noreply, on_connection_init_failure(state)}
    end
  end

  # Ignore error message of `:ssl.connect`
  # They are sent to this process before the connection is transfered to `ClientConnection`.
  # They are sent even if we handle the `{:error, term}` response above.
  def handle_info({err, _}, state)
      when err in [:tcp_closed, :ssl_closed, :ssl_error, :tcp_error] do
    {:noreply, state}
  end

  def handle_info(:check_if_connection_request_succeeded, state) do
    if connected?() do
      {:noreply, State.connection_request_succeeded(state)}
    else
      Process.send_after(self(), :check_if_connection_request_succeeded, 100)
      {:noreply, state}
    end
  end

  def handle_info({:EXIT, pid, reason}, state) do
    new_state =
      if pid == state.connection_pid do
        Logger.info("Disconnected from Sink server")
        on_connection_init_failure(state)
        State.disconnected(state, reason, now())
      else
        state
      end

    {:noreply, new_state}
  end

  defp log_ssl_error(reason) do
    case reason do
      r when r in [[:econnrefused, :closed], :nxdomain] ->
        Logger.info("Can't find Sink server - #{inspect(reason)}")

      :closed ->
        # todo: proper messaging between client and server on failure to authenticate
        Logger.warn("Connection immediately closed. Probably an authentication issue.")

      _ ->
        Logger.error("Failed to connect to Sink server, #{inspect(reason)}")
    end
  end

  defp on_connection_init_failure(state) do
    new_state = State.backoff(state)
    Process.send_after(self(), :open_connection, add_jitter(new_state.connect_attempt_interval))
    new_state
  end

  defp now do
    System.monotonic_time(:millisecond)
  end

  defp add_jitter(interval) do
    variance = div(interval, 10)
    interval + Enum.random(-variance..variance)
  end
end
