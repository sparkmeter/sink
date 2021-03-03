defmodule Sink.Connection.Client do
  @moduledoc false
  use GenServer
  require Logger
  alias Sink.Connection

  defmodule State do
    defstruct [
      :port,
      :host,
      :socket,
      :transport,
      :peername,
      :next_message_id,
      :ssl_opts,
      :handler,
      :connect_attempt_interval,
      :disconnect_msg,
      :disconnect_reason,
      inflight: %{}
    ]

    @first_connect_attempt 50

    @doc """
    This was meant to mean "is the client connected?". However that may not be accurate since
    the client can lose network connection and :ssl/:gen_tcp won't know. Probably need to change
    the name to communicate this and add some code to check last msg received from Server and
    ability to send a ping.

    """
    def connected?(%State{socket: nil}), do: false
    def connected?(%State{socket: _}), do: true

    def init(port, host, ssl_opts, handler) do
      %State{
        port: port,
        host: host,
        ssl_opts: ssl_opts,
        handler: handler,
        connect_attempt_interval: @first_connect_attempt
      }
    end

    def put_inflight(
          %State{inflight: inflight, next_message_id: next_message_id} = state,
          {ack_handler, ack_key}
        ) do
      state
      |> Map.put(:inflight, Map.put(inflight, next_message_id, {ack_handler, ack_key}))
      |> Map.put(:next_message_id, Connection.next_message_id(next_message_id))
    end

    def find_inflight(%State{inflight: inflight}, message_id), do: inflight[message_id]

    def inflight?(%State{inflight: inflight}, ack_key) do
      inflight
      |> Map.values()
      |> Enum.any?(fn {_, key} -> key == ack_key end)
    end

    def remove_inflight(%State{inflight: inflight} = state, message_id) do
      Map.put(state, :inflight, Map.delete(inflight, message_id))
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

    def connected(%State{} = state, socket) do
      %State{
        state
        | socket: socket,
          next_message_id: Sink.Connection.next_message_id(nil),
          connect_attempt_interval: nil
      }
    end

    def disconnected(%State{} = state, msg, reason \\ nil) do
      %State{
        state
        | socket: nil,
          connect_attempt_interval: nil,
          disconnect_msg: msg,
          disconnect_reason: reason
      }
    end
  end

  # Client

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def init(port: port, host: host, ssl_opts: ssl_opts, handler: handler) do
    state = State.init(port, host, ssl_opts, handler)

    Process.send_after(self(), :open_connection, state.connect_attempt_interval)

    {:ok, state}
  end

  def connected? do
    GenServer.call(Process.whereis(__MODULE__), :connected?)
  end

  @doc """
  Returns the internal state of the Sink.Connection.Client process.

  Useful if you want to check the config parameters and state of the connection process.
  """
  def info do
    :sys.get_state(Process.whereis(__MODULE__))
  end

  def ack(message_id) do
    if connected?() do
      pid = Process.whereis(__MODULE__)
      GenServer.call(pid, {:ack, message_id})
    else
      {:error, :no_connection}
    end
  end

  def publish(binary, ack_key) do
    if connected?() do
      pid = Process.whereis(__MODULE__)
      GenServer.call(pid, {:publish, binary, ack_key})
    else
      {:error, :no_connection}
    end
  end

  # Server callbacks

  def handle_call(:connected?, _from, state) do
    {:reply, State.connected?(state), state}
  end

  def handle_call({:ack, message_id}, _from, state) do
    frame = Sink.Connection.Protocol.encode_frame(:ack, message_id, <<>>)
    :ok = :ssl.send(state.socket, frame)

    {:reply, :ok, state}
  end

  def handle_call({:publish, payload, ack_key}, {from, _}, state) do
    if State.inflight?(state, ack_key) do
      {:reply, {:error, :inflight}, state}
    else
      encoded = Sink.Connection.Protocol.encode_frame(:publish, state.next_message_id, payload)

      :ok = :ssl.send(state.socket, encoded)
      {:reply, :ok, State.put_inflight(state, {from, ack_key})}
    end
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
        Logger.info("Connected to Sink Server @ #{state.host}")
        # todo: send message to handler that we're connected

        {:noreply, State.connected(state, socket)}

      {:error, :econnrefused} ->
        # can't find Sink Server, retry
        new_state = State.backoff(state)
        Process.send_after(self(), :open_connection, new_state.connect_attempt_interval)
        {:noreply, new_state}

      {:error, :timeout} ->
        new_state = State.backoff(state)
        Process.send_after(self(), :open_connection, new_state.connect_attempt_interval)
        {:noreply, new_state}

      {:error, :closed} ->
        # closed by server
        new_state = State.backoff(state)
        Process.send_after(self(), :open_connection, new_state.connect_attempt_interval)
        {:noreply, new_state}
    end
  end

  def handle_info(
        {:ssl_error, {:sslsocket, {:gen_tcp, _port, :tls_connection, :undefined}, _},
         {:tls_alert, {:unknown_ca, _}}},
        state
      ) do
    Logger.warn("Unable to connect to Sink Server - unknown server certificate authority")
    Process.send_after(self(), :open_connection, 60_000)

    pid = Process.whereis(state.handler)
    send(pid, {:ssl_error, :unknown_ca})

    {:noreply, state}
  end

  # Response to data

  def handle_info(
        {:ssl, _socket, message},
        %State{handler: handler} = state
      ) do
    new_state =
      message
      |> Sink.Connection.Protocol.decode_frame()
      |> case do
        {:ack, message_id} ->
          {ack_handler, ack_key} = State.find_inflight(state, message_id)

          send(ack_handler, {:ack, ack_key})

          State.remove_inflight(state, message_id)

        {:publish, message_id, payload} ->
          {event_type_id, key, offset, event_data} =
            Connection.Protocol.decode_payload(:publish, payload)

          # send the event to handler
          pid = Process.whereis(handler)
          send(pid, {:publish, {event_type_id, key}, offset, event_data, message_id})

          state
      end

    {:noreply, new_state}
  end

  # Connection Management

  def handle_info({:tcp_closed, _}, %State{peername: peername} = state) do
    Logger.info(fn ->
      "Peer #{peername} disconnected"
    end)

    # todo? probably don't return a `:stop
    Process.send_after(self(), :open_connection, 5_000)

    {:stop, :normal, State.disconnected(state, :tcp_closed)}
  end

  def handle_info({:ssl_closed, _}, %State{peername: peername} = state) do
    Logger.info(fn ->
      "SSL Peer #{peername} disconnected"
    end)

    # todo? probably don't return a `:stop
    Process.send_after(self(), :open_connection, 5_000)

    {:stop, :normal, State.disconnected(state, :ssl_closed)}
  end

  def handle_info({:tcp_error, _, reason}, %State{peername: peername} = state) do
    Logger.info(fn ->
      "Error with peer #{peername}: #{inspect(reason)}"
    end)

    # todo? probably don't return a `:stop
    {:stop, :normal, State.disconnected(state, :tcp_error, reason)}
  end
end
