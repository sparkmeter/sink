defmodule Sink.Connection.Client do
  @moduledoc false
  @first_connect_attempt 50

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
      inflight: %{}
    ]

    def connected?(%State{socket: nil}), do: false
    def connected?(%State{socket: _}), do: true

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
  end

  # Client

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def init(port: port, host: host, ssl_opts: ssl_opts, handler: handler) do
    Process.send_after(self(), :open_connection, @first_connect_attempt)

    {:ok, %State{port: port, host: host, ssl_opts: ssl_opts, handler: handler}}
  end

  def connected? do
    GenServer.call(Process.whereis(__MODULE__), :connected?)
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

        {:noreply,
         %State{
           state
           | socket: socket,
             #            transport: transport,
             #            peername: peername,
             next_message_id: Connection.next_message_id(nil)
         }}

      {:error, :econnrefused} ->
        # can't find Sink Server, retry
        Process.send_after(self(), :open_connection, 5_000)
        {:noreply, state}

      {:error, :timeout} ->
        Process.send_after(self(), :open_connection, 5_000)
        {:noreply, state}

      {:error, :closed} ->
        Process.send_after(self(), :open_connection, 5_000)
        {:noreply, state}
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

    Process.send_after(self(), :open_connection, 5_000)

    {:stop, :normal, state}
  end

  def handle_info({:ssl_closed, _}, %State{peername: peername} = state) do
    Logger.info(fn ->
      "SSL Peer #{peername} disconnected"
    end)

    Process.send_after(self(), :open_connection, 5_000)

    {:stop, :normal, state}
  end

  def handle_info({:tcp_error, _, reason}, %State{peername: peername} = state) do
    Logger.info(fn ->
      "Error with peer #{peername}: #{inspect(reason)}"
    end)

    {:stop, :normal, state}
  end
end
