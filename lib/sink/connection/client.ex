defmodule Sink.Connection.Client do
  @moduledoc false
  @first_connect_attempt 50

  use GenServer
  require Logger
  alias Sink.Connection

  defmodule State do
    defstruct [
      :port,
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

  def init(port: port, ssl_opts: ssl_opts, handler: handler) do
    Process.send_after(self(), :open_connection, @first_connect_attempt)

    {:ok, %State{port: port, ssl_opts: ssl_opts, handler: handler}}
  end

  def connected? do
    GenServer.call(Process.whereis(__MODULE__), :connected?)
  end

  def publish(binary, ack_key: ack_key) do
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

  def handle_call({:publish, binary, ack_key}, {from, _}, state) do
    if State.inflight?(state, ack_key) do
      {:reply, {:error, :inflight}, state}
    else
      encoded = Connection.encode_publish(state.next_message_id, binary)
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

    case :ssl.connect('localhost', state.port, opts, 5_000) do
      {:ok, socket} ->
        # todo: send message to handler that we're connected

        {:noreply,
         %State{
           state
           | socket: socket,
             #            transport: transport,
             #            peername: peername,
             next_message_id: Connection.next_message_id(nil)
         }}

      _result ->
        Process.send_after(self(), :open_connection, 5_000)
        {:noreply, state}
    end
  end

  # Response to data

  def handle_info(
        {:ssl, socket, message},
        %State{handler: handler} = state
      ) do
    new_state =
      message
      |> Connection.decode_message()
      |> case do
        {:ack, message_id} ->
          {ack_handler, ack_key} = State.find_inflight(state, message_id)

          send(ack_handler, {:ack, ack_key})

          State.remove_inflight(state, message_id)

        {:publish, message_id, event_frame} ->
          # send the event to handler
          pid = Process.whereis(handler)
          send(pid, {:publish_event, event_frame})
          # ack the message
          ack = Connection.encode_ack(message_id)
          :ok = :ssl.send(socket, ack)

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
