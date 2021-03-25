defmodule Sink.Connection.ClientConnection do
  @moduledoc """
  Manages the socket connection and data flow with a Sink Server.
  """
  use GenServer
  require Logger
  alias Sink.Connection

  @mod_transport Application.compile_env!(:sink, :transport)

  defmodule State do
    defstruct [
      :socket,
      :peername,
      :next_message_id,
      :handler,
      inflight: %{}
    ]

    def init(socket, handler) do
      %State{
        socket: socket,
        next_message_id: Connection.next_message_id(nil),
        handler: handler
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
  end

  # Client

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def init(socket: socket, handler: handler) do
    state = State.init(socket, handler)

    {:ok, state}
  end

  @doc """
  Returns the internal state. Useful if you want to check the config parameters and state
  of the connection process.
  """
  def info do
    :sys.get_state(Process.whereis(__MODULE__))
  end

  # Server callbacks

  def handle_call({:ack, message_id}, _from, state) do
    frame = Connection.Protocol.encode_frame(:ack, message_id, <<>>)
    result = @mod_transport.send(state.socket, frame)

    {:reply, result, state}
  end

  def handle_call({:publish, payload, ack_key}, {from, _}, state) do
    if State.inflight?(state, ack_key) do
      {:reply, {:error, :inflight}, state}
    else
      encoded = Connection.Protocol.encode_frame(:publish, state.next_message_id, payload)

      :ok = @mod_transport.send(state.socket, encoded)
      {:reply, :ok, State.put_inflight(state, {from, ack_key})}
    end
  end

  # Response to data

  def handle_info(
        {:ssl, _socket, message},
        %State{handler: handler} = state
      ) do
    new_state =
      message
      |> Connection.Protocol.decode_frame()
      |> case do
        {:ack, message_id} ->
          {ack_from, ack_key} = State.find_inflight(state, message_id)

          case handler.handle_ack(ack_from, ack_key) do
            :ok ->
              State.remove_inflight(state, message_id)
              # todo: error handling
              # :error ->
              # what to do if we can't ack?
              # rescue ->
              # how do we handle a failed ack?
          end

        {:publish, message_id, payload} ->
          {event_type_id, key, offset, event_data} =
            Connection.Protocol.decode_payload(:publish, payload)

          try do
            case handler.handle_publish({event_type_id, key}, offset, event_data, message_id) do
              :ack ->
                {:reply, :ok, state} = handle_call({:ack, message_id}, self(), state)
                state
                # todo: error handling
                # :error ->
                # send a nack
                # maybe put the connection into an error state
                # rescue ->
                # nack
                # maybe put the connection into an error state
            end
          rescue
            e ->
              formatted = Exception.format(:error, e, __STACKTRACE__)
              Logger.error(formatted)
              state
          end

        :ping ->
          frame = Connection.Protocol.encode_frame(:pong)
          :ok = @mod_transport.send(state.socket, frame)

          state

        :pong ->
          state
      end

    {:noreply, new_state}
  end

  def handle_info({:tcp_closed, _}, state) do
    {:stop, :normal, state}
  end

  def handle_info({:ssl_closed, _}, state) do
    {:stop, :normal, state}
  end

  def handle_info({:ssl_error, reason}, state) do
    {:stop, {:error, reason}, state}
  end

  def handle_info({:tcp_error, _, reason}, state) do
    {:stop, {:error, reason}, state}
  end
end
