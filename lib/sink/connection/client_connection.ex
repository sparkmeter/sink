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
      :last_sent_at,
      :last_received_at,
      :start_time,
      :keepalive_interval,
      inflight: %{}
    ]

    @default_keepalive_interval 60_000

    def init(socket, handler, now) do
      %State{
        socket: socket,
        next_message_id: Connection.next_message_id(nil),
        handler: handler,
        last_sent_at: now,
        last_received_at: now,
        start_time: now,
        keepalive_interval:
          Application.get_env(:sink, :keepalive_interval, @default_keepalive_interval)
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

    def log_sent(%State{} = state, now) do
      %State{state | last_sent_at: now}
    end

    def log_received(%State{} = state, now) do
      %State{state | last_received_at: now}
    end

    @doc """
    If we haven't sent or received a message within the keepalive timeframe, we should send a ping

    https://www.hivemq.com/blog/mqtt-essentials-part-10-alive-client-take-over/

    Inspired by MQTT. Client should be regularly sending and receiving data. If no
    actual message is sent within the keepalive timefame, a PING will be sent.
    """
    def should_send_ping?(
          %State{} = state,
          now
        ) do
      cond do
        state.keepalive_interval > now - state.last_sent_at -> false
        state.keepalive_interval > now - state.last_received_at -> false
        true -> true
      end
    end

    @doc """
    If the we haven't received a message in 1.5x keepalive_interval we probably don't have
    a connection
    """
    def alive?(%State{} = state, now) do
      state.keepalive_interval * 1.5 > now - state.last_received_at
    end
  end

  # Client

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def init(socket: socket, handler: handler) do
    state = State.init(socket, handler, now())
    schedule_maybe_ping(state.keepalive_interval)
    schedule_check_keepalive(state.keepalive_interval)

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

      new_state =
        state
        |> State.put_inflight({from, ack_key})
        |> State.log_sent(now())

      {:reply, :ok, new_state}
    end
  end

  # keepalive

  def handle_info(:tick_maybe_send_ping, state) do
    schedule_maybe_ping(state.keepalive_interval)

    if State.should_send_ping?(state, now()) do
      frame = Connection.Protocol.encode_frame(:ping)
      :ok = @mod_transport.send(state.socket, frame)

      {:noreply, State.log_sent(state, now())}
    else
      {:noreply, state}
    end
  end

  def handle_info(:tick_check_keepalive, state) do
    schedule_check_keepalive(state.keepalive_interval)

    if State.alive?(state, now()) do
      {:noreply, state}
    else
      {:stop, :normal, state}
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
                State.log_sent(state, now())
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

          State.log_sent(state, now())

        :pong ->
          state
      end
      |> State.log_received(now())

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

  defp now do
    System.monotonic_time(:millisecond)
  end

  defp schedule_check_keepalive(keepalive_interval) do
    Process.send_after(self(), :tick_check_keepalive, div(keepalive_interval, 2))
  end

  defp schedule_maybe_ping(keepalive_interval) do
    Process.send_after(self(), :tick_maybe_send_ping, div(keepalive_interval, 10))
  end
end
