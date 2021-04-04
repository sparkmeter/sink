defmodule Sink.Connection.ClientConnection do
  @moduledoc """
  Manages the socket connection and data flow with a Sink Server.
  """
  use GenServer
  require Logger
  alias Sink.Connection

  @mod_transport Application.compile_env!(:sink, :transport)

  defmodule State do
    alias Sink.Connection.{Inflight, Stats}

    defstruct [
      :socket,
      :peername,
      :handler,
      :stats,
      :inflight
    ]

    def init(socket, handler, now) do
      %State{
        socket: socket,
        handler: handler,
        stats: Stats.init(now),
        inflight: Inflight.init()
      }
    end

    def put_inflight(%State{} = state, ack_key) do
      %State{state | inflight: Inflight.put_inflight(state.inflight, ack_key)}
    end

    def find_inflight(%State{} = state, message_id) do
      Inflight.find_inflight(state.inflight, message_id)
    end

    def inflight?(%State{} = state, ack_key) do
      Inflight.inflight?(state.inflight, ack_key)
    end

    def remove_inflight(%State{} = state, message_id) do
      %State{state | inflight: Inflight.remove_inflight(state.inflight, message_id)}
    end

    def log_sent(%State{} = state, now) do
      %State{state | stats: Stats.log_sent(state.stats, now)}
    end

    def log_received(%State{} = state, now) do
      %State{state | stats: Stats.log_received(state.stats, now)}
    end

    def should_send_ping?(%State{} = state, now) do
      Stats.should_send_ping?(state.stats, now)
    end

    def alive?(%State{} = state, now) do
      Stats.alive?(state.stats, now)
    end
  end

  # Client

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def init(socket: socket, handler: handler) do
    state = State.init(socket, handler, now())
    schedule_maybe_ping(state.stats.keepalive_interval)
    schedule_check_keepalive(state.stats.keepalive_interval)

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
    frame = Connection.Protocol.encode_frame(:ack, message_id)
    result = @mod_transport.send(state.socket, frame)

    {:reply, result, state}
  end

  def handle_call({:publish, payload, ack_key}, _, state) do
    if State.inflight?(state, ack_key) do
      {:reply, {:error, :inflight}, state}
    else
      encoded =
        Connection.Protocol.encode_frame(:publish, state.inflight.next_message_id, payload)

      :ok = @mod_transport.send(state.socket, encoded)

      new_state =
        state
        |> State.put_inflight(ack_key)
        |> State.log_sent(now())

      {:reply, :ok, new_state}
    end
  end

  # keepalive

  def handle_info(:tick_maybe_send_ping, state) do
    schedule_maybe_ping(state.stats.keepalive_interval)

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
          ack_key = State.find_inflight(state, message_id)

          case handler.handle_ack(ack_key) do
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
