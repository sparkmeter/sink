defmodule Sink.Connection.ClientConnection do
  @moduledoc """
  Manages the socket connection and data flow with a Sink Server.
  """
  use GenServer
  require Logger
  alias Sink.Connection.Protocol

  defmodule State do
    alias Sink.Connection.{Inflight, Stats}

    defstruct [
      :socket,
      :peername,
      :handler,
      :transport,
      :stats,
      :inflight
    ]

    def init(socket, handler, transport, now) do
      %State{
        socket: socket,
        handler: handler,
        transport: transport,
        stats: Stats.init(now),
        inflight: Inflight.init()
      }
    end

    def get_inflight(%State{} = state) do
      Inflight.get_inflight(state.inflight)
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

    def get_received_nacks(%State{} = state) do
      Inflight.get_received_nacks(state.inflight)
    end

    def get_sent_nacks(%State{} = state) do
      Inflight.get_sent_nacks(state.inflight)
    end

    def put_received_nack(%State{} = state, message_id, ack_key, nack_data) do
      Map.update!(
        state,
        :inflight,
        &Inflight.put_received_nack(&1, message_id, ack_key, nack_data)
      )
    end

    def put_sent_nack(%State{} = state, message_id, ack_key, nack_data) do
      Map.update!(state, :inflight, &Inflight.put_sent_nack(&1, message_id, ack_key, nack_data))
    end
  end

  # Client

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Returns the internal state. Useful if you want to check the config parameters and state
  of the connection process.
  """
  def info do
    :sys.get_state(Process.whereis(__MODULE__))
  end

  def get_inflight() do
    {:ok, GenServer.call(__MODULE__, :get_inflight)}
  catch
    :exit, _ ->
      {:error, :no_connection}
  end

  def get_received_nacks() do
    {:ok, GenServer.call(__MODULE__, :get_received_nacks)}
  catch
    :exit, _ ->
      {:error, :no_connection}
  end

  def get_sent_nacks() do
    {:ok, GenServer.call(__MODULE__, :get_sent_nacks)}
  catch
    :exit, _ ->
      {:error, :no_connection}
  end

  def stop(reason) do
    {:ok, GenServer.call(__MODULE__, {:stop, reason})}
  catch
    :exit, _ ->
      {:error, :no_connection}
  end

  # Server callbacks

  def init(init_arg) do
    Process.flag(:trap_exit, true)

    socket = Keyword.fetch!(init_arg, :socket)
    handler = Keyword.fetch!(init_arg, :handler)
    transport = Keyword.fetch!(init_arg, :transport)
    state = State.init(socket, handler, transport, now())
    schedule_maybe_ping(state.stats.keepalive_interval)
    schedule_check_keepalive(state.stats.keepalive_interval)

    :ok = handler.up()

    {:ok, state}
  end

  def handle_call(:connection_status, _from, state) do
    {:reply, {:connected, now() - state.stats.start_time}, state}
  end

  def handle_call({:ack, message_id}, _from, state) do
    frame = Protocol.encode_frame(:ack, message_id)

    case state.transport.send(state.socket, frame) do
      :ok -> {:reply, :ok, state}
      {:error, _} = err -> {:stop, :normal, err, state}
    end
  end

  def handle_call({:publish, event, ack_key}, _, state) do
    if State.inflight?(state, ack_key) do
      {:reply, {:error, :inflight}, state}
    else
      payload = Protocol.encode_payload(:publish, event)
      encoded = Protocol.encode_frame(:publish, state.inflight.next_message_id, payload)

      case state.transport.send(state.socket, encoded) do
        :ok ->
          new_state =
            state
            |> State.put_inflight(ack_key)
            |> State.log_sent(now())

          {:reply, :ok, new_state}

        {:error, _} = err ->
          {:stop, :normal, err, state}
      end
    end
  end

  def handle_call(:get_inflight, _from, state) do
    {:reply, State.get_inflight(state), state}
  end

  def handle_call(:get_received_nacks, _from, state) do
    {:reply, State.get_received_nacks(state), state}
  end

  def handle_call(:get_sent_nacks, _from, state) do
    {:reply, State.get_sent_nacks(state), state}
  end

  def handle_call({:stop, reason}, from, state) do
    {:stop, {:shutdown, {:external, reason, from}}, state}
  end

  # keepalive

  def handle_info(:tick_maybe_send_ping, state) do
    schedule_maybe_ping(state.stats.keepalive_interval)

    if State.should_send_ping?(state, now()) do
      frame = Protocol.encode_frame(:ping)

      case state.transport.send(state.socket, frame) do
        :ok -> {:noreply, State.log_sent(state, now())}
        {:error, _} -> {:stop, :normal, state}
      end
    else
      {:noreply, state}
    end
  end

  def handle_info(:tick_check_keepalive, state) do
    schedule_check_keepalive(state.stats.keepalive_interval)

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
    {new_state, response_message} =
      message
      |> Protocol.decode_frame()
      |> case do
        {:ack, message_id} ->
          ack_key = State.find_inflight(state, message_id)

          # todo: error handling
          # :error ->
          # what to do if we can't ack?
          # rescue ->
          # how do we handle a failed ack?
          :ok = handler.handle_ack(ack_key)
          {State.remove_inflight(state, message_id), nil}

        {:nack, message_id, payload} ->
          nack_data = Protocol.decode_payload(:nack, payload)
          ack_key = State.find_inflight(state, message_id)
          new_state = State.put_received_nack(state, message_id, ack_key, nack_data)
          {_event_type_id, _, _} = ack_key

          :ok = handler.handle_nack(ack_key, nack_data)
          {new_state, nil}

        {:publish, message_id, payload} ->
          event = Protocol.decode_payload(:publish, payload)

          try do
            handler.handle_publish(event, message_id)
          catch
            kind, e ->
              formatted = Exception.format(kind, e, __STACKTRACE__)
              Logger.error(formatted)
              {:nack, {<<>>, formatted}}
          end
          |> case do
            :ack ->
              frame = Protocol.encode_frame(:ack, message_id)
              {state, {:ack, frame}}

            {:nack, {machine_message, human_message}} ->
              nack_data = {machine_message, human_message}
              ack_key = {event.event_type_id, event.key, event.offset}
              nack_payload = Protocol.encode_payload(:nack, nack_data)
              frame = Protocol.encode_frame(:nack, message_id, nack_payload)

              after_send = fn state ->
                State.put_sent_nack(state, message_id, ack_key, nack_data)
              end

              {state, {:nack, frame, after_send}}
          end

        :ping ->
          frame = Protocol.encode_frame(:pong)
          {state, {:ping, frame}}

        :pong ->
          {state, nil}
      end

    new_state = State.log_received(new_state, now())

    case normalize_response_message_tuple(response_message) do
      {_type, message, callback} ->
        case state.transport.send(state.socket, message) do
          :ok ->
            new_state = new_state |> callback.() |> State.log_sent(now())
            {:noreply, new_state}

          {:error, _} ->
            {:stop, :normal, new_state}
        end

      nil ->
        {:noreply, new_state}
    end
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

  def terminate(reason, %State{} = state) do
    :ok = state.handler.down()

    case reason do
      {:shutdown, {:external, _, from}} -> GenServer.reply(from, :ok)
      _ -> :ok
    end

    state
  end

  defp normalize_response_message_tuple({type, message}),
    do: {type, message, &Function.identity/1}

  defp normalize_response_message_tuple(other), do: other

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
