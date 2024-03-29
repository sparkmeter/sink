defmodule Sink.Connection.ClientConnection do
  @moduledoc """
  Manages the socket connection and data flow with a Sink Server.
  """
  use GenServer
  require Logger
  alias Sink.Connection.Client.ConnectionStatus
  alias Sink.Connection.Protocol
  alias Sink.Event

  defmodule State do
    alias Sink.Connection.{Inflight, Stats}

    defstruct [
      :socket,
      :peername,
      :handler,
      :transport,
      :connection_status,
      :stats,
      :inflight
    ]

    def init(socket, handler, transport, instance_ids, now) do
      %State{
        socket: socket,
        handler: handler,
        transport: transport,
        connection_status: ConnectionStatus.init(instance_ids),
        stats: Stats.init(now),
        inflight: Inflight.init()
      }
    end

    def connected?(state) do
      ConnectionStatus.connected?(state.connection_status)
    end

    def connection_response(state, result) do
      %__MODULE__{
        state
        | connection_status: ConnectionStatus.connection_response(state.connection_status, result)
      }
    end

    def instance_ids(state) do
      ConnectionStatus.instance_ids(state.connection_status)
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

  def connected?() do
    GenServer.call(__MODULE__, :connected?)
  catch
    :exit, _ ->
      false
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
    instance_ids = handler.instance_ids()
    state = State.init(socket, handler, transport, instance_ids, now())
    schedule_maybe_ping(state.stats.keepalive_interval)
    schedule_check_keepalive(state.stats.keepalive_interval)

    {:ok, state, {:continue, :send_connection_request}}
  end

  def handle_continue(:send_connection_request, state) do
    application_version = state.handler.application_version()
    instance_ids = State.instance_ids(state)

    frame =
      {:connection_request, {application_version, {instance_ids.client, instance_ids.server}}}
      |> Protocol.encode_frame()

    case state.transport.send(state.socket, frame) do
      :ok -> {:noreply, state}
      {:error, _} -> {:stop, :normal, state}
    end
  end

  def handle_call(:connection_status, _from, state) do
    {:reply, {:connected, now() - state.stats.start_time}, state}
  end

  def handle_call(:connected?, _from, state) do
    {:reply, State.connected?(state), state}
  end

  def handle_call({:ack, message_id}, _from, state) do
    frame = Protocol.encode_frame({:ack, message_id})

    case state.transport.send(state.socket, frame) do
      :ok -> {:reply, :ok, state}
      {:error, _} = err -> {:stop, :normal, err, state}
    end
  end

  def handle_call({:publish, event, ack_key}, _, state) do
    with {:no_connection, false} <- {:no_connection, !State.connected?(state)},
         {:inflight, false} <- {:inflight, State.inflight?(state, ack_key)} do
      payload = Protocol.encode_payload(:publish, event)
      encoded = Protocol.encode_frame({:publish, state.inflight.next_message_id, payload})

      case state.transport.send(state.socket, encoded) do
        :ok ->
          Sink.Telemetry.publish(:sent, %{event_type_id: event.event_type_id})

          new_state =
            state
            |> State.put_inflight(ack_key)
            |> State.log_sent(now())

          {:reply, :ok, new_state}

        {:error, _} = err ->
          {:stop, :normal, err, state}
      end
    else
      {reason, true} ->
        {:reply, {:error, reason}, state}
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
        {:connection_response, result} ->
          result =
            with {:quarantined, payload} <- result do
              {:quarantined, Protocol.decode_payload(:nack, payload)}
            end

          :ok = handler.handle_connection_response(result)
          new_state = State.connection_response(state, result)

          if State.connected?(new_state) do
            Sink.Telemetry.start(:connection, %{})
          end

          {new_state, nil}

        {:ack, message_id} ->
          ack_key = State.find_inflight(state, message_id)
          {event_type_id, _, _} = ack_key
          Sink.Telemetry.ack(:received, %{event_type_id: event_type_id})

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
          {event_type_id, _, _} = ack_key
          Sink.Telemetry.nack(:received, %{event_type_id: event_type_id})

          :ok = handler.handle_nack(ack_key, nack_data)
          {new_state, nil}

        {:publish, message_id, payload} ->
          event = Protocol.decode_payload(:publish, payload)
          Sink.Telemetry.publish(:received, %{event_type_id: event.event_type_id})

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
              {state, {:ack, {:ack, message_id}}}

            {:nack, nack_data} ->
              after_send = fn state ->
                State.put_sent_nack(state, message_id, Event.ack_key(event), nack_data)
              end

              nack_payload = Protocol.encode_payload(:nack, nack_data)
              {state, {:nack, {:nack, message_id, nack_payload}, after_send}}
          end

        :ping ->
          Sink.Telemetry.ping(:received, %{})

          after_send = fn state ->
            Sink.Telemetry.pong(:sent, %{})
            state
          end

          {state, {:ping, :pong, after_send}}

        :pong ->
          Sink.Telemetry.pong(:received, %{})
          {state, nil}
      end

    new_state = State.log_received(new_state, now())

    case normalize_response_message_tuple(response_message) do
      {_type, message, callback} ->
        frame = Sink.Connection.Protocol.encode_frame(message)

        case state.transport.send(state.socket, frame) do
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
    if State.connected?(state) do
      :ok = state.handler.down()
      Sink.Telemetry.stop(:connection, state.stats.start_time, %{reason: reason})
    end

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
