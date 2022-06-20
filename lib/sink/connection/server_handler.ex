defmodule Sink.Connection.ServerHandler do
  @moduledoc """
  Handles client connections.

  Each server connection handles one client connection.
  """
  use GenServer
  require Logger
  alias Sink.Connection
  alias Sink.Connection.Protocol
  alias Sink.Event

  @behaviour :ranch_protocol

  @registry __MODULE__

  defmodule State do
    alias Sink.Connection.{Inflight, Stats}

    defstruct [
      :client_id,
      :socket,
      :transport,
      :peername,
      :handler,
      :ssl_opts,
      :connection_state,
      :stats,
      :inflight
    ]

    def init(client_id, socket, transport, peername, handler, ssl_opts, instantiated_ats, now) do
      %State{
        client_id: client_id,
        socket: socket,
        transport: transport,
        peername: peername,
        handler: handler,
        ssl_opts: ssl_opts,
        connection_state: {:awaiting_connection_request, instantiated_ats},
        stats: Stats.init(now),
        inflight: Inflight.init()
      }
    end

    def connection_response(
          %State{connection_state: {:awaiting_connection_request, instantiated_ats}} = state,
          :ok
        ) do
      %State{state | connection_state: {:ok, instantiated_ats}}
    end

    def connection_response(
          %State{connection_state: {:awaiting_connection_request, {nil, server_at}}} = state,
          {:hello_new_client, client_at}
        ) do
      %State{state | connection_state: {:ok, {client_at, server_at}}}
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

  @doc """
  Starts the handler with `:proc_lib.spawn_link/3`.
  """
  @impl :ranch_protocol
  def start_link(ref, _socket, ranch_transport, opts) do
    pid = :proc_lib.spawn_link(__MODULE__, :init, [ref, ranch_transport, opts])

    {:ok, pid}
  end

  @doc """
  Publish an event for a Sink client
  """
  def publish(client_id, %Event{} = event, ack_key) do
    case whereis(client_id) do
      nil -> {:error, :no_connection}
      pid -> GenServer.call(pid, {:publish, event, ack_key})
    end
  catch
    :exit, _ ->
      {:error, :no_connection}
  end

  def whereis(client_id) do
    @registry
    |> Registry.lookup(client_id)
    |> case do
      [] -> nil
      [{pid, _}] -> pid
    end
  end

  def connected?(client_id) do
    @registry
    |> Registry.lookup(client_id)
    |> case do
      [] -> false
      [{_pid, _}] -> true
    end
  end

  def get_inflight(client_id) do
    {:ok, client_id |> whereis() |> GenServer.call(:get_inflight)}
  catch
    :exit, _ ->
      {:error, :no_connection}
  end

  def get_received_nacks(client_id) do
    {:ok, client_id |> whereis() |> GenServer.call(:get_received_nacks)}
  catch
    :exit, _ ->
      {:error, :no_connection}
  end

  def get_sent_nacks(client_id) do
    {:ok, client_id |> whereis() |> GenServer.call(:get_sent_nacks)}
  catch
    :exit, _ ->
      {:error, :no_connection}
  end

  @doc """
  Remove an existing connection because the client has reconnected.
  """
  def boot_duplicate(pid) do
    ref = Process.monitor(pid)
    Process.exit(pid, :normal)

    receive do
      {:DOWN, ^ref, :process, ^pid, _} -> :ok
    end
  end

  @doc """
  Initiates the handler, acknowledging the connection was accepted.
  Finally it makes the existing process into a `:gen_server` process and
  enters the `:gen_server` receive loop with `:gen_server.enter_loop/3`.
  """
  def init(ref, ranch_transport, opts) do
    Process.flag(:trap_exit, true)

    {:ok, socket} =
      case ranch_transport do
        ranch_transport when ranch_transport in [:ranch_ssl, :ranch_tcp] -> :ranch.handshake(ref)
        mod -> mod.handshake(ref)
      end

    handler = Keyword.fetch!(opts, :handler)
    ssl_opts = Keyword.fetch!(opts, :ssl_opts)
    transport = Keyword.fetch!(opts, :transport)
    peername = stringify_peername(socket, transport)

    Logger.info(fn ->
      "Peer #{peername} connecting"
    end)

    ssl_opts =
      [:binary] ++
        Keyword.merge(ssl_opts,
          packet: 2,
          active: true
        )

    :ok = ranch_transport.setopts(socket, active: true, packet: 2)

    {:ok, peer_cert} = transport.peercert(socket)

    case handler.authenticate_client(peer_cert) do
      {:ok, client_id} ->
        instantiated_ats = handler.instantiated_ats()

        :ok =
          case Registry.register(@registry, client_id, DateTime.utc_now()) do
            {:ok, _} ->
              :ok

            {:error, {:already_registered, existing_pid}} ->
              boot_duplicate(existing_pid)
              register_when_clear(client_id)
          end

        :ok = handler.up(client_id)

        Sink.Telemetry.start(:connection, %{client_id: client_id, peername: peername})

        state =
          State.init(
            client_id,
            socket,
            transport,
            peername,
            handler,
            ssl_opts,
            instantiated_ats,
            now()
          )

        schedule_check_keepalive(state.stats.keepalive_interval)

        :gen_server.enter_loop(
          __MODULE__,
          [],
          state,
          via_tuple(client_id)
        )

      {:error, exception} ->
        # Log errors in authenticate_client, if needed
        Logger.warn("Connection refused: " <> Exception.message(exception))
        {:error, exception}
    end
  end

  @impl GenServer
  def init(_), do: raise("Not to be started through GenServer.start_link/3.")

  @impl GenServer
  def terminate(reason, %State{} = state) do
    Sink.Telemetry.stop(
      :connection,
      state.stats.start_time,
      %{client_id: state.client_id, peername: state.peername, reason: reason}
    )

    :ok = state.handler.down(state.client_id)

    state
  end

  # Server callbacks

  @impl GenServer
  def handle_call({:ack, message_id}, _from, state) do
    frame = Protocol.encode_frame(:ack, message_id)

    case state.transport.send(state.socket, frame) do
      :ok -> {:reply, :ok, State.log_sent(state, now())}
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
          Sink.Telemetry.publish(:sent, %{
            client_id: state.client_id,
            event_type_id: event.event_type_id
          })

          {:reply, :ok, State.put_inflight(state, ack_key)}

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

  # keepalive

  def handle_info(:tick_check_keepalive, state) do
    schedule_check_keepalive(state.stats.keepalive_interval)

    if State.alive?(state, now()) do
      {:noreply, state}
    else
      {:stop, :normal, state}
    end
  end

  # Response to data

  @impl GenServer
  def handle_info(
        {:ssl, _socket, message},
        %State{client_id: client_id, handler: handler} = state
      ) do
    {new_state, response_message} =
      message
      |> Connection.Protocol.decode_frame()
      |> case do
        {:connection_request, instantiated_ats} ->
          result =
            case check_connection_request(state.connection_state, instantiated_ats) do
              :ok ->
                :ok

              :hello_new_client ->
                {client_at, _} = instantiated_ats
                {:awaiting_connection_request, {_, server_at}} = state.connection_state

                :ok =
                  handler.handle_connection_response(client_id, {:hello_new_client, client_at})

                {:hello_new_client, server_at}
            end

          frame = Connection.Protocol.encode_frame(:connection_response, result)
          {State.connection_response(state, result), {:connection_response, frame}}

        {:ack, message_id} ->
          ack_key = State.find_inflight(state, message_id)
          {event_type_id, _, _} = ack_key
          Sink.Telemetry.ack(:received, %{client_id: client_id, event_type_id: event_type_id})

          # todo: error handling
          # :error ->
          # what to do if we can't ack?
          # rescue ->
          # how do we handle a failed ack?
          :ok = handler.handle_ack(client_id, ack_key)
          {State.remove_inflight(state, message_id), nil}

        {:nack, message_id, payload} ->
          nack_data = Connection.Protocol.decode_payload(:nack, payload)
          ack_key = State.find_inflight(state, message_id)
          new_state = State.put_received_nack(state, message_id, ack_key, nack_data)
          {event_type_id, _, _} = ack_key
          Sink.Telemetry.nack(:received, %{client_id: client_id, event_type_id: event_type_id})

          :ok = handler.handle_nack(client_id, ack_key, nack_data)
          {new_state, nil}

        {:publish, message_id, payload} ->
          event = Connection.Protocol.decode_payload(:publish, payload)

          Sink.Telemetry.publish(:received, %{
            client_id: client_id,
            event_type_id: event.event_type_id
          })

          # send the event to handler
          try do
            handler.handle_publish(client_id, event, message_id)
          catch
            kind, e ->
              formatted = Exception.format(kind, e, __STACKTRACE__)
              Logger.error(formatted)
              {:nack, {<<>>, formatted}}
          end
          |> case do
            :ack ->
              frame = Protocol.encode_frame(:ack, message_id)

              :ok =
                Sink.Connection.Freshness.update(client_id, event.event_type_id, event.timestamp)

              after_send = fn state ->
                Sink.Telemetry.ack(:sent, %{
                  client_id: client_id,
                  event_type_id: event.event_type_id
                })

                state
              end

              {state, {:ack, frame, after_send}}

            {:nack, nack_data} ->
              ack_key = {event.event_type_id, event.key, event.offset}
              payload = Protocol.encode_payload(:nack, nack_data)
              frame = Protocol.encode_frame(:nack, message_id, payload)

              after_send = fn state ->
                Sink.Telemetry.nack(:sent, %{
                  client_id: state.client_id,
                  event_type_id: event.event_type_id
                })

                State.put_sent_nack(state, message_id, ack_key, nack_data)
              end

              {state, {:nack, frame, after_send}}
          end

        :ping ->
          Sink.Telemetry.ping(:received, %{})

          after_send = fn state ->
            Sink.Telemetry.pong(:sent, %{client_id: client_id})
            state
          end

          frame = Sink.Connection.Protocol.encode_frame(:pong)
          {state, {:ping, frame, after_send}}

        :pong ->
          Sink.Telemetry.pong(:received, %{client_id: client_id})
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

  # Connection Management

  def handle_info({:tcp_closed, _}, state) do
    {:stop, :normal, state}
  end

  def handle_info({:ssl_closed, _}, state) do
    {:stop, :normal, state}
  end

  def handle_info({:tcp_error, _, _reason}, state) do
    {:stop, :normal, state}
  end

  def handle_info({:EXIT, _from, :normal}, state) do
    {:stop, :normal, state}
  end

  defp check_connection_request(
         {:awaiting_connection_request, {s_c_at, s_s_at}},
         {c_c_at, c_s_at}
       ) do
    cond do
      {s_s_at, s_c_at} == {c_s_at, c_c_at} ->
        :ok

      # if the server has never seen the client and the client has never seen the server or know what server to expect
      is_nil(s_c_at) && !is_nil(c_c_at) && (is_nil(c_s_at) || s_s_at == c_s_at) ->
        :hello_new_client
    end
  end

  # Helpers

  defp normalize_response_message_tuple({type, message}),
    do: {type, message, &Function.identity/1}

  defp normalize_response_message_tuple(other), do: other

  defp via_tuple(client_id) do
    {:via, Registry, {@registry, client_id}}
  end

  defp stringify_peername(socket, transport) do
    {:ok, {addr, port}} = transport.peername(socket)

    address =
      addr
      |> :inet_parse.ntoa()
      |> to_string()

    "#{address}:#{port}"
  end

  defp register_when_clear(client_id) do
    case Registry.register(@registry, client_id, DateTime.utc_now()) do
      {:ok, _} ->
        :ok

      {:error, {:already_registered, _existing_pid}} ->
        :timer.sleep(10)
        register_when_clear(client_id)
    end
  end

  defp now do
    System.monotonic_time(:millisecond)
  end

  defp schedule_check_keepalive(keepalive_interval) do
    Process.send_after(self(), :tick_check_keepalive, div(keepalive_interval, 2))
  end
end
