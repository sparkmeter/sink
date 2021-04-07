defmodule Sink.Connection.ServerHandler do
  @moduledoc """
  Handles client connections.

  Each server connection handles one client connection.
  """
  use GenServer
  require Logger
  alias Sink.Connection
  alias Sink.Connection.Protocol

  @registry __MODULE__
  @mod_transport Application.compile_env!(:sink, :transport)
  # @is_not_test is a short term ugly hack to only call :ranch if we are not in test,
  # this could be replaced with another Mox mock
  @is_not_test @mod_transport == Sink.Connection.Transport.SSL

  defmodule State do
    alias Sink.Connection.{Inflight, Stats}

    defstruct [
      :client_id,
      :socket,
      :transport,
      :peername,
      :handler,
      :ssl_opts,
      :stats,
      :inflight
    ]

    def init(client_id, socket, transport, peername, handler, ssl_opts, now) do
      %State{
        client_id: client_id,
        socket: socket,
        transport: transport,
        peername: peername,
        handler: handler,
        ssl_opts: ssl_opts,
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

    def alive?(%State{} = state, now) do
      Stats.alive?(state.stats, now)
    end

    def put_received_nack(%State{} = state, message_id, ack_key, nack_data) do
      Map.update!(state, :inflight, &Inflight.put_received_nack(&1, message_id, ack_key, nack_data))
    end

    def put_sent_nack(%State{} = state, message_id, ack_key, nack_data) do
      Map.update!(state, :inflight, &Inflight.put_sent_nack(&1, message_id, ack_key, nack_data))
    end

    def received_nacks_by_event_type_id(%State{} = state) do
      Inflight.received_nacks_by_event_type_id(state.inflight)
    end

    def sent_nacks_by_event_type_id(%State{} = state) do
      Inflight.sent_nacks_by_event_type_id(state.inflight)
    end
  end

  # Client

  @doc """
  Starts the handler with `:proc_lib.spawn_link/3`.
  """
  def start_link(ref, socket, transport, opts) do
    pid = :proc_lib.spawn_link(__MODULE__, :init, [{ref, socket, transport, opts}])

    {:ok, pid}
  end

  @doc """
  Send an ACK back to the Sink client for the message_id
  """
  def ack(client_id, message_id) do
    case whereis(client_id) do
      nil -> {:error, :no_connection}
      pid -> GenServer.call(pid, {:ack, message_id})
    end
  end

  @doc """
  Sends a "publish" message to a Sink client
  """
  def publish(client_id, binary, ack_key) do
    case whereis(client_id) do
      nil -> {:error, :no_connection}
      pid -> GenServer.call(pid, {:publish, binary, ack_key})
    end
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

  def get_received_nacks_by_event_type_id(client_id) do
    client_id
    |> whereis()
    |> GenServer.call(:get_received_nacks_by_event_type_id)
  end

  def get_sent_nacks_by_event_type_id(client_id) do
    client_id
    |> whereis()
    |> GenServer.call(:get_sent_nacks_by_event_type_id)
  end

  @doc """
  Remove an existing connection because the client has reconnected.
  """
  def boot_duplicate(pid) do
    if Process.exit(pid, :normal) do
      :ok
    else
      :error
    end
  end

  @doc """
  Initiates the handler, acknowledging the connection was accepted.
  Finally it makes the existing process into a `:gen_server` process and
  enters the `:gen_server` receive loop with `:gen_server.enter_loop/3`.
  """
  @impl true
  def init({ref, socket, transport, opts}) do
    Process.flag(:trap_exit, true)
    handler = opts[:handler]
    ssl_opts = opts[:ssl_opts]
    peername = stringify_peername(socket)

    Logger.info(fn ->
      "Peer #{peername} connecting"
    end)

    # remove this conditional once hack for testing is removed
    if @is_not_test do
      :ok = :ranch.accept_ack(ref)
    end

    ssl_opts =
      [:binary] ++
        Keyword.merge(ssl_opts,
          packet: 2,
          active: true
        )

    :ok = transport.setopts(socket, active: true, packet: 2)

    {:ok, peer_cert} = @mod_transport.peercert(socket)

    case handler.authenticate_client(peer_cert) do
      {:ok, client_id} ->
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

        state = State.init(client_id, socket, transport, peername, handler, ssl_opts, now())
        schedule_check_keepalive(state.stats.keepalive_interval)

        :gen_server.enter_loop(
          __MODULE__,
          [],
          state,
          via_tuple(client_id)
        )

      {:error, :unknown_client} ->
        # todo: log or alert somewhere, maybe do JIT provisioning
        Logger.error("unknown client !")
        {:error, :unknown_client}
    end
  end

  @impl true
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

  @impl true
  def handle_call({:ack, message_id}, _from, state) do
    frame = Protocol.encode_frame(:ack, message_id)
    :ok = @mod_transport.send(state.socket, frame)

    {:reply, :ok, State.log_sent(state, now())}
  end

  def handle_call({:publish, payload, ack_key}, _, state) do
    if State.inflight?(state, ack_key) do
      {:reply, {:error, :inflight}, state}
    else
      encoded = Protocol.encode_frame(:publish, state.inflight.next_message_id, payload)

      :ok = @mod_transport.send(state.socket, encoded)
      {:reply, :ok, State.put_inflight(state, ack_key)}
    end
  end

  def handle_call(:get_received_nacks_by_event_type_id, _from, state) do
    {:reply, State.received_nacks_by_event_type_id(state), state}
  end

  def handle_call(:get_sent_nacks_by_event_type_id, _from, state) do
    {:reply, State.sent_nacks_by_event_type_id(state), state}
  end

  # keepalive

  def handle_info(:tick_check_keepalive, state) do
    schedule_check_keepalive(state.keepalive_interval)

    if State.alive?(state, now()) do
      {:noreply, state}
    else
      {:stop, :normal, state}
    end
  end

  # Response to data

  @impl true
  def handle_info(
        {:ssl, _socket, message},
        %State{client_id: client_id, handler: handler} = state
      ) do
    new_state =
      message
      |> Connection.Protocol.decode_frame()
      |> case do
        {:ack, message_id} ->
          ack_key = State.find_inflight(state, message_id)

          case handler.handle_ack(client_id, ack_key) do
            :ok ->
              State.remove_inflight(state, message_id)
              # todo: error handling
              # :error ->
              # what to do if we can't ack?
              # rescue ->
              # how do we handle a failed ack?
          end

        {:nack, message_id, payload} ->
          nack_data = Connection.Protocol.decode_payload(:nack, payload)
          ack_key = State.find_inflight(state, message_id)
          new_state = State.put_received_nack(state, message_id, ack_key, nack_data)
          {event_type_id, _, _} = ack_key
          Sink.Telemetry.nack(:received, %{client_id: client_id, event_type_id: event_type_id})

          :ok = handler.handle_nack(client_id, ack_key, nack_data)
          new_state

        {:publish, message_id, payload} ->
          {event_type_id, key, offset, event_data} =
            Connection.Protocol.decode_payload(:publish, payload)

          # send the event to handler
          try do
            handler.handle_publish(
              {client_id, event_type_id, key},
              offset,
              event_data,
              message_id
            )
          rescue
            e ->
              formatted = Exception.format(:error, e, __STACKTRACE__)
              Logger.error(formatted)
              {:nack, {<<>>, formatted}}
          end
          |> case do
            :ack ->
              {:reply, :ok, state} = handle_call({:ack, message_id}, self(), state)
              state

            {:nack, nack_data} ->
              ack_key = {event_type_id, key, offset}
              payload = Protocol.encode_payload(:nack, nack_data)
              frame = Protocol.encode_frame(:nack, message_id, payload)
              :ok = @mod_transport.send(state.socket, frame)

              Sink.Telemetry.nack(:sent, %{
                client_id: state.client_id,
                event_type_id: event_type_id
              })

              State.put_sent_nack(state, message_id, ack_key, nack_data)
          end
          |> State.log_sent(now())

        :ping ->
          frame = Sink.Connection.Protocol.encode_frame(:pong)
          :ok = @mod_transport.send(state.socket, frame)
          state

        :pong ->
          state
      end
      |> State.log_received(now())

    {:noreply, new_state}
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

  # Helpers

  defp via_tuple(client_id) do
    {:via, Registry, {@registry, client_id}}
  end

  defp stringify_peername(socket) do
    {:ok, {addr, port}} = @mod_transport.peername(socket)

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
