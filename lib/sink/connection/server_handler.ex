defmodule Sink.Connection.ServerHandler do
  @moduledoc """
  Handles incoming connections
  """
  use GenServer
  require Logger
  alias Sink.Connection

  @registry __MODULE__
  @mod_transport Application.compile_env!(:sink, :transport)
  # @is_not_test is a short term ugly hack to only call :ranch if we are not in test,
  # this could be replaced with another Mox mock
  @is_not_test @mod_transport == Sink.Connection.Transport.SSL

  defmodule State do
    defstruct [
      :client_id,
      :socket,
      :transport,
      :peername,
      :handler,
      :next_message_id,
      :ssl_opts,
      :last_received_at,
      :keepalive_interval,
      :start_time,
      inflight: %{}
    ]

    @default_keepalive_interval 60_000

    def init(client_id, socket, transport, peername, handler, ssl_opts, now) do
      %State{
        client_id: client_id,
        socket: socket,
        transport: transport,
        peername: peername,
        handler: handler,
        ssl_opts: ssl_opts,
        next_message_id: Sink.Connection.next_message_id(nil),
        start_time: now,
        last_received_at: now,
        keepalive_interval:
          Application.get_env(:sink, :keepalive_interval, @default_keepalive_interval)
      }
    end

    def put_inflight(
          %State{inflight: inflight, next_message_id: next_message_id} = state,
          {ack_from, ack_key}
        ) do
      state
      |> Map.put(:inflight, Map.put(inflight, next_message_id, {ack_from, ack_key}))
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

    def log_received(%State{} = state, now) do
      %State{state | last_received_at: now}
    end

    def alive?(%State{} = state, now) do
      state.keepalive_interval * 1.5 > now - state.last_received_at
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
        schedule_check_keepalive(state.keepalive_interval)

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
  def terminate(
        reason,
        %State{client_id: client_id, peername: peername, start_time: start_time, handler: handler} =
          state
      ) do
    Sink.Telemetry.stop(
      :connection,
      start_time,
      %{client_id: client_id, peername: peername, reason: reason}
    )

    :ok = handler.down(client_id)

    state
  end

  # Server callbacks

  @impl true
  def handle_call({:ack, message_id}, _from, state) do
    frame = Sink.Connection.Protocol.encode_frame(:ack, message_id, <<>>)
    :ok = @mod_transport.send(state.socket, frame)

    {:reply, :ok, state}
  end

  def handle_call({:publish, payload, ack_key}, {from, _}, state) do
    if State.inflight?(state, ack_key) do
      {:reply, {:error, :inflight}, state}
    else
      encoded = Sink.Connection.Protocol.encode_frame(:publish, state.next_message_id, payload)

      :ok = @mod_transport.send(state.socket, encoded)
      {:reply, :ok, State.put_inflight(state, {from, ack_key})}
    end
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
          {ack_from, ack_key} = State.find_inflight(state, message_id)

          case handler.handle_ack(ack_from, client_id, ack_key) do
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

          # send the event to handler
          case handler.handle_publish(
                 {client_id, event_type_id, key},
                 offset,
                 event_data,
                 message_id
               ) do
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
