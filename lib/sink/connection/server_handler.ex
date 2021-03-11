defmodule Sink.Connection.ServerHandler do
  @moduledoc """
  Handles incoming connections
  """
  use GenServer
  require Logger
  alias Sink.Connection

  @registry __MODULE__

  defmodule State do
    defstruct [
      :client_id,
      :socket,
      :transport,
      :peername,
      :handler,
      :next_message_id,
      :ssl_opts,
      :start_time,
      inflight: %{}
    ]

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
  Initiates the handler, acknowledging the connection was accepted.
  Finally it makes the existing process into a `:gen_server` process and
  enters the `:gen_server` receive loop with `:gen_server.enter_loop/3`.
  """
  def init({ref, socket, transport, opts}) do
    handler = opts[:handler]
    ssl_opts = opts[:ssl_opts]
    peername = stringify_peername(socket)

    Logger.info(fn ->
      "Peer #{peername} connecting"
    end)

    :ok = :ranch.accept_ack(ref)

    ssl_opts =
      [:binary] ++
        Keyword.merge(ssl_opts,
          packet: 2,
          active: true
        )

    :ok = transport.setopts(socket, active: true, packet: 2)

    {:ok, peer_cert} = :ssl.peercert(socket)

    case handler.authenticate_client(peer_cert) do
      {:ok, client_id} ->
        {:ok, _} = Registry.register(@registry, client_id, DateTime.utc_now())

        Sink.Telemetry.start(:connection, %{client_id: client_id, peername: peername})

        :gen_server.enter_loop(
          __MODULE__,
          [],
          %State{
            client_id: client_id,
            socket: socket,
            transport: transport,
            peername: peername,
            handler: handler,
            ssl_opts: ssl_opts,
            next_message_id: Connection.next_message_id(nil),
            start_time: System.monotonic_time()
          },
          via_tuple(client_id)
        )

      {:error, :unknown_client} ->
        # todo: log or alert somewhere, maybe do JIT provisioning
        Logger.error("unknown client !")
        {:error, :unknown_client}
    end
  end

  # Server callbacks

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

  # Response to data

  def handle_info(
        {:ssl, _socket, message},
        %State{client_id: client_id, handler: handler} = state
      ) do
    new_state =
      message
      |> Connection.Protocol.decode_frame()
      |> case do
        {:ack, message_id} ->
          {ack_handler, ack_key} = State.find_inflight(state, message_id)

          send(ack_handler, {:ack, client_id, ack_key})

          State.remove_inflight(state, message_id)

        {:publish, message_id, payload} ->
          {event_type_id, key, offset, event_data} =
            Connection.Protocol.decode_payload(:publish, payload)

          # send the event to handler
          pid = Process.whereis(handler)
          send(pid, {:publish, {client_id, event_type_id, key}, offset, event_data, message_id})

          state
      end

    {:noreply, new_state}
  end

  # Connection Management

  def handle_info(
        {:tcp_closed, _},
        %State{client_id: client_id, peername: peername, start_time: start_time} = state
      ) do
    Logger.info(fn ->
      "Peer #{peername} disconnected"
    end)

    Sink.Telemetry.stop(
      :connection,
      start_time,
      %{client_id: client_id, peername: peername, reason: :tcp_closed}
    )

    {:stop, :normal, state}
  end

  def handle_info(
        {:ssl_closed, _},
        %State{client_id: client_id, peername: peername, start_time: start_time} = state
      ) do
    Logger.info(fn ->
      "SSL Peer #{peername} disconnected"
    end)

    Sink.Telemetry.stop(
      :connection,
      start_time,
      %{client_id: client_id, peername: peername, reason: :ssl_closed}
    )

    {:stop, :normal, state}
  end

  def handle_info(
        {:tcp_error, _, reason},
        %State{client_id: client_id, peername: peername, start_time: start_time} = state
      ) do
    Logger.info(fn ->
      "Error with peer #{peername}: #{inspect(reason)}"
    end)

    Sink.Telemetry.stop(
      :connection,
      start_time,
      %{client_id: client_id, peername: peername, reason: :tcp_error}
    )

    {:stop, :normal, state}
  end

  # Helpers

  defp via_tuple(client_id) do
    {:via, Registry, {@registry, client_id}}
  end

  defp stringify_peername(socket) do
    {:ok, {addr, port}} = :ssl.peername(socket)

    address =
      addr
      |> :inet_parse.ntoa()
      |> to_string()

    "#{address}:#{port}"
  end
end
