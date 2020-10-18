defmodule Sink.Connection.ServerHandler do
  @moduledoc """
  Handles incoming connections
  """
  use GenServer
  require Logger
  alias Sink.Connection

  defmodule State do
    defstruct [
      :client_id,
      :socket,
      :transport,
      :peername,
      :handler,
      :next_message_id,
      :ssl_opts,
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

  def publish(pid, binary, ack_key) do
    GenServer.call(pid, {:publish, binary, ack_key})
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
    :ok = transport.setopts(socket, [:binary, active: true, packet: 2])

    ssl_opts =
      [:binary] ++
        Keyword.merge(ssl_opts,
          packet: 2,
          active: true
        )

    {:ok, new_socket} = :ranch_ssl.handshake(socket, ssl_opts, 5000)
    {:ok, peer_cert} = :ssl.peercert(new_socket)

    case handler.authenticate_client(peer_cert) do
      {:ok, client_id} ->
        process_name = String.to_atom("sink-#{client_id}")
        Process.register(self(), process_name)

        :gen_server.enter_loop(
          __MODULE__,
          [],
          %State{
            client_id: client_id,
            socket: new_socket,
            transport: transport,
            peername: peername,
            handler: handler,
            next_message_id: Connection.next_message_id(nil)
          },
          {:local, process_name}
        )

      {:error, :unknown_client} ->
        # todo: log or alert somewhere, maybe do JIT provisioning
        Logger.error("unknown client !")
        {:error, :unknown_client}
    end
  end

  # Server callbacks

  def handle_call({:publish, binary, ack_key}, {from, _}, state) do
    if State.inflight?(state, ack_key) do
      {:reply, {:error, :inflight}, state}
    else
      encoded = Connection.encode_publish(state.next_message_id, binary)
      :ok = :ssl.send(state.socket, encoded)
      {:reply, :ok, State.put_inflight(state, {from, ack_key})}
    end
  end

  # Response to data

  def handle_info(
        {:ssl, socket, message},
        %State{client_id: client_id} = state
      ) do
    new_state =
      message
      |> Connection.decode_message()
      |> case do
        {:ack, message_id} ->
          {ack_handler, ack_key} = State.find_inflight(state, message_id)

          send(ack_handler, {:ack, client_id, ack_key})

          State.remove_inflight(state, message_id)

        {:publish, message_id, _event} ->
          # write to log and send ack

          # send event to handler

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

    {:stop, :normal, state}
  end

  def handle_info({:ssl_closed, _}, %State{peername: peername} = state) do
    Logger.info(fn ->
      "SSL Peer #{peername} disconnected"
    end)

    {:stop, :normal, state}
  end

  def handle_info({:tcp_error, _, reason}, %State{peername: peername} = state) do
    Logger.info(fn ->
      "Error with peer #{peername}: #{inspect(reason)}"
    end)

    {:stop, :normal, state}
  end

  # Helpers

  defp stringify_peername(socket) do
    {:ok, {addr, port}} = :inet.peername(socket)

    address =
      addr
      |> :inet_parse.ntoa()
      |> to_string()

    "#{address}:#{port}"
  end
end
