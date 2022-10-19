defmodule Sink.Connection.ClientConnectionTest do
  @moduledoc """
  Note: we're using sleeps here, which is brittle and not recommended.

  Consider using something like https://github.com/well-ironed/liveness or some
  Process.Monitor magic.
  """
  use ExUnit.Case, async: false
  import ExUnit.CaptureLog
  import Mox
  alias Sink.Connection.ClientConnection
  alias Sink.Connection.Inflight
  alias Sink.Connection.Protocol
  alias Sink.Connection.Stats
  alias Sink.Connection.ClientConnectionHandlerMock
  alias Sink.Connection.Client.ConnectionStatus
  alias Sink.Event
  alias Sink.TestEvent
  alias Sink.Connection.Transport.SSLMock, as: TransportMock

  @protocol_version 8
  @unix_now 1_618_150_125

  setup :set_mox_from_context
  setup :verify_on_exit!

  defmodule DummyConnectionHandler do
    @behaviour Sink.Connection.ClientConnectionHandler

    def last_server_identifier, do: 1
    def application_version, do: "1.0.0"
    def handle_connection_response(_), do: :ok
    def handle_ack(_), do: :ok
    def handle_nack(_, _), do: :ok
    def handle_publish(_, _), do: :ack
    def down(), do: :ok
  end

  describe "init" do
    test "sends a connection request async after startup" do
      stub_with(ClientConnectionHandlerMock, DummyConnectionHandler)

      # check for connection request
      expect(TransportMock, :send, fn _, frame ->
        assert {:connection_request, @protocol_version, {"1.0.0", 1}} =
                 Protocol.decode_frame(frame)

        :ok
      end)

      {:ok, _} =
        start_supervised(
          {ClientConnection,
           socket: <<123>>, handler: ClientConnectionHandlerMock, transport: TransportMock}
        )

      # Wait for async startup to have finished
      ClientConnection.connected?()
    end
  end

  describe "receiving" do
    setup do
      state = %ClientConnection.State{
        socket: 123,
        peername: :fake,
        handler: ClientConnectionHandlerMock,
        transport: TransportMock,
        connection_status: ConnectionStatus.init(1),
        inflight: %Inflight{next_message_id: 100},
        stats: %Stats{
          last_sent_at: 0,
          last_received_at: 0,
          keepalive_interval: 60_000,
          start_time: 0
        }
      }

      {:ok, state: state}
    end

    test "a publish with good data decodes and forwards to handler, then acks", %{state: state} do
      event_type_id = 1
      schema_version = 1
      key = <<1, 2>>
      offset = 42
      event_data = <<9, 8, 7>>
      message_id = 1234

      event = %Event{
        event_type_id: event_type_id,
        key: key,
        offset: offset,
        timestamp: @unix_now,
        event_data: event_data,
        schema_version: schema_version
      }

      payload = Protocol.encode_payload(:publish, event)
      encoded_message = Protocol.encode_frame({:publish, message_id, payload})

      expect(TransportMock, :send, fn socket, payload ->
        assert 123 == socket
        assert <<52, 210>> == payload
        :ok
      end)

      expect(ClientConnectionHandlerMock, :handle_publish, fn ^event, ^message_id -> :ack end)

      assert {:noreply, new_state} =
               ClientConnection.handle_info({:ssl, :fake, encoded_message}, state)

      assert 100 == new_state.inflight.next_message_id
      assert 0 != new_state.stats.last_sent_at
      assert 0 != new_state.stats.last_received_at
    end

    test "if the SinkHandler raises an error we send a NACK", %{state: state} do
      event = %Event{
        event_type_id: 1,
        key: <<1, 2>>,
        offset: 42,
        timestamp: @unix_now,
        event_data: <<9, 8, 7>>,
        schema_version: 1
      }

      message_id = 1234

      payload = Protocol.encode_payload(:publish, event)
      encoded_message = Protocol.encode_frame({:publish, message_id, payload})

      # expect a NACK
      expect(TransportMock, :send, fn 123, frame ->
        assert {:nack, ^message_id, nack_payload} = Protocol.decode_frame(frame)
        {machine_message, human_message} = Protocol.decode_payload(:nack, nack_payload)

        assert <<>> == machine_message
        assert human_message =~ "boom"
        assert human_message =~ "ClientConnectionTest"

        :ok
      end)

      expect(ClientConnectionHandlerMock, :handle_publish, fn ^event, ^message_id ->
        raise(ArgumentError, message: "boom")
      end)

      assert capture_log(fn ->
               assert {:noreply, new_state} =
                        ClientConnection.handle_info({:ssl, :fake, encoded_message}, state)

               assert 100 == new_state.inflight.next_message_id
               assert 0 != new_state.stats.last_sent_at
               assert 0 != new_state.stats.last_received_at
             end) =~ "boom"
    end

    test "if the SinkHandler throws an error we send a NACK", %{state: state} do
      event = %Event{
        event_type_id: 1,
        key: <<1, 2>>,
        offset: 42,
        timestamp: @unix_now,
        event_data: <<9, 8, 7>>,
        schema_version: 1
      }

      message_id = 1234

      payload = Protocol.encode_payload(:publish, event)
      encoded_message = Protocol.encode_frame({:publish, message_id, payload})

      # expect a NACK
      expect(TransportMock, :send, fn 123, frame ->
        assert {:nack, ^message_id, nack_payload} = Protocol.decode_frame(frame)
        {machine_message, human_message} = Protocol.decode_payload(:nack, nack_payload)

        assert <<>> == machine_message
        assert human_message =~ "catch!"
        assert human_message =~ "ClientConnectionTest"

        :ok
      end)

      expect(ClientConnectionHandlerMock, :handle_publish, fn ^event, ^message_id ->
        throw("catch!")
      end)

      assert capture_log(fn ->
               assert {:noreply, new_state} =
                        ClientConnection.handle_info(
                          {:ssl, :fake, encoded_message},
                          state
                        )

               assert 100 == new_state.inflight.next_message_id
               assert 0 != new_state.stats.last_sent_at
               assert 0 != new_state.stats.last_received_at
             end) =~ "catch!"
    end

    test "an ack with good data decodes and forwards to handler", %{state: state} do
      event_type_id = 1
      key = <<1, 2>>
      offset = 42
      ack_key = {event_type_id, key, offset}
      encoded_message = Protocol.encode_frame({:ack, 100})

      expect(ClientConnectionHandlerMock, :handle_ack, 1, fn ^ack_key ->
        :ok
      end)

      state = ClientConnection.State.put_inflight(state, ack_key)

      assert {:noreply, new_state} =
               ClientConnection.handle_info({:ssl, :fake, encoded_message}, state)

      assert 101 = new_state.inflight.next_message_id
      assert false == ClientConnection.State.inflight?(new_state, ack_key)
      assert 0 == new_state.stats.last_sent_at
      assert 0 != new_state.stats.last_received_at
    end

    test "a nack with good data decodes and forwards to handler", %{state: state} do
      event_type_id = 1
      key = <<1, 2>>
      offset = 42
      message_id = 100
      ack_key = {event_type_id, key, offset}
      nack_data = {<<0, 0, 0>>, "crash!"}

      payload = Protocol.encode_payload(:nack, nack_data)
      encoded_message = Protocol.encode_frame({:nack, message_id, payload})

      expect(ClientConnectionHandlerMock, :handle_nack, fn ^ack_key, ^nack_data ->
        :ok
      end)

      state = ClientConnection.State.put_inflight(state, ack_key)

      assert {:noreply, new_state} =
               ClientConnection.handle_info({:ssl, :fake, encoded_message}, state)

      assert false == ClientConnection.State.inflight?(new_state, ack_key)
    end

    test "a ping returns a pong and increases last_sent_at", %{state: state} do
      encoded_message = Protocol.encode_frame(:ping)

      # expect a pong
      expect(TransportMock, :send, fn 123, <<96, 0>> -> :ok end)

      assert {:noreply, new_state} =
               ClientConnection.handle_info({:ssl, :fake, encoded_message}, state)

      assert 0 != new_state.stats.last_sent_at
      assert 0 != new_state.stats.last_received_at
    end

    test "a pong increases last_received_at", %{state: state} do
      encoded_message = Protocol.encode_frame(:pong)

      assert {:noreply, new_state} =
               ClientConnection.handle_info({:ssl, :fake, encoded_message}, state)

      assert 0 == new_state.stats.last_sent_at
      assert 0 != new_state.stats.last_received_at
    end
  end

  describe "publishing" do
    setup do
      state = %ClientConnection.State{
        socket: 123,
        peername: :fake,
        handler: ClientConnectionHandlerMock,
        transport: TransportMock,
        connection_status: ConnectionStatus.init(1),
        inflight: %Inflight{next_message_id: 100},
        stats: %Stats{
          last_sent_at: 0,
          last_received_at: 0,
          keepalive_interval: 60_000,
          start_time: 0
        }
      }

      {:ok, state: state}
    end

    test "if the socket is closed when sending", %{state: state} do
      event = %TestEvent{key: <<1, 2, 3>>, offset: 1, message: "hi!"}
      event_data = :erlang.term_to_binary(event)
      event_type_id = 1
      ack_key = {event_type_id, event.key, event.offset}
      timestamp = DateTime.to_unix(DateTime.utc_now())

      # return a closed error
      expect(TransportMock, :send, fn _, _ -> {:error, :closed} end)

      message = %Event{
        event_type_id: event_type_id,
        key: event.key,
        offset: event.offset,
        timestamp: timestamp,
        event_data: event_data,
        schema_version: 1
      }

      connection_status =
        1
        |> ConnectionStatus.init()
        |> ConnectionStatus.connection_response(:connected)

      state = %ClientConnection.State{state | connection_status: connection_status}

      assert {:stop, :normal, {:error, :closed}, new_state} =
               ClientConnection.handle_call({:publish, message, ack_key}, self(), state)

      assert new_state == state
    end
  end

  describe "terminate" do
    test "does not call handler.down() if the connection response wasn't received" do
      stub(ClientConnectionHandlerMock, :last_server_identifier, fn -> 1 end)
      stub(ClientConnectionHandlerMock, :application_version, fn -> "1.0.0" end)
      expect(TransportMock, :send, fn _, _ -> :ok end)

      {:ok, connection} =
        ClientConnection.start_link(
          socket: <<123>>,
          handler: ClientConnectionHandlerMock,
          transport: TransportMock
        )

      Process.exit(connection, :normal)

      stop_connection(connection)
    end
  end

  defp stop_connection(pid) do
    ref = Process.monitor(pid)
    Process.exit(pid, :normal)

    receive do
      {:DOWN, ^ref, :process, ^pid, _reason} -> :ok
    end
  end
end
