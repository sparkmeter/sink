defmodule Sink.Connection.ServerHandlerTest do
  @moduledoc """
  Note: we're using sleeps here, which is brittle and not recommended.

  Consider using something like https://github.com/well-ironed/liveness or some
  Process.Monitor magic.
  """
  use ExUnit.Case, async: false
  import ExUnit.CaptureLog
  import Mox
  alias Sink.Connection.{Inflight, Protocol, ServerHandler, Stats}
  alias Sink.Connection.Server.ConnectionStatus
  alias Sink.Event
  alias Sink.TestEvent
  alias Sink.Connection.Transport.SSLMock, as: TransportMock
  alias Sink.Connection.ServerConnectionHandlerMock

  def handshake(123), do: {:ok, 123}
  def setopts(_socket, _opts), do: :ok

  @unix_now 1_618_150_125

  setup :set_mox_from_context
  setup :verify_on_exit!

  describe "running a ServerHandler" do
    test "an established tcp connection is not considered connected yet" do
      ref = 123
      socket = 123
      transport = __MODULE__

      TransportMock
      |> expect(:peercert, fn _socket -> {:ok, <<1, 2, 3>>} end)
      |> expect(:peername, fn _socket -> {:ok, {{127, 0, 0, 1}, 51380}} end)

      ServerConnectionHandlerMock
      |> expect(:authenticate_client, fn _peer_cert -> {:ok, "test-client"} end)
      |> expect(:client_configuration, fn "test-client" -> {:ok, 1} end)

      start_opts = [
        ref,
        socket,
        transport,
        [handler: ServerConnectionHandlerMock, transport: TransportMock, ssl_opts: []]
      ]

      {:ok, pid} = start_supervised({ServerHandler, start_opts})

      # Wait for async startup to complete
      await_async_startup(pid)

      assert false == ServerHandler.connected?("test-client")
    end

    test "drops an existing client connection when a fresh one comes in" do
      ref = 123
      socket = 123
      transport = __MODULE__

      TransportMock
      |> expect(:peercert, 2, fn _socket -> {:ok, <<1, 2, 3>>} end)
      |> expect(:peername, 2, fn _socket -> {:ok, {{127, 0, 0, 1}, 51380}} end)

      ServerConnectionHandlerMock
      |> expect(:authenticate_client, 2, fn _peer_cert -> {:ok, "test-client"} end)
      |> expect(:client_configuration, 2, fn "test-client" -> {:ok, 1} end)

      start_opts = [
        ref,
        socket,
        transport,
        [handler: ServerConnectionHandlerMock, transport: TransportMock, ssl_opts: []]
      ]

      {:ok, pid_first} =
        {ServerHandler, start_opts}
        |> Supervisor.child_spec(id: ServerHandlerOld, restart: :temporary)
        |> start_supervised()

      # Wait for async startup to complete
      await_async_startup(pid_first)

      {:ok, pid_new} = start_supervised({ServerHandler, start_opts})

      # Wait for async startup to complete
      await_async_startup(pid_new)

      refute Process.alive?(pid_first)
      assert Process.alive?(pid_new)
    end
  end

  describe "receiving" do
    setup :sample_state

    test "unsupported protocol version", %{state: state} do
      test = self()

      expect(TransportMock, :send, fn 123, frame ->
        send(test, {:send, frame})
        :ok
      end)

      expect(
        ServerConnectionHandlerMock,
        :handle_connection_response,
        fn "test-client", {:unsupported_protocol_version, 11} ->
          :ok
        end
      )

      # pretend we have a connection request from protocol version 11
      encoded_message = <<11>> <> <<1, 2, 3>>

      assert {:stop, _new_state, _} =
               ServerHandler.handle_info({:ssl, :fake, encoded_message}, state)

      assert_received {:send, resp}

      assert resp ==
               Protocol.encode_frame({:connection_response, {:unsupported_protocol_version, 11}})
    end
  end

  describe "receiving (publish)" do
    setup :sample_state

    test "with good data decodes and forwards to handler, then acks", %{state: state} do
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

      expect(TransportMock, :send, fn 123, <<52, 210>> -> :ok end)

      expect(
        ServerConnectionHandlerMock,
        :handle_publish,
        fn "test-client", ^event, ^message_id -> :ack end
      )

      assert {:noreply, new_state} =
               ServerHandler.handle_info({:ssl, :fake, encoded_message}, state)

      assert 100 = new_state.inflight.next_message_id
      assert 0 != new_state.stats.last_received_at
    end

    test "with a handler returns a nack", %{state: state} do
      event = %Event{
        event_type_id: 1,
        key: <<1, 2>>,
        offset: 42,
        timestamp: @unix_now,
        event_data: <<9, 8, 7>>,
        schema_version: 1
      }

      ack_key = {event.event_type_id, event.key, event.offset}
      message_id = 1234

      payload = Protocol.encode_payload(:publish, event)
      encoded_message = Protocol.encode_frame({:publish, message_id, payload})
      nack_data = {<<0, 0, 0>>, "crash!"}

      TransportMock
      |> expect(:send, fn 123, frame ->
        response_payload = Protocol.encode_payload(:nack, nack_data)
        expected_nack_frame = Protocol.encode_frame({:nack, message_id, response_payload})

        assert expected_nack_frame == frame
        :ok
      end)

      expect(
        ServerConnectionHandlerMock,
        :handle_publish,
        fn "test-client", ^event, ^message_id -> {:nack, nack_data} end
      )

      assert {:noreply, new_state} =
               ServerHandler.handle_info({:ssl, :fake, encoded_message}, state)

      assert [{1234, ack_key, nack_data}] == new_state.inflight.sent_nacks
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
      test = self()

      payload = Protocol.encode_payload(:publish, event)
      encoded_message = Protocol.encode_frame({:publish, message_id, payload})

      # expect a NACK
      expect(TransportMock, :send, fn 123, frame ->
        send(test, {:send, frame})
        :ok
      end)

      expect(
        ServerConnectionHandlerMock,
        :handle_publish,
        fn "test-client", ^event, ^message_id ->
          raise(ArgumentError, message: "boom")
        end
      )

      assert capture_log(fn ->
               assert {:noreply, new_state} =
                        ServerHandler.handle_info(
                          {:ssl, :fake, encoded_message},
                          state
                        )

               assert 100 == new_state.inflight.next_message_id
               assert 0 != new_state.stats.last_sent_at
               assert 0 != new_state.stats.last_received_at
             end) =~ "boom"

      assert_received {:send, frame}
      assert {:nack, ^message_id, nack_payload} = Protocol.decode_frame(frame)
      {machine_message, human_message} = Protocol.decode_payload(:nack, nack_payload)

      assert <<>> == machine_message
      assert human_message =~ "boom"
      assert human_message =~ "ServerHandlerTest"
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
      expect(TransportMock, :send, fn 123, _frame ->
        :ok
      end)

      expect(
        ServerConnectionHandlerMock,
        :handle_publish,
        fn "test-client", ^event, ^message_id -> throw("catch!") end
      )

      assert capture_log(fn ->
               assert {:noreply, new_state} =
                        ServerHandler.handle_info(
                          {:ssl, :fake, encoded_message},
                          state
                        )

               assert 100 == new_state.inflight.next_message_id
               assert 0 != new_state.stats.last_sent_at
               assert 0 != new_state.stats.last_received_at
             end) =~ "catch!"
    end
  end

  describe "receiving (ack)" do
    setup :sample_state

    test "with good data decodes and forwards to handler", %{state: state} do
      event_type_id = 1
      key = <<1, 2>>
      offset = 42
      ack_key = {event_type_id, key, offset}
      encoded_message = Protocol.encode_frame({:ack, 100})

      ServerConnectionHandlerMock
      |> expect(:handle_ack, fn "test-client", ^ack_key ->
        :ok
      end)

      state = ServerHandler.State.put_inflight(state, ack_key)

      assert {:noreply, new_state} =
               ServerHandler.handle_info({:ssl, :fake, encoded_message}, state)

      assert 101 = new_state.inflight.next_message_id
      assert false == ServerHandler.State.inflight?(new_state, ack_key)
    end
  end

  describe "receiving (ping)" do
    setup :sample_state

    test "returns a pong", %{state: state} do
      encoded_message = Protocol.encode_frame(:ping)

      # expect a pong
      TransportMock
      |> expect(:send, fn 123, <<96, 0>> -> :ok end)

      assert {:noreply, _new_state} =
               ServerHandler.handle_info({:ssl, :fake, encoded_message}, state)
    end
  end

  describe "receiving (pong)" do
    setup :sample_state

    test "nothing (for now)", %{state: state} do
      encoded_message = Protocol.encode_frame(:pong)

      assert {:noreply, _new_state} =
               ServerHandler.handle_info({:ssl, :fake, encoded_message}, state)
    end
  end

  describe "receiving (nack)" do
    setup :sample_state

    test "with good data decodes and forwards to handler", %{state: state} do
      event_type_id = 1
      key = <<1, 2>>
      offset = 42
      message_id = 100
      ack_key = {event_type_id, key, offset}
      nack_data = {<<0, 0, 0>>, "crash!"}

      payload = Protocol.encode_payload(:nack, nack_data)
      encoded_message = Protocol.encode_frame({:nack, message_id, payload})

      expect(
        ServerConnectionHandlerMock,
        :handle_nack,
        fn "test-client", ^ack_key, ^nack_data ->
          :ok
        end
      )

      state = ServerHandler.State.put_inflight(state, ack_key)

      assert {:noreply, new_state} =
               ServerHandler.handle_info({:ssl, :fake, encoded_message}, state)

      assert false == ServerHandler.State.inflight?(new_state, ack_key)
    end
  end

  describe "publish" do
    setup :sample_state

    test "sends an {:error, :no_connection} if there is no connection" do
      message = %Event{
        event_type_id: 1,
        key: <<1, 2>>,
        offset: 1,
        timestamp: 1_618_150_125,
        event_data: "Hi",
        schema_version: 3
      }

      assert {:error, :no_connection} == ServerHandler.publish("fake", message, {1, <<>>, 3})
    end

    test "returns an {:error, :closed} if the connection closed while sending", %{state: state} do
      event = %TestEvent{key: <<1, 2, 3>>, offset: 1, message: "hi!"}
      event_data = :erlang.term_to_binary(event)
      event_type_id = 1
      ack_key = {event_type_id, event.key, event.offset}
      timestamp = DateTime.to_unix(DateTime.utc_now())

      expect(TransportMock, :send, fn _, _ -> {:error, :closed} end)

      message = %Event{
        event_type_id: event_type_id,
        key: event.key,
        offset: event.offset,
        timestamp: timestamp,
        event_data: event_data,
        schema_version: 1
      }

      state = Map.update!(state, :connection_status, &Map.put(&1, :connection_state, :connected))

      assert {:stop, :normal, {:error, :closed}, new_state} =
               ServerHandler.handle_call({:publish, message, ack_key}, self(), state)

      assert new_state == state
    end
  end

  # TODO remove me after connection request is deployed
  describe "connection without connection request" do
    setup do
      existing = Application.get_env(:sink, :require_connection_request)
      Application.put_env(:sink, :require_connection_request, false)

      on_exit(fn ->
        Application.put_env(:sink, :require_connection_request, existing)
      end)

      :ok
    end

    setup :sample_state

    test "sends an event", %{state: state} do
      event = %TestEvent{key: <<1, 2, 3>>, offset: 1, message: "hi!"}
      event_type_id = 1
      ack_key = {event_type_id, event.key, event.offset}

      message = %Event{
        event_type_id: 1,
        key: <<1, 2>>,
        offset: 1,
        timestamp: 1_618_150_125,
        event_data: "Hi",
        schema_version: 3
      }

      expect(TransportMock, :send, fn _, _ -> :ok end)

      # assert :ok == ServerHandler.publish("fake", message, {1, <<>>, 3})
      ServerHandler.handle_call({:publish, message, ack_key}, self(), state)
    end
  end

  defp sample_state(_) do
    state = %ServerHandler.State{
      client: "test-client",
      socket: 123,
      transport: TransportMock,
      peername: :fake,
      connection_status: ConnectionStatus.init({:ok, 1}),
      handler: ServerConnectionHandlerMock,
      ssl_opts: :fake,
      inflight: %Inflight{
        next_message_id: 100
      },
      stats: %Stats{
        last_sent_at: 0,
        last_received_at: 0,
        keepalive_interval: 60_000,
        start_time: 0
      }
    }

    {:ok, state: state}
  end

  defp await_async_startup(pid) do
    # This is only handled after startup completed
    :sys.get_state(pid)
  end
end
