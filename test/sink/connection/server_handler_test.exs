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
  alias Sink.Event
  alias Sink.TestEvent

  def handshake(123), do: {:ok, 123}
  def setopts(_socket, _opts), do: :ok

  @mod_transport Sink.Connection.Transport.SSLMock
  @handler Sink.Connection.ServerConnectionHandlerMock
  @sample_state %ServerHandler.State{
    client_id: "test-client",
    socket: 123,
    transport: @mod_transport,
    peername: :fake,
    handler: @handler,
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
  @unix_now 1_618_150_125

  setup :set_mox_from_context
  setup :verify_on_exit!

  describe "new client connecting" do
    test "succeeds" do
      ref = 123
      socket = 123
      transport = __MODULE__

      opts = [
        handler: @handler,
        transport: @mod_transport,
        ssl_opts: []
      ]

      @mod_transport
      |> expect(:peercert, fn _socket -> {:ok, <<1, 2, 3>>} end)
      |> expect(:peername, fn _socket -> {:ok, {{127, 0, 0, 1}, 51380}} end)

      @handler
      |> expect(:authenticate_client, fn _peer_cert -> {:ok, "test-client"} end)
      |> expect(:up, fn "test-client" -> :ok end)
      |> expect(:down, fn "test-client" -> :ok end)

      {:ok, pid} = ServerHandler.start_link(ref, socket, transport, opts)

      :timer.sleep(5)
      assert Process.alive?(pid)

      assert ServerHandler.connected?("test-client")

      # teardown
      Process.exit(pid, :normal)
      :timer.sleep(5)
    end

    test "boots an existing client" do
      ref = 123
      socket = 123
      transport = __MODULE__

      opts = [
        handler: @handler,
        transport: @mod_transport,
        ssl_opts: []
      ]

      @mod_transport
      |> expect(:peercert, 2, fn _socket -> {:ok, <<1, 2, 3>>} end)
      |> expect(:peername, 2, fn _socket -> {:ok, {{127, 0, 0, 1}, 51380}} end)

      @handler
      |> expect(:authenticate_client, 2, fn _peer_cert -> {:ok, "test-client"} end)
      |> expect(:up, 2, fn "test-client" -> :ok end)
      |> expect(:down, 2, fn "test-client" -> :ok end)

      {:ok, pid_og} = ServerHandler.start_link(ref, socket, transport, opts)

      :timer.sleep(5)
      assert Process.alive?(pid_og)

      {:ok, pid_new} = ServerHandler.start_link(ref, socket, transport, opts)
      :timer.sleep(50)
      refute Process.alive?(pid_og)
      assert Process.alive?(pid_new)

      # teardown
      Process.exit(pid_new, :normal)
      :timer.sleep(5)
    end
  end

  describe "receiving (publish)" do
    test "with good data decodes and forwards to handler, then acks" do
      event_type_id = 1
      schema_version = 1
      key = <<1, 2>>
      offset = 42
      event_data = <<9, 8, 7>>
      message_id = 1234

      message = %Event{
        event_type_id: event_type_id,
        key: key,
        offset: offset,
        timestamp: @unix_now,
        event_data: event_data,
        schema_version: schema_version
      }

      payload = Protocol.encode_payload(:publish, message)
      encoded_message = Protocol.encode_frame(:publish, message_id, payload)

      @mod_transport
      |> expect(:send, fn 123, <<52, 210>> -> :ok end)

      expect(
        @handler,
        :handle_publish,
        fn {"test-client", ^event_type_id, ^key},
           ^offset,
           ^schema_version,
           @unix_now,
           ^event_data,
           ^message_id ->
          :ack
        end
      )

      assert {:noreply, new_state} =
               ServerHandler.handle_info({:ssl, :fake, encoded_message}, @sample_state)

      assert 100 = new_state.inflight.next_message_id
      assert 0 != new_state.stats.last_received_at
    end

    test "with a handler returns a nack" do
      event_type_id = 1
      schema_version = 1
      key = <<1, 2>>
      offset = 42
      ack_key = {event_type_id, key, offset}
      event_data = <<9, 8, 7>>
      message_id = 1234

      message = %Event{
        event_type_id: event_type_id,
        key: key,
        offset: offset,
        timestamp: @unix_now,
        event_data: event_data,
        schema_version: schema_version
      }

      payload = Protocol.encode_payload(:publish, message)
      encoded_message = Protocol.encode_frame(:publish, message_id, payload)
      nack_data = {<<0, 0, 0>>, "crash!"}

      @mod_transport
      |> expect(:send, fn 123, frame ->
        response_payload = Protocol.encode_payload(:nack, nack_data)
        expected_nack_frame = Protocol.encode_frame(:nack, message_id, response_payload)

        assert expected_nack_frame == frame
        :ok
      end)

      expect(
        @handler,
        :handle_publish,
        fn {"test-client", ^event_type_id, ^key},
           ^offset,
           ^schema_version,
           @unix_now,
           ^event_data,
           ^message_id ->
          {:nack, nack_data}
        end
      )

      state = @sample_state

      assert {:noreply, new_state} =
               ServerHandler.handle_info({:ssl, :fake, encoded_message}, state)

      assert [{1234, ack_key, nack_data}] == new_state.inflight.sent_nacks
    end

    test "if the SinkHandler raises an error we send a NACK" do
      event_type_id = 1
      schema_version = 1
      key = <<1, 2>>
      offset = 42
      event_data = <<9, 8, 7>>
      message_id = 1234

      message = %Event{
        event_type_id: event_type_id,
        key: key,
        offset: offset,
        timestamp: @unix_now,
        event_data: event_data,
        schema_version: schema_version
      }

      payload = Protocol.encode_payload(:publish, message)
      encoded_message = Protocol.encode_frame(:publish, message_id, payload)

      # expect a NACK
      @mod_transport
      |> expect(:send, fn 123, frame ->
        assert {:nack, ^message_id, nack_payload} = Protocol.decode_frame(frame)
        {machine_message, human_message} = Protocol.decode_payload(:nack, nack_payload)

        assert <<>> == machine_message
        assert human_message =~ "boom"
        assert human_message =~ "ServerHandlerTest"

        :ok
      end)

      expect(
        @handler,
        :handle_publish,
        fn {"test-client", ^event_type_id, ^key},
           ^offset,
           ^schema_version,
           @unix_now,
           ^event_data,
           ^message_id ->
          raise(ArgumentError, message: "boom")
        end
      )

      assert capture_log(fn ->
               assert {:noreply, new_state} =
                        ServerHandler.handle_info(
                          {:ssl, :fake, encoded_message},
                          @sample_state
                        )

               assert 100 == new_state.inflight.next_message_id
               assert 0 != new_state.stats.last_sent_at
               assert 0 != new_state.stats.last_received_at
             end) =~ "boom"
    end

    test "if the SinkHandler throws an error we send a NACK" do
      event_type_id = 1
      schema_version = 1
      key = <<1, 2>>
      offset = 42
      event_data = <<9, 8, 7>>
      message_id = 1234

      message = %Event{
        event_type_id: event_type_id,
        key: key,
        offset: offset,
        timestamp: @unix_now,
        event_data: event_data,
        schema_version: schema_version
      }

      payload = Protocol.encode_payload(:publish, message)
      encoded_message = Protocol.encode_frame(:publish, message_id, payload)

      # expect a NACK
      @mod_transport
      |> expect(:send, fn 123, frame ->
        assert {:nack, ^message_id, nack_payload} = Protocol.decode_frame(frame)
        {machine_message, human_message} = Protocol.decode_payload(:nack, nack_payload)

        assert <<>> == machine_message
        assert human_message =~ "catch!"
        assert human_message =~ "ServerHandlerTest"

        :ok
      end)

      expect(
        @handler,
        :handle_publish,
        fn {"test-client", ^event_type_id, ^key},
           ^offset,
           ^schema_version,
           @unix_now,
           ^event_data,
           ^message_id ->
          throw("catch!")
        end
      )

      assert capture_log(fn ->
               assert {:noreply, new_state} =
                        ServerHandler.handle_info(
                          {:ssl, :fake, encoded_message},
                          @sample_state
                        )

               assert 100 == new_state.inflight.next_message_id
               assert 0 != new_state.stats.last_sent_at
               assert 0 != new_state.stats.last_received_at
             end) =~ "catch!"
    end
  end

  describe "receiving (ack)" do
    test "with good data decodes and forwards to handler" do
      event_type_id = 1
      key = <<1, 2>>
      offset = 42
      ack_key = {event_type_id, key, offset}
      encoded_message = Protocol.encode_frame(:ack, 100)

      @handler
      |> expect(:handle_ack, fn "test-client", ^ack_key ->
        :ok
      end)

      state = ServerHandler.State.put_inflight(@sample_state, ack_key)

      assert {:noreply, new_state} =
               ServerHandler.handle_info({:ssl, :fake, encoded_message}, state)

      assert 101 = new_state.inflight.next_message_id
      assert false == ServerHandler.State.inflight?(new_state, ack_key)
    end
  end

  describe "receiving (ping)" do
    test "returns a pong" do
      encoded_message = Protocol.encode_frame(:ping)

      # expect a pong
      @mod_transport
      |> expect(:send, fn 123, <<96, 0>> -> :ok end)

      assert {:noreply, _new_state} =
               ServerHandler.handle_info({:ssl, :fake, encoded_message}, @sample_state)
    end
  end

  describe "receiving (pong)" do
    test "nothing (for now)" do
      encoded_message = Protocol.encode_frame(:pong)

      assert {:noreply, _new_state} =
               ServerHandler.handle_info({:ssl, :fake, encoded_message}, @sample_state)
    end
  end

  describe "receiving (nack)" do
    test "with good data decodes and forwards to handler" do
      event_type_id = 1
      key = <<1, 2>>
      offset = 42
      message_id = 100
      ack_key = {event_type_id, key, offset}
      nack_data = {<<0, 0, 0>>, "crash!"}

      payload = Protocol.encode_payload(:nack, nack_data)
      encoded_message = Protocol.encode_frame(:nack, message_id, payload)

      @handler
      |> expect(:handle_nack, fn "test-client", ^ack_key, ^nack_data ->
        :ok
      end)

      state =
        @sample_state
        |> ServerHandler.State.put_inflight(ack_key)

      assert {:noreply, new_state} =
               ServerHandler.handle_info({:ssl, :fake, encoded_message}, state)

      assert false == ServerHandler.State.inflight?(new_state, ack_key)
    end
  end

  describe "publish" do
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

    test "returns an {:error, :closed} if the connection closed while sending" do
      event = %TestEvent{key: <<1, 2, 3>>, offset: 1, message: "hi!"}
      event_data = :erlang.term_to_binary(event)
      event_type_id = 1
      ack_key = {event_type_id, event.key, event.offset}
      timestamp = DateTime.to_unix(DateTime.utc_now())

      expect(@mod_transport, :send, fn _, _ -> {:error, :closed} end)

      message = %Event{
        event_type_id: event_type_id,
        key: event.key,
        offset: event.offset,
        timestamp: timestamp,
        event_data: event_data,
        schema_version: 1
      }

      assert {:stop, :normal, {:error, :closed}, state} =
               ServerHandler.handle_call({:publish, message, ack_key}, self(), @sample_state)

      assert state == @sample_state
    end
  end
end
