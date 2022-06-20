defmodule Sink.Connection.ClientConnectionTest do
  @moduledoc """
  Note: we're using sleeps here, which is brittle and not recommended.

  Consider using something like https://github.com/well-ironed/liveness or some
  Process.Monitor magic.
  """
  use ExUnit.Case, async: false
  import ExUnit.CaptureLog
  import Mox
  alias Sink.Connection.{ClientConnection, Inflight, Protocol, Stats}
  alias Sink.Event
  alias Sink.TestEvent

  def setopts(_socket, _opts), do: :ok

  @mod_transport Sink.Connection.Transport.SSLMock
  @handler Sink.Connection.ClientConnectionHandlerMock
  @sample_state %ClientConnection.State{
    socket: 123,
    peername: :fake,
    handler: @handler,
    transport: @mod_transport,
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

  describe "receiving" do
    test "a publish with good data decodes and forwards to handler, then acks" do
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
      encoded_message = Protocol.encode_frame(:publish, message_id, payload)

      @mod_transport
      |> expect(:send, fn socket, payload ->
        assert 123 == socket
        assert <<52, 210>> == payload
        :ok
      end)

      expect(
        @handler,
        :handle_publish,
        fn ^event, ^message_id -> :ack end
      )

      assert {:noreply, new_state} =
               ClientConnection.handle_info({:ssl, :fake, encoded_message}, @sample_state)

      assert 100 == new_state.inflight.next_message_id
      assert 0 != new_state.stats.last_sent_at
      assert 0 != new_state.stats.last_received_at
    end

    test "if the SinkHandler raises an error we send a NACK" do
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
      encoded_message = Protocol.encode_frame(:publish, message_id, payload)

      # expect a NACK
      @mod_transport
      |> expect(:send, fn 123, frame ->
        assert {:nack, ^message_id, nack_payload} = Protocol.decode_frame(frame)
        {machine_message, human_message} = Protocol.decode_payload(:nack, nack_payload)

        assert <<>> == machine_message
        assert human_message =~ "boom"
        assert human_message =~ "ClientConnectionTest"

        :ok
      end)

      expect(
        @handler,
        :handle_publish,
        fn ^event, ^message_id ->
          raise(ArgumentError, message: "boom")
        end
      )

      assert capture_log(fn ->
               assert {:noreply, new_state} =
                        ClientConnection.handle_info(
                          {:ssl, :fake, encoded_message},
                          @sample_state
                        )

               assert 100 == new_state.inflight.next_message_id
               assert 0 != new_state.stats.last_sent_at
               assert 0 != new_state.stats.last_received_at
             end) =~ "boom"
    end

    test "if the SinkHandler throws an error we send a NACK" do
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
      encoded_message = Protocol.encode_frame(:publish, message_id, payload)

      # expect a NACK
      @mod_transport
      |> expect(:send, fn 123, frame ->
        assert {:nack, ^message_id, nack_payload} = Protocol.decode_frame(frame)
        {machine_message, human_message} = Protocol.decode_payload(:nack, nack_payload)

        assert <<>> == machine_message
        assert human_message =~ "catch!"
        assert human_message =~ "ClientConnectionTest"

        :ok
      end)

      expect(
        @handler,
        :handle_publish,
        fn ^event, ^message_id -> throw("catch!") end
      )

      assert capture_log(fn ->
               assert {:noreply, new_state} =
                        ClientConnection.handle_info(
                          {:ssl, :fake, encoded_message},
                          @sample_state
                        )

               assert 100 == new_state.inflight.next_message_id
               assert 0 != new_state.stats.last_sent_at
               assert 0 != new_state.stats.last_received_at
             end) =~ "catch!"
    end

    test "an ack with good data decodes and forwards to handler" do
      event_type_id = 1
      key = <<1, 2>>
      offset = 42
      ack_key = {event_type_id, key, offset}
      encoded_message = Protocol.encode_frame(:ack, 100)

      @handler
      |> expect(:handle_ack, 1, fn ^ack_key ->
        :ok
      end)

      state = ClientConnection.State.put_inflight(@sample_state, ack_key)

      assert {:noreply, new_state} =
               ClientConnection.handle_info({:ssl, :fake, encoded_message}, state)

      assert 101 = new_state.inflight.next_message_id
      assert false == ClientConnection.State.inflight?(new_state, ack_key)
      assert 0 == new_state.stats.last_sent_at
      assert 0 != new_state.stats.last_received_at
    end

    test "a nack with good data decodes and forwards to handler" do
      event_type_id = 1
      key = <<1, 2>>
      offset = 42
      message_id = 100
      ack_key = {event_type_id, key, offset}
      nack_data = {<<0, 0, 0>>, "crash!"}

      payload = Protocol.encode_payload(:nack, nack_data)
      encoded_message = Protocol.encode_frame(:nack, message_id, payload)

      @handler
      |> expect(:handle_nack, fn ^ack_key, ^nack_data ->
        :ok
      end)

      state = ClientConnection.State.put_inflight(@sample_state, ack_key)

      assert {:noreply, new_state} =
               ClientConnection.handle_info({:ssl, :fake, encoded_message}, state)

      assert false == ClientConnection.State.inflight?(new_state, ack_key)
    end

    test "a ping returns a pong and increases last_sent_at" do
      encoded_message = Protocol.encode_frame(:ping)

      # expect a pong
      @mod_transport
      |> expect(:send, fn 123, <<96, 0>> -> :ok end)

      assert {:noreply, new_state} =
               ClientConnection.handle_info({:ssl, :fake, encoded_message}, @sample_state)

      assert 0 != new_state.stats.last_sent_at
      assert 0 != new_state.stats.last_received_at
    end

    test "a pong increases last_received_at" do
      encoded_message = Protocol.encode_frame(:pong)

      assert {:noreply, new_state} =
               ClientConnection.handle_info({:ssl, :fake, encoded_message}, @sample_state)

      assert 0 == new_state.stats.last_sent_at
      assert 0 != new_state.stats.last_received_at
    end
  end

  describe "publishing" do
    test "if the socket is closed when sending" do
      event = %TestEvent{key: <<1, 2, 3>>, offset: 1, message: "hi!"}
      event_data = :erlang.term_to_binary(event)
      event_type_id = 1
      ack_key = {event_type_id, event.key, event.offset}
      timestamp = DateTime.to_unix(DateTime.utc_now())

      # return a closed error
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
               ClientConnection.handle_call({:publish, message, ack_key}, self(), @sample_state)

      assert state == @sample_state
    end
  end

  describe "terminate" do
    test "does not call handler.down()if the connection response wasn't received" do
      expect(@handler, :instantiated_ats, fn -> {1, 2} end)
      expect(@mod_transport, :send, fn _, _ -> :ok end)

      {:ok, connection} =
        ClientConnection.start_link(
          socket: <<123>>,
          handler: @handler,
          transport: Sink.Connection.Transport.SSLMock
        )

      Process.exit(connection, :normal)

      stop_connection(connection)
    end
  end

  describe "init" do
    test "sends a connection request" do
      expect(@handler, :instantiated_ats, fn ->
        {1, 2}
      end)

      # check for connection request
      expect(@mod_transport, :send, fn _, frame ->
        assert {:connection_request, {1, 2}} = Protocol.decode_frame(frame)
        :ok
      end)

      {:ok, connection} =
        ClientConnection.start_link(
          socket: <<123>>,
          handler: @handler,
          transport: Sink.Connection.Transport.SSLMock
        )

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
