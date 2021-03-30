defmodule Sink.Connection.ClientConnectionTest do
  @moduledoc """
  Note: we're using sleeps here, which is brittle and not recommended.

  Consider using something like https://github.com/well-ironed/liveness or some
  Process.Monitor magic.
  """
  use ExUnit.Case, async: false
  import ExUnit.CaptureLog
  import Mox
  alias Sink.Connection.{Protocol, ClientConnection}

  def setopts(_socket, _opts), do: :ok

  @mod_transport Sink.Connection.Transport.SSLMock
  @handler Sink.Connection.ClientConnectionHandlerMock
  @sample_state %ClientConnection.State{
    socket: 123,
    peername: :fake,
    handler: @handler,
    next_message_id: 100,
    last_sent_at: 0,
    last_received_at: 0
  }

  setup :set_mox_from_context
  setup :verify_on_exit!

  describe "receiving" do
    test "a publish with good data decodes and forwards to handler, then acks" do
      event_type_id = 1
      key = <<1, 2>>
      offset = 42
      event_data = <<9, 8, 7>>
      message_id = 1234
      payload = Protocol.encode_payload(:publish, {event_type_id, key, offset, event_data})
      encoded_message = Protocol.encode_frame(:publish, message_id, payload)

      @mod_transport
      |> expect(:send, fn 123, <<52, 210>> -> :ok end)

      @handler
      |> expect(:handle_publish, 1, fn {^event_type_id, ^key},
                                       ^offset,
                                       ^event_data,
                                       ^message_id ->
        :ack
      end)

      assert {:noreply, new_state} =
               ClientConnection.handle_info({:ssl, :fake, encoded_message}, @sample_state)

      assert 100 == new_state.next_message_id
      assert 0 != new_state.last_sent_at
      assert 0 != new_state.last_received_at
    end

    test "if the SinkHandler raises an error we don't do anything for now (we will send a NACK once they're implemented)" do
      # todo: change once NACKs are implemented
      event_type_id = 1
      key = <<1, 2>>
      offset = 42
      event_data = <<9, 8, 7>>
      message_id = 1234
      payload = Protocol.encode_payload(:publish, {event_type_id, key, offset, event_data})
      encoded_message = Protocol.encode_frame(:publish, message_id, payload)

      # eventually expect a NACK
      # @mod_transport
      # |> expect(:send, fn 123, NACK -> :ok end)

      @handler
      |> expect(:handle_publish, 1, fn {^event_type_id, ^key},
                                       ^offset,
                                       ^event_data,
                                       ^message_id ->
        raise(ArgumentError, message: "boom")
      end)

      assert capture_log(fn ->
               assert {:noreply, new_state} =
                        ClientConnection.handle_info(
                          {:ssl, :fake, encoded_message},
                          @sample_state
                        )

               assert 100 == new_state.next_message_id
               assert 0 == new_state.last_sent_at
               assert 0 != new_state.last_received_at
             end) =~ "boom"
    end

    test "an ack with good data decodes and forwards to handler" do
      event_type_id = 1
      key = <<1, 2>>
      offset = 42
      message_id = 1234
      ack_key = {event_type_id, key, offset}

      payload = Protocol.encode_payload(:ack, 1, message_id)
      encoded_message = Protocol.encode_frame(:ack, 100, payload)
      my_pid = self()

      @handler
      |> expect(:handle_ack, 1, fn ^my_pid, ^ack_key ->
        :ok
      end)

      state = ClientConnection.State.put_inflight(@sample_state, {self(), ack_key})

      assert {:noreply, new_state} =
               ClientConnection.handle_info({:ssl, :fake, encoded_message}, state)

      assert 101 = new_state.next_message_id
      assert false == ClientConnection.State.inflight?(new_state, ack_key)
      assert 0 == new_state.last_sent_at
      assert 0 != new_state.last_received_at
    end

    test "a ping returns a pong and increases last_sent_at" do
      encoded_message = Protocol.encode_frame(:ping)

      # expect a pong
      @mod_transport
      |> expect(:send, fn 123, <<96, 0>> -> :ok end)

      assert {:noreply, new_state} =
               ClientConnection.handle_info({:ssl, :fake, encoded_message}, @sample_state)

      assert assert 0 != new_state.last_sent_at
      assert assert 0 != new_state.last_received_at
    end

    test "a pong increases last_received_at" do
      encoded_message = Protocol.encode_frame(:pong)

      assert {:noreply, new_state} =
               ClientConnection.handle_info({:ssl, :fake, encoded_message}, @sample_state)

      assert 0 == new_state.last_sent_at
      assert 0 != new_state.last_received_at
    end
  end

  describe "checking if a we should ping" do
    test "should not ping if we have received something since keepalive" do
      state = %ClientConnection.State{
        @sample_state
        | last_sent_at: 1_000_001,
          last_received_at: 1_000_000,
          keepalive_interval: 60_000
      }

      now = 1_060_000

      assert false == ClientConnection.State.should_send_ping?(state, now)
    end

    test "should not ping if we haven't received anything but have pinged since keepalive" do
      state = %ClientConnection.State{
        @sample_state
        | last_sent_at: 1_000_001,
          last_received_at: 1_000_000,
          keepalive_interval: 60_000
      }

      now = 1_060_000

      assert false == ClientConnection.State.should_send_ping?(state, now)
    end

    test "should ping if we haven't received or sent anything since keepalive" do
      state = %ClientConnection.State{
        @sample_state
        | last_sent_at: 1_000_000,
          last_received_at: 1_000_000,
          keepalive_interval: 60_000
      }

      now = 1_060_000

      assert true == ClientConnection.State.should_send_ping?(state, now)
    end
  end

  describe "checking if a connection is alive" do
    test "should be alive if we have received a message in a reasonable amount of time" do
      state = %ClientConnection.State{
        @sample_state
        | last_received_at: 1_000_000,
          keepalive_interval: 60_000
      }

      now = 1_089_999

      assert true == ClientConnection.State.alive?(state, now)
    end

    test "should be dead if we have not received a message in a reasonable amount of time" do
      state = %ClientConnection.State{
        @sample_state
        | last_received_at: 1_000_000,
          keepalive_interval: 60_000
      }

      now = 1_090_000

      assert false == ClientConnection.State.alive?(state, now)
    end
  end
end
