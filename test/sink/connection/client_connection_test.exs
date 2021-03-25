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

      state = %ClientConnection.State{
        socket: 123,
        peername: :fake,
        handler: @handler,
        next_message_id: 100
      }

      assert {:noreply, new_state} =
               ClientConnection.handle_info({:ssl, :fake, encoded_message}, state)

      assert new_state == state
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

      state = %ClientConnection.State{
        socket: 123,
        peername: :fake,
        handler: @handler,
        next_message_id: 100
      }

      assert capture_log(fn ->
               assert {:noreply, new_state} =
                        ClientConnection.handle_info({:ssl, :fake, encoded_message}, state)

               assert new_state == state
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

      state =
        %ClientConnection.State{
          socket: 123,
          peername: :fake,
          handler: @handler,
          next_message_id: 100
        }
        |> ClientConnection.State.put_inflight({self(), ack_key})

      assert {:noreply, new_state} =
               ClientConnection.handle_info({:ssl, :fake, encoded_message}, state)

      assert 101 = new_state.next_message_id
      assert false == ClientConnection.State.inflight?(new_state, ack_key)
    end

    test "a ping returns a pong" do
      encoded_message = Protocol.encode_frame(:ping)

      # expect a pong
      @mod_transport
      |> expect(:send, fn 123, <<96, 0>> -> :ok end)

      state = %ClientConnection.State{
        socket: 123,
        peername: :fake,
        handler: @handler,
        next_message_id: 100
      }

      assert {:noreply, new_state} =
               ClientConnection.handle_info({:ssl, :fake, encoded_message}, state)
    end

    test "a pong does nothing (for now)" do
      encoded_message = Protocol.encode_frame(:pong)

      state = %ClientConnection.State{
        socket: 123,
        peername: :fake,
        handler: @handler,
        next_message_id: 100
      }

      assert {:noreply, new_state} =
               ClientConnection.handle_info({:ssl, :fake, encoded_message}, state)
    end
  end
end
