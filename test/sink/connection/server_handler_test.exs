defmodule Sink.Connection.ServerHandlerTest do
  @moduledoc """
  Note: we're using sleeps here, which is brittle and not recommended.

  Consider using something like https://github.com/well-ironed/liveness or some
  Process.Monitor magic.
  """
  use ExUnit.Case, async: false
  import Mox
  alias Sink.Connection.{Protocol, ServerHandler}

  def setopts(_socket, _opts), do: :ok

  @mod_transport Sink.Connection.Transport.SSLMock
  @handler Sink.Connection.ServerConnectionHandlerMock
  @sample_state %ServerHandler.State{
    client_id: "test-client",
    socket: 123,
    transport: __MODULE__,
    peername: :fake,
    handler: @handler,
    ssl_opts: :fake,
    next_message_id: 100,
    start_time: 0,
    last_received_at: 0
  }

  setup :set_mox_from_context
  setup :verify_on_exit!

  describe "new client connecting" do
    test "succeeds" do
      ref = 123
      socket = 123
      transport = __MODULE__

      opts = [
        handler: @handler,
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
      |> expect(:handle_publish, 1, fn {"test-client", ^event_type_id, ^key},
                                       ^offset,
                                       ^event_data,
                                       ^message_id ->
        :ack
      end)

      assert {:noreply, new_state} =
               ServerHandler.handle_info({:ssl, :fake, encoded_message}, @sample_state)

      assert 100 = new_state.next_message_id
      assert 0 != new_state.last_received_at
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
      |> expect(:handle_ack, 1, fn ^my_pid, "test-client", ^ack_key ->
        :ok
      end)

      state = ServerHandler.State.put_inflight(@sample_state, {self(), ack_key})

      assert {:noreply, new_state} =
               ServerHandler.handle_info({:ssl, :fake, encoded_message}, state)

      assert 101 = new_state.next_message_id
      assert false == ServerHandler.State.inflight?(new_state, ack_key)
    end

    test "a ping returns a pong" do
      encoded_message = Protocol.encode_frame(:ping)

      # expect a pong
      @mod_transport
      |> expect(:send, fn 123, <<96, 0>> -> :ok end)

      assert {:noreply, new_state} =
               ServerHandler.handle_info({:ssl, :fake, encoded_message}, @sample_state)
    end

    test "a pong does nothing (for now)" do
      encoded_message = Protocol.encode_frame(:pong)

      assert {:noreply, new_state} =
               ServerHandler.handle_info({:ssl, :fake, encoded_message}, @sample_state)
    end
  end

  describe "checking if a connection is alive" do
    test "should be alive if we have received a message in a reasonable amount of time" do
      state = %ServerHandler.State{
        @sample_state
        | last_received_at: 1_000_000,
          keepalive_interval: 60_000
      }

      now = 1_089_999

      assert true == ServerHandler.State.alive?(state, now)
    end

    test "should be dead if we have not received a message in a reasonable amount of time" do
      state = %ServerHandler.State{
        @sample_state
        | last_received_at: 1_000_000,
          keepalive_interval: 60_000
      }

      now = 1_090_000

      assert false == ServerHandler.State.alive?(state, now)
    end
  end
end
