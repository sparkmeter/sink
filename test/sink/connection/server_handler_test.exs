defmodule Sink.Connection.ServerHandlerTest do
  use ExUnit.Case, async: false
  import Mox
  alias Sink.Connection.ServerHandler

  def setopts(_socket, _opts), do: :ok
  def authenticate_client(_peer_cert), do: {:ok, "test-client"}

  @mod_transport Sink.Connection.Transport.SSLMock

  setup :set_mox_from_context
  setup :verify_on_exit!

  describe "new client connecting" do
    test "succeeds" do
      ref = 123
      socket = 123
      transport = __MODULE__

      opts = [
        handler: __MODULE__,
        ssl_opts: []
      ]

      @mod_transport
      |> expect(:peercert, fn _socket -> {:ok, <<1, 2, 3>>} end)
      |> expect(:peername, fn _socket -> {:ok, {{127, 0, 0, 1}, 51380}} end)

      {:ok, pid} = ServerHandler.start_link(ref, socket, transport, opts)

      :timer.sleep(5)
      assert Process.alive?(pid)

      assert ServerHandler.connected?("test-client")
    end

    test "boots an existing client" do
      ref = 123
      socket = 123
      transport = __MODULE__

      opts = [
        handler: __MODULE__,
        ssl_opts: []
      ]

      @mod_transport
      |> expect(:peercert, 2, fn _socket -> {:ok, <<1, 2, 3>>} end)
      |> expect(:peername, 2, fn _socket -> {:ok, {{127, 0, 0, 1}, 51380}} end)

      {:ok, pid_og} = ServerHandler.start_link(ref, socket, transport, opts)

      :timer.sleep(5)
      assert Process.alive?(pid_og)

      {:ok, pid_new} = ServerHandler.start_link(ref, socket, transport, opts)
      :timer.sleep(50)
      refute Process.alive?(pid_og)
      assert Process.alive?(pid_new)
    end
  end
end
