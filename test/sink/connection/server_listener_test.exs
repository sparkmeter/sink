defmodule Sink.Connection.ServerListenerTest do
  use ExUnit.Case, async: false
  import Mox
  alias Sink.Connection.ServerListener
  alias Sink.Connection.ServerHandlerMock
  alias Sink.Test.Certificates

  setup :set_mox_global
  setup :verify_on_exit!

  setup do
    server_ssl_opts = [
      certfile: Certificates.server_cert_file(),
      keyfile: Certificates.server_key_file(),
      cacertfile: Certificates.ca_cert_file(),
      secure_renegotiate: true,
      reuse_sessions: true,
      verify: :verify_peer,
      fail_if_no_peer_cert: true
    ]

    {:ok, server_ssl_opts: server_ssl_opts}
  end

  describe "start_link/1" do
    test "starts ranch on port", %{server_ssl_opts: server_ssl_opts} do
      opts = [
        port: 15000,
        ssl_opts: server_ssl_opts,
        server_handler: ServerHandlerMock
      ]

      assert {:ok, listener} = ServerListener.start_link(opts)

      ensure_stopped(listener)
    end

    @tag :capture_log
    test "requires ssl_opts to be filled" do
      opts = [port: 15000, ssl_opts: [], handler: ServerHandlerMock]

      assert {:error, {:error, :no_cert}} = ServerListener.start_link(opts)
    end

    test "process stops when ranch listener crashes", %{server_ssl_opts: server_ssl_opts} do
      opts = [
        port: 15000,
        ssl_opts: server_ssl_opts,
        server_handler: ServerHandlerMock
      ]

      {:ok, pid} = ServerListener.start_link(opts)
      ref = Process.monitor(pid)

      :ranch.stop_listener(:sink)

      receive do
        {:DOWN, ^ref, :process, ^pid, _reason} -> :ok
      end
    end

    test "on connection the handler is started with the necessary opts" do
      test = self()

      expect(ServerHandlerMock, :start_link, fn ref, _socket, _transport, opts ->
        send(test, {:handler_opts, opts})

        {:ok,
         :proc_lib.spawn_link(fn ->
           :ranch.handshake(ref)
         end)}
      end)

      opts = [
        port: 15000,
        ssl_opts: [
          certfile: Certificates.server_cert_file(),
          keyfile: Certificates.server_key_file(),
          cacertfile: Certificates.ca_cert_file(),
          secure_renegotiate: true,
          reuse_sessions: true,
          verify: :verify_peer,
          fail_if_no_peer_cert: true
        ],
        server_handler: ServerHandlerMock
      ]

      {:ok, listener} = ServerListener.start_link(opts)

      {:ok, _socket} =
        :ssl.connect(
          ~c"localhost",
          15000,
          [
            :binary,
            packet: 2,
            active: true,
            certfile: Certificates.client_cert_file(),
            keyfile: Certificates.client_key_file(),
            cacertfile: Certificates.ca_cert_file(),
            secure_renegotiate: true,
            reuse_sessions: true,
            verify: :verify_peer,
            fail_if_no_peer_cert: true
          ],
          5_000
        )

      assert_receive {:handler_opts, opts}
      assert opts[:ssl_opts][:certfile] == Certificates.server_cert_file()
      assert opts[:ssl_opts][:keyfile] == Certificates.server_key_file()
      assert opts[:ssl_opts][:cacertfile] == Certificates.ca_cert_file()
      assert opts[:ssl_opts][:secure_renegotiate] == true
      assert opts[:ssl_opts][:reuse_sessions] == true
      assert opts[:ssl_opts][:verify] == :verify_peer
      assert opts[:ssl_opts][:fail_if_no_peer_cert] == true

      ensure_stopped(listener)
    end
  end

  defp ensure_stopped(pid) do
    ref = Process.monitor(pid)
    Process.exit(pid, :normal)

    receive do
      {:DOWN, ^ref, :process, ^pid, _reason} -> :ok
    end
  end
end
