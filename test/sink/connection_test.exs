defmodule Sink.ConnectionTest do
  use ExUnit.Case, async: false
  import ExUnit.CaptureLog
  import Mox
  alias Sink.Event
  alias Sink.Test.Certificates

  @client_id "abc123"
  @mod_transport Sink.Connection.Transport.SSLMock
  @client_handler Sink.Connection.ClientConnectionHandlerMock
  @server_handler Sink.Connection.ServerConnectionHandlerMock
  @event %Event{
    event_type_id: 1,
    key: <<1, 2, 3>>,
    offset: 1,
    timestamp: DateTime.to_unix(DateTime.utc_now()),
    event_data: :erlang.term_to_binary(%{message: "hi!"}),
    schema_version: 1
  }
  @ack_key {@event.event_type_id, @event.key, @event.offset}
  @version "1.0.0"
  @time_to_connect 250

  setup :set_mox_global
  setup :verify_on_exit!

  setup do
    ssl = [
      cacertfile: Certificates.ca_cert_file(),
      secure_renegotiate: true,
      reuse_sessions: true,
      verify: :verify_peer,
      fail_if_no_peer_cert: true
    ]

    server_ssl =
      [
        certfile: Certificates.server_cert_file(),
        keyfile: Certificates.server_key_file()
      ] ++ ssl

    client_ssl =
      [
        certfile: Certificates.client_cert_file(),
        keyfile: Certificates.client_key_file()
      ] ++ ssl

    Sink.Connection.Freshness.reset()
    on_exit(fn -> Sink.Connection.Freshness.reset() end)

    {:ok, server_ssl: server_ssl, client_ssl: client_ssl}
  end

  describe "connecting" do
    test "hello new client", %{server_ssl: server_ssl, client_ssl: client_ssl} do
      stub(@server_handler, :authenticate_client, fn _ -> {:ok, "abc123"} end)
      stub(@client_handler, :instance_ids, fn -> %{client: 1, server: nil} end)

      stub(@server_handler, :client_configuration, fn "abc123" ->
        {:ok, %{server: 2, client: nil}}
      end)

      expect(@client_handler, :application_version, fn -> @version end)
      expect(@server_handler, :supported_application_version?, fn "abc123", @version -> true end)
      expect(@client_handler, :handle_connection_response, fn {:hello_new_client, 2} -> :ok end)
      expect(@server_handler, :down, fn _ -> :ok end)
      expect(@client_handler, :down, fn -> :ok end)

      expect(@server_handler, :handle_connection_response, fn "abc123", {:hello_new_client, 1} ->
        :ok
      end)

      start_supervised!(
        {Sink.Connection.ServerListener,
         port: 9999, ssl_opts: server_ssl, handler: @server_handler}
      )

      start_supervised!(
        {Sink.Connection.Client,
         port: 9999, host: "localhost", ssl_opts: client_ssl, handler: @client_handler}
      )

      Process.sleep(@time_to_connect)

      assert Sink.Connection.Client.connected?()
      assert Sink.Connection.ServerHandler.connected?("abc123")

      stop_supervised!(Sink.Connection.Client)
      stop_supervised!(Sink.Connection.ServerListener)
    end

    test "hello new client after failed initial attempt", %{
      server_ssl: server_ssl,
      client_ssl: client_ssl
    } do
      stub(@server_handler, :authenticate_client, fn _ -> {:ok, "abc123"} end)
      stub(@client_handler, :instance_ids, fn -> %{client: 1, server: nil} end)

      stub(@server_handler, :client_configuration, fn "abc123" ->
        {:ok, %{client: 1, server: 2}}
      end)

      expect(@client_handler, :application_version, fn -> @version end)
      expect(@server_handler, :supported_application_version?, fn "abc123", @version -> true end)
      expect(@client_handler, :handle_connection_response, fn {:hello_new_client, 2} -> :ok end)
      expect(@server_handler, :down, fn _ -> :ok end)
      expect(@client_handler, :down, fn -> :ok end)

      expect(@server_handler, :handle_connection_response, fn "abc123", {:hello_new_client, 1} ->
        :ok
      end)

      start_supervised!(
        {Sink.Connection.ServerListener,
         port: 9999, ssl_opts: server_ssl, handler: @server_handler}
      )

      start_supervised!(
        {Sink.Connection.Client,
         port: 9999, host: "localhost", ssl_opts: client_ssl, handler: @client_handler}
      )

      Process.sleep(@time_to_connect)

      assert Sink.Connection.Client.connected?()
      assert Sink.Connection.ServerHandler.connected?("abc123")

      stop_supervised!(Sink.Connection.Client)
      stop_supervised!(Sink.Connection.ServerListener)
    end

    test "successful reconnection", %{
      server_ssl: server_ssl,
      client_ssl: client_ssl
    } do
      expect(@server_handler, :authenticate_client, fn _ -> {:ok, "abc123"} end)
      expect(@client_handler, :instance_ids, fn -> %{client: 1, server: 2} end)
      expect(@client_handler, :application_version, fn -> @version end)
      expect(@server_handler, :supported_application_version?, fn "abc123", @version -> true end)

      expect(@server_handler, :client_configuration, fn "abc123" ->
        {:ok, %{client: 1, server: 2}}
      end)

      stub(@mod_transport, :send, fn _, _ -> :ok end)

      expect(@server_handler, :handle_connection_response, fn "abc123", :connected -> :ok end)

      expect(@server_handler, :down, fn _ -> :ok end)
      expect(@client_handler, :handle_connection_response, fn :connected -> :ok end)
      expect(@client_handler, :down, fn -> :ok end)

      start_supervised!(
        {Sink.Connection.ServerListener,
         port: 9999, ssl_opts: server_ssl, handler: @server_handler}
      )

      start_supervised!(
        {Sink.Connection.Client,
         port: 9999, host: "localhost", ssl_opts: client_ssl, handler: @client_handler}
      )

      refute Sink.Connection.Client.connected?()
      refute Sink.Connection.ServerHandler.connected?("abc123")

      # # give it time to connect

      Process.sleep(@time_to_connect)

      assert Sink.Connection.Client.connected?()
      assert Sink.Connection.ServerHandler.connected?("abc123")

      stop_supervised!(Sink.Connection.Client)
      stop_supervised!(Sink.Connection.ServerListener)
    end

    test "server id mismatch", %{server_ssl: server_ssl, client_ssl: client_ssl} do
      stub(@server_handler, :authenticate_client, fn _ -> {:ok, "abc123"} end)
      stub(@client_handler, :instance_ids, fn -> %{client: 1, server: 3} end)

      stub(@server_handler, :client_configuration, fn "abc123" ->
        {:ok, %{client: 1, server: 2}}
      end)

      expect(@client_handler, :application_version, fn -> @version end)
      expect(@server_handler, :supported_application_version?, fn "abc123", @version -> true end)

      expect(@client_handler, :handle_connection_response, fn :instance_id_mismatch ->
        :ok
      end)

      expect(@server_handler, :handle_connection_response, fn "abc123", :instance_id_mismatch ->
        :ok
      end)

      start_supervised!(
        {Sink.Connection.ServerListener,
         port: 9999, ssl_opts: server_ssl, handler: @server_handler}
      )

      start_supervised!(
        {Sink.Connection.Client,
         port: 9999, host: "localhost", ssl_opts: client_ssl, handler: @client_handler}
      )

      # # give it time to connect

      Process.sleep(@time_to_connect)

      refute Sink.Connection.Client.connected?()
      refute Sink.Connection.ServerHandler.connected?("abc123")

      assert {:error, :no_connection} == Sink.Connection.Client.publish(@event, @ack_key)

      assert {:error, :no_connection} ==
               Sink.Connection.ServerHandler.publish("abc123", @event, @ack_key)

      stop_supervised!(Sink.Connection.Client)
      stop_supervised!(Sink.Connection.ServerListener)
    end

    test "client id mismatch", %{server_ssl: server_ssl, client_ssl: client_ssl} do
      stub(@server_handler, :authenticate_client, fn _ -> {:ok, "abc123"} end)
      stub(@client_handler, :instance_ids, fn -> %{client: 3, server: 2} end)

      stub(@server_handler, :client_configuration, fn "abc123" ->
        {:ok, %{client: 1, server: 2}}
      end)

      expect(@client_handler, :application_version, fn -> @version end)
      expect(@server_handler, :supported_application_version?, fn "abc123", @version -> true end)

      expect(@client_handler, :handle_connection_response, fn :instance_id_mismatch ->
        :ok
      end)

      expect(@server_handler, :handle_connection_response, fn "abc123", :instance_id_mismatch ->
        :ok
      end)

      start_supervised!(
        {Sink.Connection.ServerListener,
         port: 9999, ssl_opts: server_ssl, handler: @server_handler}
      )

      start_supervised!(
        {Sink.Connection.Client,
         port: 9999, host: "localhost", ssl_opts: client_ssl, handler: @client_handler}
      )

      # # give it time to connect

      Process.sleep(@time_to_connect)

      refute Sink.Connection.Client.connected?()
      refute Sink.Connection.ServerHandler.connected?("abc123")

      assert {:error, :no_connection} == Sink.Connection.Client.publish(@event, @ack_key)

      assert {:error, :no_connection} ==
               Sink.Connection.ServerHandler.publish("abc123", @event, @ack_key)

      stop_supervised!(Sink.Connection.Client)
      stop_supervised!(Sink.Connection.ServerListener)
    end

    test "quarantined client", %{server_ssl: server_ssl, client_ssl: client_ssl} do
      expected_response = {:quarantined, {<<1, 1, 1>>, "blocked"}}
      stub(@server_handler, :authenticate_client, fn _ -> {:ok, "abc123"} end)
      stub(@client_handler, :instance_ids, fn -> %{client: 1, server: 2} end)
      stub(@server_handler, :client_configuration, fn "abc123" -> expected_response end)
      expect(@client_handler, :application_version, fn -> @version end)
      expect(@server_handler, :supported_application_version?, fn "abc123", @version -> true end)
      expect(@client_handler, :handle_connection_response, fn ^expected_response -> :ok end)

      expect(@server_handler, :handle_connection_response, fn "abc123", ^expected_response ->
        :ok
      end)

      start_supervised!(
        {Sink.Connection.ServerListener,
         port: 9999, ssl_opts: server_ssl, handler: @server_handler}
      )

      start_supervised!(
        {Sink.Connection.Client,
         port: 9999, host: "localhost", ssl_opts: client_ssl, handler: @client_handler}
      )

      # give it time to connect

      Process.sleep(@time_to_connect)

      # check that the client is connected, but not active

      refute Sink.Connection.Client.connected?()
      refute Sink.Connection.ServerHandler.connected?("abc123")

      stop_supervised!(Sink.Connection.Client)
      stop_supervised!(Sink.Connection.ServerListener)
    end

    test "unsupported application version", %{server_ssl: server_ssl, client_ssl: client_ssl} do
      expected_response = :unsupported_application_version
      stub(@server_handler, :authenticate_client, fn _ -> {:ok, "abc123"} end)
      stub(@client_handler, :instance_ids, fn -> %{client: 1, server: 2} end)

      stub(@server_handler, :client_configuration, fn "abc123" ->
        {:ok, %{client: 1, server: 2}}
      end)

      expect(@client_handler, :application_version, fn -> @version end)
      expect(@server_handler, :supported_application_version?, fn "abc123", @version -> false end)
      expect(@client_handler, :handle_connection_response, fn ^expected_response -> :ok end)

      expect(@server_handler, :handle_connection_response, fn "abc123", ^expected_response ->
        :ok
      end)

      start_supervised!(
        {Sink.Connection.ServerListener,
         port: 9999, ssl_opts: server_ssl, handler: @server_handler}
      )

      start_supervised!(
        {Sink.Connection.Client,
         port: 9999, host: "localhost", ssl_opts: client_ssl, handler: @client_handler}
      )

      # give it time to connect

      Process.sleep(@time_to_connect)

      # check that the client is connected, but not active

      refute Sink.Connection.Client.connected?()
      refute Sink.Connection.ServerHandler.connected?("abc123")

      stop_supervised!(Sink.Connection.Client)
      stop_supervised!(Sink.Connection.ServerListener)
    end
  end

  describe "once connected" do
    setup %{server_ssl: server_ssl, client_ssl: client_ssl} do
      stub_with(@mod_transport, Sink.Connection.Transport.SSL)
      stub(@server_handler, :authenticate_client, fn _ -> {:ok, "abc123"} end)
      stub(@client_handler, :instance_ids, fn -> %{client: 1, server: 2} end)

      stub(@server_handler, :client_configuration, fn "abc123" ->
        {:ok, %{client: 1, server: 2}}
      end)

      stub(@client_handler, :application_version, fn -> @version end)
      stub(@server_handler, :supported_application_version?, fn "abc123", @version -> true end)
      stub(@mod_transport, :send, fn _, _ -> :ok end)
      stub(@client_handler, :handle_connection_response, fn :connected -> :ok end)
      stub(@server_handler, :handle_connection_response, fn "abc123", :connected -> :ok end)
      stub(@server_handler, :down, fn _ -> :ok end)
      stub(@client_handler, :down, fn -> :ok end)

      start_supervised!(
        {Sink.Connection.ServerListener,
         port: 9999, ssl_opts: server_ssl, handler: @server_handler}
      )

      start_supervised!(
        {Sink.Connection.Client,
         port: 9999, host: "localhost", ssl_opts: client_ssl, handler: @client_handler}
      )

      # give it time to connect
      :timer.sleep(@time_to_connect)

      :ok
    end

    test "server sends message to client, client acks it" do
      test = self()

      expect(
        @client_handler,
        :handle_publish,
        fn event, _message_id ->
          send(test, {{:client, :publish}, event})
          :ack
        end
      )

      expect(@server_handler, :handle_ack, fn client_id, ack_key ->
        send(test, {:ack, client_id, ack_key})
        :ok
      end)

      assert Sink.Connection.Client.connected?()
      assert Sink.Connection.ServerHandler.connected?(@client_id)

      # send the event
      assert :ok == Sink.Connection.ServerHandler.publish(@client_id, @event, @ack_key)

      Process.sleep(100)

      assert_received {{:client, :publish}, @event}
      assert_received {:ack, "abc123", @ack_key}

      stop_supervised!(Sink.Connection.Client)
      stop_supervised!(Sink.Connection.ServerListener)
    end

    @tag :capture_log
    test "server sends message to client, client nacks it" do
      test = self()

      expect(
        @client_handler,
        :handle_publish,
        fn event, _message_id ->
          send(test, {{:client, :publish}, event})
          raise RuntimeError, "nack reason"
        end
      )

      expect(@server_handler, :handle_nack, fn client_id, ack_key, nack_key ->
        send(test, {:nack, client_id, ack_key, nack_key})
        :ok
      end)

      # send the event
      Sink.Connection.ServerHandler.publish(@client_id, @event, @ack_key)

      Process.sleep(100)

      assert_received {{:client, :publish}, @event}
      assert_received {:nack, "abc123", @ack_key, {_machine, human}}
      assert human =~ "nack reason"

      stop_supervised!(Sink.Connection.Client)
      stop_supervised!(Sink.Connection.ServerListener)
    end

    test "client sends message to server, server acks it" do
      test = self()

      expect(
        @server_handler,
        :handle_publish,
        fn _client_id, event, _message_id ->
          send(test, {{:server, :publish}, event})
          :ack
        end
      )

      expect(@client_handler, :handle_ack, fn ack_key ->
        send(test, {:ack, ack_key})
        :ok
      end)

      # send the event
      Sink.Connection.Client.publish(@event, @ack_key)

      Process.sleep(100)

      assert_received {{:server, :publish}, @event}
      assert_received {:ack, @ack_key}

      stop_supervised!(Sink.Connection.Client)
      stop_supervised!(Sink.Connection.ServerListener)
    end

    @tag :capture_log
    test "client sends message to server, server nacks it" do
      test = self()

      expect(
        @server_handler,
        :handle_publish,
        fn "abc123", event, _message_id ->
          send(test, {{:server, :publish}, event})
          raise RuntimeError, "nack reason"
        end
      )

      expect(@client_handler, :handle_nack, fn ack_key, nack_key ->
        send(test, {:nack, ack_key, nack_key})
        :ok
      end)

      # send the event
      Sink.Connection.Client.publish(@event, @ack_key)

      Process.sleep(100)

      assert_received {{:server, :publish}, @event}
      assert_received {:nack, @ack_key, {_machine, human}}
      assert human =~ "nack reason"

      stop_supervised!(Sink.Connection.Client)
      stop_supervised!(Sink.Connection.ServerListener)
    end

    test "tracking freshness" do
      test = self()

      expect(
        @server_handler,
        :handle_publish,
        fn _client_id, event, _message_id ->
          send(test, {{:server, :publish}, event})
          :ack
        end
      )

      expect(@client_handler, :handle_ack, fn ack_key ->
        send(test, {:ack, ack_key})
        :ok
      end)

      # send the event
      Sink.Connection.Client.publish(@event, @ack_key)

      Process.sleep(100)

      assert_received {{:server, :publish}, @event}
      assert_received {:ack, @ack_key}

      assert Sink.Connection.Freshness.get_freshness("abc123", 1) == @event.timestamp

      stop_supervised!(Sink.Connection.Client)
      stop_supervised!(Sink.Connection.ServerListener)
    end
  end

  test "client can handle not getting authenticated", %{
    server_ssl: server_ssl,
    client_ssl: client_ssl
  } do
    stub_with(@mod_transport, Sink.Connection.Transport.SSL)

    stub(@server_handler, :authenticate_client, fn _ ->
      {:error, RuntimeError.exception("Not allowed here!")}
    end)

    stub(@client_handler, :instance_ids, fn -> %{client: 1, server: 2} end)
    stub(@mod_transport, :send, fn _, _ -> :ok end)

    logs =
      capture_log(fn ->
        start_supervised!(
          {Sink.Connection.ServerListener,
           port: 9999, ssl_opts: server_ssl, handler: @server_handler}
        )

        client =
          start_supervised!(
            {Sink.Connection.Client,
             port: 9999, host: "localhost", ssl_opts: client_ssl, handler: @client_handler}
          )

        assert Process.alive?(client)

        # give it time to connect
        :timer.sleep(@time_to_connect)

        assert Process.alive?(client)
      end)

    refute logs =~ "already_started"

    stop_supervised!(Sink.Connection.Client)
    stop_supervised!(Sink.Connection.ServerListener)
  end
end
