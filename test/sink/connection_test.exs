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
  @time_to_connect 300

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
    test "client and server instantiated_at match expected", %{
      server_ssl: server_ssl,
      client_ssl: client_ssl
    } do
      stub(@server_handler, :authenticate_client, fn _ -> {:ok, "abc123"} end)
      stub(@client_handler, :instantiated_ats, fn -> {1, 2} end)
      stub(@server_handler, :instantiated_ats, fn "abc123" -> {:ok, {1, 2}} end)
      stub(@mod_transport, :send, fn _, _ -> :ok end)
      stub(@server_handler, :handle_connection_response, fn {"abc123", 1}, :ok -> :ok end)
      expect(@client_handler, :handle_connection_response, fn :ok -> :ok end)
      stub(@server_handler, :up, fn _ -> :ok end)
      stub(@client_handler, :up, fn -> :ok end)
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

      refute Sink.Connection.Client.connected?()
      refute Sink.Connection.ServerHandler.connected?("abc123")

      # # give it time to connect

      Process.sleep(@time_to_connect)

      assert Sink.Connection.Client.connected?()
      assert Sink.Connection.Client.active?()
      assert Sink.Connection.ServerHandler.connected?("abc123")
      assert Sink.Connection.ServerHandler.active?("abc123")

      stop_supervised!(Sink.Connection.Client)
      stop_supervised!(Sink.Connection.ServerListener)
    end

    test "hello new client", %{server_ssl: server_ssl, client_ssl: client_ssl} do
      stub(@server_handler, :authenticate_client, fn _ -> {:ok, "abc123"} end)
      stub(@client_handler, :instantiated_ats, fn -> {1, nil} end)
      stub(@server_handler, :instantiated_ats, fn "abc123" -> {:ok, {nil, 2}} end)
      expect(@client_handler, :handle_connection_response, fn {:hello_new_client, 2} -> :ok end)
      stub(@server_handler, :up, fn _ -> :ok end)
      stub(@client_handler, :up, fn -> :ok end)
      stub(@server_handler, :down, fn _ -> :ok end)
      stub(@client_handler, :down, fn -> :ok end)

      expect(@server_handler, :handle_connection_response, fn {"abc123", 1}, :hello_new_client ->
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

      assert Sink.Connection.Client.connected?()
      assert Sink.Connection.Client.active?()
      assert Sink.Connection.ServerHandler.connected?("abc123")
      assert Sink.Connection.ServerHandler.active?("abc123")

      stop_supervised!(Sink.Connection.Client)
      stop_supervised!(Sink.Connection.ServerListener)
    end

    test "mismatched client", %{server_ssl: server_ssl, client_ssl: client_ssl} do
      stub(@server_handler, :authenticate_client, fn _ -> {:ok, "abc123"} end)
      stub(@client_handler, :instantiated_ats, fn -> {1, nil} end)
      stub(@server_handler, :instantiated_ats, fn "abc123" -> {:ok, {5, 2}} end)
      expect(@client_handler, :handle_connection_response, fn {:mismatched_client, 5} -> :ok end)

      expect(@server_handler, :handle_connection_response, fn {"abc123", 1},
                                                              {:mismatched_client, 5} ->
        :ok
      end)

      stub(@server_handler, :up, fn _ -> :ok end)
      stub(@client_handler, :up, fn -> :ok end)
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

      # # give it time to connect

      Process.sleep(@time_to_connect)

      assert Sink.Connection.Client.connected?()
      refute Sink.Connection.Client.active?()
      assert Sink.Connection.ServerHandler.connected?("abc123")
      refute Sink.Connection.ServerHandler.active?("abc123")

      assert {:error, :inactive} == Sink.Connection.Client.publish(@event, @ack_key)

      assert {:error, :inactive} ==
               Sink.Connection.ServerHandler.publish("abc123", @event, @ack_key)

      stop_supervised!(Sink.Connection.Client)
      stop_supervised!(Sink.Connection.ServerListener)
    end

    test "mismatched server", %{server_ssl: server_ssl, client_ssl: client_ssl} do
      stub(@server_handler, :authenticate_client, fn _ -> {:ok, "abc123"} end)
      stub(@client_handler, :instantiated_ats, fn -> {1, 5} end)
      stub(@server_handler, :instantiated_ats, fn "abc123" -> {:ok, {1, 2}} end)
      expect(@client_handler, :handle_connection_response, fn {:mismatched_server, 2} -> :ok end)

      expect(@server_handler, :handle_connection_response, fn {"abc123", 1},
                                                              {:mismatched_server, 5} ->
        :ok
      end)

      stub(@server_handler, :up, fn _ -> :ok end)
      stub(@client_handler, :up, fn -> :ok end)
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

      # # give it time to connect

      Process.sleep(@time_to_connect)

      assert Sink.Connection.Client.connected?()
      refute Sink.Connection.Client.active?()
      assert Sink.Connection.ServerHandler.connected?("abc123")
      refute Sink.Connection.ServerHandler.active?("abc123")

      stop_supervised!(Sink.Connection.Client)
      stop_supervised!(Sink.Connection.ServerListener)
    end

    test "quarantined", %{server_ssl: server_ssl, client_ssl: client_ssl} do
      expected_response = {:quarantined, {<<1, 1, 1>>, "blocked"}}
      stub(@server_handler, :authenticate_client, fn _ -> {:ok, "abc123"} end)
      stub(@client_handler, :instantiated_ats, fn -> {1, 2} end)
      stub(@server_handler, :instantiated_ats, fn "abc123" -> expected_response end)
      expect(@client_handler, :handle_connection_response, fn ^expected_response -> :ok end)

      expect(@server_handler, :handle_connection_response, fn {"abc123", nil},
                                                              ^expected_response ->
        :ok
      end)

      expect(@server_handler, :down, fn _ -> :ok end)
      expect(@client_handler, :down, fn -> :ok end)

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

      assert Sink.Connection.Client.connected?()
      refute Sink.Connection.Client.active?()
      assert Sink.Connection.ServerHandler.connected?("abc123")
      refute Sink.Connection.ServerHandler.active?("abc123")

      stop_supervised!(Sink.Connection.Client)
      stop_supervised!(Sink.Connection.ServerListener)
    end
  end

  test "server sends message to client, client acks it", %{
    server_ssl: server_ssl,
    client_ssl: client_ssl
  } do
    test = self()
    stub_with(@mod_transport, Sink.Connection.Transport.SSL)
    stub(@server_handler, :authenticate_client, fn _ -> {:ok, "abc123"} end)
    stub(@client_handler, :instantiated_ats, fn -> {1, 2} end)
    stub(@server_handler, :instantiated_ats, fn "abc123" -> {:ok, {1, 2}} end)
    stub(@mod_transport, :send, fn _, _ -> :ok end)
    stub(@client_handler, :handle_connection_response, fn :ok -> :ok end)
    stub(@server_handler, :handle_connection_response, fn {"abc123", 1}, :ok -> :ok end)
    stub(@server_handler, :up, fn _ -> :ok end)
    stub(@client_handler, :up, fn -> :ok end)
    stub(@server_handler, :down, fn _ -> :ok end)
    stub(@client_handler, :down, fn -> :ok end)

    expect(
      @client_handler,
      :handle_publish,
      fn event, _message_id ->
        send(test, {{:client, :publish}, event})
        :ack
      end
    )

    expect(@server_handler, :handle_ack, fn {client_id, 1}, ack_key ->
      send(test, {:ack, client_id, ack_key})
      :ok
    end)

    start_supervised!(
      {Sink.Connection.ServerListener, port: 9999, ssl_opts: server_ssl, handler: @server_handler}
    )

    start_supervised!(
      {Sink.Connection.Client,
       port: 9999, host: "localhost", ssl_opts: client_ssl, handler: @client_handler}
    )

    # give it time to connect
    :timer.sleep(@time_to_connect)

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
  test "server sends message to client, client nacks it", %{
    server_ssl: server_ssl,
    client_ssl: client_ssl
  } do
    test = self()
    stub_with(@mod_transport, Sink.Connection.Transport.SSL)
    stub(@server_handler, :authenticate_client, fn _ -> {:ok, "abc123"} end)
    stub(@client_handler, :instantiated_ats, fn -> {1, 2} end)
    stub(@server_handler, :instantiated_ats, fn "abc123" -> {:ok, {1, 2}} end)
    stub(@mod_transport, :send, fn _, _ -> :ok end)
    stub(@client_handler, :handle_connection_response, fn :ok -> :ok end)
    stub(@server_handler, :handle_connection_response, fn {"abc123", 1}, :ok -> :ok end)
    stub(@server_handler, :up, fn _ -> :ok end)
    stub(@client_handler, :up, fn -> :ok end)
    stub(@server_handler, :down, fn _ -> :ok end)
    stub(@client_handler, :down, fn -> :ok end)

    expect(
      @client_handler,
      :handle_publish,
      fn event, _message_id ->
        send(test, {{:client, :publish}, event})
        raise RuntimeError, "nack reason"
      end
    )

    expect(@server_handler, :handle_nack, fn {client_id, 1}, ack_key, nack_key ->
      send(test, {:nack, client_id, ack_key, nack_key})
      :ok
    end)

    start_supervised!(
      {Sink.Connection.ServerListener, port: 9999, ssl_opts: server_ssl, handler: @server_handler}
    )

    start_supervised!(
      {Sink.Connection.Client,
       port: 9999, host: "localhost", ssl_opts: client_ssl, handler: @client_handler}
    )

    # give it time to connect
    :timer.sleep(@time_to_connect)

    assert Sink.Connection.Client.connected?()
    assert Sink.Connection.ServerHandler.connected?(@client_id)

    # send the event
    Sink.Connection.ServerHandler.publish(@client_id, @event, @ack_key)

    Process.sleep(100)

    assert_received {{:client, :publish}, @event}
    assert_received {:nack, "abc123", @ack_key, {_machine, human}}
    assert human =~ "nack reason"

    stop_supervised!(Sink.Connection.Client)
    stop_supervised!(Sink.Connection.ServerListener)
  end

  test "client sends message to server, server acks it", %{
    server_ssl: server_ssl,
    client_ssl: client_ssl
  } do
    test = self()
    stub_with(@mod_transport, Sink.Connection.Transport.SSL)
    stub(@server_handler, :authenticate_client, fn _ -> {:ok, "abc123"} end)
    stub(@client_handler, :instantiated_ats, fn -> {1, 2} end)
    stub(@server_handler, :instantiated_ats, fn "abc123" -> {:ok, {1, 2}} end)
    stub(@mod_transport, :send, fn _, _ -> :ok end)
    stub(@client_handler, :handle_connection_response, fn :ok -> :ok end)
    stub(@server_handler, :handle_connection_response, fn {"abc123", 1}, :ok -> :ok end)
    stub(@server_handler, :up, fn _ -> :ok end)
    stub(@client_handler, :up, fn -> :ok end)
    stub(@server_handler, :down, fn _ -> :ok end)
    stub(@client_handler, :down, fn -> :ok end)

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

    start_supervised!(
      {Sink.Connection.ServerListener, port: 9999, ssl_opts: server_ssl, handler: @server_handler}
    )

    start_supervised!(
      {Sink.Connection.Client,
       port: 9999, host: "localhost", ssl_opts: client_ssl, handler: @client_handler}
    )

    # give it time to connect
    :timer.sleep(@time_to_connect)

    assert Sink.Connection.Client.connected?()
    assert Sink.Connection.ServerHandler.connected?(@client_id)

    # send the event
    Sink.Connection.Client.publish(@event, @ack_key)

    Process.sleep(100)

    assert_received {{:server, :publish}, @event}
    assert_received {:ack, @ack_key}

    stop_supervised!(Sink.Connection.Client)
    stop_supervised!(Sink.Connection.ServerListener)
  end

  @tag :capture_log
  test "client sends message to server, server nacks it", %{
    server_ssl: server_ssl,
    client_ssl: client_ssl
  } do
    test = self()
    stub_with(@mod_transport, Sink.Connection.Transport.SSL)
    stub(@server_handler, :authenticate_client, fn _ -> {:ok, "abc123"} end)
    stub(@client_handler, :instantiated_ats, fn -> {1, 2} end)
    stub(@server_handler, :instantiated_ats, fn "abc123" -> {:ok, {1, 2}} end)
    stub(@mod_transport, :send, fn _, _ -> :ok end)
    stub(@client_handler, :handle_connection_response, fn :ok -> :ok end)
    stub(@server_handler, :handle_connection_response, fn {"abc123", 1}, :ok -> :ok end)
    stub(@server_handler, :up, fn _ -> :ok end)
    stub(@client_handler, :up, fn -> :ok end)
    stub(@server_handler, :down, fn _ -> :ok end)
    stub(@client_handler, :down, fn -> :ok end)

    expect(
      @server_handler,
      :handle_publish,
      fn {"abc123", _}, event, _message_id ->
        send(test, {{:server, :publish}, event})
        raise RuntimeError, "nack reason"
      end
    )

    expect(@client_handler, :handle_nack, fn ack_key, nack_key ->
      send(test, {:nack, ack_key, nack_key})
      :ok
    end)

    start_supervised!(
      {Sink.Connection.ServerListener, port: 9999, ssl_opts: server_ssl, handler: @server_handler}
    )

    start_supervised!(
      {Sink.Connection.Client,
       port: 9999, host: "localhost", ssl_opts: client_ssl, handler: @client_handler}
    )

    # give it time to connect
    :timer.sleep(@time_to_connect)

    assert Sink.Connection.Client.connected?()
    assert Sink.Connection.ServerHandler.connected?(@client_id)

    # send the event
    Sink.Connection.Client.publish(@event, @ack_key)

    Process.sleep(100)

    assert_received {{:server, :publish}, @event}
    assert_received {:nack, @ack_key, {_machine, human}}
    assert human =~ "nack reason"

    stop_supervised!(Sink.Connection.Client)
    stop_supervised!(Sink.Connection.ServerListener)
  end

  test "tracking freshness", %{
    server_ssl: server_ssl,
    client_ssl: client_ssl
  } do
    test = self()
    stub_with(@mod_transport, Sink.Connection.Transport.SSL)
    stub(@server_handler, :authenticate_client, fn _ -> {:ok, "abc123"} end)
    stub(@client_handler, :instantiated_ats, fn -> {1, 2} end)
    stub(@server_handler, :instantiated_ats, fn "abc123" -> {:ok, {1, 2}} end)
    stub(@mod_transport, :send, fn _, _ -> :ok end)
    stub(@server_handler, :handle_connection_response, fn {"abc123", 1}, :ok -> :ok end)
    stub(@client_handler, :handle_connection_response, fn :ok -> :ok end)
    stub(@server_handler, :up, fn _ -> :ok end)
    stub(@client_handler, :up, fn -> :ok end)
    stub(@server_handler, :down, fn _ -> :ok end)
    stub(@client_handler, :down, fn -> :ok end)

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

    start_supervised!(
      {Sink.Connection.ServerListener, port: 9999, ssl_opts: server_ssl, handler: @server_handler}
    )

    start_supervised!(
      {Sink.Connection.Client,
       port: 9999, host: "localhost", ssl_opts: client_ssl, handler: @client_handler}
    )

    # give it time to connect
    :timer.sleep(@time_to_connect)

    assert Sink.Connection.Client.connected?()
    assert Sink.Connection.ServerHandler.connected?(@client_id)

    # send the event
    Sink.Connection.Client.publish(@event, @ack_key)

    Process.sleep(100)

    assert_received {{:server, :publish}, @event}
    assert_received {:ack, @ack_key}

    assert Sink.Connection.Freshness.get_freshness("abc123", 1) == @event.timestamp

    stop_supervised!(Sink.Connection.Client)
    stop_supervised!(Sink.Connection.ServerListener)
  end

  test "client can handle not getting authenticated", %{
    server_ssl: server_ssl,
    client_ssl: client_ssl
  } do
    stub_with(@mod_transport, Sink.Connection.Transport.SSL)

    stub(@server_handler, :authenticate_client, fn _ ->
      {:error, RuntimeError.exception("Not allowed here!")}
    end)

    stub(@client_handler, :instantiated_ats, fn -> {1, 2} end)
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
        :timer.sleep(10000)

        assert Process.alive?(client)
      end)

    refute logs =~ "already_started"

    stop_supervised!(Sink.Connection.Client)
    stop_supervised!(Sink.Connection.ServerListener)
  end
end
