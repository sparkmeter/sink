defmodule Sink.ConnectionTest do
  use ExUnit.Case, async: false
  import ExUnit.CaptureLog
  import Mox
  alias Sink.Connection.ClientConnectionHandlerMock
  alias Sink.Connection.ServerConnectionHandlerMock
  alias Sink.Connection.Transport.SSLMock
  alias Sink.Event
  alias Sink.TestEvent
  alias Sink.Test.Certificates

  @client_id "abc123"

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

  test "client can connect", %{server_ssl: server_ssl, client_ssl: client_ssl} do
    stub_with(SSLMock, Sink.Connection.Transport.SSL)
    stub(ServerConnectionHandlerMock, :authenticate_client, fn _ -> {:ok, "abc123"} end)
    stub(ServerConnectionHandlerMock, :up, fn _ -> :ok end)
    stub(ServerConnectionHandlerMock, :down, fn _ -> :ok end)
    stub(ClientConnectionHandlerMock, :up, fn -> :ok end)
    stub(ClientConnectionHandlerMock, :down, fn -> :ok end)

    start_supervised!(
      {Sink.Connection.ServerListener,
       port: 9999, ssl_opts: server_ssl, handler: ServerConnectionHandlerMock}
    )

    start_supervised!(
      {Sink.Connection.Client,
       port: 9999, host: "localhost", ssl_opts: client_ssl, handler: ClientConnectionHandlerMock}
    )

    refute Sink.Connection.Client.connected?()
    refute Sink.Connection.ServerHandler.connected?("abc123")

    # # give it time to connect

    Process.sleep(300)

    assert Sink.Connection.Client.connected?()
    assert Sink.Connection.ServerHandler.connected?("abc123")

    stop_supervised!(Sink.Connection.Client)
    stop_supervised!(Sink.Connection.ServerListener)
  end

  test "server sends message to client, client acks it", %{
    server_ssl: server_ssl,
    client_ssl: client_ssl
  } do
    test = self()
    stub_with(SSLMock, Sink.Connection.Transport.SSL)
    stub(ServerConnectionHandlerMock, :authenticate_client, fn _ -> {:ok, "abc123"} end)
    stub(ServerConnectionHandlerMock, :up, fn _ -> :ok end)
    stub(ServerConnectionHandlerMock, :down, fn _ -> :ok end)
    stub(ClientConnectionHandlerMock, :up, fn -> :ok end)
    stub(ClientConnectionHandlerMock, :down, fn -> :ok end)

    expect(
      ClientConnectionHandlerMock,
      :handle_publish,
      fn topic, offset, schema_version, timestamp, data, _message_id ->
        send(test, {{:client, :publish}, topic, offset, schema_version, timestamp, data})
        :ack
      end
    )

    expect(ServerConnectionHandlerMock, :handle_ack, fn client_id, ack_key ->
      send(test, {:ack, client_id, ack_key})
      :ok
    end)

    start_supervised!(
      {Sink.Connection.ServerListener,
       port: 9999, ssl_opts: server_ssl, handler: ServerConnectionHandlerMock}
    )

    start_supervised!(
      {Sink.Connection.Client,
       port: 9999, host: "localhost", ssl_opts: client_ssl, handler: ClientConnectionHandlerMock}
    )

    # give it time to connect
    :timer.sleep(300)

    assert Sink.Connection.Client.connected?()
    assert Sink.Connection.ServerHandler.connected?(@client_id)

    # send the message
    event = %TestEvent{key: <<1, 2, 3>>, offset: 1, message: "hi!", version: 1}
    event_data = :erlang.term_to_binary(event)
    event_type_id = 1
    ack_key = {event_type_id, event.key, event.offset}
    timestamp = DateTime.to_unix(DateTime.utc_now())

    message = %Event{
      event_type_id: event_type_id,
      key: event.key,
      offset: event.offset,
      timestamp: timestamp,
      event_data: event_data,
      schema_version: event.version
    }

    Sink.Connection.ServerHandler.publish(@client_id, message, ack_key)

    Process.sleep(100)

    assert_received {{:client, :publish}, topic, offset, schema_version, timestamp, data}
    assert topic == {event_type_id, event.key}
    assert offset == event.offset
    assert schema_version == event.version
    assert timestamp == timestamp
    assert data == event_data

    assert_received {:ack, "abc123", ^ack_key}

    stop_supervised!(Sink.Connection.Client)
    stop_supervised!(Sink.Connection.ServerListener)
  end

  @tag :capture_log
  test "server sends message to client, client nacks it", %{
    server_ssl: server_ssl,
    client_ssl: client_ssl
  } do
    test = self()
    stub_with(SSLMock, Sink.Connection.Transport.SSL)
    stub(ServerConnectionHandlerMock, :authenticate_client, fn _ -> {:ok, "abc123"} end)
    stub(ServerConnectionHandlerMock, :up, fn _ -> :ok end)
    stub(ServerConnectionHandlerMock, :down, fn _ -> :ok end)
    stub(ClientConnectionHandlerMock, :up, fn -> :ok end)
    stub(ClientConnectionHandlerMock, :down, fn -> :ok end)

    expect(
      ClientConnectionHandlerMock,
      :handle_publish,
      fn topic, offset, schema_version, timestamp, data, _message_id ->
        send(test, {{:client, :publish}, topic, offset, schema_version, timestamp, data})
        raise RuntimeError, "nack reason"
      end
    )

    expect(ServerConnectionHandlerMock, :handle_nack, fn client_id, ack_key, nack_key ->
      send(test, {:nack, client_id, ack_key, nack_key})
      :ok
    end)

    start_supervised!(
      {Sink.Connection.ServerListener,
       port: 9999, ssl_opts: server_ssl, handler: ServerConnectionHandlerMock}
    )

    start_supervised!(
      {Sink.Connection.Client,
       port: 9999, host: "localhost", ssl_opts: client_ssl, handler: ClientConnectionHandlerMock}
    )

    # give it time to connect
    :timer.sleep(300)

    assert Sink.Connection.Client.connected?()
    assert Sink.Connection.ServerHandler.connected?(@client_id)

    # send the message
    event = %TestEvent{key: <<1, 2, 3>>, offset: 1, message: "hi!", version: 1}
    event_data = :erlang.term_to_binary(event)
    event_type_id = 1
    ack_key = {event_type_id, event.key, event.offset}
    timestamp = DateTime.to_unix(DateTime.utc_now())

    message = %Event{
      event_type_id: event_type_id,
      key: event.key,
      offset: event.offset,
      timestamp: timestamp,
      event_data: event_data,
      schema_version: event.version
    }

    Sink.Connection.ServerHandler.publish(@client_id, message, ack_key)

    Process.sleep(100)

    assert_received {{:client, :publish}, topic, offset, schema_version, timestamp, data}
    assert topic == {event_type_id, event.key}
    assert offset == event.offset
    assert schema_version == event.version
    assert timestamp == timestamp
    assert data == event_data

    assert_received {:nack, "abc123", ^ack_key, {_machine, human}}
    assert human =~ "nack reason"

    stop_supervised!(Sink.Connection.Client)
    stop_supervised!(Sink.Connection.ServerListener)
  end

  test "client sends message to server, server acks it", %{
    server_ssl: server_ssl,
    client_ssl: client_ssl
  } do
    test = self()
    stub_with(SSLMock, Sink.Connection.Transport.SSL)
    stub(ServerConnectionHandlerMock, :authenticate_client, fn _ -> {:ok, "abc123"} end)
    stub(ServerConnectionHandlerMock, :up, fn _ -> :ok end)
    stub(ServerConnectionHandlerMock, :down, fn _ -> :ok end)
    stub(ClientConnectionHandlerMock, :up, fn -> :ok end)
    stub(ClientConnectionHandlerMock, :down, fn -> :ok end)

    expect(
      ServerConnectionHandlerMock,
      :handle_publish,
      fn topic, offset, schema_version, timestamp, data, _message_id ->
        send(test, {{:server, :publish}, topic, offset, schema_version, timestamp, data})
        :ack
      end
    )

    expect(ClientConnectionHandlerMock, :handle_ack, fn ack_key ->
      send(test, {:ack, ack_key})
      :ok
    end)

    start_supervised!(
      {Sink.Connection.ServerListener,
       port: 9999, ssl_opts: server_ssl, handler: ServerConnectionHandlerMock}
    )

    start_supervised!(
      {Sink.Connection.Client,
       port: 9999, host: "localhost", ssl_opts: client_ssl, handler: ClientConnectionHandlerMock}
    )

    # give it time to connect
    :timer.sleep(300)

    assert Sink.Connection.Client.connected?()
    assert Sink.Connection.ServerHandler.connected?(@client_id)

    # send the message
    event = %TestEvent{key: <<1, 2, 3>>, offset: 1, message: "hi!", version: 1}
    event_data = :erlang.term_to_binary(event)
    event_type_id = 1
    ack_key = {event_type_id, event.key, event.offset}
    timestamp = DateTime.to_unix(DateTime.utc_now())

    message = %Event{
      event_type_id: event_type_id,
      key: event.key,
      offset: event.offset,
      timestamp: timestamp,
      event_data: event_data,
      schema_version: event.version
    }

    Sink.Connection.Client.publish(message, ack_key)

    Process.sleep(100)

    assert_received {{:server, :publish}, topic, offset, schema_version, timestamp, data}
    assert topic == {"abc123", event_type_id, event.key}
    assert offset == event.offset
    assert schema_version == event.version
    assert timestamp == timestamp
    assert data == event_data

    assert_received {:ack, ^ack_key}

    stop_supervised!(Sink.Connection.Client)
    stop_supervised!(Sink.Connection.ServerListener)
  end

  @tag :capture_log
  test "client sends message to server, server nacks it", %{
    server_ssl: server_ssl,
    client_ssl: client_ssl
  } do
    test = self()
    stub_with(SSLMock, Sink.Connection.Transport.SSL)
    stub(ServerConnectionHandlerMock, :authenticate_client, fn _ -> {:ok, "abc123"} end)
    stub(ServerConnectionHandlerMock, :up, fn _ -> :ok end)
    stub(ServerConnectionHandlerMock, :down, fn _ -> :ok end)
    stub(ClientConnectionHandlerMock, :up, fn -> :ok end)
    stub(ClientConnectionHandlerMock, :down, fn -> :ok end)

    expect(
      ServerConnectionHandlerMock,
      :handle_publish,
      fn topic, offset, schema_version, timestamp, data, _message_id ->
        send(test, {{:server, :publish}, topic, offset, schema_version, timestamp, data})
        raise RuntimeError, "nack reason"
      end
    )

    expect(ClientConnectionHandlerMock, :handle_nack, fn ack_key, nack_key ->
      send(test, {:nack, ack_key, nack_key})
      :ok
    end)

    start_supervised!(
      {Sink.Connection.ServerListener,
       port: 9999, ssl_opts: server_ssl, handler: ServerConnectionHandlerMock}
    )

    start_supervised!(
      {Sink.Connection.Client,
       port: 9999, host: "localhost", ssl_opts: client_ssl, handler: ClientConnectionHandlerMock}
    )

    # give it time to connect
    :timer.sleep(300)

    assert Sink.Connection.Client.connected?()
    assert Sink.Connection.ServerHandler.connected?(@client_id)

    # send the message
    event = %TestEvent{key: <<1, 2, 3>>, offset: 1, message: "hi!", version: 1}
    event_data = :erlang.term_to_binary(event)
    event_type_id = 1
    ack_key = {event_type_id, event.key, event.offset}
    timestamp = DateTime.to_unix(DateTime.utc_now())

    message = %Event{
      event_type_id: event_type_id,
      key: event.key,
      offset: event.offset,
      timestamp: timestamp,
      event_data: event_data,
      schema_version: event.version
    }

    Sink.Connection.Client.publish(message, ack_key)

    Process.sleep(100)

    assert_received {{:server, :publish}, topic, offset, schema_version, timestamp, data}
    assert topic == {"abc123", event_type_id, event.key}
    assert offset == event.offset
    assert schema_version == event.version
    assert timestamp == timestamp
    assert data == event_data

    assert_received {:nack, ^ack_key, {_machine, human}}
    assert human =~ "nack reason"

    stop_supervised!(Sink.Connection.Client)
    stop_supervised!(Sink.Connection.ServerListener)
  end

  test "tracking freshness", %{
    server_ssl: server_ssl,
    client_ssl: client_ssl
  } do
    test = self()
    stub_with(SSLMock, Sink.Connection.Transport.SSL)
    stub(ServerConnectionHandlerMock, :authenticate_client, fn _ -> {:ok, "abc123"} end)
    stub(ServerConnectionHandlerMock, :up, fn _ -> :ok end)
    stub(ServerConnectionHandlerMock, :down, fn _ -> :ok end)
    stub(ClientConnectionHandlerMock, :up, fn -> :ok end)
    stub(ClientConnectionHandlerMock, :down, fn -> :ok end)

    expect(
      ServerConnectionHandlerMock,
      :handle_publish,
      fn topic, offset, schema_version, timestamp, data, _message_id ->
        send(test, {{:server, :publish}, topic, offset, schema_version, timestamp, data})
        :ack
      end
    )

    expect(ClientConnectionHandlerMock, :handle_ack, fn ack_key ->
      send(test, {:ack, ack_key})
      :ok
    end)

    start_supervised!(
      {Sink.Connection.ServerListener,
       port: 9999, ssl_opts: server_ssl, handler: ServerConnectionHandlerMock}
    )

    start_supervised!(
      {Sink.Connection.Client,
       port: 9999, host: "localhost", ssl_opts: client_ssl, handler: ClientConnectionHandlerMock}
    )

    # give it time to connect
    :timer.sleep(300)

    assert Sink.Connection.Client.connected?()
    assert Sink.Connection.ServerHandler.connected?(@client_id)

    # send the message
    event = %TestEvent{key: <<1, 2, 3>>, offset: 1, message: "hi!", version: 1}
    event_data = :erlang.term_to_binary(event)
    event_type_id = 1
    ack_key = {event_type_id, event.key, event.offset}
    timestamp = DateTime.to_unix(DateTime.utc_now())

    message = %Event{
      event_type_id: event_type_id,
      key: event.key,
      offset: event.offset,
      timestamp: timestamp,
      event_data: event_data,
      schema_version: event.version
    }

    Sink.Connection.Client.publish(message, ack_key)

    Process.sleep(100)

    assert_received {{:server, :publish}, topic, offset, schema_version, timestamp, data}
    assert topic == {"abc123", event_type_id, event.key}
    assert offset == event.offset
    assert schema_version == event.version
    assert timestamp == timestamp
    assert data == event_data

    assert_received {:ack, ^ack_key}

    assert Sink.Connection.Freshness.get_freshness("abc123", 1) == timestamp

    stop_supervised!(Sink.Connection.Client)
    stop_supervised!(Sink.Connection.ServerListener)
  end

  test "client can handle not getting authenticated", %{
    server_ssl: server_ssl,
    client_ssl: client_ssl
  } do
    stub_with(SSLMock, Sink.Connection.Transport.SSL)

    stub(ServerConnectionHandlerMock, :authenticate_client, fn _ ->
      {:error, RuntimeError.exception("Not allowed here!")}
    end)

    stub(ServerConnectionHandlerMock, :up, fn _ -> :ok end)
    stub(ServerConnectionHandlerMock, :down, fn _ -> :ok end)
    stub(ClientConnectionHandlerMock, :up, fn -> :ok end)
    stub(ClientConnectionHandlerMock, :down, fn -> :ok end)

    logs =
      capture_log(fn ->
        start_supervised!(
          {Sink.Connection.ServerListener,
           port: 9999, ssl_opts: server_ssl, handler: ServerConnectionHandlerMock}
        )

        client =
          start_supervised!(
            {Sink.Connection.Client,
             port: 9999,
             host: "localhost",
             ssl_opts: client_ssl,
             handler: ClientConnectionHandlerMock}
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
