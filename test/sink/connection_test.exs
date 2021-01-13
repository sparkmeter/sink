defmodule Sink.ConnectionTest do
  use ExUnit.Case, async: false

  alias Sink.TestEvent

  @base "/Users/mike/Code/sparkmeter/gladys/"
  @client_certfile @base <> "local-integration/pki/integration-edith-1-cert.pem"
  @client_keyfile @base <> "local-integration/pki/integration-edith-1-key.pem"
  @server_certfile @base <> "local-integration/pki/integration-broker-1-cert.pem"
  @server_keyfile @base <> "local-integration/pki/integration-broker-1-key.pem"
  @cacertfile @base <> "local-integration/pki/integration-ca-1-cert.pem"

  @client_id "abc123"
  @server_ssl_opts [
    ip: {0, 0, 0, 0},
    # protocol: :tls,
    keyfile: @server_keyfile,
    certfile: @server_certfile,
    cacertfile: @cacertfile,
    secure_renegotiate: true,
    reuse_sessions: true,
    verify: :verify_peer,
    fail_if_no_peer_cert: true
  ]
  @client_ssl_opts [
    # protocol: :tls,
    certfile: @client_certfile,
    keyfile: @client_keyfile,
    cacertfile: @cacertfile,
    secure_renegotiate: true,
    reuse_sessions: true,
    verify: :verify_peer,
    fail_if_no_peer_cert: true
  ]
  @key <<1, 2, 3>>

  setup do
    # Explicitly get a connection before each test
    # :ok = Ecto.Adapters.SQL.Sandbox.checkout(Sink.TestRepo)
    :ok
  end

  @tag :skip
  test "client can connect" do
    start_supervised!({Sink.Connection.ServerListener, port: 9999, ssl_opts: @server_ssl_opts})

    start_supervised!({Sink.Connection.Client, port: 9999, ssl_opts: @client_ssl_opts})

    assert false == Sink.Connection.Client.connected?()
    assert false == Sink.Connection.ServerHandler.connected?(@client_id)

    # give it time to connect
    :timer.sleep(100)

    assert true == Sink.Connection.Client.connected?()
    assert true == Sink.Connection.ServerHandler.connected?(@client_id)
  end

  @tag :skip
  test "server sends message to client, client acks it" do
    # start the server, log a event, subscribe our client to that event_type
    start_supervised!({Sink.Connection.ServerListener, port: 9999, ssl_opts: @server_ssl_opts})

    # start the client
    start_supervised!({Sink.Connection.Client, port: 9999, ssl_opts: @client_ssl_opts})

    # give it time to connect
    :timer.sleep(100)

    assert true == Sink.Connection.Client.connected?()
    assert true == Sink.Connection.ServerHandler.connected?(@client_id)

    # send the message
    event = %TestEvent{key: @key, offset: 1, message: "hi!"}
    binary = :erlang.term_to_binary(event)
    ack_key = {@client_id, "test_event", event.key, event.offset}
    Sink.Connection.ServerHandler.publish(@client_id, binary, ack_key)

    # wait for ack
    receive do
      {:ack, recv_ack_key} ->
        assert ack_key == recv_ack_key
    after
      1_000 ->
        assert false, "timeout waiting for ack from client"
    end
  end

  @tag :skip
  test "client sends message to server, server acks it" do
    # start the client
    start_supervised!({Sink.Connection.Client, port: 9999, ssl_opts: @client_ssl_opts})

    # start the server
    start_supervised!({Sink.Connection.ServerListener, port: 9999, ssl_opts: @server_ssl_opts})

    # give it time to connect
    :timer.sleep(100)

    assert true == Sink.Connection.Client.connected?()
    assert true == Sink.Connection.ServerHandler.connected?(@client_id)

    event = %TestEvent{key: @key, offset: 1, message: "hi!"}
    binary = :erlang.term_to_binary(event)
    ack_key = {"test_event", event.key, event.offset}
    Sink.Connection.Client.publish(binary, ack_key: ack_key)

    # wait for ack
    receive do
      {:ack, recv_ack_key} ->
        assert ack_key == recv_ack_key
    after
      1_000 ->
        assert false, "timeout waiting for ack from server"
    end
  end
end
