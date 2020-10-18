defmodule ExampleApp.ExampleServerHandler do
  @moduledoc """
  Handles incoming connections
  """
  use Sink.Connection.Handler
  require Logger

  @impl true
  def authenticate_client(_cert, peername) do
    Logger.info("authenticating client at '#{peername}'")

    {:ok, peername}
  end

  @impl true
  def connection({:ssl_closed}, client_id) do
    Logger.info("ssl closed for client '#{client_id}'")

    :ok
  end

  def connection({:tcp_closed, _}, client_id) do
    Logger.info("tcp closed for client '#{client_id}'")

    :ok
  end

  def connection({:tcp_error, _, _reason}, client_id) do
    Logger.info("tcp error for client '#{client_id}'")

    :ok
  end

  @impl true
  def handle_publish(events, client_id) do
    Logger.info("received #{length(events)} events from client_id '#{client_id}'")

    :ok
  end
end
