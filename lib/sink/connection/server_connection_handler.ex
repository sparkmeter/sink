defmodule Sink.Connection.ServerConnectionHandler do
  @moduledoc """
  Defines the interface for connection events.
  """
  alias Sink.Connection.Protocol

  @type ack_key() :: {event_type_id(), key(), offset()}
  @type client_id() :: String.t()
  @type event_type_id() :: pos_integer()
  @type key() :: binary()
  @type offset() :: non_neg_integer()
  @type schema_version() :: non_neg_integer()
  @type event_data() :: binary()
  @type message_id() :: non_neg_integer()
  @type peer_cert() :: binary()
  @type connection_responses ::
          :connected
          | {:hello_new_client, client_instance_id :: Protocol.instance_id()}
          | :instance_id_mismatch
          | {:quarantined, Protocol.nack_data()}
          | :unsupported_protocol_version
          | :unsupported_application_version

  @type handler_state :: term

  @doc """
  Run implementer's authentication logic
  """
  @callback authenticate_client(peer_cert()) :: {:ok, client_id()} | {:error, Exception.t()}

  @doc """
  Return either `{:ok, %{server: Protocol.instance_id(), client: nil | Protocol.instance_id()}}` to
  make the handshake by or `{:quarantined, reason}` to disconnect.
  """

  @callback client_configuration(client_id()) ::
              {:ok,
               %{
                 server: Protocol.instance_id(),
                 client: nil | Protocol.instance_id()
               }, handler_state}
              | {:quarantined, {binary(), binary()}, handler_state}

  @doc """
  Check that the client's firmware version is compatible with the server.
  """
  @callback supported_application_version?(
              client_id(),
              Protocol.application_version(),
              handler_state
            ) ::
              boolean()

  @doc """
  Run implementer's logic for handling a "connection response"
  """
  @callback handle_connection_response(client_id(), connection_responses, handler_state) ::
              :ok

  @doc """
  Run implementer's logic for handling a "ack"
  """
  @callback handle_ack(client_id(), ack_key(), handler_state) :: :ok

  @doc """
  Run implementer's logic for handling a "nack"
  """
  @callback handle_nack(client_id(), ack_key(), Protocol.nack_data(), handler_state) :: :ok

  @doc """
  Run implementer's logic for handling a "publish" message.

  Should respond with either an ack or a nack with information about the nack
  """
  @callback handle_publish(client_id(), Sink.Event.t(), message_id(), handler_state) ::
              :ack | {:nack, Protocol.nack_data()}

  @doc """
  The connection has been closed
  """
  @callback down(client_id(), handler_state) :: :ok
end
