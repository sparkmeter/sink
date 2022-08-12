defmodule Sink.Connection.ServerConnectionHandler do
  @moduledoc """
  Defines the interface for connection events.
  """
  alias Sink.Connection

  @type ack_key() :: {event_type_id(), key(), offset()}
  @type client_id() :: String.t()
  @type client() :: {String.t(), Connection.timestamp()}
  @type event_type_id() :: pos_integer()
  @type key() :: binary()
  @type offset() :: non_neg_integer()
  @type schema_version() :: non_neg_integer()
  @type event_data() :: binary()
  @type message_id() :: non_neg_integer()
  @type nack_data() :: {binary(), String.t()}
  @type peer_cert() :: binary()

  @doc """
  When the client and server were instantiated. Used to ensure the client and server
  haven't been wiped or reset between the last connection. See the connection
  request/response documentation for more.

  todo: consider coming up with a different name for this
  """

  @callback instantiated_ats(client_id()) ::
              {:ok, non_neg_integer() | nil, non_neg_integer()}
              | {:quarantined, {binary(), binary()}}

  @doc """
  Check that the client's firmware version is compatible with the server.
  """
  @callback supported_version?(client_id(), String.t()) :: boolean()

  @doc """
  The connection has been closed
  """
  @callback down(client()) :: :ok

  @doc """
  Run implementer's authentication logic
  """
  @callback authenticate_client(peer_cert()) :: {:ok, client_id()} | {:error, Exception.t()}

  @doc """
  Run implementer's logic for handling a "connection response"
  """
  @callback handle_connection_response(client(), Connection.connection_responses()) :: :ok

  @doc """
  Run implementer's logic for handling a "ack"
  """
  @callback handle_ack(client(), ack_key()) :: :ok

  @doc """
  Run implementer's logic for handling a "nack"
  """
  @callback handle_nack(client(), ack_key(), nack_data()) :: :ok

  @doc """
  Run implementer's logic for handling a "publish" message.

  Should respond with either an ack or a nack with information about the nack
  """
  @callback handle_publish(client(), Sink.Event.t(), message_id()) ::
              :ack | {:nack, nack_data()}
end
