defmodule Sink.Connection.ClientConnectionHandler do
  @moduledoc """
  Defines the interface for connection events.
  """
  alias Sink.Connection
  alias Sink.Connection.Protocol

  @type ack_key() :: {event_type_id(), key(), offset()}
  @type event_type_id() :: pos_integer()
  @type key() :: binary()
  @type offset() :: non_neg_integer()
  @type schema_version() :: non_neg_integer()
  @type event_data() :: binary()
  @type message_id() :: non_neg_integer()

  @doc """
  Tell the connection the server_identifier of previous connections if present
  """
  @callback last_server_identifier() :: Protocol.server_identifier() | nil

  @doc """
  Tell the connection what application version is running.
  """
  @callback version() :: Protocol.version()

  @doc """
  Run implementer's logic for handling a "connection response"
  """
  @callback handle_connection_response(Connection.connection_responses()) :: :ok

  @doc """
  Run implementer's logic for handling a "ack"
  """
  @callback handle_ack(ack_key()) :: :ok

  @doc """
  Run implementer's logic for handling a "nack"
  """
  @callback handle_nack(ack_key(), Protocol.nack_data()) :: :ok

  @doc """
  Run implementer's logic for handling a "publish" message
  """
  @callback handle_publish(Sink.Event.t(), message_id()) :: :ack

  @doc """
  The connection has been closed
  """
  @callback down() :: :ok
end
