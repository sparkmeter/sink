defmodule Sink.Connection.ClientConnectionHandler do
  @moduledoc """
  Defines the interface for connection events.
  """
  alias Sink.Connection

  @type ack_key() :: {event_type_id(), key(), offset()}
  @type event_type_id() :: pos_integer()
  @type key() :: binary()
  @type offset() :: non_neg_integer()
  @type schema_version() :: non_neg_integer()
  @type event_data() :: binary()
  @type message_id() :: non_neg_integer()
  @type nack_data() :: {binary(), String.t()}

  @doc """
  Tell the connection when the client was instantiated_at and when it expects the server
  was instantiated at. Used to ensure the client and server haven't been wiped or reset
  between the last connection. See the connection request/response documentation for more.
  """
  @callback instantiated_ats() :: {non_neg_integer(), non_neg_integer() | nil}

  @doc """
  The connection has been closed
  """
  @callback down() :: :ok

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
  @callback handle_nack(ack_key(), nack_data()) :: :ok

  @doc """
  Run implementer's logic for handling a "publish" message
  """
  @callback handle_publish(Sink.Event.t(), message_id()) :: :ack
end
