defmodule Sink.Connection.ClientConnectionHandler do
  @moduledoc """
  Defines the interface for connection events.
  """
  alias Sink.Connection.Protocol

  @type ack_key() :: {event_type_id(), key(), offset()}
  @type event_type_id() :: pos_integer()
  @type key() :: binary()
  @type offset() :: non_neg_integer()
  @type schema_version() :: non_neg_integer()
  @type event_data() :: binary()
  @type message_id() :: non_neg_integer()
  @type connection_responses ::
          :connected
          | {:hello_new_client, server_instance_id :: Protocol.instance_id()}
          | :instance_id_mismatch
          | {:quarantined, Protocol.nack_data()}
          | :unsupported_protocol_version
          | :unsupported_application_version

  @doc """
  Tell the connection the instance_id of previous connections if present
  """
  @callback instance_ids() :: %{
              server: nil | Protocol.instance_id(),
              client: Protocol.instance_id()
            }

  @doc """
  Tell the connection what application version is running.
  """
  @callback application_version() :: Protocol.application_version()

  @doc """
  Run implementer's logic for handling a "connection response"
  """
  @callback handle_connection_response(connection_responses) :: :ok

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
