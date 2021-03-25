defmodule Sink.Connection.ClientConnectionHandler do
  @moduledoc """
  Defines the interface for connection events.
  """

  @type ack_key() :: any()
  @type event_type_id() :: pos_integer()
  @type key() :: binary()
  @type offset() :: non_neg_integer()
  @type event_data() :: binary()
  @type message_id() :: non_neg_integer()

  @doc """
  The connection has been established and authenticated
  """
  @callback up() :: :ok

  @doc """
  The connection has been closed
  """
  @callback down() :: :ok

  @doc """
  Run implementer's logic for handling a "ack"

  Takes the pid of the originator and an arbitrary value for ack_key
  """
  @callback handle_ack(pid(), ack_key()) :: :ok

  @doc """
  Run implementer's logic for handling a "publish" message
  """
  @callback handle_publish(
              {event_type_id(), key()},
              offset(),
              event_data(),
              message_id()
            ) :: :ack
end
