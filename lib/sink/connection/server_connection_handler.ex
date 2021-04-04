defmodule Sink.Connection.ServerConnectionHandler do
  @moduledoc """
  Defines the interface for connection events.
  """

  @type ack_key() :: {event_type_id(), key(), offset()}
  @type client_id() :: String.t()
  @type event_type_id() :: pos_integer()
  @type key() :: binary()
  @type offset() :: non_neg_integer()
  @type event_data() :: binary()
  @type message_id() :: non_neg_integer()
  @type peer_cert() :: binary()

  @doc """
  The connection has been established and authenticated
  """
  @callback up(client_id()) :: :ok

  @doc """
  The connection has been closed
  """
  @callback down(client_id()) :: :ok

  @doc """
  Run implementer's authentication logic
  """
  @callback authenticate_client(peer_cert()) :: {:ok, client_id()} | {:error, :unknown_client}

  @doc """
  Run implementer's logic for handling a "ack"
  """
  @callback handle_ack(client_id(), ack_key()) :: :ok

  @doc """
  Run implementer's logic for handling a "publish" message.

  Should respond with either an ack or a nack with information about the nack
  """
  @callback handle_publish(
              {client_id(), event_type_id(), key()},
              offset(),
              event_data(),
              message_id()
            ) :: :ack
end
