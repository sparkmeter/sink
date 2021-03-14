defmodule Sink.Connection.ServerConnectionHandler do
  @moduledoc """
  Defines the interface for connection events.
  """

  @type ack_key() :: any()
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

  Takes the pid of the originator and an arbitrary value for ack_key
  """
  @callback handle_ack(pid(), client_id(), ack_key()) :: :ok

  @doc """
  Run implementer's logic for handling a "publish" message
  """
  @callback handle_publish(
              {client_id(), event_type_id(), key()},
              offset(),
              event_data(),
              message_id()
            ) :: :ack
end
