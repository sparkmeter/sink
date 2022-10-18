defmodule Sink.Connection.Client.ConnectionStatus do
  @moduledoc """
  State machine that manages a connection's state and state transitions
  """
  alias Sink.Connection.Protocol

  @type t :: %__MODULE__{
          connection_state: :requesting_connection | :connected | :disconnecting,
          server_identifier: Protocol.server_identifierentifier() | nil,
          reason: nil | binary
        }

  defstruct [
    :connection_state,
    :server_identifier,
    :reason
  ]

  @spec init(Protocol.server_identifier() | nil) :: t
  def init(server_identifier) when is_nil(server_identifier) or is_integer(server_identifier) do
    %__MODULE__{
      connection_state: :requesting_connection,
      server_identifier: server_identifier,
      reason: nil
    }
  end

  @doc """
  Is the client connected to the server?

  This will only be false if the connection request / response hasn't completed.
  """
  def connected?(%__MODULE__{connection_state: :connected}), do: true
  def connected?(%__MODULE__{connection_state: _}), do: false

  def server_identifier(%__MODULE__{server_identifier: hash}), do: hash

  def connection_response(
        %__MODULE__{connection_state: :requesting_connection} = state,
        {:hello_new_client, server_identifier}
      ) do
    %__MODULE__{state | connection_state: :connected, server_identifier: server_identifier}
  end

  def connection_response(
        %__MODULE__{connection_state: :requesting_connection} = state,
        :connected
      ) do
    %__MODULE__{state | connection_state: :connected}
  end

  def connection_response(
        %__MODULE__{connection_state: :requesting_connection} = state,
        :server_identifier_mismatch
      ) do
    %__MODULE__{state | connection_state: :disconnecting}
  end

  def connection_response(
        %__MODULE__{connection_state: :requesting_connection} = state,
        {:quarantined, reason}
      ) do
    %__MODULE__{state | connection_state: :disconnecting, reason: reason}
  end

  def connection_response(
        %__MODULE__{connection_state: :requesting_connection} = state,
        :unsupported_application_version
      ) do
    %__MODULE__{state | connection_state: :disconnecting}
  end
end
