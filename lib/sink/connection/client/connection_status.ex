defmodule Sink.Connection.Client.ConnectionStatus do
  @moduledoc """
  State machine that manages a connection's state and state transitions
  """
  alias Sink.Connection.Protocol

  @type t :: %__MODULE__{
          connection_state: :requesting_connection | :connected | :disconnecting,
          instance_id: Protocol.instance_id() | nil,
          reason: nil | binary
        }

  defstruct [
    :connection_state,
    :instance_id,
    :reason
  ]

  @spec init(Protocol.instance_id() | nil) :: t
  def init(instance_id) when is_nil(instance_id) or is_integer(instance_id) do
    %__MODULE__{
      connection_state: :requesting_connection,
      instance_id: instance_id,
      reason: nil
    }
  end

  @doc """
  Is the client connected to the server?

  This will only be false if the connection request / response hasn't completed.
  """
  def connected?(%__MODULE__{connection_state: :connected}), do: true
  def connected?(%__MODULE__{connection_state: _}), do: false

  def instance_id(%__MODULE__{instance_id: hash}), do: hash

  def connection_response(
        %__MODULE__{connection_state: :requesting_connection} = state,
        {:hello_new_client, instance_id}
      ) do
    %__MODULE__{state | connection_state: :connected, instance_id: instance_id}
  end

  def connection_response(
        %__MODULE__{connection_state: :requesting_connection} = state,
        :connected
      ) do
    %__MODULE__{state | connection_state: :connected}
  end

  def connection_response(
        %__MODULE__{connection_state: :requesting_connection} = state,
        :instance_id_mismatch
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
