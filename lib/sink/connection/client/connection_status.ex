defmodule Sink.Connection.Client.ConnectionStatus do
  @moduledoc """
  State machine that manages a connection's state and state transitions
  """
  alias Sink.Connection.Protocol

  @type t :: %__MODULE__{
          connection_state: :requesting_connection | :connected | :disconnecting,
          instance_ids: %{server: nil | Protocol.instance_id(), client: Protocol.instance_id()},
          reason: nil | binary
        }

  defstruct [
    :connection_state,
    :instance_ids,
    :reason
  ]

  @spec init(%{server: nil | Protocol.instance_id(), client: Protocol.instance_id()}) :: t
  def init(%{server: server, client: client} = instance_ids)
      when is_nil(server) or (is_integer(server) and is_integer(client)) do
    %__MODULE__{
      connection_state: :requesting_connection,
      instance_ids: instance_ids,
      reason: nil
    }
  end

  @doc """
  Is the client connected to the server?

  This will only be false if the connection request / response hasn't completed.
  """
  def connected?(%__MODULE__{connection_state: :connected}), do: true
  def connected?(%__MODULE__{connection_state: _}), do: false

  def instance_ids(%__MODULE__{instance_ids: map}), do: map

  def connection_response(
        %__MODULE__{connection_state: :requesting_connection} = state,
        {:hello_new_client, server_instance_id}
      ) do
    instance_ids = Map.put(state.instance_ids, :server, server_instance_id)
    %__MODULE__{state | connection_state: :connected, instance_ids: instance_ids}
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
