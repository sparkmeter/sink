defmodule Sink.Connection.Client.ConnectionStatus do
  @moduledoc """
  State machine that manages a connection's state and state transitions
  """

  defstruct [
    :connection_state,
    :client_instantiated_at,
    :server_instantiated_at,
    :reason
  ]

  def init(instantiated_ats) do
    {client_instantiated_at, server_instantiated_at} = instantiated_ats

    %__MODULE__{
      connection_state: :requesting_connection,
      client_instantiated_at: client_instantiated_at,
      server_instantiated_at: server_instantiated_at
    }
  end

  @doc """
  Is the client connected to the server?

  This will only be false if the connection request / response hasn't completed.
  """
  def connected?(%__MODULE__{connection_state: :connected}), do: true
  def connected?(%__MODULE__{connection_state: _}), do: false

  def instantiated_ats(%__MODULE__{} = state) do
    {state.client_instantiated_at, state.server_instantiated_at}
  end

  def connection_response(
        %__MODULE__{connection_state: :requesting_connection} = state,
        :connected
      ) do
    %__MODULE__{state | connection_state: :connected}
  end

  def connection_response(
        %__MODULE__{connection_state: :requesting_connection} = state,
        {:hello_new_client, server_instantiated_at}
      ) do
    %__MODULE__{
      state
      | connection_state: :connected,
        server_instantiated_at: server_instantiated_at
    }
  end

  def connection_response(
        %__MODULE__{connection_state: :requesting_connection} = state,
        :mismatched_client
      ) do
    %__MODULE__{state | connection_state: :disconnecting}
  end

  def connection_response(
        %__MODULE__{connection_state: :requesting_connection} = state,
        :mismatched_server
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
