defmodule Sink.Connection.Server.ConnectionStatus do
  @moduledoc """
  State machine that manages a connection's state and state transitions
  """
  alias Sink.Connection.Protocol

  @type t :: %__MODULE__{
          connection_state:
            :awaiting_connection_request | :connected | :disconnecting | :quarantined,
          server_identifier: nil | binary,
          application_version: term,
          reason: nil | binary
        }

  defstruct [
    :connection_state,
    :server_identifier,
    :application_version,
    :reason
  ]

  @spec init({:ok, Protocol.server_identifier()} | {:quarantine, String.t()}) :: t
  def init(server_identifier_or_quarantine) do
    case server_identifier_or_quarantine do
      {:ok, server_identifier} ->
        %__MODULE__{
          connection_state: :awaiting_connection_request,
          server_identifier: server_identifier
        }

      {:quarantined, reason} ->
        %__MODULE__{
          connection_state: :quarantined,
          reason: reason
        }
    end
  end

  @doc """
  Is the client connected?

  This will only be false if the connection request / response hasn't completed.
  """
  def connected?(state) do
    # todo: remove this app env after connection request has been deployed
    !Application.get_env(:sink, :require_connection_request) ||
      state.connection_state == :connected
  end

  def should_disconnect?(state) do
    state.connection_state == :disconnecting
  end

  def client_instantiated_at(state) do
    if state.client_instantiated_ats do
      {c_instantiated_at, _} = state.client_instantiated_ats
      c_instantiated_at
    else
      nil
    end
  end

  def unsupported_application_version(%__MODULE__{} = state) do
    %__MODULE__{state | connection_state: :disconnecting}
  end

  @doc """
  Handle a connection request from a client.

  When a server receives a connection request message it checks the message details against
  what it expects for that client to ensure everything matches. If something does not match
  then the server will respond with what the mismatch is and either close the connection (tbd)
  or keep it connected, but not active.
  """
  def connection_request(
        %__MODULE__{} = state,
        {:unsupported_protocol_version, _protocol_version}
      ) do
    %__MODULE__{state | connection_state: :disconnecting}
  end

  def connection_request(
        %__MODULE__{connection_state: :quarantined} = state,
        _version,
        _c_instantiated_ats
      ) do
    {{:quarantined, state.reason}, state}
  end

  def connection_request(state, version, client_server_identifier) do
    case check_connection_request(
           state.connection_state,
           state.server_identifier,
           client_server_identifier
         ) do
      {:ok, resp} ->
        {resp, %__MODULE__{state | connection_state: :connected, application_version: version}}

      {:error, resp} ->
        {resp,
         %__MODULE__{state | connection_state: :disconnecting, application_version: version}}
    end
  end

  defp check_connection_request(
         :awaiting_connection_request,
         server_identifier,
         server_identifier
       )
       when is_integer(server_identifier) do
    {:ok, :connected}
  end

  defp check_connection_request(:awaiting_connection_request, server_identifier, nil)
       when is_integer(server_identifier) do
    {:ok, {:hello_new_client, server_identifier}}
  end

  defp check_connection_request(
         :awaiting_connection_request,
         server_identifier,
         client_server_identifier
       )
       when is_integer(server_identifier) and is_integer(client_server_identifier) do
    {:error, :server_identifier_mismatch}
  end
end
