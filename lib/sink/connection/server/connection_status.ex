defmodule Sink.Connection.Server.ConnectionStatus do
  @moduledoc """
  State machine that manages a connection's state and state transitions
  """
  alias Sink.Connection.Protocol

  @type t :: %__MODULE__{
          connection_state:
            :awaiting_connection_request | :connected | :disconnecting | :quarantined,
          instance_ids: %{server: Protocol.instance_id(), client: nil | Protocol.instance_id()},
          application_version: term,
          reason: nil | binary
        }

  defstruct [
    :connection_state,
    :instance_ids,
    :application_version,
    :reason
  ]

  @spec init(
          {:ok, Protocol.instance_id(), Protocol.instance_id() | nil}
          | {:quarantine, Protocol.nack_data()}
        ) :: t
  def init(instance_ids_or_quarantine) do
    case instance_ids_or_quarantine do
      {:ok, %{server: _, client: _} = instance_ids} ->
        %__MODULE__{
          connection_state: :awaiting_connection_request,
          instance_ids: Map.take(instance_ids, [:server, :client])
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

  def unsupported_application_version(%__MODULE__{} = state) do
    %__MODULE__{state | connection_state: :disconnecting}
  end

  @doc """
  Handle a connection request from a client.

  When a server receives a connection request message it checks the message details against
  what it expects for that client to ensure everything matches. If something does not match
  then the server will respond with what the mismatch is and close the connection.
  """
  def connection_request(
        %__MODULE__{} = state,
        :unsupported_protocol_version
      ) do
    %__MODULE__{state | connection_state: :disconnecting}
  end

  def connection_request(
        %__MODULE__{connection_state: :quarantined} = state,
        _version,
        _instance_ids
      ) do
    {{:quarantined, state.reason}, state}
  end

  def connection_request(state, version, instance_ids_inc) do
    case check_connection_request(
           state.connection_state,
           state.instance_ids,
           instance_ids_inc
         ) do
      {:ok, resp} ->
        {resp, %__MODULE__{state | connection_state: :connected, application_version: version}}

      {:error, resp} ->
        {resp,
         %__MODULE__{state | connection_state: :disconnecting, application_version: version}}
    end
  end

  defp check_connection_request(:awaiting_connection_request, server_ids, client_ids) do
    case Sink.Connection.instance_id_handshake(server_ids, client_ids) do
      {:ok, :known} -> {:ok, :connected}
      {:ok, :new} -> {:ok, {:hello_new_client, server_ids.server}}
      {:error, :mismatch} -> {:error, :instance_id_mismatch}
    end
  end
end
