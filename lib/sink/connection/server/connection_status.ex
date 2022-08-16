defmodule Sink.Connection.Server.ConnectionStatus do
  @moduledoc """
  State machine that manages a connection's state and state transitions
  """

  defstruct [
    :connection_state,
    :client_instantiated_ats,
    :server_instantiated_ats,
    :application_version,
    :reason
  ]

  def init(instantiated_ats_resp) do
    case instantiated_ats_resp do
      {:ok, instantiated_ats} ->
        %__MODULE__{
          connection_state: :awaiting_connection_request,
          server_instantiated_ats: instantiated_ats
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

  def connection_request(state, version, c_instantiated_ats) do
    case check_connection_request(
           state.connection_state,
           state.server_instantiated_ats,
           c_instantiated_ats
         ) do
      :connected ->
        {:connected,
         %__MODULE__{
           state
           | connection_state: :connected,
             client_instantiated_ats: c_instantiated_ats,
             application_version: version
         }}

      :hello_new_client ->
        {c_instantiated_at, _} = c_instantiated_ats
        {_, s_intantiated_at} = state.server_instantiated_ats
        new_s_instantiated_ats = {c_instantiated_at, s_intantiated_at}

        {{:hello_new_client, s_intantiated_at},
         %__MODULE__{
           state
           | connection_state: :connected,
             server_instantiated_ats: new_s_instantiated_ats,
             client_instantiated_ats: c_instantiated_ats,
             application_version: version
         }}

      :mismatched_client ->
        {:mismatched_client,
         %__MODULE__{
           state
           | connection_state: :disconnecting,
             client_instantiated_ats: c_instantiated_ats,
             application_version: version
         }}

      :mismatched_server ->
        {:mismatched_server,
         %__MODULE__{
           state
           | connection_state: :disconnecting,
             client_instantiated_ats: c_instantiated_ats,
             application_version: version
         }}
    end
  end

  defp check_connection_request(
         :awaiting_connection_request,
         {s_c_at, s_s_at},
         {c_c_at, c_s_at}
       ) do
    cond do
      {s_s_at, s_c_at} == {c_s_at, c_c_at} ->
        :connected

      # if the server has never seen the client and the client has never seen the server or know what server to expect
      is_nil(s_c_at) && (is_nil(c_s_at) || s_s_at == c_s_at) ->
        :hello_new_client

      s_c_at != c_c_at ->
        :mismatched_client

      s_s_at != c_s_at ->
        :mismatched_server
    end
  end
end
