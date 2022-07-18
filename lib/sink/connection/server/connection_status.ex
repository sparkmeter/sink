defmodule Sink.Connection.Server.ConnectionStatus do
  @moduledoc """
  State machine that manages a connection's state and state transitions
  """

  defstruct [
    :connection_state,
    :client_instantiated_ats,
    :server_instantiated_ats,
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

  def connected?(state) do
    state.connection_state != :awaiting_connection_request
  end

  def active?(state) do
    state.connection_state == :connected
  end

  def connection_request(state, c_instantiated_ats) do
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
             client_instantiated_ats: c_instantiated_ats
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
             client_instantiated_ats: c_instantiated_ats
         }}

      :mismatched_client ->
        {c_c_instantiated_at, _} = c_instantiated_ats
        {s_c_instantiated_at, _} = state.server_instantiated_ats

        {{:mismatched_client, c_c_instantiated_at, s_c_instantiated_at},
         %__MODULE__{
           state
           | connection_state: :mismatched_client,
             client_instantiated_ats: c_instantiated_ats
         }}

      :mismatched_server ->
        {_, c_s_instantiated_at} = c_instantiated_ats
        {_, s_s_instantiated_at} = state.server_instantiated_ats

        {{:mismatched_server, c_s_instantiated_at, s_s_instantiated_at},
         %__MODULE__{
           state
           | connection_state: :mismatched_server,
             client_instantiated_ats: c_instantiated_ats
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

  #  def quarantine(state) do
  #  end

  #  def unquarantine(state) do
  #  end
end
