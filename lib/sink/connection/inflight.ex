defmodule Sink.Connection.Inflight do
  @moduledoc """
  Encapsulates the details of messages in flight
  """
  defstruct [
    :next_message_id,
    inflight: %{},
    received_nacks: [],
    sent_nacks: []
  ]

  @max_message_id (:math.pow(2, 12) - 1) |> Kernel.trunc()

  def init() do
    %__MODULE__{
      next_message_id: Sink.Connection.next_message_id(nil)
    }
  end

  def advance_message_id(%__MODULE__{} = state) do
    Map.put(state, :next_message_id, next_message_id(state.next_message_id))
  end

  @doc """
  Get the event_type_id, key, and offset of messages that are sent, but not ACK'd or NACK'd

  Note: results will be returned in an arbitrary order since the underlying data structure
  is a map.
  """
  def get_inflight(%__MODULE__{} = state) do
    Map.values(state.inflight)
  end

  def put_inflight(%__MODULE__{} = state, {event_id, key, offset}) do
    state
    |> Map.put(:inflight, Map.put(state.inflight, state.next_message_id, {event_id, key, offset}))
    |> advance_message_id()
  end

  def find_inflight(%__MODULE__{} = state, message_id), do: state.inflight[message_id]

  def inflight?(%__MODULE__{} = state, {event_id, key, offset}) do
    state.inflight
    |> Map.values()
    |> Enum.any?(fn ack_key -> ack_key == {event_id, key, offset} end)
  end

  def remove_inflight(%__MODULE__{} = state, message_id) do
    Map.put(state, :inflight, Map.delete(state.inflight, message_id))
  end

  def get_received_nacks(%__MODULE__{} = state) do
    Enum.map(state.received_nacks, fn {_message_id, ack_key, nack_data} ->
      {ack_key, nack_data}
    end)
  end

  @doc """
  Mark a message that was sent by this connection as NACK'd, remove from inflight
  """
  def put_received_nack(%__MODULE__{} = state, message_id, ack_key, nack_data) do
    nack = {message_id, ack_key, nack_data}

    state
    |> Map.put(:received_nacks, [nack | state.received_nacks])
    |> remove_inflight(message_id)
  end

  def received_nack_count(%__MODULE__{} = state), do: length(state.received_nacks)

  @doc """
  Mark a message that was sent to this connection as NACK'd
  """
  def put_sent_nack(%__MODULE__{} = state, message_id, ack_key, nack_data) do
    nack = {message_id, ack_key, nack_data}
    %__MODULE__{state | sent_nacks: [nack | state.sent_nacks]}
  end

  def sent_nack_count(%__MODULE__{} = state), do: length(state.sent_nacks)

  def received_nacks_by_event_type_id(%__MODULE__{} = state) do
    group_nacks_by_event_type_id(state.received_nacks)
  end

  def sent_nacks_by_event_type_id(%__MODULE__{} = state) do
    group_nacks_by_event_type_id(state.sent_nacks)
  end

  defp next_message_id(nil) do
    Enum.random(0..@max_message_id)
  end

  defp next_message_id(@max_message_id) do
    0
  end

  defp next_message_id(message_id) do
    message_id + 1
  end

  defp group_nacks_by_event_type_id(nacks) do
    nacks
    |> Enum.frequencies_by(fn {_, {event_type_id, _, _}, _} ->
      event_type_id
    end)
  end
end
