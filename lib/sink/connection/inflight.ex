defmodule Sink.Connection.Inflight do
  @moduledoc """
  Encapsulates the details of messages in flight
  """
  defstruct [
    :next_message_id,
    inflight: %{}
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

  defp next_message_id(nil) do
    Enum.random(0..@max_message_id)
  end

  defp next_message_id(@max_message_id) do
    0
  end

  defp next_message_id(message_id) do
    message_id + 1
  end
end
