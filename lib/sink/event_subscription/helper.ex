defmodule Sink.EventSubscription.Helper do
  @moduledoc false

  def get_excluded_inflight_topics(inflight) do
    inflight
    |> Enum.map(fn {event_type_id, key, _offset} ->
      {event_type_id, key}
    end)
    |> Enum.uniq()
  end

  def get_excluded_nack_topics(received_nacks) do
    received_nacks
    |> Enum.map(fn {ack_key, _nack_data} ->
      {event_type_id, key, _offset} = ack_key

      {event_type_id, key}
    end)
    |> Enum.uniq()
  end

  def get_nacked_event_types([], _), do: []

  def get_nacked_event_types(received_nacks, nack_threshold) do
    received_nacks
    |> group_nacks_by_event_type_id()
    |> Enum.filter(fn {event_type_id, count} -> count >= nack_threshold end)
    |> Enum.map(fn {event_type_id, count} -> event_type_id end)
  end

  def group_nacks_by_event_type_id(nacks) do
    nacks
    |> Enum.frequencies_by(fn {{event_type_id, _, _}, _} ->
      event_type_id
    end)
  end
end
