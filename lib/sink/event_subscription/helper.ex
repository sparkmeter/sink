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
end
