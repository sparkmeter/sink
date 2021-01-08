defmodule Slim do
  alias Slim.Events

  @event_type_map %{
    0 => Events.UserEvent
  }
  @event_type_rev_map %{
    Events.UserEvent => 0
  }

  def get_event_type(event_type_id), do: Map.fetch!(@event_type_map, event_type_id)
  def get_event_type_id(event_type), do: Map.fetch!(@event_type_rev_map, event_type)

  def encode_event(event) do
    event_type = event.__struct__
    event_type_id = get_event_type_id(event_type)
    key = event_type.key(event)
    offset = event_type.offset(event)

    event_data =
      event
      |> Map.from_struct()
      |> :erlang.term_to_binary()

    {:ok, event_type_id, key, offset, event_data}
  end

  def decode_event({event_type_id, key}, offset, event_data) do
    map = :erlang.binary_to_term(event_data)
    event_type = get_event_type(event_type_id)

    event =
      struct(event_type, map)
      |> event_type.set_key(key)
      |> event_type.set_offset(offset)

    {:ok, event}
  end
end
