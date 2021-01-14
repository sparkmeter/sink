defmodule Slim do
  alias Slim.Events

  @event_type_map %{
    0 => Events.UserEvent,
    1 => Events.TariffEvent
  }
  @event_type_rev_map %{
    Events.UserEvent => 0,
    Events.TariffEvent => 1
  }
  @event_schema %{
    Events.UserEvent => "io.slim.user_event",
    Events.TariffEvent => "io.slim.tariff_event"
  }

  def get_event_type(event_type_id), do: Map.fetch!(@event_type_map, event_type_id)
  def get_event_type_id(event_type), do: Map.fetch!(@event_type_rev_map, event_type)
  def get_schema_name(event_type), do: Map.fetch!(@event_schema, event_type)

  def encode_event(event) do
    event_type = event.__struct__
    event_type_id = get_event_type_id(event_type)
    key = event_type.key(event)
    offset = event_type.offset(event)

    {:ok, event_data} =
      event
      |> Map.from_struct()
      |> Avrora.encode(schema_name: get_schema_name(event_type), format: :plain)

    {:ok, event_type_id, key, offset, event_data}
  end

  def decode_event({event_type_id, key}, offset, event_data) do
    schema_name =
      event_type_id
      |> get_event_type()
      |> get_schema_name()

    {:ok, decoded} = Avrora.decode(event_data, schema_name: schema_name)
    event_type = get_event_type(event_type_id)

    decoded_with_atom_keys = decode_with_atom_keys(decoded)

    event =
      event_type
      |> struct(decoded_with_atom_keys)
      |> event_type.set_key(key)
      |> event_type.set_offset(offset)

    {:ok, event}
  end

  defp decode_with_atom_keys(nil), do: nil
  defp decode_with_atom_keys(:null), do: nil

  defp decode_with_atom_keys(map = %{}) do
    map
    |> Enum.map(fn {k, v} -> {String.to_existing_atom(k), decode_with_atom_keys(v)} end)
    |> Enum.into(%{})
  end

  defp decode_with_atom_keys(v), do: v

  # REMOVE EVERYTHING BELOW THIS LINE ONCE TIDAL IS REPLACED
  def register_schemas do
    schema_dir = Path.join(["..", "sink", "priv", "schemas", "io", "slim"])

    _ =
      [
        Path.join([schema_dir, "user_event.avsc"]),
        Path.join([schema_dir, "tariff_event.avsc"])
      ]
      |> Enum.map(&parse_schema/1)
      |> Enum.map(&register_schema/1)

    :ok
  end

  @spec parse_schema(String.t()) :: Avrora.Schema.t()
  defp parse_schema(file_path) do
    body = File.read!(file_path)
    {:ok, schema} = Avrora.Schema.parse(body)
    schema
  end

  # Register the given schema with Avrora
  @spec register_schema(Avrora.Schema.t()) :: :ok
  defp register_schema(schema) do
    registry_storage = Avrora.Config.self().memory_storage()
    registry_storage.put(schema.id, schema)
    registry_storage.put(schema.full_name, schema)
    registry_storage.expire(schema.full_name, :infinity)
    :ok
  end
end
