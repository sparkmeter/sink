defmodule Slim do
  @doc """
  Slim is the application specific encodings for Nova events.

  Note: event_type_id is encoded as a VarInt. Since control messages are expected to be
   much less frequent so they start at 256 (two bytes vs 1 for 255 and under) to save
   room for more frequent messages we will add as the system grows number of messages.
  """
  alias Slim.Events

  @event_type_map %{
    0 => Events.UserEvent,
    1 => Events.TariffEvent,
    2 => Events.MeterEvent,
    3 => Events.CustomerMeterConfigEvent,
    4 => Events.CustomerEvent,
    5 => Events.MeterReadingEvent,
    6 => Events.MeterConfigEvent,
    7 => Events.MeterConfigAppliedEvent,
    8 => Events.CustomerMeterTransactionEvent,
    9 => Events.CloudCreditEvent,
    # Control Messages
    256 => Events.SystemConfigEvent,
    257 => Events.UpdateFirmwareEvent
  }
  @event_type_rev_map for {n, mod} <- @event_type_map, into: %{}, do: {mod, n}
  @event_schema for {_n, mod} <- @event_type_map, into: %{}, do: {mod, mod.avro_schema()}

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
      |> Avrora.encode_plain(schema_name: get_schema_name(event_type))

    {:ok, event_type_id, key, offset, event_data}
  end

  def decode_event({_event_type_id, _key}, _offset, nil) do
    {:error, "cannot decode nil event"}
  end

  def decode_event({event_type_id, key}, offset, event_data) do
    schema_name =
      event_type_id
      |> get_event_type()
      |> get_schema_name()

    with {:ok, decoded} <- Avrora.decode_plain(event_data, schema_name: schema_name) do
      event_type = get_event_type(event_type_id)

      decoded_with_atom_keys = decode_with_atom_keys(decoded)

      event =
        event_type
        |> struct(decoded_with_atom_keys)
        |> event_type.set_key(key)
        |> event_type.set_offset(offset)

      {:ok, event}
    end
  end

  defp decode_with_atom_keys(nil), do: nil
  defp decode_with_atom_keys(:null), do: nil

  defp decode_with_atom_keys(map = %{}) do
    map
    |> Enum.map(fn {k, v} -> {String.to_atom(k), decode_with_atom_keys(v)} end)
    |> Enum.into(%{})
  end

  defp decode_with_atom_keys(v), do: v

  # REMOVE EVERYTHING BELOW THIS LINE ONCE TIDAL IS REPLACED
  def register_schemas do
    schema_dir = Application.app_dir(:sink, "priv/schemas/io/slim")

    _ =
      [
        "user_event.avsc",
        "tariff_event.avsc",
        "meter_event.avsc",
        "customer_meter_config_event.avsc",
        "customer_event.avsc",
        "meter_reading_event.avsc",
        "meter_config_event.avsc",
        "meter_config_applied_event.avsc",
        "system_config_event.avsc",
        "update_firmware_event.avsc",
        "customer_meter_transaction_event.avsc",
        "cloud_credit_event.avsc"
      ]
      |> Enum.map(&Path.join([schema_dir, &1]))
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
