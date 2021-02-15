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
    8 => Events.CustomerMeterBillEvent,
    9 => Events.CloudCreditEvent,
    # Control Messages
    256 => Events.SystemConfigEvent,
    257 => Events.UpdateFirmwareEvent
  }
  @event_type_rev_map %{
    Events.UserEvent => 0,
    Events.TariffEvent => 1,
    Events.MeterEvent => 2,
    Events.CustomerMeterConfigEvent => 3,
    Events.CustomerEvent => 4,
    Events.MeterReadingEvent => 5,
    Events.MeterConfigEvent => 6,
    Events.MeterConfigAppliedEvent => 7,
    Events.CustomerMeterBillEvent => 8,
    Events.CloudCreditEvent => 9,
    # Control Messages
    Events.SystemConfigEvent => 256,
    Events.UpdateFirmwareEvent => 257
  }
  @event_schema %{
    Events.UserEvent => "io.slim.user_event",
    Events.TariffEvent => "io.slim.tariff_event",
    Events.MeterEvent => "io.slim.meter_event",
    Events.CustomerMeterConfigEvent => "io.slim.customer_meter_config_event",
    Events.CustomerEvent => "io.slim.customer_event",
    Events.MeterReadingEvent => "io.slim.meter_reading_event",
    Events.MeterConfigEvent => "io.slim.meter_config_event",
    Events.MeterConfigAppliedEvent => "io.slim.meter_config_applied_event",
    Events.CustomerMeterBillEvent => "io.slim.customer_meter_bill_event",
    Events.CloudCreditEvent => "io.slim.cloud_credit_event",
    # Control Messages
    Events.SystemConfigEvent => "io.slim.system_config_event",
    Events.UpdateFirmwareEvent => "io.slim.update_firmware_event"
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

    case Avrora.decode(event_data, schema_name: schema_name, format: :plain) do
      {:ok, decoded} ->
        event_type = get_event_type(event_type_id)

        decoded_with_atom_keys = decode_with_atom_keys(decoded)

        event =
          event_type
          |> struct(decoded_with_atom_keys)
          |> event_type.set_key(key)
          |> event_type.set_offset(offset)

        {:ok, event}

      {:error, error} ->
        {:error, error}
    end
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
    schema_dir = Path.join([to_string(:code.priv_dir(:sink)), "schemas", "io", "slim"])

    _ =
      [
        Path.join([schema_dir, "user_event.avsc"]),
        Path.join([schema_dir, "tariff_event.avsc"]),
        Path.join([schema_dir, "meter_event.avsc"]),
        Path.join([schema_dir, "customer_meter_config_event.avsc"]),
        Path.join([schema_dir, "customer_event.avsc"]),
        Path.join([schema_dir, "meter_reading_event.avsc"]),
        Path.join([schema_dir, "meter_config_event.avsc"]),
        Path.join([schema_dir, "meter_config_applied_event.avsc"]),
        Path.join([schema_dir, "system_config_event.avsc"]),
        Path.join([schema_dir, "update_firmware_event.avsc"]),
        Path.join([schema_dir, "customer_meter_bill_event.avsc"]),
        Path.join([schema_dir, "cloud_credit_event.avsc"])
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
