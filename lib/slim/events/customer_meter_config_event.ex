defmodule Slim.Events.CustomerMeterConfigEvent do
  @behaviour Slim.Event

  @type t() :: %__MODULE__{}

  defstruct [
    :id,
    :meter_id,
    :customer_id,
    :tariff_id,
    :operating_mode,
    :offset,
    :updated_by_id,
    :timestamp
  ]

  @impl true
  def avro_schema(), do: "io.slim.customer_meter_config_event"

  @impl true
  def key(event), do: event.id

  @impl true
  def offset(event), do: event.offset

  @impl true
  def set_key(event, encoded_key), do: %__MODULE__{event | id: encoded_key}

  @impl true
  def set_offset(event, encoded_offset), do: %__MODULE__{event | offset: encoded_offset}
end
