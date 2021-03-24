defmodule Slim.Events.MeterEvent do
  @behaviour Slim.Event

  @type t() :: %__MODULE__{}

  defstruct [:id, :serial_number, :address, :offset, :updated_by_id, :timestamp]

  @impl true
  def avro_schema(), do: "io.slim.meter_event"

  @impl true
  def key(meter_event), do: meter_event.id

  @impl true
  def offset(meter_event), do: meter_event.offset

  @impl true
  def set_key(event, encoded_key), do: %__MODULE__{event | id: encoded_key}

  @impl true
  def set_offset(event, encoded_offset), do: %__MODULE__{event | offset: encoded_offset}
end
