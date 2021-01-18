defmodule Slim.Events.MeterEvent do
  @type t() :: %__MODULE__{}

  defstruct [:id, :serial_number, :address, :offset, :updated_by_id]

  def key(meter_event), do: meter_event.id

  def offset(meter_event), do: meter_event.offset

  def set_key(event, encoded_key), do: %__MODULE__{event | id: encoded_key}

  def set_offset(event, encoded_offset), do: %__MODULE__{event | offset: encoded_offset}
end
