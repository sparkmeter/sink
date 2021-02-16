defmodule Slim.Events.CustomerMeterConfigEvent do
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

  def key(event), do: event.id

  def offset(event), do: event.offset

  def set_key(event, encoded_key), do: %__MODULE__{event | id: encoded_key}

  def set_offset(event, encoded_offset), do: %__MODULE__{event | offset: encoded_offset}
end
