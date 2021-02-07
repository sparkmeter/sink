defmodule Slim.Events.CustomerMeterBillEvent do
  @type t() :: %__MODULE__{}

  defstruct [
    :customer_meter_config_id,
    :customer_meter_config_offset,
    :meter_reading_offset,
    :amount,
    :balance,
    :timestamp,
    :offset
  ]

  def key(event), do: event.customer_meter_config_id

  def offset(event), do: event.offset

  def set_key(event, encoded_key), do: %__MODULE__{event | customer_meter_config_id: encoded_key}

  def set_offset(event, encoded_offset), do: %__MODULE__{event | offset: encoded_offset}
end
