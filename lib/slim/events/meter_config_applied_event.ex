defmodule Slim.Events.MeterConfigAppliedEvent do
  @type t() :: %__MODULE__{}

  defstruct [
    :meter_id,
    :last_meter_config_offset,
    :offset,
    :timestamp
  ]

  def key(meter_config), do: meter_config.meter_id

  def offset(meter_config), do: meter_config.offset

  def set_key(event, encoded_key), do: %__MODULE__{event | meter_id: encoded_key}

  def set_offset(event, encoded_offset), do: %__MODULE__{event | offset: encoded_offset}
end
