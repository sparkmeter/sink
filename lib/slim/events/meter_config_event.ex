defmodule Slim.Events.MeterConfigEvent do
  @type t() :: %__MODULE__{}

  defstruct [
    :meter_id,
    :enabled,
    :power_limit,
    :current_limit,
    :startup_delay,
    :throttle_on_time,
    :throttle_off_time,
    :throttle_count_limit,
    :offset,
    :updated_at
  ]

  def key(meter_config), do: meter_config.meter_id

  def offset(meter_config), do: meter_config.offset

  def set_key(event, encoded_key), do: %__MODULE__{event | meter_id: encoded_key}

  def set_offset(event, encoded_offset), do: %__MODULE__{event | offset: encoded_offset}
end
