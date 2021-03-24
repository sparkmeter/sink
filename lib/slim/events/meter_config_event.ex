defmodule Slim.Events.MeterConfigEvent do
  @behaviour Slim.Event

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
    :timestamp
  ]

  @impl true
  def avro_schema(), do: "io.slim.meter_config_event"

  @impl true
  def key(meter_config), do: meter_config.meter_id

  @impl true
  def offset(meter_config), do: meter_config.offset

  @impl true
  def set_key(event, encoded_key), do: %__MODULE__{event | meter_id: encoded_key}

  @impl true
  def set_offset(event, encoded_offset), do: %__MODULE__{event | offset: encoded_offset}
end
