defmodule Slim.Events.MeterReadingEvent do
  @behaviour Slim.Event

  @type t() :: %__MODULE__{}

  defstruct [
    :apparent_power_avg,
    :current_avg,
    :current_max,
    :current_min,
    :energy,
    :frequency,
    :gid_mac,
    :offset,
    :period_end,
    :period_start,
    :power_factor_avg,
    :state,
    :true_power_avg,
    :true_power_inst,
    :uptime_secs,
    :voltage_avg,
    :voltage_max,
    :voltage_min,
    :timestamp
  ]

  @impl true
  def avro_schema(), do: "io.slim.meter_reading_event"

  @impl true
  def key(meter_reading_event) do
    meter_reading_event.gid_mac
  end

  @impl true
  def offset(meter_reading_event), do: meter_reading_event.offset

  @impl true
  def set_key(event, encoded_key) do
    %__MODULE__{event | gid_mac: encoded_key}
  end

  @impl true
  def set_offset(event, encoded_offset), do: %__MODULE__{event | offset: encoded_offset}
end
