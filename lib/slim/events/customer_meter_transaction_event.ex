defmodule Slim.Events.CustomerMeterTransactionEvent do
  @behaviour Slim.Event

  @type t() :: %__MODULE__{}

  defstruct [
    :customer_meter_config_id,
    :customer_meter_config_offset,
    :meter_reading_offset,
    :amount,
    :balance,
    :timestamp,
    :offset,
    :type
  ]

  @impl true
  def avro_schema(), do: "io.slim.customer_meter_transaction_event"

  @impl true
  def key(event), do: event.customer_meter_config_id

  @impl true
  def offset(event), do: event.offset

  @impl true
  def set_key(event, encoded_key), do: %__MODULE__{event | customer_meter_config_id: encoded_key}

  @impl true
  def set_offset(event, encoded_offset), do: %__MODULE__{event | offset: encoded_offset}
end
