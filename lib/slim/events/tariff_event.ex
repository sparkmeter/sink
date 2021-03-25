defmodule Slim.Events.TariffEvent do
  @behaviour Slim.Event

  @type t() :: %__MODULE__{}
  # todo @enforce_keys

  defstruct [:id, :name, :rate, :load_limit, :offset, :updated_by_id, :timestamp, :monthly_plan]

  @impl true
  def avro_schema(), do: "io.slim.tariff_event"

  @impl true
  def key(tariff_event), do: tariff_event.id

  @impl true
  def offset(tariff_event), do: tariff_event.offset

  @impl true
  def set_key(event, encoded_key), do: %__MODULE__{event | id: encoded_key}

  @impl true
  def set_offset(event, encoded_offset), do: %__MODULE__{event | offset: encoded_offset}
end
