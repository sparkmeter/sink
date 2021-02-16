defmodule Slim.Events.TariffEvent do
  @type t() :: %__MODULE__{}

  defstruct [:id, :name, :rate, :load_limit, :offset, :updated_by_id, :timestamp]

  def key(tariff_event), do: tariff_event.id

  def offset(tariff_event), do: tariff_event.offset

  def set_key(event, encoded_key), do: %__MODULE__{event | id: encoded_key}

  def set_offset(event, encoded_offset), do: %__MODULE__{event | offset: encoded_offset}
end
