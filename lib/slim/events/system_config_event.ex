defmodule Slim.Events.SystemConfigEvent do
  @type t() :: %__MODULE__{}

  defstruct [
    # id is the base_station_id
    :client_id,
    :nerves_hub_link_enabled,
    :aes_key,
    :controller_id,
    :offset,
    :updated_by_id,
    :timestamp
  ]

  def key(system_config_event), do: system_config_event.client_id

  def offset(system_config_event), do: system_config_event.offset

  def set_key(event, _encoded_key), do: event

  def set_offset(event, encoded_offset), do: %__MODULE__{event | offset: encoded_offset}
end
