defmodule Slim.Events.SystemConfigEvent do
  @behaviour Slim.Event

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

  @impl true
  def avro_schema(), do: "io.slim.system_config_event"

  @impl true
  def key(system_config_event), do: system_config_event.client_id

  @impl true
  def offset(system_config_event), do: system_config_event.offset

  @impl true
  def set_key(event, _encoded_key), do: event

  @impl true
  def set_offset(event, encoded_offset), do: %__MODULE__{event | offset: encoded_offset}
end
