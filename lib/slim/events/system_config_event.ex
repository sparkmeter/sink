defmodule Slim.Events.SystemConfigEvent do
  @type t() :: %__MODULE__{}

  defstruct [:nerves_hub_link_enabled, :offset, :updated_by_id, :timestamp]

  def key(_system_config_event), do: <<>>

  def offset(system_config_event), do: system_config_event.offset

  def set_key(event, _encoded_key), do: event

  def set_offset(event, encoded_offset), do: %__MODULE__{event | offset: encoded_offset}
end
