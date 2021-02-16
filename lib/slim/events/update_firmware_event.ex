defmodule Slim.Events.UpdateFirmwareEvent do
  @type t() :: %__MODULE__{}

  defstruct [:to, :offset, :updated_by_id, :timestamp]

  def key(_update_firmware_event), do: <<>>

  def offset(update_firmware_event), do: update_firmware_event.offset

  def set_key(event, _encoded_key), do: event

  def set_offset(event, encoded_offset), do: %__MODULE__{event | offset: encoded_offset}
end
