defmodule Slim.Events.UpdateFirmwareEvent do
  @behaviour Slim.Event

  @type t() :: %__MODULE__{}

  defstruct [:to, :offset, :updated_by_id, :timestamp]

  @impl true
  def avro_schema(), do: "io.slim.update_firmware_event"

  @impl true
  def key(_update_firmware_event), do: <<>>

  @impl true
  def offset(update_firmware_event), do: update_firmware_event.offset

  @impl true
  def set_key(event, _encoded_key), do: event

  @impl true
  def set_offset(event, encoded_offset), do: %__MODULE__{event | offset: encoded_offset}
end
