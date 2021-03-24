defmodule Slim.Events.UserEvent do
  @behaviour Slim.Event

  @type t() :: %__MODULE__{}

  defstruct [:id, :email, :offset, :username, :shared_secret, :updated_by_id, :timestamp]

  @impl true
  def avro_schema(), do: "io.slim.user_event"

  @impl true
  def key(user_event), do: user_event.id

  @impl true
  def offset(user_event), do: user_event.offset

  @impl true
  def set_key(event, encoded_key), do: %__MODULE__{event | id: encoded_key}

  @impl true
  def set_offset(event, encoded_offset), do: %__MODULE__{event | offset: encoded_offset}
end
