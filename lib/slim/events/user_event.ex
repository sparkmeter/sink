defmodule Slim.Events.UserEvent do
  @type t() :: %__MODULE__{}

  defstruct [:id, :email, :offset, :username, :shared_secret, :updated_by_id]

  def key(user_event), do: user_event.id

  def offset(user_event), do: user_event.offset

  def set_key(event, encoded_key), do: %__MODULE__{event | id: encoded_key}

  def set_offset(event, encoded_offset), do: %__MODULE__{event | offset: encoded_offset}
end
