defmodule Slim.Cloud.UserCloud do
  defstruct [:id, :email, :offset, :username, :shared_secret, :updated_by_id]

  def key(user_cloud), do: user_cloud.id

  def offset(user_cloud), do: user_cloud.offset

  def set_key(event, encoded_key), do: %__MODULE__{event | id: encoded_key}

  def set_offset(event, encoded_offset), do: %__MODULE__{event | offset: encoded_offset}
end
