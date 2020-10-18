defmodule Slim.Ground.UserGround do
  defstruct [
    :id,
    :uuid,
    :email,
    :username,
    :shared_secret,
    :created_by_uuid,
    :version_number,
    :version_requestor
  ]

  def event_type_id, do: Slim.get_event_type_id(__MODULE__)

  def key(user_ground) do
    :binary.encode_unsigned(user_ground.id)
  end

  def offset(user_ground), do: user_ground.version_number

  def schema_id, do: 1
end
