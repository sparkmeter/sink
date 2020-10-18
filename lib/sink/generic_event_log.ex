defmodule Sink.GenericEventLog do
  @moduledoc """
  Documentation for `Sink`.
  """
  import Ecto.Query, only: [from: 2]
  @repo Application.fetch_env!(:sink, :ecto_repo)

  def get(event_log, event_type_id, key, offset) do
    @repo.get_by(event_log,
      event_type_id: event_type_id,
      key: key,
      offset: offset
    )
    |> case do
      nil ->
        nil

      %{serialized: serialized} ->
        serialized
    end
  end

  def get_latest(event_log, event_type_id, key) do
    from(e_log in event_log,
      where: e_log.event_type_id == ^event_type_id and e_log.key == ^key,
      order_by: [desc: :offset],
      limit: 1
    )
    |> @repo.one()
    |> case do
      nil ->
        nil

      %{serialized: serialized} ->
        serialized
    end
  end

  def log(event_log, event_type_id, key, offset, binary) do
    record =
      struct(event_log.__struct__, %{
        event_type_id: event_type_id,
        key: key,
        offset: offset,
        serialized: binary
      })

    @repo.insert(record)
  end
end
