defmodule Sink.EventLog.EctoClientEventLog do
  @moduledoc """
  Documentation for `Sink`.
  """
  import Ecto.Query, only: [from: 2]
  @repo Application.fetch_env!(:sink, :ecto_repo)

  def check_dupe(event_log, {client_id, event_type_id, key}, offset, {event_data, timestamp}) do
    case get(event_log, {client_id, event_type_id, key}, offset) do
      nil -> {:ok, false}
      {^event_data, ^timestamp} -> {:ok, true}
      mismatch -> {:error, :data_mismatch, mismatch}
    end
  end

  def get(event_log, {client_id, event_type_id, key}, offset) do
    @repo.get_by(event_log,
      client_id: client_id,
      event_type_id: event_type_id,
      key: key,
      offset: offset
    )
    |> case do
      nil ->
        nil

      %{event_data: event_data, timestamp: timestamp} ->
        {event_data, timestamp}
    end
  end

  def get_latest(event_log, {client_id, event_type_id, key}) do
    from(e_log in event_log,
      where:
        e_log.client_id == ^client_id and e_log.event_type_id == ^event_type_id and
          e_log.key == ^key,
      order_by: [desc: :offset],
      limit: 1
    )
    |> @repo.one()
    |> case do
      nil ->
        nil

      %{event_data: event_data, offset: offset, timestamp: timestamp} ->
        {offset, event_data, timestamp}
    end
  end

  def log(event_log, {client_id, event_type_id, key}, offset, {event_data, timestamp}) do
    record =
      struct(event_log.__struct__, %{
        client_id: client_id,
        event_type_id: event_type_id,
        key: key,
        offset: offset,
        event_data: event_data,
        timestamp: timestamp
      })

    {:ok, _} = @repo.insert(record)

    :ok
  end
end
