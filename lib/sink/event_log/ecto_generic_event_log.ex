defmodule Sink.EventLog.EctoGenericEventLog do
  @moduledoc """
  Documentation for `Sink`.
  """
  import Ecto.Query, only: [from: 2]
  @repo Application.fetch_env!(:sink, :ecto_repo)

  @doc """
  Check the EventLog to see if we have the event and the event_data matches.

  a `true` means it is a duplicate.
  """
  def check_dupe(event_log, {event_type_id, key}, offset, event_data) do
    case get(event_log, {event_type_id, key}, offset) do
      nil -> {:ok, false}
      ^event_data -> {:ok, true}
      mismatch_data -> {:error, :data_mismatch, mismatch_data}
    end
  end

  def get(event_log, {event_type_id, key}, offset) do
    @repo.get_by(event_log,
      event_type_id: event_type_id,
      key: key,
      offset: offset
    )
    |> case do
      nil ->
        nil

      #      %{event_data: event_data} ->
      #        event_data
      record ->
        record.event_data
    end
  end

  @spec get_latest(any(), {non_neg_integer(), binary()}) :: nil | {non_neg_integer(), binary()}
  def get_latest(event_log, {event_type_id, key}) do
    from(e_log in event_log,
      where: e_log.event_type_id == ^event_type_id and e_log.key == ^key,
      order_by: [desc: :offset],
      limit: 1
    )
    |> @repo.one()
    |> case do
      nil ->
        nil

      %{event_data: event_data, offset: offset} ->
        {offset, event_data}
    end
  end

  def log(event_log, {event_type_id, key}, offset, binary) do
    record =
      struct(event_log.__struct__, %{
        event_type_id: event_type_id,
        key: key,
        offset: offset,
        event_data: binary
      })

    {:ok, _} = @repo.insert(record)

    :ok
  end
end
