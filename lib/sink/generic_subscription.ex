defmodule Sink.GenericSubscription do
  @moduledoc """
  Documentation for `Sink`.
  """
  import Ecto.Query, only: [from: 2]
  @repo Application.fetch_env!(:sink, :ecto_repo)

  def get(subscription_table, event_type_id, key) do
    @repo.get_by(subscription_table,
      event_type_id: event_type_id,
      key: key
    )
  end

  def subscribe(subscription_table, event_type_id, key, producer_offset) do
    subscription_table
    |> struct(%{
      event_type_id: event_type_id,
      key: key,
      consumer_offset: 0,
      producer_offset: producer_offset
    })
    |> @repo.insert()
  end

  def update_or_create(subscription_table, event_type_id, key, offset) do
    case @repo.get_by(subscription_table,
           event_type_id: event_type_id,
           key: key
         ) do
      nil ->
        subscription_table
        |> struct(%{
          event_type_id: event_type_id,
          key: key,
          consumer_offset: 0,
          producer_offset: offset
        })
        |> @repo.insert()

      subscription ->
        @repo.update(subscription, %{producer_offset: offset})
    end
  end

  def ack(subscription, event_type_id, key, offset) do
    @repo.get_by(subscription,
      event_type_id: event_type_id,
      key: key
    )
    |> Ecto.Changeset.change(consumer_offset: offset)
    |> @repo.update()
    |> case do
      {:ok, _} -> :ok
    end
  end

  def queue(subscription_table) do
    from(sub in subscription_table,
      where: sub.consumer_offset < sub.producer_offset,
      select: {
        sub.event_type_id,
        sub.key,
        sub.consumer_offset,
        sub.producer_offset
      }
    )
    |> @repo.all()
  end
end
