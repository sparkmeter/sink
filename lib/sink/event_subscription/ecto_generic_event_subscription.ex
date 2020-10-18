defmodule Sink.EventSubscription.EctoGenericEventSubscription do
  @moduledoc """
  Documentation for `Sink`.
  """
  import Ecto.Query, only: [from: 2]
  @repo Application.fetch_env!(:sink, :ecto_repo)

  def get_offsets(subscription_table, {event_type_id, key}) do
    @repo.get_by(subscription_table,
      event_type_id: event_type_id,
      key: key
    )
    |> case do
      nil ->
        nil

      %{consumer_offset: consumer_offset, producer_offset: producer_offset} ->
        {consumer_offset, producer_offset}
    end
  end

  def subscribe(subscription_table, {event_type_id, key}, producer_offset) do
    subscription_table
    |> struct(%{
      event_type_id: event_type_id,
      key: key,
      consumer_offset: 0,
      producer_offset: producer_offset
    })
    |> @repo.insert()
    |> case do
      {:ok, _} ->
        :ok
    end
  end

  def update_or_create(subscription_table, {event_type_id, key}, offset) do
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

        :ok

      subscription ->
        {:ok, _} = @repo.update(subscription, %{producer_offset: offset})

        :ok
    end
  end

  def ack(subscription_table, {event_type_id, key}, offset) do
    @repo.get_by(subscription_table,
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
    |> Enum.map(fn {event_type_id, key, consumer_offset, producer_offset} ->
      {{event_type_id, key}, consumer_offset, producer_offset}
    end)
  end
end
