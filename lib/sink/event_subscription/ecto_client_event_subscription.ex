defmodule Sink.EventSubscription.EctoClientEventSubscription do
  @moduledoc """
  Documentation for `Sink`.
  """
  import Ecto.Query, only: [from: 2]
  @repo Application.fetch_env!(:sink, :ecto_repo)

  def get_offsets(subscription_table, {client_id, event_type_id, key}) do
    @repo.get_by(subscription_table,
      client_id: client_id,
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

  def subscribe(subscription_table, {client_id, event_type_id, key}, producer_offset) do
    subscription_table
    |> struct(%{
      client_id: client_id,
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

  def update_or_create(subscription_table, {client_id, event_type_id, key}, offset) do
    case @repo.get_by(subscription_table,
           client_id: client_id,
           event_type_id: event_type_id,
           key: key
         ) do
      nil ->
        subscription_table
        |> struct(%{
          client_id: client_id,
          event_type_id: event_type_id,
          key: key,
          consumer_offset: 0,
          producer_offset: offset
        })
        |> @repo.insert()

        :ok

      subscription ->
        {:ok, _} =
          subscription
          |> Ecto.Changeset.change(producer_offset: offset)
          |> @repo.update()

        :ok
    end
  end

  def update_producer_offset(subscription_table, {event_type_id, key}, offset) do
    from(sub in subscription_table,
      where: sub.event_type_id == ^event_type_id,
      where: sub.key == ^key
    )
    |> @repo.update_all(set: [producer_offset: offset])
  end

  def ack(subscription_table, {client_id, event_type_id, key}, offset) do
    @repo.get_by(subscription_table,
      client_id: client_id,
      event_type_id: event_type_id,
      key: key
    )
    |> Ecto.Changeset.change(consumer_offset: offset)
    |> @repo.update()
    |> case do
      {:ok, _} -> :ok
    end
  end

  def queue(subscription_table, config_table, client_id) do
    from(sub in subscription_table,
      where: sub.client_id == ^client_id,
      where: sub.consumer_offset < sub.producer_offset,
      join: c in ^config_table,
      on: sub.event_type_id == c.event_type_id,
      select: {
        sub.event_type_id,
        sub.key,
        sub.consumer_offset,
        sub.producer_offset
      },
      order_by: [asc: c.order]
    )
    |> @repo.all()
    |> Enum.map(fn {event_type_id, key, consumer_offset, producer_offset} ->
      {event_type_id, key, consumer_offset, producer_offset}
    end)
  end
end
