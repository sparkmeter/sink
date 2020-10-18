defmodule Sink.ClientSubscription do
  @moduledoc """
  Documentation for `Sink`.
  """
  import Ecto.Query, only: [from: 2]
  @repo Application.fetch_env!(:sink, :ecto_repo)

  def get(subscription_table, _event_log, client_id, event_type_id, key) do
    @repo.get_by(subscription_table,
      client_id: client_id,
      event_type_id: event_type_id,
      key: key
    )
  end

  def subscribe(subscription_table, client_id, event_type_id, key, producer_offset) do
    subscription_table
    |> struct(%{
      client_id: client_id,
      event_type_id: event_type_id,
      key: key,
      consumer_offset: 0,
      producer_offset: producer_offset
    })
    |> @repo.insert()
  end

  def update_or_create_subscription(subscription_table, _event_log, client_id, event) do
    event_module = event.__struct__

    case @repo.get_by(subscription_table,
           client_id: client_id,
           event_type_id: event_module.event_type_id(),
           key: event_module.key(event)
         ) do
      nil ->
        subscription_table
        |> struct(%{
          client_id: client_id,
          event_type_id: event_module.event_type_id,
          key: event_module.key(event),
          consumer_offset: 0,
          producer_offset: event.offset
        })
        |> @repo.insert()

      subscription ->
        @repo.update(subscription, %{producer_offset: event.offset})
    end
  end

  def ack(subscription, client_id, event_type_id, key, offset) do
    @repo.get_by(subscription,
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

  def queue(subscription_table, client_id) do
    from(sub in subscription_table,
      where: sub.client_id == ^client_id,
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
