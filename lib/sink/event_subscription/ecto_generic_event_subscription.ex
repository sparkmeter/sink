defmodule Sink.EventSubscription.EctoGenericEventSubscription do
  @moduledoc """
  Documentation for `Sink`.
  """
  import Ecto.Query, only: [from: 2]
  alias Sink.EventSubscription.Helper
  @repo Application.fetch_env!(:sink, :ecto_repo)
  @default_limit 20
  @default_nack_threshold 5

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

  def queue(subscription_table, config_table, opts \\ []) do
    args =
      Enum.into(opts, %{
        inflight: [],
        limit: @default_limit,
        nack_threshold: @default_nack_threshold,
        received_nacks: []
      })

    excluded_inflight_topics = Helper.get_excluded_inflight_topics(args.inflight)
    excluded_nack_topics = Helper.get_excluded_nack_topics(args.received_nacks)
    nacked_event_types = Helper.get_nacked_event_types(args.received_nacks, args.nack_threshold)

    from(sub in subscription_table,
      where: sub.consumer_offset < sub.producer_offset,
      join: c in ^config_table,
      on: sub.event_type_id == c.event_type_id,
      select: {
        sub.event_type_id,
        sub.key,
        sub.consumer_offset,
        sub.producer_offset
      },
      order_by: [asc: c.order],
      limit: ^args.limit
    )
    |> exclude_topics(excluded_inflight_topics ++ excluded_nack_topics)
    |> exclude_nacked_event_types(nacked_event_types)
    |> @repo.all()
    |> Enum.map(fn {event_type_id, key, consumer_offset, producer_offset} ->
      {event_type_id, key, consumer_offset, producer_offset}
    end)
  end

  def exclude_topics(query, []), do: query

  def exclude_topics(query, [{event_type_id, key} | tail]) do
    from(sub in query,
      where: not (sub.event_type_id == ^event_type_id and sub.key == ^key)
    )
    |> exclude_topics(tail)
  end

  defp exclude_nacked_event_types(query, []), do: query

  defp exclude_nacked_event_types(query, nacked_event_types) do
    from(sub in query,
      where: sub.event_type_id not in ^nacked_event_types
    )
  end
end
