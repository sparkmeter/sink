defmodule Sink.EventSubscription.TestEctoGenericEventSubscription do
  @moduledoc false
  use Ecto.Schema
  alias Sink.EventSubscription.EctoGenericEventSubscription

  @primary_key false
  schema "test_ecto_generic_event_subscriptions" do
    field(:event_type_id, :integer, primary_key: true)
    field(:key, :binary, primary_key: true)
    field(:consumer_offset, :integer)
    field(:producer_offset, :integer)
  end

  def get_offsets({event_type_id, key}) do
    EctoGenericEventSubscription.get_offsets(__MODULE__, {event_type_id, key})
  end

  def subscribe({event_type_id, key}, producer_offset) do
    EctoGenericEventSubscription.subscribe(__MODULE__, {event_type_id, key}, producer_offset)
  end

  def update_or_create({event_type_id, key}, producer_offset) do
    EctoGenericEventSubscription.update_or_create(
      __MODULE__,
      {event_type_id, key},
      producer_offset
    )
  end

  def ack({event_type_id, key}, offset) do
    EctoGenericEventSubscription.ack(__MODULE__, {event_type_id, key}, offset)
  end

  def queue() do
    EctoGenericEventSubscription.queue(__MODULE__, Sink.TestEctoEventTypeConfig)
  end

  def queue(opts) do
    EctoGenericEventSubscription.queue(__MODULE__, Sink.TestEctoEventTypeConfig, opts)
  end
end
