defmodule Sink.EventSubscription.TestEctoClientEventSubscription do
  @moduledoc false
  use Ecto.Schema
  alias Sink.EventSubscription.EctoClientEventSubscription

  @primary_key false
  schema "test_ecto_client_event_subscriptions" do
    field(:client_id, :string, primary_key: true)
    field(:event_type_id, :integer, primary_key: true)
    field(:key, :binary, primary_key: true)
    field(:consumer_offset, :integer)
    field(:producer_offset, :integer)
  end

  def get_offsets({client_id, event_type_id, key}) do
    EctoClientEventSubscription.get_offsets(__MODULE__, {client_id, event_type_id, key})
  end

  def subscribe({client_id, event_type_id, key}, producer_offset) do
    EctoClientEventSubscription.subscribe(
      __MODULE__,
      {client_id, event_type_id, key},
      producer_offset
    )
  end

  def update_or_create({client_id, event_type_id, key}, producer_offset) do
    EctoClientEventSubscription.update_or_create(
      __MODULE__,
      {client_id, event_type_id, key},
      producer_offset
    )
  end

  def ack({client_id, event_type_id, key}, offset) do
    EctoClientEventSubscription.ack(__MODULE__, {client_id, event_type_id, key}, offset)
  end

  def queue(client_id) do
    EctoClientEventSubscription.queue(__MODULE__, Sink.TestEctoEventTypeConfig, client_id)
  end
end
