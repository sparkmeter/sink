defmodule Sink.Test.GroundIncomingSubscription do
  @moduledoc """
  This would be in the cloud and would contain all the subscriptions to ground events
  received from the ground
  """
  use Ecto.Schema

  @primary_key false
  schema "ground_incoming_subscription" do
    field(:client_id, :string, primary_key: true)
    field(:event_type, :string, primary_key: true)
    field(:key, :binary, primary_key: true)
    field(:consumer_offset, :integer)
    field(:producer_offset, :integer)
  end
end
