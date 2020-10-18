defmodule Sink.Test.GroundOutgoingSubscription do
  @moduledoc """
  This would be on the ground and would contain all the subscriptions to ground events
  to be sent to the cloud
  """
  use Ecto.Schema

  @primary_key false
  schema "ground_outgoing_subscription" do
    field(:event_type, :string, primary_key: true)
    field(:key, :binary, primary_key: true)
    field(:consumer_offset, :integer)
    field(:producer_offset, :integer)
  end
end
