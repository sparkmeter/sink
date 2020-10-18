defmodule Sink.Test.CloudOutgoingSubscription do
  @moduledoc """
  This would be on the cloud and would contain all the subscriptions to cloud events
  to be sent to the ground
  """
  use Ecto.Schema

  @primary_key false
  schema "cloud_outgoing_subscription" do
    field(:client_id, :string, primary_key: true)
    field(:event_type, :string, primary_key: true)
    field(:key, :binary, primary_key: true)
    field(:consumer_offset, :integer)
    field(:producer_offset, :integer)
  end
end
