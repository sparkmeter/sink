defmodule Sink.Test.CloudIncomingSubscription do
  @moduledoc """
  This would be on the device (the "ground") and would contain all the subscriptions to
  events received from the cloud
  """
  use Ecto.Schema

  @primary_key false
  schema "cloud_incoming_subscription" do
    field(:event_type, :string, primary_key: true)
    field(:key, :binary, primary_key: true)
    field(:consumer_offset, :integer)
    field(:producer_offset, :integer)
  end
end
