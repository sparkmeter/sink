defmodule Sink.Test.CloudOutgoingLog do
  @moduledoc """
  An event log, on the cloud, of all events generated in the cloud
  """
  use Ecto.Schema

  @primary_key false
  schema "cloud_outgoing_log" do
    field(:client_id, :string, primary_key: true)
    field(:event_type, :string, primary_key: true)
    field(:key, :binary, primary_key: true)
    field(:offset, :integer, primary_key: true)
    field(:serialized, :binary)
  end

  def event_module("test_event") do
    Sink.TestEvent
  end
end
