defmodule Sink.Test.CloudIncomingLog do
  @moduledoc """
  An event log, on the ground, of all events received from the cloud
  """
  use Ecto.Schema

  @primary_key false
  schema "cloud_incoming_log" do
    field(:event_type, :string, primary_key: true)
    field(:key, :binary, primary_key: true)
    field(:offset, :integer, primary_key: true)
    field(:serialized, :binary)
  end

  def event_module("test_event") do
    Sink.TestEvent
  end
end
