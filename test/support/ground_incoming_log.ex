defmodule Sink.Test.GroundIncomingLog do
  @moduledoc """
  An event log, in the cloud, of all events received from the ground
  """
  use Ecto.Schema

  @primary_key false
  schema "ground_incoming_log" do
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
