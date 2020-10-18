defmodule Sink.Test.GroundOutgoingLog do
  @moduledoc """
  An event log, on the ground, of all events generated on the ground
  """
  use Ecto.Schema

  @primary_key false
  schema "ground_outgoing_log" do
    field(:event_type, :string, primary_key: true)
    field(:key, :binary, primary_key: true)
    field(:offset, :integer, primary_key: true)
    field(:serialized, :binary)
  end

  def event_module("test_event") do
    Sink.TestEvent
  end
end
