defmodule Sink.EventLog.TestEctoGenericEventLog do
  @moduledoc false
  use Ecto.Schema
  alias Sink.EventLog.EctoGenericEventLog

  @primary_key false
  schema "test_ecto_generic_event_log" do
    field(:event_type_id, :integer, primary_key: true)
    field(:key, :binary, primary_key: true)
    field(:offset, :integer, primary_key: true)
    field(:timestamp, :integer)
    field(:event_data, :binary)
  end

  # @impl true
  def check_dupe({event_type_id, key}, offset, {event_data, timestamp}) do
    EctoGenericEventLog.check_dupe(__MODULE__, {event_type_id, key}, offset, {event_data, timestamp})
  end

  # @impl true
  def get_latest({event_type_id, key}) do
    EctoGenericEventLog.get_latest(__MODULE__, {event_type_id, key})
  end

  # @impl true
  def get({event_type_id, key}, offset) do
    EctoGenericEventLog.get(__MODULE__, {event_type_id, key}, offset)
  end

  # @impl true
  def log({event_type_id, key}, offset, {event_data, timestamp}) do
    EctoGenericEventLog.log(__MODULE__, {event_type_id, key}, offset, {event_data, timestamp})
  end
end
