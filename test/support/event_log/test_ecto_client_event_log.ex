defmodule Sink.EventLog.TestEctoClientEventLog do
  @moduledoc false
  use Ecto.Schema
  alias Sink.EventLog.EctoClientEventLog

  @primary_key false
  schema "test_ecto_client_event_log" do
    field(:client_id, :string, primary_key: true)
    field(:event_type_id, :integer, primary_key: true)
    field(:key, :binary, primary_key: true)
    field(:offset, :integer, primary_key: true)
    field(:event_data, :binary)
  end

  # @impl true
  def get_latest({client_id, event_type_id, key}) do
    EctoClientEventLog.get_latest(__MODULE__, {client_id, event_type_id, key})
  end

  # @impl true
  def get({client_id, event_type_id, key}, offset) do
    EctoClientEventLog.get(__MODULE__, {client_id, event_type_id, key}, offset)
  end

  # @impl true
  def log({client_id, event_type_id, key}, offset, event_data) do
    EctoClientEventLog.log(__MODULE__, {client_id, event_type_id, key}, offset, event_data)
  end
end
