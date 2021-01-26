defmodule Sink.TestEctoEventTypeConfig do
  @moduledoc false
  use Ecto.Schema
  alias Sink.EventLog.EctoGenericEventLog

  @config %{
    order: [3, 2, 1, 4]
  }

  @primary_key false
  schema "test_ecto_event_type_configs" do
    field(:event_type_id, :integer, primary_key: true)
    field(:order, :integer)
  end

  def get(event_type_id) do
    Sink.EctoEventTypeConfig.get(__MODULE__, event_type_id)
  end

  def reset do
    Sink.EctoEventTypeConfig.reset(__MODULE__, @config)
  end
end
