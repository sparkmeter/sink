defmodule Sink.TestLogSubscriptions do
  @moduledoc false
  use Ecto.Schema

  @primary_key false
  schema "test_log_subscription" do
    field(:client_id, :string, primary_key: true)
    field(:event_type, :string, primary_key: true)
    field(:key, :binary, primary_key: true)
    field(:consumer_offset, :integer)
    field(:producer_offset, :integer)
  end
end
