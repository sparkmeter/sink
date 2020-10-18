defmodule Sink.TestClientLog do
  @moduledoc false
  use Ecto.Schema

  @primary_key false
  schema "test_client_log" do
    field(:client_id, :string, primary_key: true)
    field(:event_type, :string, primary_key: true)
    field(:key, :binary, primary_key: true)
    field(:offset, :integer, primary_key: true)
    field(:serialized, :binary)
  end
end
