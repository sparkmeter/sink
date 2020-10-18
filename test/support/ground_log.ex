defmodule Sink.Test.GroundLog do
  @moduledoc false
  use Ecto.Schema

  @primary_key false
  schema "ground_log" do
    field(:event_type, :string, primary_key: true)
    field(:key, :binary, primary_key: true)
    field(:offset, :integer, primary_key: true)
    field(:serialized, :binary)
  end
end
