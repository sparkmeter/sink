defmodule AvroBugTest do
  @moduledoc """
  See https://app.clubhouse.io/sparkmeter/story/44918/investigate-fix-bug-in-avrora-erlavro#activity-45101
  """
  use ExUnit.Case, async: true

  @schema_name "io.slim.system_config_event"
  @user_id "8cb15ae7-3b9e-4135-ae3f-6d3ee79ea197" |> Ecto.UUID.dump() |> elem(1)

  describe "encode/decode data" do
    test "when nerves_hub_link_enabled encodes to 1" do
      data = %{
        "nerves_hub_link_enabled" => true,
        "updated_at" => 1_586_632_500,
        "updated_by_id" => @user_id
      }

      {:ok, encoded} = Avrora.encode(data, schema_name: @schema_name, format: :plain)

      assert {:ok, decoded} = Avrora.decode(encoded, schema_name: @schema_name)
      assert decoded == data
    end

    test "when nerves_hub_link_enabled encodes to 0" do
      data = %{
        "nerves_hub_link_enabled" => false,
        "updated_at" => 1_586_632_500,
        "updated_by_id" => @user_id
      }

      {:ok, encoded} = Avrora.encode(data, schema_name: @schema_name, format: :plain)

      assert {:ok, decoded} = Avrora.decode(encoded, schema_name: @schema_name)
      assert decoded == data
    end
  end
end
