defmodule SlimTest do
  use ExUnit.Case, async: true

  # @user_uuid UUID.dump!("8cb15ae7-3b9e-4135-ae3f-6d3ee79ea197")
  @user_uuid "8cb15ae7-3b9e-4135-ae3f-6d3ee79ea197" |> Ecto.UUID.dump() |> elem(1)

  describe "encode/decode events" do
    test "UserCloud" do
      event = %Slim.Cloud.UserCloud{
        id: @user_uuid,
        email: "slim@sparkmeter.io",
        offset: 1,
        username: "slim",
        shared_secret: "12345",
        updated_by_id: @user_uuid
      }

      assert {:ok, event_type_id, key, offset, event_data} = Slim.encode_event(event)
      assert event.id == key
      assert event.offset == offset
      assert {:ok, event} == Slim.decode_event({event_type_id, key}, offset, event_data)
    end
  end
end
