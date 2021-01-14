defmodule SlimTest do
  use ExUnit.Case, async: true
  alias Slim.Events

  # @user_uuid UUID.dump!("8cb15ae7-3b9e-4135-ae3f-6d3ee79ea197")
  @meter_id "00015ae7-3b9e-4135-ae3f-6d3ee79ea197" |> Ecto.UUID.dump() |> elem(1)
  @tariff_uuid "0cb15ae7-3b9e-4135-ae3f-6d3ee79ea197" |> Ecto.UUID.dump() |> elem(1)
  @user_uuid "8cb15ae7-3b9e-4135-ae3f-6d3ee79ea197" |> Ecto.UUID.dump() |> elem(1)

  describe "encode/decode events" do
    test "MeterEvent" do
      event = %Events.MeterEvent{
        id: @meter_id,
        serial_number: "SM60R-01-00001551",
        address: "742 Evergreen Terrace",
        offset: 1,
        updated_by_id: @user_uuid
      }

      assert {:ok, event_type_id, key, offset, event_data} = Slim.encode_event(event)
      assert event.id == key
      assert event.offset == offset
      assert {:ok, event} == Slim.decode_event({event_type_id, key}, offset, event_data)
    end

    test "TariffEvent" do
      event = %Events.TariffEvent{
        id: @tariff_uuid,
        name: "Tariff 1",
        load_limit: 9_000,
        rate: :erlang.float_to_binary(10.50),
        offset: 1,
        updated_by_id: @user_uuid
      }

      assert {:ok, event_type_id, key, offset, event_data} = Slim.encode_event(event)
      assert event.id == key
      assert event.offset == offset
      assert {:ok, event} == Slim.decode_event({event_type_id, key}, offset, event_data)
    end

    test "UserEvent" do
      event = %Events.UserEvent{
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
