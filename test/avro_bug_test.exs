defmodule AvroBugTest do
  @moduledoc """
  See https://app.clubhouse.io/sparkmeter/story/44918/investigate-fix-bug-in-avrora-erlavro#activity-45101
  """
  use ExUnit.Case, async: true
  alias Slim.Events

  @user_id "8cb15ae7-3b9e-4135-ae3f-6d3ee79ea197" |> Ecto.UUID.dump() |> elem(1)
  @base_station_id "springfield-station"

  describe "encode/decode" do
    test "when nerves_hub_link_enabled encodes to 1" do
      event = %Events.SystemConfigEvent{
        nerves_hub_link_enabled: true,
        aes_key: Base.decode16!("CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"),
        controller_id: 16,
        timestamp: 1_586_632_500,
        updated_by_id: @user_id,
        client_id: @base_station_id
      }

      assert {:ok, event_type_id, key, offset, event_data} = Slim.encode_event(event)
      assert @base_station_id == key
      assert event.offset == offset
      assert {:ok, event} == Slim.decode_event({event_type_id, key}, offset, event_data)
    end

    test "when nerves_hub_link_enabled encodes to 0" do
      event = %Events.SystemConfigEvent{
        nerves_hub_link_enabled: false,
        aes_key: Base.decode16!("CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"),
        controller_id: 16,
        timestamp: 1_586_632_500,
        updated_by_id: @user_id,
        client_id: @base_station_id
      }

      assert {:ok, event_type_id, key, offset, event_data} = Slim.encode_event(event)
      assert @base_station_id == key
      assert event.offset == offset
      assert {:ok, event} == Slim.decode_event({event_type_id, key}, offset, event_data)
    end
  end
end
