defmodule Sink.EventLog.EctoClientEventLogTest do
  use ExUnit.Case, async: false
  doctest Sink
  alias Sink.EventLog.TestEctoClientEventLog

  @client_id "test-client"
  @event_type_id 1
  @event_key <<1, 2, 3>>
  @event_data <<0, 0, 0>>
  @offset 1

  setup do
    # Explicitly get a connection before each test
    :ok = Ecto.Adapters.SQL.Sandbox.checkout(Sink.TestRepo)
  end

  describe "with a client_id" do
    test "logs an event, can retrieve the same event" do
      assert :ok =
               TestEctoClientEventLog.log(
                 {@client_id, @event_type_id, @event_key},
                 @offset,
                 @event_data
               )

      assert @event_data ==
               TestEctoClientEventLog.get({@client_id, @event_type_id, @event_key}, @offset)
    end

    test "gets latest" do
      assert :ok = TestEctoClientEventLog.log({@client_id, @event_type_id, @event_key}, 1, <<1>>)
      assert :ok = TestEctoClientEventLog.log({@client_id, @event_type_id, @event_key}, 2, <<2>>)

      assert {2, <<2>>} ==
               TestEctoClientEventLog.get_latest({@client_id, @event_type_id, @event_key})
    end
  end

  describe "check_dupe" do
    test "is {:ok, nil} for a record that doesn't exist" do
      assert {:ok, false} =
               TestEctoClientEventLog.check_dupe(
                 {@client_id, @event_type_id, @event_key},
                 1,
                 <<2>>
               )
    end

    test "is {:ok, :dupe} for a record that exists" do
      assert :ok =
               TestEctoClientEventLog.log(
                 {@client_id, @event_type_id, @event_key},
                 1,
                 @event_data
               )

      assert {:ok, true} =
               TestEctoClientEventLog.check_dupe(
                 {@client_id, @event_type_id, @event_key},
                 1,
                 @event_data
               )
    end

    test "is {:error, :data_mismatch, _data} for a record that exists" do
      assert :ok =
               TestEctoClientEventLog.log(
                 {@client_id, @event_type_id, @event_key},
                 1,
                 @event_data
               )

      assert {:error, :data_mismatch, @event_data} =
               TestEctoClientEventLog.check_dupe({@client_id, @event_type_id, @event_key}, 1, 2)
    end
  end
end
