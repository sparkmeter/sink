defmodule Sink.EventLog.EctoGenericEventLogTest do
  use ExUnit.Case, async: false
  doctest Sink
  alias Sink.EventLog.TestEctoGenericEventLog

  @event_type_id 1
  @event_key <<1, 2, 3>>
  @event_data <<0, 0, 0>>
  @offset 1

  setup do
    # Explicitly get a connection before each test
    :ok = Ecto.Adapters.SQL.Sandbox.checkout(Sink.TestRepo)
  end

  describe "with no client_id" do
    test "logs an event, can retrieve the same event" do
      assert :ok = TestEctoGenericEventLog.log({@event_type_id, @event_key}, @offset, @event_data)

      assert @event_data == TestEctoGenericEventLog.get({@event_type_id, @event_key}, @offset)
    end

    test "gets latest" do
      assert :ok = TestEctoGenericEventLog.log({@event_type_id, @event_key}, 1, <<1>>)
      assert :ok = TestEctoGenericEventLog.log({@event_type_id, @event_key}, 2, <<2>>)

      assert {2, <<2>>} == TestEctoGenericEventLog.get_latest({@event_type_id, @event_key})
    end
  end
end
