defmodule Sink.EventSubscription.EctoEventClientSubscriptionTest do
  use ExUnit.Case, async: false
  doctest Sink
  alias Sink.EventLog.TestEctoClientEventLog
  alias Sink.EventSubscription.TestEctoClientEventSubscription

  @client_id "test-client"
  @event_type_id 1
  @event_key <<1, 2, 3>>
  @topic {@client_id, @event_type_id, @event_key}
  @event_data <<0, 0, 0>>
  @offset 1
  @timestamp 1_618_150_125

  setup do
    # Explicitly get a connection before each test
    :ok = Ecto.Adapters.SQL.Sandbox.checkout(Sink.TestRepo)
  end

  describe "with no client_id" do
    test "subscribes to a topic with an event" do
      :ok = TestEctoClientEventLog.log(@topic, @offset, {@event_data, @timestamp})

      assert :ok = TestEctoClientEventSubscription.subscribe(@topic, @offset)
    end

    test "get_offsets for an existing subscription" do
      :ok = TestEctoClientEventLog.log(@topic, @offset, {@event_data, @timestamp})
      :ok = TestEctoClientEventSubscription.subscribe(@topic, @offset)

      assert {0, 1} == TestEctoClientEventSubscription.get_offsets(@topic)
    end

    test "ack event" do
      :ok = TestEctoClientEventLog.log(@topic, @offset, {@event_data, @timestamp})
      :ok = TestEctoClientEventSubscription.subscribe(@topic, @offset)

      assert :ok == TestEctoClientEventSubscription.ack(@topic, @offset)

      assert {1, 1} == TestEctoClientEventSubscription.get_offsets(@topic)
    end

    test "update_or_create (update)" do
      :ok = TestEctoClientEventLog.log(@topic, @offset, {@event_data, @timestamp})

      assert :ok == TestEctoClientEventSubscription.update_or_create(@topic, @offset)
      assert {0, 1} == TestEctoClientEventSubscription.get_offsets(@topic)
    end

    test "event_queue (empty)" do
      :ok = TestEctoClientEventSubscription.subscribe(@topic, 0)

      assert [] = TestEctoClientEventSubscription.queue(@client_id)
    end

    test "event_queue (with a record)" do
      :ok = TestEctoClientEventLog.log(@topic, @offset, {@event_data, @timestamp})
      :ok = TestEctoClientEventSubscription.subscribe(@topic, @offset)

      assert [{@event_type_id, @event_key, 0, 1}] ==
               TestEctoClientEventSubscription.queue(@client_id)
    end
  end
end
