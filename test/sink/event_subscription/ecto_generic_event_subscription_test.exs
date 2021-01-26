defmodule Sink.EventSubscription.EctoEventGenericSubscriptionTest do
  use ExUnit.Case, async: false
  doctest Sink
  alias Sink.EventLog.TestEctoGenericEventLog
  alias Sink.EventSubscription.TestEctoGenericEventSubscription

  @event_type_id 1
  @event_key <<1, 2, 3>>
  @topic {@event_type_id, @event_key}
  @event_data <<0, 0, 0>>
  @offset 1

  setup do
    # Explicitly get a connection before each test
    :ok = Ecto.Adapters.SQL.Sandbox.checkout(Sink.TestRepo)
  end

  describe "with no client_id" do
    test "subscribes to a topic with an event" do
      :ok = TestEctoGenericEventLog.log(@topic, @offset, @event_data)

      assert :ok = TestEctoGenericEventSubscription.subscribe(@topic, @offset)
    end

    test "get_offsets for an existing subscription" do
      :ok = TestEctoGenericEventLog.log(@topic, @offset, @event_data)
      :ok = TestEctoGenericEventSubscription.subscribe(@topic, @offset)

      assert {0, 1} == TestEctoGenericEventSubscription.get_offsets(@topic)
    end

    test "ack event" do
      :ok = TestEctoGenericEventLog.log(@topic, @offset, @event_data)
      :ok = TestEctoGenericEventSubscription.subscribe(@topic, @offset)

      assert :ok == TestEctoGenericEventSubscription.ack(@topic, @offset)

      assert {1, 1} == TestEctoGenericEventSubscription.get_offsets(@topic)
    end

    test "update_or_create (update)" do
      :ok = TestEctoGenericEventLog.log(@topic, @offset, @event_data)

      assert :ok == TestEctoGenericEventSubscription.update_or_create(@topic, @offset)
      assert {0, 1} == TestEctoGenericEventSubscription.get_offsets(@topic)
    end

    test "event_queue (empty)" do
      :ok = TestEctoGenericEventSubscription.subscribe(@topic, 0)

      assert [] = TestEctoGenericEventSubscription.queue()
    end

    test "event_queue (with a record)" do
      :ok = TestEctoGenericEventLog.log(@topic, @offset, @event_data)
      :ok = TestEctoGenericEventSubscription.subscribe(@topic, @offset)

      assert [{@topic, 0, 1}] == TestEctoGenericEventSubscription.queue()
    end

    test "event_queue (prioritization ordering)" do
      :ok = Sink.TestEctoEventTypeConfig.reset()
      # event_type_id_order = [3,2,1,4]
      # event_type_id_order is defined in TestEctoEventTypeConfig
      :ok = TestEctoGenericEventLog.log({1, <<>>}, 1, <<>>)
      :ok = TestEctoGenericEventLog.log({2, <<>>}, 1, <<>>)
      :ok = TestEctoGenericEventLog.log({3, <<>>}, 1, <<>>)
      :ok = TestEctoGenericEventLog.log({4, <<>>}, 1, <<>>)
      :ok = TestEctoGenericEventSubscription.subscribe({1, <<>>}, 1)
      :ok = TestEctoGenericEventSubscription.subscribe({2, <<>>}, 1)
      :ok = TestEctoGenericEventSubscription.subscribe({3, <<>>}, 1)
      :ok = TestEctoGenericEventSubscription.subscribe({4, <<>>}, 1)

      assert [
               {{3, <<>>}, 0, 1},
               {{2, <<>>}, 0, 1},
               {{1, <<>>}, 0, 1},
               {{4, <<>>}, 0, 1}
             ] == TestEctoGenericEventSubscription.queue()
    end
  end
end
