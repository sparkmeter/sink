defmodule Sink.EventSubscription.EctoEventGenericSubscriptionTest do
  use ExUnit.Case, async: false
  doctest Sink
  alias Sink.EventLog.TestEctoGenericEventLog
  alias Sink.EventSubscription.TestEctoGenericEventSubscription

  @event_data <<0, 0, 0>>
  @event_key <<1, 2, 3>>
  @event_type_id 1
  @nack_data {<<>>, ""}
  @offset 1
  @timestamp 1_618_150_125
  @topic {@event_type_id, @event_key}

  setup do
    # Explicitly get a connection before each test
    :ok = Ecto.Adapters.SQL.Sandbox.checkout(Sink.TestRepo)
  end

  describe "with no client_id" do
    test "subscribes to a topic with an event" do
      :ok = TestEctoGenericEventLog.log(@topic, @offset, {@event_data, @timestamp})

      assert :ok = TestEctoGenericEventSubscription.subscribe(@topic, @offset)
    end

    test "get_offsets for an existing subscription" do
      :ok = TestEctoGenericEventLog.log(@topic, @offset, {@event_data, @timestamp})
      :ok = TestEctoGenericEventSubscription.subscribe(@topic, @offset)

      assert {0, 1} == TestEctoGenericEventSubscription.get_offsets(@topic)
    end

    test "ack event" do
      :ok = TestEctoGenericEventLog.log(@topic, @offset, {@event_data, @timestamp})
      :ok = TestEctoGenericEventSubscription.subscribe(@topic, @offset)

      assert :ok == TestEctoGenericEventSubscription.ack(@topic, @offset)

      assert {1, 1} == TestEctoGenericEventSubscription.get_offsets(@topic)
    end

    test "update_or_create (update)" do
      :ok = TestEctoGenericEventLog.log(@topic, @offset, {@event_data, @timestamp})

      assert :ok == TestEctoGenericEventSubscription.update_or_create(@topic, @offset)
      assert {0, 1} == TestEctoGenericEventSubscription.get_offsets(@topic)
    end
  end

  describe "queue" do
    setup do
      :ok = Sink.TestEctoEventTypeConfig.reset()
    end

    test "(empty)" do
      :ok = TestEctoGenericEventSubscription.subscribe(@topic, 0)

      assert [] = TestEctoGenericEventSubscription.queue()
    end

    test "(with a record)" do
      :ok = TestEctoGenericEventLog.log(@topic, @offset, {@event_data, @timestamp})
      :ok = TestEctoGenericEventSubscription.subscribe(@topic, @offset)

      assert [{@event_type_id, @event_key, 0, 1}] == TestEctoGenericEventSubscription.queue()
    end

    test "a message in flight can be excluded from the queue" do
      :ok = TestEctoGenericEventLog.log(@topic, @offset, {@event_data, @timestamp})
      :ok = TestEctoGenericEventSubscription.subscribe(@topic, @offset)

      ack_key = {@event_type_id, @event_key, @offset}
      assert [] == TestEctoGenericEventSubscription.queue(inflight: [ack_key])
    end

    test "should not return already nacked events" do
      [
        {1, <<>>},
        {2, <<>>},
        {3, <<>>},
        {4, <<>>}
      ]
      |> Enum.each(fn topic ->
        :ok = TestEctoGenericEventLog.log(topic, @offset, {@event_data, @timestamp})
        :ok = TestEctoGenericEventSubscription.subscribe(topic, @offset)
      end)

      result =
        TestEctoGenericEventSubscription.queue(
          received_nacks: [
            {{2, <<>>, 1}, @nack_data},
            {{3, <<>>, 1}, @nack_data}
          ]
        )

      assert [
               {1, <<>>, 0, 1},
               {4, <<>>, 0, 1}
             ] == result
    end

    test "limit works if we have already nacked events" do
      [
        {1, <<>>},
        {2, <<>>},
        {3, <<>>},
        {4, <<>>}
      ]
      |> Enum.each(fn topic ->
        :ok = TestEctoGenericEventLog.log(topic, @offset, {@event_data, @timestamp})
        :ok = TestEctoGenericEventSubscription.subscribe(topic, @offset)
      end)

      result =
        TestEctoGenericEventSubscription.queue(
          received_nacks: [
            {{2, <<>>, 1}, @nack_data},
            {{3, <<>>, 1}, @nack_data}
          ],
          limit: 1
        )

      assert [
               {1, <<>>, 0, 1}
             ] == result
    end

    test "a nacked event type id is excluded from the queue" do
      :ok = TestEctoGenericEventLog.log(@topic, @offset, {@event_data, @timestamp})
      :ok = TestEctoGenericEventSubscription.subscribe(@topic, @offset)

      result =
        TestEctoGenericEventSubscription.queue(
          received_nacks: [
            {{@event_type_id, <<>>, 1}, @nack_data}
          ],
          nack_threshold: 1
        )

      assert [] == result
    end

    test "queue (prioritization ordering)" do
      # event_type_id_order = [3,2,1,4]
      # event_type_id_order is defined in TestEctoEventTypeConfig
      :ok = TestEctoGenericEventLog.log({1, <<>>}, 1, {<<>>, @timestamp})
      :ok = TestEctoGenericEventLog.log({2, <<>>}, 1, {<<>>, @timestamp})
      :ok = TestEctoGenericEventLog.log({3, <<>>}, 1, {<<>>, @timestamp})
      :ok = TestEctoGenericEventLog.log({4, <<>>}, 1, {<<>>, @timestamp})
      :ok = TestEctoGenericEventSubscription.subscribe({1, <<>>}, 1)
      :ok = TestEctoGenericEventSubscription.subscribe({2, <<>>}, 1)
      :ok = TestEctoGenericEventSubscription.subscribe({3, <<>>}, 1)
      :ok = TestEctoGenericEventSubscription.subscribe({4, <<>>}, 1)

      assert [
               {3, <<>>, 0, 1},
               {2, <<>>, 0, 1},
               {1, <<>>, 0, 1},
               {4, <<>>, 0, 1}
             ] == TestEctoGenericEventSubscription.queue()
    end
  end
end
