defmodule Sink.EventSubscription.EctoEventClientSubscriptionTest do
  use ExUnit.Case, async: false
  doctest Sink
  alias Sink.EventLog.TestEctoClientEventLog
  alias Sink.EventSubscription.TestEctoClientEventSubscription

  @client_id "test-client"
  @event_data <<0, 0, 0>>
  @event_key <<1, 2, 3>>
  @event_type_id 1
  @nack_data {<<>>, ""}
  @offset 1
  @timestamp 1_618_150_125
  @topic {@client_id, @event_type_id, @event_key}

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
  end

  describe "queue" do
    setup do
      :ok = Sink.TestEctoEventTypeConfig.reset()
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

    test "should not return already nacked events" do
      [
        {@client_id, 1, <<>>},
        {@client_id, 2, <<>>},
        {@client_id, 3, <<>>},
        {@client_id, 4, <<>>}
      ]
      |> Enum.each(fn topic ->
        :ok = TestEctoClientEventLog.log(topic, @offset, {@event_data, @timestamp})
        :ok = TestEctoClientEventSubscription.subscribe(topic, @offset)
      end)

      result =
        TestEctoClientEventSubscription.queue(@client_id,
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

    test "a message in flight can be excluded from the queue" do
      :ok = TestEctoClientEventLog.log(@topic, @offset, {@event_data, @timestamp})
      :ok = TestEctoClientEventSubscription.subscribe(@topic, @offset)

      ack_key = {@event_type_id, @event_key, @offset}
      assert [] == TestEctoClientEventSubscription.queue(@client_id, inflight: [ack_key])
    end

    test "limit works if we have already nacked events" do
      [
        {@client_id, 1, <<>>},
        {@client_id, 2, <<>>},
        {@client_id, 3, <<>>},
        {@client_id, 4, <<>>}
      ]
      |> Enum.each(fn topic ->
        :ok = TestEctoClientEventLog.log(topic, @offset, {@event_data, @timestamp})
        :ok = TestEctoClientEventSubscription.subscribe(topic, @offset)
      end)

      result =
        TestEctoClientEventSubscription.queue(@client_id,
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
  end
end
