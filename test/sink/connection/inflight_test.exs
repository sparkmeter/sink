defmodule Sink.Connection.InflightTest do
  @moduledoc false
  use ExUnit.Case, async: true
  alias Sink.Connection.Inflight

  @event_type_id 1
  @nack_data {<<>>, ""}

  describe "get_inflight" do
    test "with a few inflight messages" do
      state =
        %Inflight{
          next_message_id: 100
        }
        |> Inflight.put_inflight({@event_type_id, <<1>>, 1})
        |> Inflight.put_inflight({@event_type_id, <<2>>, 1})

      inflight_topics = Inflight.get_inflight(state)

      assert [
               {@event_type_id, <<1>>, 1},
               {@event_type_id, <<2>>, 1}
             ] == inflight_topics
    end
  end

  describe "put_received_nack" do
    test "removes from 'inflight'" do
      state =
        %Inflight{
          next_message_id: 100
        }
        |> Inflight.put_inflight({@event_type_id, <<1>>, 1})
        |> Inflight.put_received_nack(100, {@event_type_id, <<1>>, 1}, @nack_data)

      assert false == Inflight.inflight?(state, {@event_type_id, <<1>>, 1})
    end
  end

  describe "get_received_nacks" do
    test "returns the received nacks" do
      state =
        %Inflight{
          next_message_id: 100
        }
        |> Inflight.put_inflight({@event_type_id, <<1>>, 1})
        |> Inflight.put_inflight({@event_type_id, <<2>>, 1})
        |> Inflight.put_received_nack(100, {@event_type_id, <<1>>, 1}, @nack_data)
        |> Inflight.put_received_nack(101, {@event_type_id, <<2>>, 1}, @nack_data)

      assert [
               {{@event_type_id, <<2>>, 1}, @nack_data},
               {{@event_type_id, <<1>>, 1}, @nack_data}
             ] == Inflight.get_received_nacks(state)
    end
  end

  describe "received_nacks_by_event_type" do
    test "correctly groups nacks" do
      state =
        %Inflight{
          next_message_id: 100
        }
        |> Inflight.put_received_nack(100, {1, <<1>>, 1}, @nack_data)
        |> Inflight.put_received_nack(101, {1, <<2>>, 1}, @nack_data)
        |> Inflight.put_received_nack(102, {1, <<3>>, 1}, @nack_data)
        |> Inflight.put_received_nack(103, {2, <<2>>, 1}, @nack_data)

      assert %{
               1 => 3,
               2 => 1
             } == Inflight.received_nacks_by_event_type_id(state)
    end
  end

  describe "sent_nacks_by_event_type" do
    test "correctly groups nacks" do
      state =
        %Inflight{
          next_message_id: 100
        }
        |> Inflight.put_sent_nack(100, {1, <<1>>, 1}, @nack_data)
        |> Inflight.put_sent_nack(101, {1, <<2>>, 1}, @nack_data)
        |> Inflight.put_sent_nack(102, {1, <<3>>, 1}, @nack_data)
        |> Inflight.put_sent_nack(103, {2, <<1>>, 1}, @nack_data)

      assert %{
               1 => 3,
               2 => 1
             } == Inflight.sent_nacks_by_event_type_id(state)
    end
  end
end
