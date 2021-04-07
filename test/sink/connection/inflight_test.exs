defmodule Sink.Connection.InflightTest do
  @moduledoc false
  use ExUnit.Case, async: true
  alias Sink.Connection.Inflight

  @nack_data {<<>>, ""}

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
