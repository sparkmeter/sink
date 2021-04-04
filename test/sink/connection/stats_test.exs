defmodule Sink.Connection.StatsTest do
  @moduledoc false
  use ExUnit.Case, async: true
  alias Sink.Connection.Stats

  describe "checking if a we should ping" do
    test "should not ping if we have received something since keepalive" do
      state = %Stats{
        last_sent_at: 1_000_001,
        last_received_at: 1_000_000,
        keepalive_interval: 60_000
      }

      now = 1_060_000

      assert false == Stats.should_send_ping?(state, now)
    end

    test "should not ping if we haven't received anything but have pinged since keepalive" do
      state = %Stats{
        last_sent_at: 1_000_001,
        last_received_at: 1_000_000,
        keepalive_interval: 60_000
      }

      now = 1_060_000

      assert false == Stats.should_send_ping?(state, now)
    end

    test "should ping if we haven't received or sent anything since keepalive" do
      state = %Stats{
        last_sent_at: 1_000_000,
        last_received_at: 1_000_000,
        keepalive_interval: 60_000
      }

      now = 1_060_000

      assert true == Stats.should_send_ping?(state, now)
    end
  end

  describe "checking if a connection is alive" do
    test "should be alive if we have received a message in a reasonable amount of time" do
      state = %Stats{
        last_received_at: 1_000_000,
        keepalive_interval: 60_000
      }

      now = 1_089_999

      assert true == Stats.alive?(state, now)
    end

    test "should be dead if we have not received a message in a reasonable amount of time" do
      state = %Stats{
        last_received_at: 1_000_000,
        keepalive_interval: 60_000
      }

      now = 1_090_000

      assert false == Stats.alive?(state, now)
    end
  end
end
