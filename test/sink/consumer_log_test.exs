defmodule Sink.ConsumerLogTest do
  use ExUnit.Case
  alias Sink.{Consumer, DataLog}

  @consumer "alice"
  @log_topic "hello"

  describe "append, at" do
    setup do
      Consumer.init()
      DataLog.init()
      DataLog.append(@log_topic, "world")
      DataLog.append(@log_topic, "WORLD")

      :ok
    end

    test "tracks the consumer's offset w/ack" do
      assert :ok == Consumer.subscribe(@consumer, @log_topic)
      assert nil == Consumer.offset(@consumer, @log_topic)

      assert {0, "world"} == Consumer.read_next(@consumer, @log_topic)
      assert :ok == Consumer.ack(@consumer, @log_topic, 0)
      assert 0 == Consumer.offset(@consumer, @log_topic)

      assert {1, "WORLD"} == Consumer.read_next(@consumer, @log_topic)
      assert :ok == Consumer.ack(@consumer, @log_topic, 1)
      assert 1 == Consumer.offset(@consumer, @log_topic)
    end
  end
end
