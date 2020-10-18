defmodule Sink.Consumer do
  alias Sink.DataLog

  @consumer_offsets_table :_sink_log_consumer_offsets

  def init do
    :ets.new(@consumer_offsets_table, [:named_table, :protected, :set])

    :ok
  end

  def subscribe(consumer, topic) do
    :ets.insert(@consumer_offsets_table, {{consumer, topic}, nil})

    :ok
  end

  def read_next(consumer, topic) do
    offset = case offset(consumer, topic) do
      nil -> 0
      num -> num + 1
    end

    {offset, DataLog.at(topic, offset)}
  end

  def ack(consumer, topic, offset) do
    :ets.insert(@consumer_offsets_table, {{consumer, topic}, offset})

    :ok
  end

  def offset(consumer, topic) do
    case :ets.lookup(@consumer_offsets_table, {consumer, topic}) do
      [{_, offset}] -> offset
      [] -> {:error, :not_subscribed}
    end
  end
end
