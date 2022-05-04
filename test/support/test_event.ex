defmodule Sink.TestEvent do
  defstruct [:key, :offset, :message, :version]

  def event_type(), do: "test_event"

  def key(%__MODULE__{key: key}), do: key

  def offset(%__MODULE__{offset: offset}), do: offset

  def serialize(%__MODULE__{} = event) do
    :erlang.term_to_binary(event)
  end

  def deserialize(binary) do
    :erlang.binary_to_term(binary)
  end
end
