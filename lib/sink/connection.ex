defmodule Sink.Connection do
  @moduledoc false
  @max_message_id (:math.pow(2, 12) - 1) |> Kernel.trunc()

  def next_message_id(nil) do
    Enum.random(0..@max_message_id)
  end

  def next_message_id(@max_message_id) do
    0
  end

  def next_message_id(message_id) do
    message_id + 1
  end
end
