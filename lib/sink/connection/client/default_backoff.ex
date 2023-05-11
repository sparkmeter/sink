defmodule Sink.Connection.Client.DefaultBackoff do
  @behaviour Sink.Connection.Client.Backoff

  # Always backoff 5 minutes when the server actively denied a connection
  def backoff_duration(_, true), do: :timer.minutes(5)

  # Backoff in growing steps for other issues in connecting
  def backoff_duration(1, false), do: 50
  def backoff_duration(2, false), do: :timer.seconds(1)
  def backoff_duration(3, false), do: :timer.seconds(5)
  def backoff_duration(4, false), do: :timer.seconds(30)
  def backoff_duration(_, false), do: :timer.seconds(60)
end
