defmodule Sink.Connection.Client.Backoff do
  @moduledoc """
  A behaviour for implementing client backoff on consecutive unsuccessful connection attempts.
  """

  @typedoc "Number of consecutive connection attempts having been unsuccessful before"
  @type attempts :: pos_integer()

  @typedoc "True if the last connection attempt received a connection request rejection"
  @type connection_request_rejected :: boolean()

  @typedoc "Backoff duration in milliseconds"
  @type backoff :: non_neg_integer()

  @doc """
  Calculate the backoff to apply.
  """
  @callback backoff_duration(attempts, connection_request_rejected) :: backoff

  @doc false
  def add_jitter(interval) do
    variance = div(interval, 10)
    interval + Enum.random(-variance..variance)
  end
end
