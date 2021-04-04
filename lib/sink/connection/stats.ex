defmodule Sink.Connection.Stats do
  @moduledoc """
  Encapsulates the details of a Sink Connection.
  """
  defstruct [
    :last_received_at,
    :last_sent_at,
    :keepalive_interval,
    :start_time
  ]

  @default_keepalive_interval 60_000

  def init(now) do
    %__MODULE__{
      last_received_at: now,
      last_sent_at: now,
      start_time: now,
      keepalive_interval:
        Application.get_env(:sink, :keepalive_interval, @default_keepalive_interval)
    }
  end

  def log_received(%__MODULE__{} = state, now) do
    %__MODULE__{state | last_received_at: now}
  end

  def log_sent(%__MODULE__{} = state, now) do
    %__MODULE__{state | last_sent_at: now}
  end

  def alive?(%__MODULE__{} = state, now) do
    state.keepalive_interval * 1.5 > now - state.last_received_at
  end

  @doc """
  If we haven't sent or received a message within the keepalive timeframe, we should send a ping

  https://www.hivemq.com/blog/mqtt-essentials-part-10-alive-client-take-over/

  Inspired by MQTT. Client should be regularly sending and receiving data. If no
  actual message is sent within the keepalive timefame, a PING will be sent.
  """
  def should_send_ping?(
        %__MODULE__{} = state,
        now
      ) do
    cond do
      state.keepalive_interval > now - state.last_sent_at -> false
      state.keepalive_interval > now - state.last_received_at -> false
      true -> true
    end
  end
end
