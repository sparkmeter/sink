defmodule Sink.Telemetry do
  @moduledoc """
  Emit telemetry events for connection status and connection messages.

  For the server, the `client_id` will be present in the metadata. For the client,
  the `client_id` tag is not present.

  - [:sink, :connection, :start]
    is executed when a new connection is created
    meta: %{client_id, peername}
      - client_id: the client ID
      - peername: <IP:PORT>
    measurements: %{system_time}

  - [:sink, :connection, :stop]
    is executed when an exisiting connection closed
    meta: %{client_id, peername, reason}
      - client_id: the client ID
      - peername: <IP:PORT>
      - reason:
        - ssl_closed
        - tcp_closed
        - tcp_error
    measurements: %{duration}

  - [:sink, :connection, :exception]
    TODO: when connection processes are monitored and unexpectedly go down add
    hooks for this event

  - [:sink, :connection, :sent, :publish]
    is executed when a PUBLISH is sent
    meta: %{client_id}
    - client_id: the client ID

  - [:sink, :connection, :received, :publish]
    is executed when a PUBLISH is received
    meta: %{client_id}
    - client_id: the client ID

  - [:sink, :connection, :sent, :nack]
    is executed when a NACK is sent
    meta: %{client_id}
    - client_id: the client ID

  - [:sink, :connection, :received, :nack]
    is executed when a NACK is received
    meta: %{client_id}
    - client_id: the client ID

  - [:sink, :connection, :sent, :ack]
    is executed when an ACK is sent
    meta: %{client_id}
    - client_id: the client ID

  - [:sink, :connection, :received, :ack]
    is executed when an ACK is received
    meta: %{client_id}
    - client_id: the client ID

  - [:sink, :connection, :sent, :ping]
    is executed when a PING is sent
    meta: %{client_id}
    - client_id: the client ID

  - [:sink, :connection, :received, :ping]
    is executed when a PING is received
    meta: %{client_id}
    - client_id: the client ID

  - [:sink, :connection, :sent, :pong]
    is executed when a PONG is sent
    meta: %{client_id}
    - client_id: the client ID

  - [:sink, :connection, :received, :pong]
    is executed when a PONG is received
    meta: %{client_id}
    - client_id: the client ID

  """

  @doc false
  def start(name, meta, measurements \\ %{}) do
    time = System.monotonic_time()
    measurements = Map.put(measurements, :system_time, time)
    :telemetry.execute([:sink, name, :start], measurements, meta)
  end

  @doc false
  def stop(name, start_time, meta, measurements \\ %{}) do
    end_time = System.monotonic_time()
    measurements = Map.merge(measurements, %{duration: end_time - start_time})
    :telemetry.execute([:sink, name, :stop], measurements, meta)
  end

  @doc false
  def exception(event, start_time, kind, reason, stack, meta \\ %{}, extra_measurements \\ %{}) do
    end_time = System.monotonic_time()
    measurements = Map.merge(extra_measurements, %{duration: end_time - start_time})

    meta =
      meta
      |> Map.put(:kind, kind)
      |> Map.put(:error, reason)
      |> Map.put(:stacktrace, stack)

    :telemetry.execute([:sink, event, :exception], measurements, meta)
  end

  def ack(direction, meta \\ %{}) do
    :telemetry.execute([:sink, :connection, direction, :ack], %{}, meta)
  end

  def nack(direction, meta \\ %{}) do
    :telemetry.execute([:sink, :connection, direction, :nack], %{}, meta)
  end

  def publish(direction, meta \\ %{}) do
    :telemetry.execute([:sink, :connection, direction, :publish], %{}, meta)
  end

  def ping(direction, meta \\ %{}) do
    :telemetry.execute([:sink, :connection, direction, :ping], %{}, meta)
  end

  def pong(direction, meta \\ %{}) do
    :telemetry.execute([:sink, :connection, direction, :pong], %{}, meta)
  end
end
