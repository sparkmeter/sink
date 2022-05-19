defmodule Sink.Connection.Protocol do
  alias Sink.Connection.Protocol.Helpers
  alias Sink.Event

  def encode_frame(:ack, message_id) do
    message_type_id = 3
    <<message_type_id::4, message_id::integer-size(12)>>
  end

  def encode_frame(:ping) do
    message_type_id = 5
    message_id = 0
    <<message_type_id::4, message_id::integer-size(12)>>
  end

  def encode_frame(:pong) do
    message_type_id = 6
    message_id = 0
    <<message_type_id::4, message_id::integer-size(12)>>
  end

  def encode_frame(message_type, message_id, payload) do
    message_type_id =
      case message_type do
        :publish -> 4
        :nack -> 7
      end

    header = <<message_type_id::4, message_id::integer-size(12)>>

    header <> payload
  end

  def decode_frame(message) do
    <<message_type_id::4, message_id::integer-size(12), payload::binary>> = message

    case message_type_id do
      3 -> {:ack, message_id}
      4 -> {:publish, message_id, payload}
      5 -> :ping
      6 -> :pong
      7 -> {:nack, message_id, payload}
    end
  end

  def encode_payload(:publish, %Event{} = event) do
    Varint.LEB128.encode(event.event_type_id) <>
      Varint.LEB128.encode(event.schema_version) <>
      Varint.LEB128.encode(byte_size(event.key)) <>
      event.key <>
      Varint.LEB128.encode(event.offset) <>
      Varint.LEB128.encode(event.timestamp) <>
      Varint.LEB128.encode(byte_size(event.event_data)) <>
      event.event_data
  end

  def encode_payload(:nack, {machine_message, human_message}) do
    Varint.LEB128.encode(byte_size(machine_message)) <>
      machine_message <>
      human_message
  end

  def decode_payload(:ack, <<message_type_id::4, message_id::integer-size(12)>>) do
    {message_type_id, message_id}
  end

  def decode_payload(:nack, <<payload::binary>>) do
    {machine_message, human_message} = Helpers.decode_chunk(payload)

    {machine_message, human_message}
  end

  def decode_payload(:publish, payload) do
    {event_type_id, rest} = Varint.LEB128.decode(payload)
    {schema_version, rest} = Varint.LEB128.decode(rest)
    {key, rest} = Helpers.decode_chunk(rest)
    {offset, rest} = Varint.LEB128.decode(rest)
    {timestamp, rest} = Varint.LEB128.decode(rest)
    {event_data, <<>>} = Helpers.decode_chunk(rest)

    %Event{
      event_type_id: event_type_id,
      schema_version: schema_version,
      key: key,
      offset: offset,
      timestamp: timestamp,
      event_data: event_data
    }
  end
end
