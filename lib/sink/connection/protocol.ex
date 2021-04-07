defmodule Sink.Connection.Protocol do
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

  def encode_payload(:publish, {event_type_id, key, offset, event_data}) do
    Varint.LEB128.encode(event_type_id) <>
      Varint.LEB128.encode(byte_size(key)) <>
      key <>
      Varint.LEB128.encode(offset) <>
      Varint.LEB128.encode(byte_size(event_data)) <>
      event_data
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
    {machine_message, human_message} = _parse_varint_delimited_value(payload)

    {machine_message, human_message}
  end

  def decode_payload(:publish, payload) do
    {event_type_id, rest} = Varint.LEB128.decode(payload)
    {key, rest} = _parse_varint_delimited_value(rest)
    {offset, rest} = Varint.LEB128.decode(rest)
    {event_data, <<>>} = _parse_varint_delimited_value(rest)

    {event_type_id, key, offset, event_data}
  end

  def _parse_varint_delimited_value(binary) do
    {value_length, value_and_rest} = Varint.LEB128.decode(binary)
    <<value::binary-size(value_length), rest::binary>> = value_and_rest

    {value, rest}
  end
end
