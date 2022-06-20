defmodule Sink.Connection.Protocol do
  alias Sink.Connection.Protocol.Helpers
  alias Sink.Event

  # when we get to Sink 1.0 this will be 0 and we'll delete / deprecate anything
  # 8 and higher
  @protocol_version 8
  @message_type_id_connection_request 0
  @message_type_id_connection_response 1

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

  def encode_frame(:ack, message_id) do
    message_type_id = 3
    <<message_type_id::4, message_id::integer-size(12)>>
  end

  def encode_frame(:connection_request, {client_instantiated_at, server_instantiated_at}) do
    header = <<@message_type_id_connection_request::4, @protocol_version::4>>
    client_chunk = <<client_instantiated_at::integer-size(32)>>

    server_chunk =
      if !is_nil(server_instantiated_at) do
        <<server_instantiated_at::integer-size(32)>>
      else
        <<>>
      end

    header <> client_chunk <> server_chunk
  end

  def encode_frame(:connection_response, :ok) do
    <<@message_type_id_connection_response::integer-size(4), 0::integer-size(4)>>
  end

  def encode_frame(:connection_response, {:hello_new_client, server_instantiated_at}) do
    <<@message_type_id_connection_response::integer-size(4), 1::integer-size(4)>> <>
      <<server_instantiated_at::integer-size(32)>>
  end

  def encode_frame(:connection_response, {:mismatched_client, client_instantiated_at}) do
    <<@message_type_id_connection_response::integer-size(4), 2::integer-size(4)>> <>
      <<client_instantiated_at::integer-size(32)>>
  end

  def encode_frame(:connection_response, {:mismatched_server, server_instantiated_at}) do
    <<@message_type_id_connection_response::integer-size(4), 3::integer-size(4)>> <>
      <<server_instantiated_at::integer-size(32)>>
  end

  def encode_frame(:connection_response, {:quarantined, {machine_message, human_message}}) do
    header = <<@message_type_id_connection_response::integer-size(4), 4::integer-size(4)>>

    machine_chunk =
      Helpers.maybe_encode_varint(byte_size(machine_message), true) <> machine_message

    human_chunk = Helpers.maybe_encode_varint(byte_size(human_message), true) <> human_message
    header <> machine_chunk <> human_chunk
  end

  def encode_frame(:connection_response, :unquarantined) do
    <<@message_type_id_connection_response::integer-size(4), 5::integer-size(4)>>
  end

  def encode_frame(:connection_response, :unsupported_protocol_version) do
    <<@message_type_id_connection_response::integer-size(4), 6::integer-size(4)>>
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
    <<message_type_id::integer-size(4), header_rest::integer-size(4), rest::binary>> = message

    case message_type_id do
      @message_type_id_connection_request ->
        decode_connection_request(rest)

      @message_type_id_connection_response ->
        decode_connection_response(header_rest, rest)

      _ ->
        <<message_type_id::4, message_id::integer-size(12), payload::binary>> = message

        case message_type_id do
          3 -> {:ack, message_id}
          4 -> {:publish, message_id, payload}
          5 -> :ping
          6 -> :pong
          7 -> {:nack, message_id, payload}
        end
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

  defp decode_connection_request(rest) do
    <<client_instantiated_at::integer-size(32), maybe_server::binary>> = rest

    if maybe_server != <<>> do
      <<server_instantiated_at::integer-size(32)>> = maybe_server
      {:connection_request, {client_instantiated_at, server_instantiated_at}}
    else
      {:connection_request, {client_instantiated_at, nil}}
    end
  end

  defp decode_connection_response(0, <<>>), do: {:connection_response, :ok}

  defp decode_connection_response(1, rest) do
    <<server_instantiated_at::integer-size(32)>> = rest
    {:connection_response, {:hello_new_client, server_instantiated_at}}
  end

  defp decode_connection_response(2, rest) do
    <<client_instantiated_at::integer-size(32)>> = rest
    {:connection_response, {:mismatched_client, client_instantiated_at}}
  end

  defp decode_connection_response(3, rest) do
    <<server_instantiated_at::integer-size(32)>> = rest
    {:connection_response, {:mismatched_server, server_instantiated_at}}
  end

  defp decode_connection_response(4, rest) do
    {machine_chunk, rest} = Helpers.decode_chunk(rest)
    {human_chunk, <<>>} = Helpers.decode_chunk(rest)
    {:connection_response, {:quarantined, {machine_chunk, human_chunk}}}
  end

  defp decode_connection_response(5, <<>>) do
    {:connection_response, :unquarantined}
  end

  defp decode_connection_response(6, <<>>) do
    {:connection_response, :unsupported_protocol_version}
  end
end
