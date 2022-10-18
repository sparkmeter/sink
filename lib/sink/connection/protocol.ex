defmodule Sink.Connection.Protocol do
  alias Sink.Connection.Protocol.Helpers
  alias Sink.Event

  @typedoc "32-bit integer to uniquely identify a given server instance"
  @type server_identifier :: integer

  @typedoc "Client application version in string format"
  @type application_version :: String.t()

  @typedoc "ID for a published message."
  @type message_id :: pos_integer()

  @type message ::
          {:connection_request, {application_version, server_identifier | nil}}
          | {:connection_response,
             :connected
             | {:hello_new_client, server_identifier}
             | :server_identifier_mismatch
             | {:quarantined, payload :: binary}
             | {:unsupported_protocol_version, term}
             | :unsupported_application_version}
          | {:ack, message_id}
          | {:publish, message_id, payload :: binary}
          | :ping
          | :pong
          | {:nack, message_id, payload :: binary}

  @type nack_data() :: {binary(), String.t()}

  # when we get to Sink 1.0 this will be 0 and we'll delete / deprecate anything
  # 8 and higher
  @protocol_version 8
  @supported_protocol_versions [8]
  @message_type_id_connection_request 0
  @message_type_id_connection_response 1

  @doc """
  Encode various types of messages into their encompasing frame.
  """
  @spec encode_frame(message) :: binary

  # Connection Request (0)

  def encode_frame({:connection_request, @protocol_version, {version, server_identifier}}) do
    encode_frame({:connection_request, {version, server_identifier}})
  end

  def encode_frame({:connection_request, protocol_version, _}) do
    raise "Received invalid protocol version: #{protocol_version}"
  end

  def encode_frame({:connection_request, {version, server_identifier}}) do
    version_chunk = Helpers.encode_chunk(version)

    id_chunk =
      case server_identifier do
        id when is_integer(id) -> <<id::32>>
        nil -> <<>>
      end

    payload = version_chunk <> id_chunk
    do_encode_frame(@message_type_id_connection_request, @protocol_version, payload)
  end

  # Connection Response (1)

  def encode_frame({:connection_response, :connected}) do
    do_encode_frame(@message_type_id_connection_response, 0)
  end

  def encode_frame({:connection_response, {:hello_new_client, server_identifier}}) do
    payload = <<server_identifier::32>>
    do_encode_frame(@message_type_id_connection_response, 1, payload)
  end

  def encode_frame({:connection_response, :server_identifier_mismatch}) do
    do_encode_frame(@message_type_id_connection_response, 2)
  end

  def encode_frame({:connection_response, {:quarantined, payload}}) do
    do_encode_frame(@message_type_id_connection_response, 3, payload)
  end

  def encode_frame({:connection_response, {:unsupported_protocol_version, protocol_version}}) do
    payload = <<protocol_version::8>>
    do_encode_frame(@message_type_id_connection_response, 4, payload)
  end

  def encode_frame({:connection_response, :unsupported_application_version}) do
    do_encode_frame(@message_type_id_connection_response, 5)
  end

  # Other

  def encode_frame({:ack, message_id}) do
    do_encode_frame(3, message_id)
  end

  def encode_frame({:publish, message_id, payload}) do
    do_encode_frame(4, message_id, payload)
  end

  def encode_frame(:ping) do
    do_encode_frame(5, 0)
  end

  def encode_frame(:pong) do
    do_encode_frame(6, 0)
  end

  def encode_frame({:nack, message_id, payload}) do
    do_encode_frame(7, message_id, payload)
  end

  defp do_encode_frame(message_type_id, message_id, payload \\ <<>>)

  defp do_encode_frame(message_type_id, message_type, payload)
       when message_type_id in [
              @message_type_id_connection_request,
              @message_type_id_connection_response
            ] and is_integer(message_type) and is_binary(payload) do
    <<message_type_id::4, message_type::4, payload::binary>>
  end

  defp do_encode_frame(message_type_id, message_id, payload)
       when is_integer(message_type_id) and is_integer(message_id) and is_binary(payload) do
    <<message_type_id::4, message_id::12, payload::binary>>
  end

  @doc """
  Encode payloads of messages
  """
  @spec encode_payload(:publish, Event.t()) :: binary()
  @spec encode_payload(:nack, nack_data()) :: binary()
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

  @doc """
  Decodes various types of messages from their encompasing frame.
  """
  @spec encode_frame(binary) :: message

  # Connection Request (0)

  def decode_frame(<<@message_type_id_connection_request::4, protocol_version::4, rest::binary>>) do
    if protocol_version not in @supported_protocol_versions do
      {:error, :unsupported_protocol_version, protocol_version}
    else
      {version, id_chunk} = Helpers.decode_chunk(rest)

      server_identifier =
        case id_chunk do
          <<>> -> nil
          <<server_identifier::32>> -> server_identifier
        end

      {:connection_request, protocol_version, {version, server_identifier}}
    end
  end

  # Connection Response (1)

  def decode_frame(<<@message_type_id_connection_response::4, 0::4>>) do
    {:connection_response, :connected}
  end

  def decode_frame(<<@message_type_id_connection_response::4, 1::4, server_identifier::32>>) do
    {:connection_response, {:hello_new_client, server_identifier}}
  end

  def decode_frame(<<@message_type_id_connection_response::4, 2::4>>) do
    {:connection_response, :server_identifier_mismatch}
  end

  def decode_frame(<<@message_type_id_connection_response::4, 3::4, payload::binary>>) do
    {:connection_response, {:quarantined, payload}}
  end

  def decode_frame(<<@message_type_id_connection_response::4, 4::4, protocol_version::8>>) do
    {:connection_response, {:unsupported_protocol_version, protocol_version}}
  end

  def decode_frame(<<@message_type_id_connection_response::4, 5::4>>) do
    {:connection_response, :unsupported_application_version}
  end

  # Other

  def decode_frame(<<3::4, message_id::12>>) do
    {:ack, message_id}
  end

  def decode_frame(<<4::4, message_id::12, payload::binary>>) do
    {:publish, message_id, payload}
  end

  def decode_frame(<<5::4, _::12>>) do
    :ping
  end

  def decode_frame(<<6::4, _::12>>) do
    :pong
  end

  def decode_frame(<<7::4, message_id::12, payload::binary>>) do
    {:nack, message_id, payload}
  end

  @doc """
  Decode payloads of messages
  """
  @spec decode_payload(:publish, binary()) :: Event.t()
  @spec decode_payload(:nack, binary()) :: nack_data()
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
