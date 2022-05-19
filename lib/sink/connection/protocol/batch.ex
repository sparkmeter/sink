defmodule Sink.Connection.Protocol.Batch do
  @moduledoc """
  Encoding/decoding for event batches.

  Because Sink knows the contents of a batch of events, it can run its own compression
  attempts as well as rearrange the data to allow for better compression by programs
  like gzip.

  The encoding format is designed to be space efficient, easy to extract, and allow
  processing parts of a batch without needing to decode the entire binary.

  ### How the Events Are Encoded

  Batches of events are transformed from row based data to column oriented data by
  extracting each attribute (event_type_id, key_length, key, etc.) and encoding all
  attributes together. For example, all event_type_id are encoded into one chunk. This
  is to improve compression. The batch encoder will analyze the data to determine which
  flags to use.

  A header describes what flags were used to encode the data and any values that should
  be used when decoding the data. This is necessary to decode the events from the body.
  The body then contains all the events encoded using the flags and values from the
  header.

  Some of these flags can be determined by analyzing the data before actually encoding
  anything. Other flags (ex: compression) can only be determined after the data has
  been encoded.

  ### Compression Strategies

  #### Homogenous Value

  If a value is the same (homogenous) for the entire batch, the value will be included
  in the header and excluded from the body. This is used for event_type_id,
  schema_version, key_length, and event_data_length.

  #### Running Delta

  If a value is usually very similar to the previous value, the value will be included
  in the header and only the delta will be included in the body. This is used for
  timestamps and offsets.

  If the delta value is smaller than the original value then it will result in a smaller
  varint. Typically events will be transmitted sequentially, so timestamps are increasing
  in a small increment. Sensor data or other events with large offsets are also often
  generated and transmitted with similar, large offsets as a system ages (ex: a group
  of sensors that were all installed at the same time will have a very similar number
  of total readings and thus offset).

  #### Gzip

  After Sink has tried all its tricks to compress the data, it will attempt to gzip
  the body. If the result is of a smaller size, the compression is applied.
  """
  import Sink.Connection.Protocol.Helpers

  @typedoc """
  Flags used when encoding/decoding the body of a batch.
  """
  @type flags :: %{
          event_type_id: boolean(),
          schema_version: boolean(),
          key_length: boolean(),
          offset: boolean(),
          event_data_length: boolean(),
          compressed: boolean(),
          timestamp: boolean()
        }

  @type data_flags :: %{
          event_type_id: boolean(),
          schema_version: boolean(),
          key_length: boolean(),
          offset: boolean(),
          event_data_length: boolean(),
          timestamp: boolean()
        }

  @typedoc """
  Values used when decoding the body of a batch.
  """
  @type header_values :: %{
          event_type_id: non_neg_integer(),
          schema_version: non_neg_integer(),
          key_length: non_neg_integer(),
          offset: non_neg_integer(),
          event_data_length: non_neg_integer(),
          timestamp: non_neg_integer()
        }

  @typedoc """
  A list of events intended for batch encoding
  """
  @type batch :: [Sink.Event.t()]

  @doc """
  Analyze the batch of events to determine the best ways to compress them.

  Implementation note: Using streams here would allow encoding an extremely large
  number of events (ex: from Ecto.Repo.stream) that wouldn't fit in memory.
  """
  @spec compute_flags(batch) :: data_flags()
  def compute_flags([first_event | rest]) do
    base = %{
      event_type_id: true,
      schema_version: true,
      key_length: true,
      event_data_length: true
    }

    {_last, flags} =
      Enum.reduce_while(rest, {first_event, base}, fn event, {last_event, acc} ->
        new_acc = %{
          event_type_id: acc.event_type_id && event.event_type_id == last_event.event_type_id,
          schema_version: acc.schema_version && event.schema_version == last_event.schema_version,
          key_length: acc.key_length && byte_size(event.key) == byte_size(last_event.key),
          event_data_length:
            acc.event_data_length &&
              byte_size(event.event_data) == byte_size(last_event.event_data)
        }

        if new_acc |> Map.values() |> Enum.any?(& &1) do
          {:cont, {event, new_acc}}
        else
          {:halt, {event, new_acc}}
        end
      end)

    Map.merge(flags, %{offset: true, timestamp: true})
  end

  @doc """
  Encode a batch of events into a binary

  #### Todo
  * only use the running delta encoder if this actually saves space. This can be checked
  when encoding the body by comparing the running delta vs just using varints.
  """
  @spec encode(batch) :: binary()
  def encode([first_event | _] = events) do
    flags = compute_flags(events)

    {body, compressed} =
      events
      |> encode_body_with_flags(flags)
      |> maybe_compress()

    encode_header(
      first_event,
      %{
        event_type_id: flags.event_type_id,
        schema_version: flags.schema_version,
        key_length: flags.key_length,
        offset: flags.offset,
        event_data_length: flags.event_data_length,
        compressed: compressed,
        timestamp: flags.timestamp,
        row_id: nil
      }
    ) <>
      body
  end

  @doc """
  Decode a binary into a batch of events.
  """
  @spec decode(binary()) :: {:ok, batch} | :error
  def decode(payload) do
    {header_values, flags, body} = decode_header(payload)
    decode_body_with_flags(body, header_values, flags)
  rescue
    _e in [MatchError, FunctionClauseError] ->
      :error
  end

  @doc """
  Encode an event batch body based on flags.
  """
  @spec encode_body_with_flags(batch, data_flags()) :: binary()
  def encode_body_with_flags([first_event | _] = events, flags) do
    acc =
      events
      |> Enum.reduce(
        {<<>>, <<>>, <<>>, <<>>, <<>>, <<>>, <<>>, <<>>, first_event.offset,
         first_event.timestamp},
        fn event, acc ->
          {event_type_ids, schema_versions, key_lengths, keys, offsets, timestamps,
           event_data_lengths, event_data, last_offset, last_timestamp} = acc

          {event_type_ids <> Varint.LEB128.encode(event.event_type_id),
           schema_versions <> Varint.LEB128.encode(event.schema_version),
           key_lengths <> Varint.LEB128.encode(byte_size(event.key)), keys <> event.key,
           offsets <> maybe_encode_delta_varint(event.offset, last_offset, flags.offset),
           timestamps <>
             maybe_encode_delta_varint(event.timestamp, last_timestamp, flags.timestamp),
           event_data_lengths <> Varint.LEB128.encode(byte_size(event.event_data)),
           event_data <> event.event_data, event.offset, event.timestamp}
        end
      )

    {event_type_ids, schema_versions, key_lengths, keys, offsets, timestamps, event_data_lengths,
     event_data, _offset, _timestamp} = acc

    Varint.LEB128.encode(length(events)) <>
      maybe_encode_chunk(event_type_ids, flags.event_type_id) <>
      maybe_encode_chunk(schema_versions, flags.schema_version) <>
      maybe_encode_chunk(key_lengths, flags.key_length) <>
      encode_chunk(keys) <>
      encode_chunk(offsets) <>
      encode_chunk(timestamps) <>
      maybe_encode_chunk(event_data_lengths, flags.event_data_length) <>
      event_data
  end

  @doc """
  Decode a batch body based on flags.

  Implementation note: This could be written much more cpu and memory efficiently by
  using pointers at the start of each section to walk through the chunks and build up
  the event list.
  """
  @spec decode_body_with_flags(binary(), header_values(), flags()) :: {:ok, batch}
  def decode_body_with_flags(payload, header_values, flags) do
    # todo:
    {num_events, rest} = payload |> maybe_decompress(flags.compressed) |> Varint.LEB128.decode()

    {event_type_ids, rest} = maybe_decode_chunk(rest, flags.event_type_id)
    {schema_versions, rest} = maybe_decode_chunk(rest, flags.schema_version)
    {key_lengths, rest} = maybe_decode_chunk(rest, flags.key_length)
    {keys_chunk, rest} = decode_chunk(rest)

    keys =
      if flags.key_length do
        decode_fixed_length_chunk(keys_chunk, header_values.key_length, num_events)
      else
        decode_variable_length_chunk(keys_chunk, key_lengths)
      end

    {offsets_chunk, rest} = decode_chunk(rest)
    offsets = maybe_decode_delta_chunk(offsets_chunk, flags.offset, header_values.offset)
    {timestamps_chunk, rest} = decode_chunk(rest)

    timestamps =
      maybe_decode_delta_chunk(timestamps_chunk, flags.timestamp, header_values.timestamp)

    {event_data_lengths, rest} = maybe_decode_chunk(rest, flags.event_data_length)
    # the event_data chunk is the last binary
    event_data =
      if flags.event_data_length do
        decode_fixed_length_chunk(rest, header_values.event_data_length, num_events)
      else
        decode_variable_length_chunk(rest, event_data_lengths)
      end

    # now that we have all the event chunks decoded, assemble it into events

    event_type_ids =
      if header_values.event_type_id do
        List.duplicate(header_values.event_type_id, num_events)
      else
        event_type_ids
      end

    schema_versions =
      if header_values.schema_version do
        List.duplicate(header_values.schema_version, num_events)
      else
        schema_versions
      end

    events =
      Enum.zip_reduce(
        [event_type_ids, keys, offsets, timestamps, event_data, schema_versions],
        [],
        fn [event_type_id, key, offset, timestamp, event_data, schema_version], acc ->
          [
            %Sink.Event{
              event_type_id: event_type_id,
              key: key,
              offset: offset,
              timestamp: timestamp,
              event_data: event_data,
              schema_version: schema_version,
              row_id: nil
            }
            | acc
          ]
        end
      )
      |> Enum.reverse()

    {:ok, events}
  end

  @doc """
  Encode a batch header based on flags and the first event.
  """
  @spec encode_header(Sink.Event.t(), flags()) :: binary()
  def encode_header(event, %{
        event_type_id: event_type_id,
        schema_version: schema_version,
        key_length: key_length,
        offset: offset,
        event_data_length: event_data_length,
        compressed: compressed,
        timestamp: timestamp
      }) do
    <<
      0::integer-size(1),
      encode_bool(compressed)::1,
      encode_bool(event_type_id)::1,
      encode_bool(schema_version)::1,
      encode_bool(key_length)::1,
      encode_bool(offset)::1,
      encode_bool(timestamp)::1,
      encode_bool(event_data_length)::1
    >> <>
      maybe_encode_varint(event.event_type_id, event_type_id) <>
      maybe_encode_varint(event.schema_version, schema_version) <>
      maybe_encode_varint(byte_size(event.key), key_length) <>
      maybe_encode_varint(event.offset, offset) <>
      maybe_encode_varint(event.timestamp, timestamp) <>
      maybe_encode_varint(byte_size(event.event_data), event_data_length)
  end

  @doc """
  Decode a batch header from a binary.
  """
  @spec decode_header(binary()) :: {header_values(), flags(), binary()}
  def decode_header(<<descriptor::binary-size(1), rest::binary>>) do
    <<0::integer-size(1), b_compressed::1, b_event_type_id::1, b_schema_version::1,
      b_key_length::1, b_offset::1, b_timestamp::1, b_event_data_length::1>> = descriptor

    flags = %{
      compressed: decode_bool(b_compressed),
      event_type_id: decode_bool(b_event_type_id),
      schema_version: decode_bool(b_schema_version),
      key_length: decode_bool(b_key_length),
      offset: decode_bool(b_offset),
      timestamp: decode_bool(b_timestamp),
      event_data_length: decode_bool(b_event_data_length)
    }

    {event_type_id, rest} = maybe_decode_varint(rest, flags.event_type_id)
    {schema_version, rest} = maybe_decode_varint(rest, flags.schema_version)
    {key_length, rest} = maybe_decode_varint(rest, flags.key_length)
    {offset, rest} = maybe_decode_varint(rest, flags.offset)
    {timestamp, rest} = maybe_decode_varint(rest, flags.timestamp)
    {event_data_length, rest} = maybe_decode_varint(rest, flags.event_data_length)

    header_values = %{
      event_type_id: event_type_id,
      schema_version: schema_version,
      key_length: key_length,
      offset: offset,
      event_data_length: event_data_length,
      timestamp: timestamp
    }

    {header_values, flags, rest}
  end
end
