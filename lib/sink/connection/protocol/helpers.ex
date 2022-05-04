defmodule Sink.Connection.Protocol.Helpers do
  @doc """
  If gzip will make a binary smaller return the results.
  """
  @spec maybe_compress(binary()) :: {binary(), boolean()}
  def maybe_compress(payload) do
    compressed = :zlib.gzip(payload)

    if byte_size(compressed) < byte_size(payload) do
      {compressed, true}
    else
      {payload, false}
    end
  end

  @doc """
  Conditionally unzip a binary.
  """
  @spec maybe_decompress(binary(), boolean()) :: binary()
  def maybe_decompress(payload, false), do: payload
  def maybe_decompress(payload, true), do: :zlib.gunzip(payload)

  @doc """
  Conditionally encode an integer as a LEB128 varint.
  """
  @spec maybe_encode_varint(non_neg_integer(), boolean()) :: binary()
  def maybe_encode_varint(_value, false), do: <<>>

  def maybe_encode_varint(value, true) do
    Varint.LEB128.encode(value)
  end

  @doc """
  Conditionally encode a delta of the current value minus the previous.
  """
  @spec maybe_encode_delta_varint(non_neg_integer(), non_neg_integer(), boolean()) :: binary()
  def maybe_encode_delta_varint(current, _last, false), do: Varint.LEB128.encode(current)

  def maybe_encode_delta_varint(current, last, true) do
    (current - last)
    |> Varint.Zigzag.encode()
    |> Varint.LEB128.encode()
  end

  @doc """
  Conditionally decode a LEB128 varint from a binary.
  """
  @spec maybe_decode_varint(binary(), boolean()) :: {non_neg_integer(), binary()}
  def maybe_decode_varint(payload, false), do: {nil, payload}

  def maybe_decode_varint(payload, true) do
    Varint.LEB128.decode(payload)
  end

  @doc """
  Decode a binary into 0 or more varints
  """
  @spec decode_varints(binary()) :: [non_neg_integer()]
  def decode_varints(payload), do: decode_varints(payload, [])
  def decode_varints(<<>>, acc), do: Enum.reverse(acc)

  def decode_varints(payload, acc) do
    {varint, rest} = Varint.LEB128.decode(payload)
    decode_varints(rest, [varint | acc])
  end

  @spec encode_bool(boolean()) :: 1 | 0
  def encode_bool(true), do: 1
  def encode_bool(false), do: 0

  def decode_bool(0), do: false
  def decode_bool(1), do: true

  def maybe_encode_chunk(_data, true), do: <<>>
  def maybe_encode_chunk(data, false), do: encode_chunk(data)

  def maybe_decode_delta_chunk(chunk, false, _), do: decode_varints(chunk)

  def maybe_decode_delta_chunk(chunk, true, val) do
    decode_delta_varint(chunk, val, [])
  end

  @doc """
  Prepend a binary with a varint that describes the length of the binary.
  """
  @spec encode_chunk(binary()) :: binary()
  def encode_chunk(payload) do
    Varint.LEB128.encode(byte_size(payload)) <> payload
  end

  @doc """
  Decode a chunk of data that is prepended by a varint.
  """
  @spec decode_chunk(binary()) :: {binary(), binary()}
  def decode_chunk(payload) do
    {varint, rest} = Varint.LEB128.decode(payload)
    <<data::binary-size(varint), remaining::binary>> = rest

    {data, remaining}
  end

  @doc """
  Conditionally decode a varint prepended chunk of data.
  """
  @spec maybe_decode_chunk(binary(), bool()) :: {nil | [non_neg_integer()], binary()}
  def maybe_decode_chunk(payload, true), do: {nil, payload}

  def maybe_decode_chunk(payload, false) do
    {varints, rest} = decode_chunk(payload)
    {decode_varints(varints), rest}
  end

  @doc """
  Decode a binary that is encoded as a set number of fixed length pieces.
  """
  @spec decode_fixed_length_chunk(binary(), non_neg_integer(), non_neg_integer()) :: [binary()]
  def decode_fixed_length_chunk(chunk, piece_len, num_pieces) do
    {pieces, _rest} =
      Enum.reduce(1..num_pieces, {[], chunk}, fn _, {pieces, chunk} ->
        <<val::binary-size(piece_len), rest::binary>> = chunk
        {[val | pieces], rest}
      end)

    Enum.reverse(pieces)
  end

  @doc """
  Decode a binary that is encoded as a number of pieces of variable length.
  """
  @spec decode_variable_length_chunk(binary(), [non_neg_integer()]) :: [binary()]
  def decode_variable_length_chunk(payload, lengths) do
    decode_variable_length_chunk(payload, lengths, [])
  end

  def decode_variable_length_chunk(_payload, [], acc), do: Enum.reverse(acc)

  def decode_variable_length_chunk(payload, [payload_length | lengths], acc) do
    <<bin::binary-size(payload_length), rest::binary>> = payload
    decode_variable_length_chunk(rest, lengths, [bin | acc])
  end

  # Decode a value from the previous value and a zigzag encoded delta.
  # Not to be confused with the 2021 COVID delta variant.
  defp decode_delta_varint(<<>>, _val, acc), do: Enum.reverse(acc)

  defp decode_delta_varint(payload, val, acc) do
    {zigzag, rest} = Varint.LEB128.decode(payload)
    delta = Varint.Zigzag.decode(zigzag)
    new_val = val + delta
    decode_delta_varint(rest, new_val, [new_val | acc])
  end
end
