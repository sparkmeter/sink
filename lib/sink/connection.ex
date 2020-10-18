defmodule Sink.Connection do
  @moduledoc false
  @max_message_id (:math.pow(2, 12) - 1) |> Kernel.trunc()
  @message_type_publish 0
  @message_type_ack 1

  def connected?(client_id) do
    process_name = String.to_atom("sink-#{client_id}")
    Process.whereis(process_name) != nil
  end

  def decode_message(<<@message_type_ack::4, message_id::integer-size(12)>>) do
    {:ack, message_id}
  end

  def decode_message(<<@message_type_publish::4, message_id::integer-size(12), body::binary>>) do
    {:publish, message_id, body}
  end

  def encode_publish(message_id, binary) when is_binary(binary) do
    <<@message_type_publish::4, message_id::integer-size(12), binary::binary>>
  end

  @doc "todo: remove"
  def encode_publish(message_id, binary) do
    <<@message_type_publish::4, message_id::integer-size(12), binary::binary>>
  end

  def encode_ack(message_id) do
    <<@message_type_ack::4, message_id::integer-size(12)>>
  end

  def next_message_id(nil) do
    Enum.random(0..@max_message_id)
  end

  def next_message_id(@max_message_id) do
    0
  end

  def next_message_id(message_id) do
    message_id + 1
  end

  def publish(client_id, binary, ack_key: ack_key) do
    case whereis(client_id) do
      nil -> {:error, :no_connection}
      pid -> Sink.Connection.ServerHandler.publish(pid, binary, ack_key)
    end
  end

  def whereis(client_id) do
    process_name = String.to_atom("sink-#{client_id}")
    Process.whereis(process_name)
  end
end
