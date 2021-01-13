defmodule Sink.Connection.ProtocolTest do
  use ExUnit.Case, async: false

  alias Sink.Connection.Protocol

  @encoded_event <<1, 2, 1, 2, 1, 1, 0>>

  describe "encode_frame" do
    @tag :skip
    test "encodes a connect"
    @tag :skip
    test "encodes a connect ack"
    @tag :skip
    test "encodes a graceful disconnect"
    @tag :skip
    test "encodes a ping"

    test "encodes a ack" do
      payload = <<1, 2>>
      frame = Protocol.encode_frame(:ack, 0, payload)

      expected = <<48, 0>> <> payload
      assert expected == frame
    end

    test "encodes a publish" do
      frame = Protocol.encode_frame(:publish, 0, @encoded_event)

      expected = <<64, 0>> <> @encoded_event
      assert expected == frame
    end
  end

  describe "decode_frame" do
    @tag :skip
    test "decodes a connect"
    @tag :skip
    test "decodes a connect ack"
    @tag :skip
    test "decodes a graceful disconnect"
    @tag :skip
    test "decodes a ping"

    test "decodes a ack" do
      frame = <<48, 0>>
      {message_type, message_id} = Protocol.decode_frame(frame)

      assert :ack == message_type
      assert 0 == message_id
    end

    test "decodes a publish" do
      frame = <<64, 0>> <> @encoded_event
      {message_type, message_id, payload} = Protocol.decode_frame(frame)

      assert :publish == message_type
      assert 0 == message_id
      assert @encoded_event == payload
    end
  end

  describe "encode_payload (ack)" do
    test "encodes an ack" do
      payload = Protocol.encode_payload(:ack, 0, 585)

      assert <<2, 73>> == payload
    end
  end

  describe "decode_payload (ack)" do
    test "decodes an ack" do
      assert {0, 585} == Protocol.decode_payload(:ack, <<2, 73>>)
    end
  end

  describe "encode_payload (publish)" do
    test "encodes an event with an event_type_id, key, and event_data" do
      payload = Protocol.encode_payload(:publish, {1, <<1, 2>>, 1, <<0>>})

      expected = <<1, 2, 1, 2, 1, 1, 0>>
      assert expected == payload
    end
  end

  describe "decode_payload (publish)" do
    test "decodes an event from binary" do
      payload = <<1, 2, 1, 2, 1, 1, 0>>

      event = Protocol.decode_payload(:publish, payload)

      expected = {1, <<1, 2>>, 1, <<0>>}
      assert expected == event
    end
  end
end
