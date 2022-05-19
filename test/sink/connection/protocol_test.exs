defmodule Sink.Connection.ProtocolTest do
  use ExUnit.Case, async: false

  alias Sink.Connection.Protocol
  alias Sink.Event

  @encoded_event <<1, 2, 1, 2, 1, 1, 0>>
  @unix_now 1_618_150_125

  describe "encode_frame" do
    @tag :skip
    test "encodes a connect"
    @tag :skip
    test "encodes a connect ack"
    @tag :skip
    test "encodes a graceful disconnect"

    test "encodes a ping" do
      assert <<80, 0>> == Protocol.encode_frame(:ping)
    end

    test "encodes a pong" do
      assert <<96, 0>> == Protocol.encode_frame(:pong)
    end

    test "encodes a ack" do
      frame = Protocol.encode_frame(:ack, 0)

      expected = <<48, 0>>
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

    test "decodes a ping" do
      assert :ping == Protocol.decode_frame(<<80, 0>>)
    end

    test "decodes a pong" do
      assert :pong == Protocol.decode_frame(<<96, 0>>)
    end

    test "decodes a ack" do
      frame = <<48, 0>>
      {message_type, message_id} = Protocol.decode_frame(frame)

      assert :ack == message_type
      assert 0 == message_id
    end

    test "decodes a nack" do
      frame = <<112, 0>> <> "\x01*crash"
      {message_type, message_id, payload} = Protocol.decode_frame(frame)

      assert :nack == message_type
      assert 0 == message_id
      assert "\x01*crash" == payload
    end

    test "decodes a publish" do
      frame = <<64, 0>> <> @encoded_event
      {message_type, message_id, payload} = Protocol.decode_frame(frame)

      assert :publish == message_type
      assert 0 == message_id
      assert @encoded_event == payload
    end
  end

  describe "encode_payload (nack)" do
    test "encodes a nack" do
      payload = Protocol.encode_payload(:nack, {<<42>>, "crash"})

      assert "\x01*crash" == payload
    end
  end

  describe "decode_payload (nack)" do
    test "decodes a nack" do
      assert {<<42>>, "crash"} == Protocol.decode_payload(:nack, "\x01*crash")
    end
  end

  describe "encode_payload (publish)" do
    test "encodes an event with an event_type_id, key, and event_data" do
      event = %Event{
        event_type_id: 1,
        key: <<1, 2>>,
        offset: 9,
        timestamp: @unix_now,
        event_data: <<0>>,
        schema_version: 3
      }

      payload = Protocol.encode_payload(:publish, event)

      expected = <<1, 3, 2, 1, 2, 9, 237, 133, 204, 131, 6, 1, 0>>
      assert expected == payload
    end
  end

  describe "decode_payload (publish)" do
    test "decodes an event from binary" do
      payload = <<1, 3, 2, 1, 2, 9, 237, 133, 204, 131, 6, 1, 0>>

      event = Protocol.decode_payload(:publish, payload)

      assert %Event{
               event_type_id: 1,
               key: <<1, 2>>,
               offset: 9,
               timestamp: @unix_now,
               event_data: <<0>>,
               schema_version: 3
             } == event
    end
  end
end
