defmodule Sink.Connection.Protocol.BatchTest do
  use ExUnit.Case, async: true

  alias Sink.Connection.Protocol

  @unix_now 1_618_150_125
  @sample_event %Sink.Event{
    event_type_id: 1,
    key: <<2, 3>>,
    offset: 4,
    timestamp: @unix_now,
    event_data: <<5>>,
    schema_version: 6,
    row_id: nil
  }
  @sample_event_opposite %Sink.Event{
    event_type_id: 2,
    key: <<3, 4, 5>>,
    offset: 6,
    timestamp: @unix_now + 1,
    event_data: <<7, 8>>,
    schema_version: 9,
    row_id: nil
  }
  @no_compression %{
    event_type_id: false,
    schema_version: false,
    key_length: false,
    offset: true,
    timestamp: true,
    event_data_length: false,
    compressed: false,
    row_id: nil
  }

  describe "encode" do
    test "one event" do
      encoded = Protocol.Batch.encode([@sample_event])

      assert {:ok, decoded} = Protocol.Batch.decode(encoded)
      assert [@sample_event] == decoded
    end

    test "two completely different events" do
      events = [
        @sample_event,
        @sample_event_opposite
      ]

      encoded = Protocol.Batch.encode(events)

      assert {:ok, decoded} = Protocol.Batch.decode(encoded)
      assert events == decoded
    end

    test "homogenous event type ids" do
      events = [
        @sample_event,
        %Sink.Event{@sample_event_opposite | event_type_id: @sample_event.event_type_id}
      ]

      encoded = Protocol.Batch.encode(events)

      assert {:ok, decoded} = Protocol.Batch.decode(encoded)
      assert events == decoded

      # check that the payload is smaller

      encoded_larger =
        Protocol.Batch.encode_header(@sample_event, @no_compression) <>
          Protocol.Batch.encode_body_with_flags(events, @no_compression)

      assert byte_size(encoded) < byte_size(encoded_larger)
    end

    test "homogenous event schema_versions" do
      events = [
        @sample_event,
        %Sink.Event{@sample_event_opposite | schema_version: @sample_event.schema_version}
      ]

      encoded = Protocol.Batch.encode(events)

      assert {:ok, decoded} = Protocol.Batch.decode(encoded)
      assert events == decoded

      # check that the payload is smaller

      encoded_larger =
        Protocol.Batch.encode_header(@sample_event, @no_compression) <>
          Protocol.Batch.encode_body_with_flags(events, @no_compression)

      assert byte_size(encoded) < byte_size(encoded_larger)
    end

    test "homogenous event key lengths" do
      events = [
        @sample_event,
        %Sink.Event{@sample_event_opposite | key: <<3, 4>>}
      ]

      encoded = Protocol.Batch.encode(events)

      assert {:ok, decoded} = Protocol.Batch.decode(encoded)
      assert events == decoded

      # check that the payload is smaller

      encoded_larger =
        Protocol.Batch.encode_header(@sample_event, @no_compression) <>
          Protocol.Batch.encode_body_with_flags(events, @no_compression)

      assert byte_size(encoded) < byte_size(encoded_larger)
    end

    test "homogenous event data lengths" do
      events = [
        @sample_event,
        %Sink.Event{@sample_event_opposite | event_data: <<0>>}
      ]

      encoded = Protocol.Batch.encode(events)

      assert {:ok, decoded} = Protocol.Batch.decode(encoded)
      assert events == decoded

      # check that the payload is smaller

      encoded_larger =
        Protocol.Batch.encode_header(@sample_event, @no_compression) <>
          Protocol.Batch.encode_body_with_flags(events, @no_compression)

      assert byte_size(encoded) < byte_size(encoded_larger)
    end

    @tag :skip
    test "compressed"
  end

  describe "decode" do
    test "returns an error for garbage" do
      assert :error == Protocol.Batch.decode(<<>>)
    end
  end

  describe "compute_flags" do
    test "two completely different events" do
      events = [
        @sample_event,
        @sample_event_opposite
      ]

      assert %{
               event_type_id: false,
               schema_version: false,
               key_length: false,
               offset: true,
               timestamp: true,
               event_data_length: false
             } == Protocol.Batch.compute_flags(events)
    end

    test "homogenous event type ids" do
      events = [
        @sample_event,
        %Sink.Event{@sample_event_opposite | event_type_id: @sample_event.event_type_id}
      ]

      assert %{
               event_type_id: true,
               schema_version: false,
               key_length: false,
               offset: true,
               timestamp: true,
               event_data_length: false
             } == Protocol.Batch.compute_flags(events)
    end

    test "homogenous event schema_versions" do
      events = [
        @sample_event,
        %Sink.Event{@sample_event_opposite | schema_version: @sample_event.schema_version}
      ]

      assert %{
               event_type_id: false,
               schema_version: true,
               key_length: false,
               offset: true,
               timestamp: true,
               event_data_length: false
             } == Protocol.Batch.compute_flags(events)
    end

    test "homogenous event key lengths" do
      events = [
        @sample_event,
        %Sink.Event{@sample_event_opposite | key: <<0, 1>>}
      ]

      assert %{
               event_type_id: false,
               schema_version: false,
               key_length: true,
               offset: true,
               timestamp: true,
               event_data_length: false
             } == Protocol.Batch.compute_flags(events)
    end

    test "homogenous event data lengths" do
      events = [
        @sample_event,
        %Sink.Event{@sample_event_opposite | event_data: <<1>>}
      ]

      assert %{
               event_type_id: false,
               schema_version: false,
               key_length: false,
               offset: true,
               timestamp: true,
               event_data_length: true
             } == Protocol.Batch.compute_flags(events)
    end
  end
end
