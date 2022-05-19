defmodule Sink.Connection.Protocol.SnapshotTest do
  use ExUnit.Case, async: true

  alias Sink.Connection.Protocol.Snapshot

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

  describe "append_batch" do
    test "works" do
      first_part = Snapshot.append_batch(<<>>, [@sample_event])
      payload = Snapshot.append_batch(first_part, [@sample_event_opposite])

      assert {:ok, [@sample_event], rest} = Snapshot.decode_next_batch(payload)
      assert {:ok, [@sample_event_opposite], <<>>} = Snapshot.decode_next_batch(rest)
    end
  end

  describe "Enumerable + Collectable" do
    test "works" do
      snapshot =
        [
          [@sample_event],
          [@sample_event_opposite]
        ]
        |> Enum.into(%Snapshot{})

      assert [
               [@sample_event],
               [@sample_event_opposite]
             ] = Enum.to_list(snapshot)
    end
  end
end
