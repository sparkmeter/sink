defmodule Sink.LogTest do
  use ExUnit.Case

  @table "hello"

  describe "append, at" do
    test "appends a new record to an empty log, can read it back" do
      Sink.Log.init()

      assert nil == Sink.Log.offset(@table)

      assert {:ok, cursor} = Sink.Log.append(@table, "world")
      assert 0 == cursor
      assert 0 == Sink.Log.offset(@table)
      assert "world" == Sink.Log.at(@table, 0)

      assert {:ok, cursor} = Sink.Log.append(@table, "WORLD")
      assert 1 == cursor
      assert 1 == Sink.Log.offset(@table)
      assert "WORLD" == Sink.Log.at(@table, 1)
    end
  end
end
