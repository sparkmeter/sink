defmodule Sink.DataLogTest do
  use ExUnit.Case
  alias Sink.DataLog

  @table "hello"

  describe "append, at" do
    test "appends a new record to an empty log, can read it back" do
      DataLog.init()

      assert nil == DataLog.offset(@table)

      assert {:ok, cursor} = DataLog.append(@table, "world")
      assert 0 == cursor
      assert 0 == DataLog.offset(@table)
      assert "world" == DataLog.at(@table, 0)

      assert {:ok, cursor} = DataLog.append(@table, "WORLD")
      assert 1 == cursor
      assert 1 == DataLog.offset(@table)
      assert "WORLD" == DataLog.at(@table, 1)
    end
  end
end
