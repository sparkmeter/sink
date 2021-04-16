defmodule Sink.Connection.Client.StateTest do
  use ExUnit.Case, async: true
  alias Sink.Connection.Client.State

  describe "disconnected/2" do
    test "unsets connection_pid" do
      assert %State{connection_pid: nil} =
               State.disconnected(%State{connection_pid: self()}, "reason", 1234)
    end

    test "keeps reason and time of disconnection around" do
      state = State.disconnected(%State{}, "reason", 1234)

      assert "reason" == state.disconnect_reason
      assert 1234 == state.disconnect_time
    end
  end
end
