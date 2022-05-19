defmodule Sink.Connection.ClientTest do
  @moduledoc false
  use ExUnit.Case, async: false
  alias Sink.Connection.Client
  alias Sink.Event

  describe "publish" do
    test "sends an {:error, :no_connection} if there is no connection" do
      message = %Event{
        event_type_id: 1,
        key: <<1, 2>>,
        offset: 1,
        timestamp: 1_618_150_125,
        event_data: "Hi",
        schema_version: 3,
        row_id: nil
      }

      assert {:error, :no_connection} == Client.publish(message, {1, <<>>, 3})
    end
  end
end
