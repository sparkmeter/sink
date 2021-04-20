defmodule Sink.EventSubscription.HelperTest do
  use ExUnit.Case, async: false
  alias Sink.EventSubscription.Helper

  @nack_data {<<>>, ""}

  describe "get_nacked_event_types" do
    test "returns event_type_ids for nacks that meet or exceed the nack_threshold" do
      receieved_nacks = [
        {{1, <<1>>, 1}, @nack_data},
        {{2, <<2>>, 1}, @nack_data},
        {{2, <<2>>, 1}, @nack_data},
        {{3, <<3>>, 1}, @nack_data},
        {{3, <<3>>, 1}, @nack_data},
        {{3, <<3>>, 1}, @nack_data}
      ]

      assert [2, 3] == receieved_nacks |> Helper.get_nacked_event_types(2) |> Enum.sort()
    end
  end
end
