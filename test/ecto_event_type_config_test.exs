defmodule Sink.EctoEventTypeConfigTest do
  use ExUnit.Case, async: false
  doctest Sink

  describe "reset" do
    :ok = Sink.TestEctoEventTypeConfig.reset()

    config_db = Sink.TestEctoEventTypeConfig.get(3)
    assert 0 == config_db.order
    config_db = Sink.TestEctoEventTypeConfig.get(2)
    assert 1 == config_db.order
    config_db = Sink.TestEctoEventTypeConfig.get(1)
    assert 2 == config_db.order
    config_db = Sink.TestEctoEventTypeConfig.get(4)
    assert 3 == config_db.order
  end
end
