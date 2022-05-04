defmodule Sink.Connection.FreshnessTest do
  use ExUnit.Case, async: true
  alias Sink.Connection.Freshness

  setup do
    table = :ets.new(:table, [:set, :private, write_concurrency: true, read_concurrency: true])
    {:ok, table: table}
  end

  describe "update/3" do
    test "success", %{table: table} do
      :ok = Freshness.update(table, "abc", 1, 123)
      :ok = Freshness.update(table, "def", 1, 124)
    end
  end

  describe "get_freshness/2" do
    test "fetches the worst and best timestamp for the client_id", %{table: table} do
      :ok = Freshness.update(table, "abc", 1, 123)
      :ok = Freshness.update(table, "abc", 2, 124)
      :ok = Freshness.update(table, "abc", 3, 125)
      :ok = Freshness.update(table, "def", 1, 122)
      :ok = Freshness.update(table, "def", 2, 126)

      assert {123, 125} = Freshness.get_freshness(table, "abc")
    end

    test "empty", %{table: table} do
      assert nil == Freshness.get_freshness(table, "abc")
    end
  end

  describe "get_freshness/3" do
    test "fetches the latest timestamp for the client_id and event_type_id", %{table: table} do
      :ok = Freshness.update(table, "abc", 1, 122)
      :ok = Freshness.update(table, "abc", 1, 123)
      :ok = Freshness.update(table, "abc", 2, 124)
      :ok = Freshness.update(table, "abc", 3, 125)
      :ok = Freshness.update(table, "def", 1, 126)

      assert 123 = Freshness.get_freshness(table, "abc", 1)
    end

    test "empty", %{table: table} do
      assert nil == Freshness.get_freshness(table, "abc", 1)
    end
  end
end
