defmodule Sink.Connection.Freshness do
  @moduledoc """
  Tracking of freshness per event and base station.

  Data is tracked in an ETS table, so it can be queried without disturbing the
  TCP connection to the base station.
  """
  use GenServer

  @doc false
  def start_link(init_arg) do
    GenServer.start_link(__MODULE__, init_arg, name: __MODULE__)
  end

  @impl GenServer
  def init(_) do
    :ets.new(__MODULE__, [
      :set,
      :public,
      :named_table,
      write_concurrency: true,
      read_concurrency: true
    ])

    {:ok, nil}
  end

  @doc """
  Update timestamp in ets table
  """
  def update(table \\ __MODULE__, client_id, event_type_id, timestamp) do
    key = {client_id, event_type_id}

    # There is a race condition in fetch and update,
    # but there should only ever be one connection per client_id so
    # writes should not conflict.
    do_insert =
      case :ets.lookup(table, key) do
        [{^key, ts}] -> ts < timestamp
        [] -> true
      end

    if do_insert do
      :ets.insert(table, {key, timestamp})
    end

    :ok
  end

  @doc false
  def reset(table \\ __MODULE__) do
    :ets.delete_all_objects(table)
  end

  @doc """
  Get the latest timestamp for the given client_id.
  """
  def get_freshness(client_id) do
    get_freshness(__MODULE__, client_id)
  end

  @doc """
  Get the latest timestamp for the given client_id and event_type_id.
  """
  def get_freshness(client_id, event_type_id)

  def get_freshness(client_id, event_type_id) when is_integer(event_type_id) do
    get_freshness(__MODULE__, client_id, event_type_id)
  end

  def get_freshness(table, client_id) when is_binary(client_id) do
    :ets.match(table, {{client_id, :_}, :"$1"})
    |> List.flatten()
    |> Enum.min_max(fn -> nil end)
  end

  @doc false
  def get_freshness(table, client_id, event_type_id) do
    key = {client_id, event_type_id}

    case :ets.lookup(table, key) do
      [{^key, ts}] -> ts
      [] -> nil
    end
  end
end
