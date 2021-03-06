defmodule Sink.Connection.Server do
  @moduledoc false
  @registry Sink.Connection.ServerHandler

  @doc """
  Return a list of all the currently connected client_ids
  """
  @spec connected_clients() :: list(String.t())
  def connected_clients do
    Registry.select(@registry, [{{:"$1", :_, :_}, [], [:"$1"]}])
  end

  @doc """
  Returns the number of currently connected clients.

  For large numbers of connected clients this will be more performant than `connected_clients`
  """
  @spec connected_clients_count() :: non_neg_integer()
  def connected_clients_count do
    Registry.count(@registry)
  end

  @doc """
  Returns a DateTime of when the client connected or nil if no connection
  """
  @spec connected_at(String.t()) :: DateTime.t() | nil
  def connected_at(client_id) do
    @registry
    |> Registry.lookup(client_id)
    |> case do
      [] -> nil
      [{_pid, connected_at}] -> connected_at
    end
  end
end
