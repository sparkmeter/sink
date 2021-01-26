defmodule Sink.EctoEventTypeConfig do
  @moduledoc """
  Configuration for Event Types

  Current
  - Priority of return from subscription queue query

  Future
  - Retention policy
  - Transmit all vs only latest
  """
  import Ecto.Query, only: [from: 2]
  @repo Application.fetch_env!(:sink, :ecto_repo)

  def get(event_type_config_table, event_type_id) do
    @repo.get_by(event_type_config_table, event_type_id: event_type_id)
  end

  @doc """
  Clears any existing records and rebuilds the table
  """
  def reset(event_type_config_table, %{order: event_type_id_order}) do
    table_name = event_type_config_table.__schema__(:source)
    @repo.query("DELETE FROM #{table_name}", [])

    records =
      event_type_id_order
      |> Enum.with_index()
      |> Enum.map(fn {event_type_id, index} ->
        %{event_type_id: event_type_id, order: index}
      end)

    {_, nil} = @repo.insert_all(event_type_config_table, records)

    :ok
  end
end
