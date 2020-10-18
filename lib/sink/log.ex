defmodule Sink.Log do
  @contents_table :_sink_log_data
  @offsets_table :_sink_log_offsets

  def init do
    :ets.new(@contents_table, [:named_table, :protected, :set])
    :ets.new(@offsets_table, [:named_table, :protected, :set])

    :ok
  end

  def append(name, contents) do
    next_offset =
      case offset(name) do
        nil -> 0
        cur_offset -> cur_offset + 1
      end

    :ets.insert(@offsets_table, {name, next_offset})
    :ets.insert(@contents_table, {{name, next_offset}, contents})

    {:ok, next_offset}
  end

  def at(name, offset) do
    case :ets.lookup(@contents_table, {name, offset}) do
      [{_, contents}] -> contents
      [] -> nil
    end
  end

  def offset(name) do
    case :ets.lookup(@offsets_table, name) do
      [{_name, offset}] -> offset
      [] -> nil
    end
  end
end
