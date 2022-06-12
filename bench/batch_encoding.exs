defmodule BatchEncoding do
  @moduledoc """
  Batch encode benchmarking

  ## Usage

  Start IEx with this module compiled:

      iex -S mix run bench/batch_encoding.exs

  Within IEx all the module's functions can be called, e.g.:

      iex> BatchEncoding.snapshot_to_binary()
      iex> BatchEncoding.snapshot_to_binary("some_snapshot.sink")

  Benchmarks can be run either using

      mix run bench/batch_encoding.exs

  or from IEx

      iex> BatchEncoding.benchmark
      iex> BatchEncoding.benchmark(implementations, files)

  There's also separate functions available via IEx for benchmarking sizes
  and speed/memory usage:

  - `BatchEncoding.benchmark_sizes/2`
  - `BatchEncoding.benchmark_speed_and_memory/2`

  """
  alias Sink.Connection.Protocol.Snapshot
  alias TableRex.Table

  @sink_event_fields [
    :event_data,
    :event_type_id,
    :key,
    :offset,
    :schema_version,
    :timestamp
  ]

  @implementations [
    {Sink.Connection.Protocol.Batch, :encode, []},
    {__MODULE__, :control, []},
    {:erlang, :term_to_binary, [[:compressed, minor_version: 2]]}
  ]

  @doc """
  Convert a snapshot from gladys into a simpler to use format
  """
  def snapshot_to_binary(input \\ "snapshot.sink") do
    raw = File.read!(input)

    events =
      %Snapshot{raw: raw}
      |> Enum.to_list()
      |> List.flatten()
      |> Enum.map(fn event -> Map.take(event, @sink_event_fields) end)

    File.write(
      "snapshot_#{System.os_time(:second)}",
      :erlang.term_to_binary(events, [:compressed, minor_version: 2])
    )
  end

  @doc """
  Benchmark implementations with test files
  """
  def benchmark(implementations \\ @implementations, files \\ all()) do
    benchmark_sizes(implementations, files)
    benchmark_speed_and_memory(implementations, files)

    :ok
  end

  def benchmark_sizes(implementations \\ @implementations, files \\ all()) do
    data = load_data_from_files(files)

    measurements =
      for impl <- implementations, {file, events} <- data, into: %{} do
        {{impl, file}, impl |> encode(events) |> byte_size()}
      end

    for {file, _events} <- data do
      [file | for(impl <- implementations, do: Map.get(measurements, {impl, file}))]
    end
    |> Table.new(["" | for(impl <- implementations, do: impl_label(impl))], "Benchmark")
    |> align_value_columns()
    |> color_benchmark_table_per_series()
    |> Table.render!()
    |> IO.puts()

    for {file, _} <- data do
      series = for impl <- implementations, do: {impl, Map.get(measurements, {impl, file})}
      render_snapshot_table(file, series)
    end

    :ok
  end

  def benchmark_speed_and_memory(implementations \\ @implementations, files \\ all()) do
    Benchee.run(
      Map.new(implementations, fn impl ->
        {impl_label(impl), fn events -> encode(impl, events) end}
      end),
      time: 10,
      memory_time: 10,
      inputs: load_data_from_files(files)
    )
  end

  defp load_data_from_files(files) do
    for file <- files do
      binary = File.read!(file)
      events = :erlang.binary_to_term(binary, [:safe])
      {file, events}
    end
  end

  defp color_benchmark_table_per_series(table) do
    table.rows
    |> Enum.with_index(0)
    |> Enum.reduce(table, fn {[_file | cells], y}, table ->
      series = Enum.map(cells, & &1.raw_value)
      {min, max} = Enum.min_max(series)

      series
      |> Enum.with_index(1)
      |> Enum.reduce(table, fn {value, x}, table ->
        in_range = (value - min) / (max - min)
        {r, g, b} = lerp_rgb({0, 5, 0}, {5, 0, 0}, in_range)
        Table.put_cell_meta(table, x, y, color: IO.ANSI.color(r, g, b))
      end)
    end)
  end

  defp align_value_columns(table) do
    Enum.reduce(1..length(table.header_row), table, fn col, table ->
      Table.put_column_meta(table, col, align: :right)
    end)
  end

  defp render_snapshot_table(title, series) do
    min = Enum.min(Keyword.values(series))

    rows =
      series
      |> Enum.sort_by(fn {_, v} -> v end, :asc)
      |> Enum.map(fn {impl, value} ->
        comparison =
          case value / min do
            1.0 -> ""
            float -> "#{Float.round(float * 100) |> trunc()}%"
          end

        [impl_label(impl), value, comparison]
      end)

    Table.new(rows, ["", "Size", "Comparison"], title)
    |> Table.put_column_meta(1, align: :right)
    |> Table.put_column_meta(2, align: :right)
    |> Table.sort(1, :asc)
    |> Table.render!()
    |> IO.puts()
  end

  defp encode({m, f, a}, events), do: apply(m, f, [events | a])

  defp impl_label({m, f, a}), do: inspect(Function.capture(m, f, length(a) + 1))

  defp all do
    Path.wildcard("snapshot_*")
  end

  defp lerp_rgb({r1, g1, b1}, {r2, g2, b2}, amount) do
    r = r1 + trunc((r2 - r1) * amount)
    g = g1 + trunc((g2 - g1) * amount)
    b = b1 + trunc((b2 - b1) * amount)
    {trunc(r), trunc(g), trunc(b)}
  end

  def control(events) do
    Enum.map_join(events, fn event ->
      sink = struct!(Sink.Event, event)
      payload = Sink.Connection.Protocol.encode_payload(:publish, sink)
      Sink.Connection.Protocol.Helpers.encode_chunk(payload)
    end)
  end
end

unless IEx.started?() do
  BatchEncoding.benchmark()
end
