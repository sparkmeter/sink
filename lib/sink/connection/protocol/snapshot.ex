defmodule Sink.Connection.Protocol.Snapshot do
  @moduledoc """
  Encoding/decoding for snapshots.
  """
  @type t :: %__MODULE__{raw: raw()}
  @type raw :: binary()

  defstruct raw: <<>>

  alias Sink.Connection.Protocol.Batch
  alias Sink.Connection.Protocol.Helpers

  @spec new(raw()) :: t()
  def new(bin) when is_binary(bin), do: %__MODULE__{raw: bin}

  @spec append_batch(raw, Batch.batch()) :: raw
  def append_batch(acc, events) do
    batch = Batch.encode(events)
    acc <> Helpers.encode_chunk(batch)
  end

  @spec decode_next_batch(raw()) :: {:ok, Batch.batch(), raw()}
  def decode_next_batch(payload) do
    {batch, rest} = Helpers.decode_chunk(payload)
    {:ok, events} = Batch.decode(batch)
    {:ok, events, rest}
  end

  defimpl Enumerable do
    alias Sink.Connection.Protocol.Snapshot

    def count(_), do: {:error, __MODULE__}
    def member?(_, _), do: {:error, __MODULE__}
    def slice(_), do: {:error, __MODULE__}

    def reduce(%Snapshot{raw: raw}, acc, fun), do: reduce(raw, acc, fun)

    def reduce(_bin, {:halt, acc}, _fun), do: {:halted, acc}
    def reduce(bin, {:suspend, acc}, fun), do: {:suspended, acc, &reduce(bin, &1, fun)}
    def reduce(<<>>, {:cont, acc}, _fun), do: {:done, acc}

    def reduce(bin, {:cont, acc}, fun) do
      {:ok, events, rest} = Snapshot.decode_next_batch(bin)
      reduce(rest, fun.(events, acc), fun)
    end
  end

  defimpl Collectable do
    alias Sink.Connection.Protocol.Snapshot

    def into(snapshot) do
      collector_fun = fn
        %Snapshot{raw: raw}, {:cont, events} ->
          %Snapshot{raw: Snapshot.append_batch(raw, events)}

        snapshot_acc, :done ->
          snapshot_acc

        _snapshot_acc, :halt ->
          :ok
      end

      initial_acc = snapshot

      {initial_acc, collector_fun}
    end
  end
end
