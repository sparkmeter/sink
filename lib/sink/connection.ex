defmodule Sink.Connection do
  @moduledoc false
  alias X509.Certificate
  @max_message_id (:math.pow(2, 12) - 1) |> Kernel.trunc()

  def next_message_id(nil) do
    Enum.random(0..@max_message_id)
  end

  def next_message_id(@max_message_id) do
    0
  end

  def next_message_id(message_id) do
    message_id + 1
  end

  def cacerts_from_paths(paths) do
    Enum.map(paths, fn path ->
      path
      |> File.read!()
      |> Certificate.from_pem!()
      |> Certificate.to_der()
    end)
  end
end
