defmodule Sink.Connection.Transport.SSL do
  @moduledoc """
  Wrapper for SSL transport
  """

  @behaviour Sink.Connection.Transport

  @impl true
  def peercert(socket) do
    :ssl.peercert(socket)
  end

  @impl true
  def peername(socket) do
    :ssl.peername(socket)
  end

  @impl true
  def send(socket, data) do
    :ssl.send(socket, data)
  end
end
