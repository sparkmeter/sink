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

  def instance_id_handshake(server_instance_ids, client_instance_ids) do
    case {server_instance_ids, client_instance_ids} do
      # Both sides the same
      {%{server: server, client: client}, %{server: server, client: client}} ->
        {:ok, :known}

      # Both client and server connect for the first time
      {%{server: _server, client: nil}, %{server: nil, client: _}} ->
        {:ok, :new}

      # Client doesn't know server, but is still the same client known on the server
      # This can happen when a connection is interrupted with the server side written
      # but the client not having receiving the server_instance_id.
      {%{server: _server, client: client}, %{server: nil, client: client}} ->
        {:ok, :new}

      _ ->
        {:error, :mismatch}
    end
  end
end
