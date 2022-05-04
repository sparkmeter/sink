defmodule Sink.Test.Certificates do
  @moduledoc """
  Module for fetching certain certificates/keys
  """
  alias X509.{Certificate, PrivateKey}
  def ca_cert_file, do: Path.absname("test/support/certs/ca-cert.pem")
  def server_cert_file, do: Path.absname("test/support/certs/server-cert.pem")
  def server_key_file, do: Path.absname("test/support/certs/server-key.pem")
  def client_cert_file, do: Path.absname("test/support/certs/client-cert.pem")
  def client_key_file, do: Path.absname("test/support/certs/client-key.pem")

  def ca_certs do
    ca_cert_file()
    |> cert_pem_at_path_to_der()
    |> List.wrap()
  end

  def client_cert do
    client_cert_file() |> cert_pem_at_path_to_der()
  end

  def client_key do
    {:ECPrivateKey, key_pem_at_path_to_der(client_key_file())}
  end

  defp key_pem_at_path_to_der(path) do
    path |> File.read!() |> PrivateKey.from_pem!() |> PrivateKey.to_der()
  end

  defp cert_pem_at_path_to_der(path) do
    path |> File.read!() |> Certificate.from_pem!() |> Certificate.to_der()
  end
end
