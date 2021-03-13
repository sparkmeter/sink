defmodule Sink.Connection.Transport do
  @moduledoc false

  @type socket() :: any()

  @callback peercert(socket()) :: {:ok, binary()} | {:error, any()}
  @callback peername(socket()) ::
              {:ok, {:inet.ip_address(), :inet.port_number()}} | {:error, atom()}

  @callback send(socket(), binary()) :: :ok | {:error, any()}
end
