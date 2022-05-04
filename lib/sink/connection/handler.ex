defmodule Sink.Connection.Handler do
  @moduledoc """
  Callback for handling Sink connection changes and messages
  """

  defmacro __using__(_opts) do
    quote location: :keep do
      @behaviour Sink.Connection.Handler

      defoverridable Sink.Connection.Handler
    end
  end

  @doc """
  Invoked when a client is attempting to authenticate
  """
  @callback authenticate_client(binary(), String.t()) ::
              {:ok, String.t()} | {:error, :unauthorized_client}

  @type event() :: {non_neg_integer(), binary(), pos_integer(), binary()}

  @doc """
  Invoked when a message is received with an event
  """
  @callback handle_publish(list(event()), String.t()) :: :ok

  @type connection_status() ::
          {:tcp_closed, any()}
          | {:ssl_closed, any()}
          | {:tcp_error, any(), any()}
  @type client_id() :: String.t()
  @doc """
  Invoked when connections status changes.

  Status can be :tcp_closed, :ssl_closed, :tcp_error
  Should we message on up? Or is that handled by init()
  """
  @callback connection(connection_status(), client_id()) :: :ok
end
