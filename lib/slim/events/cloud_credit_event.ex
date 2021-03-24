defmodule Slim.Events.CloudCreditEvent do
  @moduledoc """
  A cloud payment for a customer's account.

  Each payment has a unique id, however, we want to use customer_id for the key so we
  can have the payments in the same event log with an increasing offset.
  """
  @behaviour Slim.Event

  @type t() :: %__MODULE__{}

  defstruct [
    :customer_id,
    :cloud_credit_id,
    :amount,
    :balance,
    :portfolio_id,
    :external_identifier,
    :memo,
    :updated_by_id,
    :credited_at,
    :timestamp,
    :offset
  ]

  @impl true
  def avro_schema, do: "io.slim.cloud_credit_event"

  @impl true
  def key(event), do: event.customer_id

  @impl true
  def offset(event), do: event.offset

  @impl true
  def set_key(event, encoded_key), do: %__MODULE__{event | customer_id: encoded_key}

  @impl true
  def set_offset(event, encoded_offset), do: %__MODULE__{event | offset: encoded_offset}
end
