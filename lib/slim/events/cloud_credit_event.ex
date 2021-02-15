defmodule Slim.Events.CloudCreditEvent do
  @moduledoc """
  A cloud payment for a customer's account.

  Each payment has a unique id, however, we want to use customer_id for the key so we
  can have the payments in the same event log with an increasing offset.
  """
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

  def key(event), do: event.customer_id

  def offset(event), do: event.offset

  def set_key(event, encoded_key), do: %__MODULE__{event | customer_id: encoded_key}

  def set_offset(event, encoded_offset), do: %__MODULE__{event | offset: encoded_offset}
end
