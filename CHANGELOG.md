# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]

### Added

- Adds telemetry hooks for connection start and stop events
- Splits Sink.Connection.Client into two processes - one to bring up the connection, another to handle connection data. Also traps exits which should allow notifying SinkHandler when a connection errors. [CH46694]
- Mox for testing
- Server processes will send the `sink_handler` a message when a connection goes up/down.
- Adds ServerConnectionHandler behavior
- Adds aes_key and controller_id to SystemConfig message
- Added MonthlyPlanTransactionEvent for monthly plans[CH48073]

## Changed
- Update CustomerMeterBill for amount to be negative for deduction[CH48088]
- Update CustomerMeterBill for monthly plans and rename to CustomerMeterTransaction[CH48073]
- Removed Slim


### Fixes

- If a client is connected and the same client attempts to connect this will kill the previous connection and boot that client.
- Moved to encode_plain/decode_plain API of avrora and updated to upstream 0.18 [CH45912]