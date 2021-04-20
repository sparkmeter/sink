# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]
### Added
- Tracking of uptime/downtime for clients [CH49119]
- Option to exclude topics from return of a subscription queue [CH42952]
- Option to set a limit on the number records returned for a subscription queue [CH42952]
- Nack thresholds for event type ids [CH42952]


## [0.7.0] - 2021-04-13
### Fixed
- Bug in keepalive for Sink Servers and Clients

### Changed
- Timestamp is now part of event logs and protocol

## [0.6.0] - 2021-04-07
### Added
- Adds NACKs [CH47017]

### Changed
- ACKs no longer have extra info at the end


## [0.5.0] - 2021-04-03
### Added
- Sink Client will send a ping if connection is quiet for `keepalive_interval`. [CH47748]
- Sink Client and Server will disconnect if connection is exceeds keepalive cutoff threshold. [CH47748]
- Added `check_dupe` method to Ecto EventLogs [CH47016]


## [0.4.0] - 2021-03-28
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

### Fixed
- If a client is connected and the same client attempts to connect this will kill the previous connection and boot that client.
- Moved to encode_plain/decode_plain API of avrora and updated to upstream 0.18 [CH45912]
