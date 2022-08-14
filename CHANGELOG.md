# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]


## [0.0.2] - 2022-08-??
### Added
- Connection request and response

### Changed
- Client will only reset connection backoff if it completed the connection request-response. This prevents quarantined etc clients from spamming the server.


## [0.0.1] - 2022-05-??
### Added
- Initial commit of Sink connection functionality