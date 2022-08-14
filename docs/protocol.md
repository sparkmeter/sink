# Protocol Description

This document describes the Sink protocol. It is a work in progress.

## Connection Request

When a client connects to a server it sends a connection request to tell the server several
attributes about itself and what it expects from the server. This is to ensure that the client and server are compatible and are who each other expects.

The attributes are:
* protocol version - the version of Sink the client is requesting to use
* client_instantiated_at - timestamp of when the client was created
* server_instantiated_at - timestamp of when the client thinks the server was created
* application_version

The instantiated ats are intended to prevent devices who may authenticate fine, but have different histories than the other expects. For example, if a device used to connect to a staging environment also has somehow has credentials in production or if a device was wiped and no longer has the same history the server expects.

## Connection Response

The server responds to a client's connection with one of the following responses:

### Connected

The server agrees that the client is who the server expects and the server is who the client expected.

### Hello New Client

The server has accepted the new clien'ts connection and is telling the client its `instantiated_at` value.

### Mismatched Client

The client's instantiated_at was not what the server expected. The connection will close.

### Mismatched Server

The server's instantiated_at was not what the client expected. The connection will close.

### Quarantined

The client has been quarantined and the connection will close. The client may retry at a later point.

### Unsupported Protocol Version

The client requested to use a protocol version the server does not support. The connection will close.

### Unsupported Application Version

The client is running an outdated or otherwise unsupported version the server does not support. The connection will close. The client should update and then try again.

## PUBLISH

Send a single event.

Requires an ACK or NACK response.

## ACK

Acknowledge a single event.

An ACK means everything was received and processed as expected and the subscription can consider the event delivered.

## NACK

Report a problem with a single event.

A NACK should be considered to be an exceptional state and contains information about why the event was NACK'd. There is a field for a "machine message", which is meant to be a machine readable binary message, like a http status,  determined by the implementer. The purpose is to have known error codes which might have a recovery path (ex: upgrading a device's firmware) There is also a field for a "human message", which is meant to be a human friendly description of the error and may be blank if bandwidth usage is a concern.

## PING

Request a pong from a peer.

### PONG

Respond to a pong from a peer.