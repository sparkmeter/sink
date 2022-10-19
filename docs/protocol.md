# Protocol Description

This document describes the Sink protocol. It is a work in progress.

## Connection Request

When a client connects to a server it sends a connection request to tell the 
server some attributes about itself. This is to ensure that the client and 
server are compatible and are who each other expects.

The attributes are:
* protocol version - the version of Sink the client is requesting to use
* instance_id - the instance_id of the clients last connection if there has been one
* application_version - the application version running on the client

The instance_id is intended to prevent devices who may authenticate fine, but 
had been connected to different instances before. For example, if a device used 
to connect to a staging environment also has somehow has credentials in production 
or if a device was wiped and no longer has the same history the server expects.

## Connection Response

The server responds to a client's connection with one of the following responses:

### Connected

The server agrees that the client is who the server expects and the server is 
who the client expected.

### Hello New Client

The server has accepted the new client's connection and is telling the client 
the `instance_id` value.

### Instance ID Mismatch

The client's instance_id was not what the server expected. The connection will close.

### Quarantined

The client has been quarantined and the connection will close. The client may 
retry at a later point.

### Unsupported Protocol Version

The client requested to use a protocol version the server does not support. The 
connection will close.

### Unsupported Application Version

The client is running an outdated or otherwise unsupported version the server 
does not support. The connection will close. The client should update and then 
try again.

## PUBLISH

Send a single event.

Triggers an ACK or NACK response on the peer.

## ACK

Acknowledge a single event.

An ACK means an event was received and processed as expected and the subscription 
can consider the event delivered.

## NACK

Report a problem with a single event.

A NACK should be considered to be an exceptional state and contains information 
about why the event was NACK'd. There is a field for a "machine message", which 
is meant to be a machine readable binary message, like a http status, determined 
by the implementer. The purpose is to have known error codes which might have a 
recovery path (ex: upgrading a device's firmware). There is also a field for a 
"human message", which is meant to be a human friendly description of the error 
and may be blank if bandwidth usage is a concern.

## PING

Request a pong from a peer.

### PONG

Respond to a pong from a peer.