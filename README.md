# redis-sync-fanout-queue-go

## What is it?

Priority queue with synchronous fanout delivery for each room based on Redis

## Queue guarantees

### Low latency delivery

Delivery is based on Redis PUBSUB. It is possible to reach very low latencies.

### Synchronized Fanout

All synchronous clients must ACKnowledge processing of a message before any other client can see the next message.

### At most once delivery

There are no message redelivery attempts built in. Either you get it or you do not.

### High performance, low memory footprint

Most of the heavy lifting is done in Redis.

## Infrastructure

The library leverages `ioredis` for communication with the Redis server.

## Usage

```go
TBD
```
