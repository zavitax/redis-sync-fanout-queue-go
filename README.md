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
package main

import (
	"github.com/go-redis/redis/v8"
	"context"
	"time"
	"github.com/zavitax/redis-sync-fanout-queue-go"
	"fmt"
)

var testMessageContent = "test message content"
var testRoomId = "GO-ROOM-TEST"

var redisOptions = &redis.Options{
	Addr: "127.0.0.1:6379",
	Password: "",
	DB: 0,
};

func createQueueOptions (
	testId string,
) (*redisSyncFanoutQueue.Options) {
	result := &redisSyncFanoutQueue.Options{
		RedisOptions: redisOptions,
		ClientTimeout: time.Second * 15,
		RedisKeyPrefix: fmt.Sprintf("{test-redis-sync-fanout-queue}::%v", testId),
		Sync: true,
	}

	return result
}

func createQueueClient (options *redisSyncFanoutQueue.Options) (redisSyncFanoutQueue.RedisQueueClient, error) {
	return redisSyncFanoutQueue.NewClient(context.TODO(), options);
}

func Main () {
	var minReceivedMsgCount = int64(1)
	var receivedMsgCount int64

	options := createQueueOptions(
		"TestSendReceive",
	)

	client, err := createQueueClient(options)

	if (err != nil) { return }

	err = client.Subscribe(context.TODO(), testRoomId, func (ctx context.Context, msg *redisSyncFanoutQueue.Message) (error) {
		fmt.Printf("Received: %v", msg.Data)

		return nil
	})

	if (err != nil) { return }

	client.Send(context.TODO(), testRoomId, testMessageContent, 1);

	for i := 0; i < 10 && receivedMsgCount < minReceivedMsgCount; i++ {
		time.Sleep(time.Second * 1)
	}

	client.Close()
}
```
