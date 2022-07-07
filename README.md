# redis-sync-fanout-queue-go

## What is it?

Priority queue with synchronous fanout delivery for each room based on Redis.

This queue is special by several key properties:

1. It delivers each message sent to a room to _all_ subscribers of that room.
2. It does not deliver the next message until _all_ subscribers of the room ACKnowledge the last message.
3. It is based entirely on Redis primitives.
4. Out-of-band messages are also available. They are immediately delivered to all subscribers with no regard to ACKs.
5. Subscribers can be `Sync = true` (blocking, thus requiring an ACK) or `Sync = false` (non-blocking, thus requiring an ACK).
6. Supports sharded Redis clusters out-of-the-box

This allows building distributed systems where edges process messages in a coordinated lock-step with each other.

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

### Simple use example

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

	defer client.Close()

	err = client.Subscribe(context.TODO(), testRoomId, func (ctx context.Context, msg *redisSyncFanoutQueue.Message) (error) {
		fmt.Printf("Received: %v", msg.Data)

		msg.Ack(ctx)

		return nil
	})

	if (err != nil) { return }

	client.Send(context.TODO(), testRoomId, testMessageContent, 1);

	for i := 0; i < 10 && receivedMsgCount < minReceivedMsgCount; i++ {
		time.Sleep(time.Second * 1)
	}

	err = client.Unsubscribe(context.TODO(), testRoomId)

	if (err != nil) { return }
}
```

### Sharded use example

With a simple change (replacing `NewClient` with `NewShardedClient` and passing an instance of `RedisQueueShardsProvider` created by a call to `NewRedisClusterShardProvider`) you are fully set-up to utilize a Redis cluster.

This is based on appending a `::{slot-SHARD_ID}` suffix to `Options.RedisKeyPrefix`, telling Redis "hey, assign a slot based on the "{slot-SHARD_ID}" string".

Shard IDs are calculated on a per-room basis.

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
		RedisKeyPrefix: fmt.Sprintf("test-redis-sync-fanout-queue-sharded::%v", testId),
		Sync: true,
	}

	return result
}

func createQueueClient (options *redisSyncFanoutQueue.Options) (redisSyncFanoutQueue.RedisQueueClient, error) {
	if shardProvider, err := redisSyncFanoutQueue.NewRedisClusterShardProvider(
		context.TODO(),
		options,
		100,
	); err != nil {
		return nil, err
	} else {
		return redisSyncFanoutQueue.NewShardedClient(context.TODO(), shardProvider)
	}
}

func Main () {
	var minReceivedMsgCount = int64(1)
	var receivedMsgCount int64

	options := createQueueOptions(
		"TestSendReceive",
	)

	client, err := createQueueClient(options)

	if (err != nil) { return }

	defer client.Close()

	err = client.Subscribe(context.TODO(), testRoomId, func (ctx context.Context, msg *redisSyncFanoutQueue.Message) (error) {
		fmt.Printf("Received: %v", msg.Data)

		msg.Ack(ctx)

		return nil
	})

	if (err != nil) { return }

	client.Send(context.TODO(), testRoomId, testMessageContent, 1);

	for i := 0; i < 10 && receivedMsgCount < minReceivedMsgCount; i++ {
		time.Sleep(time.Second * 1)
	}

	err = client.Unsubscribe(context.TODO(), testRoomId)

	if (err != nil) { return }
}
```

### Clients management layer example

The clients management layer manages a list of rooms & clients locally, so ACKs are sent to redis only when all the local clients have ACKnowledged a message.

This reduces stress on the redis server by performing as much as possible in-process.

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
		RedisKeyPrefix: fmt.Sprintf("{test-redis-sync-fanout-queue-with-proxy-layer}::%v", testId),
		Sync: true,
	}

	return result
}

func createRoomProxyManager(options *redisSyncFanoutQueue.Options) (redisSyncFanoutQueue.RoomProxyManager, error) {
	roomMsgProxyOptions := &redisSyncFanoutQueue.RoomProxyManagerOptions{
		RedisQueueClientProvider: func(ctx context.Context, roomEjectedFunc redisSyncFanoutQueue.HandleRoomEjectedFunc) (redisSyncFanoutQueue.RedisQueueClient, error) {
			opt := *options

			opt.HandleRoomEjected = roomEjectedFunc

			return redisSyncFanoutQueue.NewClient(context.TODO(), &opt)
		},
	}

	return redisSyncFanoutQueue.NewRoomProxyManager(context.TODO(), roomMsgProxyOptions)
}

func Main () {
	manager, err := createRoomProxyManager(createQueueOptions("TestRoomsJoinPartSendReceive"))

	if err != nil {
		t.Error(err)
		return
	}

	clientOptions := &redisSyncFanoutQueue.ClientOptions{
		MessageHandler: func(ctx context.Context, msg *redisSyncFanoutQueue.Message) error {
			fmt.Printf("Received message: %v\n", msg)

			if msg.Ack != nil {
				msg.Ack(ctx)
			}

			return nil
		},
		ClientEjectedHandler: func(ctx context.Context, client *redisSyncFanoutQueue.ClientHandle) error {
			return nil
		},
	}

	client1, _ := manager.AddClient(context.TODO(), clientOptions)
	client2, _ := manager.AddClient(context.TODO(), clientOptions)

	client1.AddRoom(context.TODO(), "room1")
	client1.AddRoom(context.TODO(), "room2")

	client2.AddRoom(context.TODO(), "room2")

	manager.Send(context.TODO(), "room1", "test message for room 1", 1)
	manager.Send(context.TODO(), "room2", "test message for room 2", 1)

	time.Sleep(time.Second)

	manager.Close()
}
```
