# redis-sync-fanout-queue-go

## What is it?

Priority queue with synchronous fanout delivery for each room based on Redis.

This queue is special by several key properties:

1. It delivers each message sent to a room to _all_ subscribers of that room.
2. It does not deliver the next message until _all_ subscribers of the room ACKnowledge the last message.
3. It is based entirely on Redis primitives.
4. Out-of-band messages are also available. They are immediately delivered to all subscribers with no regard to ACKs.

This allows building distributed systems where edges process messages in a coordinated lock-step with each other.

## Queue guarantees

### Synchronized Fanout

All clients must ACKnowledge processing of a message before any other client can see the next message, unless the message is sent Out-of-band.

### At most once delivery

There are no message redelivery attempts built in. Either you get it or you do not.

(*) In case communication breaks, client must stop sending Ping() requests (preferably send an Unsubscribe() request), obtain a new Client ID, and Subscribe() after communication has been re-established.

### High performance, low memory footprint

Most of the heavy lifting is done in Redis.

## Infrastructure

The library leverages `ioredis` for communication with the Redis server.

## Usage

### Publishing messages

```go
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/go-redis/redis/v8"

	redisSyncFanoutQueue "github.com/zavitax/redis-sync-fanout-queue-go"
)

var testMessageContent = "test message content"
var testRoomId = "GO-ROOM-TEST"

var redisOptions = &redis.Options{
	Addr:     "127.0.0.1:6379",
	Password: "",
	DB:       0,
}

func createApiOptions() *redisSyncFanoutQueue.ApiOptions {
	result := &redisSyncFanoutQueue.ApiOptions{
		RedisOptions:   redisOptions,
		ClientTimeout:  time.Second * 15,
		RedisKeyPrefix: fmt.Sprintf("{test-redis-sync-fanout-queue}::%v", "test"),
	}

	return result
}

func createApiClient(options *redisSyncFanoutQueue.ApiOptions) (redisSyncFanoutQueue.RedisQueueApiClient, error) {
	return redisSyncFanoutQueue.NewApiClient(context.TODO(), options)
}

func pub() {
	doneC := make(chan os.Signal, 1)
	signal.Notify(doneC)

	client, _ := createApiClient(createApiOptions())

	i := 0
	ticker := time.NewTicker(time.Second)
	done := false
	for !done {
		select {
		case <-doneC:
			done = true
		case <-ticker.C:
			i++

			client.Send(context.TODO(), "MSG_PRODUCER_ID", testRoomId, testMessageContent, 1)

			fmt.Printf("Send: %v\r", i)
		}
	}
	ticker.Stop()

	client.Close()
}

func main() {
	pub()
}
```

### Subscribing and maintaining a client connection

```go
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/go-redis/redis/v8"

	redisSyncFanoutQueue "github.com/zavitax/redis-sync-fanout-queue-go"
)

var testMessageContent = "test message content"
var testRoomId = "GO-ROOM-TEST"

var redisOptions = &redis.Options{
	Addr:     "127.0.0.1:6379",
	Password: "",
	DB:       0,
}

func createApiOptions() *redisSyncFanoutQueue.ApiOptions {
	result := &redisSyncFanoutQueue.ApiOptions{
		RedisOptions:   redisOptions,
		ClientTimeout:  time.Second * 15,
		RedisKeyPrefix: fmt.Sprintf("{test-redis-sync-fanout-queue}::%v", "test"),
	}

	return result
}

func createApiClient(options *redisSyncFanoutQueue.ApiOptions) (redisSyncFanoutQueue.RedisQueueApiClient, error) {
	return redisSyncFanoutQueue.NewApiClient(context.TODO(), options)
}

func sub(clientsCount int) {
	doneC := make(chan os.Signal, 1)
	signal.Notify(doneC)

	client, _ := createApiClient(createApiOptions())

	clientIds := []string{}
	for i := 0; i < clientsCount; i++ {
		clientId, _ := client.CreateClientID(context.Background())
		client.Subscribe(context.TODO(), clientId, testRoomId)
		clientIds = append(clientIds, clientId)
	}

	ticker := time.NewTicker(time.Second)
	done := false
	for !done {
		select {
		case <-doneC:
			done = true
		case <-ticker.C:
			for _, clientId := range clientIds {
				// Must call periodically for each Client ID & Room ID comibnation to keep
				// Client ID alive.
				client.Ping(context.Background(), clientId)
			}

			metrics, _ := client.GetMetrics(context.TODO(), &redisSyncFanoutQueue.GetApiMetricsOptions{
				TopRoomsLimit: 10,
			})

			fmt.Printf("Metrics: %v\n", metrics)
		}
	}
	ticker.Stop()

	for _, clientId := range clientIds {
		client.Unsubscribe(context.Background(), clientId, testRoomId)
	}

	client.Close()
}

func main() {
	sub(1)
}
```

### Delivering and ACKnowledging messages (worker)

```go
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/go-redis/redis/v8"

	redisSyncFanoutQueue "github.com/zavitax/redis-sync-fanout-queue-go"
)

var testMessageContent = "test message content"
var testRoomId = "GO-ROOM-TEST"

var redisOptions = &redis.Options{
	Addr:     "127.0.0.1:6379",
	Password: "",
	DB:       0,
}

func createApiOptions() *redisSyncFanoutQueue.ApiOptions {
	result := &redisSyncFanoutQueue.ApiOptions{
		RedisOptions:   redisOptions,
		ClientTimeout:  time.Second * 15,
		RedisKeyPrefix: fmt.Sprintf("{test-redis-sync-fanout-queue}::%v", "test"),
	}

	return result
}

func createWorkerOptions() *redisSyncFanoutQueue.WorkerOptions {
	result := &redisSyncFanoutQueue.WorkerOptions{
		RedisOptions:   redisOptions,
		RedisKeyPrefix: fmt.Sprintf("{test-redis-sync-fanout-queue}::%v", "test"),
	}

	return result
}

func createApiClient(options *redisSyncFanoutQueue.ApiOptions) (redisSyncFanoutQueue.RedisQueueApiClient, error) {
	return redisSyncFanoutQueue.NewApiClient(context.TODO(), options)
}

func worker() {
	doneC := make(chan os.Signal, 1)
	signal.Notify(doneC)

	// Used for ACKs
	client, _ := createApiClient(createApiOptions())

	wo := createWorkerOptions()
	wo.HandleRoomClientTimeout = func(ctx context.Context, clientId *string, roomId *string) error {
		return nil
	}
	wo.HandleMessage = func(ctx context.Context, clientId *string, msg *redisSyncFanoutQueue.Message) error {
		if msg.Data == nil {
			fmt.Printf("Received nil data\n")
			return nil
		}

		strData := (*msg.Data).(string)
		if strData != testMessageContent {
			fmt.Printf("Expected '%v' but received '%v'\n", testMessageContent, strData)
			return nil
		}

		fmt.Printf("Received: client[%s] room[%s]: %v\n", *clientId, msg.Room, strData)

		if msg.AckToken != nil {
			//time.Sleep(time.Second * 3)
			// This should be called by the actual client, when it's done processing the message
			client.AckMessage(ctx, *clientId, msg.AckToken)
		}

		return nil
	}

	worker, _ := createWorkerClient(wo)

	ticker := time.NewTicker(time.Second)
	done := false
	for !done {
		select {
		case <-doneC:
			done = true
		case <-ticker.C:
			metrics, _ := worker.GetMetrics(context.TODO(), &redisSyncFanoutQueue.GetWorkerMetricsOptions{})

			fmt.Printf("Metrics: %v\n", metrics)
		}
	}
	ticker.Stop()

	worker.Close()
	client.Close()
}

func main() {
	worker()
}
```

### Peeking at room's queue head

```go
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/go-redis/redis/v8"

	redisSyncFanoutQueue "github.com/zavitax/redis-sync-fanout-queue-go"
)

var testMessageContent = "test message content"
var testRoomId = "GO-ROOM-TEST"

var redisOptions = &redis.Options{
	Addr:     "127.0.0.1:6379",
	Password: "",
	DB:       0,
}

func createApiOptions() *redisSyncFanoutQueue.ApiOptions {
	result := &redisSyncFanoutQueue.ApiOptions{
		RedisOptions:   redisOptions,
		ClientTimeout:  time.Second * 15,
		RedisKeyPrefix: fmt.Sprintf("{test-redis-sync-fanout-queue}::%v", "test"),
	}

	return result
}

func createApiClient(options *redisSyncFanoutQueue.ApiOptions) (redisSyncFanoutQueue.RedisQueueApiClient, error) {
	return redisSyncFanoutQueue.NewApiClient(context.TODO(), options)
}

func peek() {
	doneC := make(chan os.Signal, 1)
	signal.Notify(doneC)

	client, _ := createApiClient(createApiOptions())

	ticker := time.NewTicker(time.Second)
	done := false
	for !done {
		select {
		case <-doneC:
			done = true
		case <-ticker.C:
			if msgs, err := client.Peek(context.TODO(), testRoomId, 0, 10); err != nil {
				panic(err)
			} else {
				for index, msg := range msgs {
					strData := (*msg.Data).(string)

					fmt.Printf("Peek: %d: %v\n", index, strData)
				}
			}
		}
	}
	ticker.Stop()

	client.Close()
}

func main() {
	peek()
}
```
