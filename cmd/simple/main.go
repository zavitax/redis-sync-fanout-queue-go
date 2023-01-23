package main

import (
	"context"
	"fmt"
	"os"
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

func createWorkerClient(options *redisSyncFanoutQueue.WorkerOptions) (redisSyncFanoutQueue.RedisQueueWorkerClient, error) {
	return redisSyncFanoutQueue.NewWorkerClient(context.TODO(), options)
}

func pubsub() {
	fmt.Printf("test\n")

	client, _ := createApiClient(createApiOptions())
	clientId1, _ := client.CreateClientID(context.Background())
	clientId2, _ := client.CreateClientID(context.Background())

	fmt.Println("Clients: ", clientId1, clientId2)

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
			client.AckMessage(ctx, *clientId, msg.AckToken)
		}

		return nil
	}

	worker, _ := createWorkerClient(wo)

	client.Subscribe(context.TODO(), clientId1, testRoomId)
	client.Subscribe(context.TODO(), clientId2, testRoomId)

	//time.Sleep(time.Second * 1)

	//client.Pong(context.TODO())

	for i := 0; i < 3; i++ {
		fmt.Printf("Send\n")
		client.Send(context.TODO(), "test", testRoomId, testMessageContent, 1)
	}

	fmt.Println("Wait")
	time.Sleep(time.Second * 5)

	metrics, _ := client.GetMetrics(context.TODO(), &redisSyncFanoutQueue.GetApiMetricsOptions{
		TopRoomsLimit: 10,
	})

	fmt.Printf("Metrics: %v\n", metrics)

	client.Unsubscribe(context.TODO(), clientId1, testRoomId)
	fmt.Printf("Send should not receive\n")
	client.Send(context.TODO(), "test", testRoomId, testMessageContent, 1)
	time.Sleep(time.Second * 30)

	fmt.Printf("Close\n")

	client.Close()
	worker.Close()
}

func peek() {
	client, _ := createApiClient(createApiOptions())
	defer client.Close()

	// clientId, _ := client.CreateClientID(context.Background())

	if msgs, err := client.Peek(context.TODO(), "room-1", 0, 10); err != nil {
		panic(err)
	} else {
		for index, msg := range msgs {
			strData := (*msg.Data).(string)

			fmt.Printf("Peek: %d: %v\n", index, strData)
		}
	}
}

func main() {
	args := os.Args[1:]
	mode := "default"

	fmt.Printf("Args: %v", args)

	if len(args) > 0 {
		mode = args[0]
	}

	switch mode {
	case "pubsub":
		pubsub()
	case "peek":
		peek()
	default:
		pubsub()
	}
}
