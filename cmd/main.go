package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
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

func createQueueOptions() *redisSyncFanoutQueue.Options {
	result := &redisSyncFanoutQueue.Options{
		RedisOptions:   redisOptions,
		ClientTimeout:  time.Second * 15,
		RedisKeyPrefix: fmt.Sprintf("{test-redis-sync-fanout-queue}::%v", "test"),
		Sync:           true,
	}

	return result
}

func createQueueClient(options *redisSyncFanoutQueue.Options) (redisSyncFanoutQueue.RedisQueueClient, error) {
	return redisSyncFanoutQueue.NewClient(context.TODO(), options)
}

func test1() {
	fmt.Printf("test\n")

	client, _ := createQueueClient(createQueueOptions())

	//if err :=
	client.Subscribe(context.TODO(), testRoomId, func(ctx context.Context, msg *redisSyncFanoutQueue.Message) error {
		if msg.Data == nil {
			fmt.Printf("Received nil data\n")
			return nil
		}

		strData := (*msg.Data).(string)
		if strData != testMessageContent {
			fmt.Printf("Expected '%v' but received '%v'\n", testMessageContent, strData)
			return nil
		}

		fmt.Printf("Received: %v\n", strData)

		msg.Ack(ctx)

		return nil
	}) // err == nil {

	//time.Sleep(time.Second * 1)

	//client.Pong(context.TODO())

	for i := 0; i < 3; i++ {
		fmt.Printf("Send\n")
		client.Send(context.TODO(), testRoomId, testMessageContent, 1)
	}

	time.Sleep(time.Second * 3)

	metrics, _ := client.GetMetrics(context.TODO(), &redisSyncFanoutQueue.GetMetricsOptions{
		TopRoomsLimit: 10,
	})

	fmt.Printf("Metrics: %v\n", metrics)

	client.Unsubscribe(context.TODO(), testRoomId)
	fmt.Printf("Send should not receive\n")
	client.Send(context.TODO(), testRoomId, testMessageContent, 1)
	time.Sleep(time.Second * 3)

	fmt.Printf("Close\n")
	client.Close()
}

func sub() {
	client, _ := createQueueClient(createQueueOptions())

	var receivedMsgCount int64

	client.Subscribe(context.TODO(), testRoomId, func(ctx context.Context, msg *redisSyncFanoutQueue.Message) error {
		if msg.Data == nil {
			fmt.Printf("Received nil data\n")
			return nil
		}

		strData := (*msg.Data).(string)
		if strData != testMessageContent {
			fmt.Printf("Expected '%v' but received '%v'\n", testMessageContent, strData)
			return nil
		}

		//fmt.Printf("Received: %v\n", strData)
		atomic.AddInt64(&receivedMsgCount, 1)

		if receivedMsgCount%100 == 0 {
			fmt.Printf("\rSUB: %d", receivedMsgCount)
		}

		msg.Ack(ctx)

		return nil
	})

	fmt.Printf("Waiting...\n")

	for {
		time.Sleep(time.Second * 1)
	}
}

func pub_single(id string) {
	client, _ := createQueueClient(createQueueOptions())

	for i := 0; i < 50000; i++ {
		if i%1000 == 0 {
			fmt.Printf("Pub: %s -> %d\n", id, i)
		}
		client.Send(context.TODO(), testRoomId, testMessageContent, 1)
	}

	client.Close()
}

func pub() {
	var wg sync.WaitGroup

	for i := 1; i <= 10; i++ {
		wg.Add(1)

		go func(id string) {
			pub_single(id)
			wg.Done()
		}(fmt.Sprintf("PGRP-%d", i))
	}

	wg.Wait()
}

func main() {
	args := os.Args[1:]
	mode := "default"

	fmt.Printf("Args: %v", args)

	if len(args) > 0 {
		mode = args[0]
	}

	switch mode {
	case "pub":
		pub()
	case "sub":
		sub()
	default:
		test1()
	}
}
