package redisSyncFanoutQueue_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
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

func setup(options *redisSyncFanoutQueue.Options) {
	fmt.Printf("Setup test: %v\n", options.RedisKeyPrefix)
	redis := redis.NewClient(redisOptions)
	redis.Do(context.Background(), "FLUSHDB").Result()
	redis.Close()
}

func createQueueOptions(
	testId string,
) *redisSyncFanoutQueue.Options {
	result := &redisSyncFanoutQueue.Options{
		RedisOptions:   redisOptions,
		ClientTimeout:  time.Second * 15,
		RedisKeyPrefix: fmt.Sprintf("{test-redis-sync-fanout-queue}::%v", testId),
		Sync:           true,
	}

	return result
}

func createQueueClient(options *redisSyncFanoutQueue.Options) (redisSyncFanoutQueue.RedisQueueClient, error) {
	setup(options)

	return redisSyncFanoutQueue.NewClient(context.TODO(), options)
}

func TestConnectDisconnect(t *testing.T) {
	client, err := createQueueClient(createQueueOptions("TestConnectDisconnect"))

	if err != nil {
		t.Error(err)
		return
	}

	client.Close()
}

func TestSendReceive(t *testing.T) {
	var minReceivedMsgCount = int64(1)
	var receivedMsgCount int64

	options := createQueueOptions(
		"TestSendReceive",
	)

	client, err := createQueueClient(options)

	if err != nil {
		t.Error(err)
		return
	}

	err = client.Subscribe(context.TODO(), testRoomId, func(ctx context.Context, msg *redisSyncFanoutQueue.Message) error {
		if msg.Data == nil {
			t.Error("Received nil data")
			return nil
		}

		strData := (*msg.Data).(string)
		if strData != testMessageContent {
			t.Errorf("Expected '%v' but received '%v'", testMessageContent, strData)
			return nil
		}

		// fmt.Printf("Received: %v\n", strData)

		atomic.AddInt64(&receivedMsgCount, 1)

		msg.Ack(ctx)

		return nil
	})

	if err != nil {
		t.Error(err)
		return
	}

	client.Send(context.TODO(), testRoomId, testMessageContent, 1)

	for i := 0; i < 10 && receivedMsgCount < minReceivedMsgCount; i++ {
		time.Sleep(time.Second * 1)
	}

	client.Close()

	if receivedMsgCount < minReceivedMsgCount {
		t.Errorf("Expected %v receivedMsgCount but received %v", minReceivedMsgCount, receivedMsgCount)
	}
}

func TestGetMetrics(t *testing.T) {
	options := createQueueOptions("TestGetMetrics")

	client, err := createQueueClient(options)

	if err != nil {
		t.Error(err)
		return
	}

	client.Send(context.TODO(), testRoomId, testMessageContent, 1)

	getMetricsOptions := &redisSyncFanoutQueue.GetMetricsOptions{
		TopRoomsLimit: 10,
	}

	_, err = client.GetMetrics(context.TODO(), getMetricsOptions)

	if err != nil {
		t.Error(err)
	}

	client.Close()
}

func TestUnsubscribe(t *testing.T) {
	var exactReceivedMsgCount = int64(1)
	var receivedMsgCount int64

	options := createQueueOptions(
		"TestUnsubscribe",
	)

	client, err := createQueueClient(options)

	if err != nil {
		t.Error(err)
		return
	}

	err = client.Subscribe(context.TODO(), testRoomId, func(ctx context.Context, msg *redisSyncFanoutQueue.Message) error {
		if msg.Data == nil {
			t.Error("Received nil data")
			return nil
		}

		strData := (*msg.Data).(string)
		if strData != testMessageContent {
			t.Errorf("Expected '%v' but received '%v'", testMessageContent, strData)
			return nil
		}

		atomic.AddInt64(&receivedMsgCount, 1)

		msg.Ack(ctx)

		return nil
	})

	if err != nil {
		t.Error(err)
		return
	}

	client.Send(context.TODO(), testRoomId, testMessageContent, 1)
	time.Sleep(time.Second * 1)
	client.Unsubscribe(context.TODO(), testRoomId)
	client.Send(context.TODO(), testRoomId, testMessageContent, 1) // Should not receive this message

	for i := 0; i < 3 && receivedMsgCount < exactReceivedMsgCount+1; i++ {
		time.Sleep(time.Second * 1)
	}

	client.Close()

	if receivedMsgCount != exactReceivedMsgCount {
		t.Errorf("Expected %v receivedMsgCount but received %v", exactReceivedMsgCount, receivedMsgCount)
	}
}

func TestMultipleMsgs(t *testing.T) {
	var exactReceivedMsgCount = int64(5)
	var receivedMsgCount int64

	options := createQueueOptions(
		"TestMultipleMsgs",
	)

	client, err := createQueueClient(options)

	if err != nil {
		t.Error(err)
		return
	}

	err = client.Subscribe(context.TODO(), testRoomId, func(ctx context.Context, msg *redisSyncFanoutQueue.Message) error {
		if msg.Data == nil {
			t.Error("Received nil data")
			return nil
		}

		strData := (*msg.Data).(string)
		if strData != testMessageContent {
			t.Errorf("Expected '%v' but received '%v'", testMessageContent, strData)
			return nil
		}

		atomic.AddInt64(&receivedMsgCount, 1)

		msg.Ack(ctx)

		return nil
	})

	if err != nil {
		t.Error(err)
		return
	}

	client.Send(context.TODO(), testRoomId, testMessageContent, 1)
	client.Send(context.TODO(), testRoomId, testMessageContent, 1)
	client.Send(context.TODO(), testRoomId, testMessageContent, 1)
	client.Send(context.TODO(), testRoomId, testMessageContent, 1)
	client.Send(context.TODO(), testRoomId, testMessageContent, 1)

	for i := 0; i < 10 && receivedMsgCount < exactReceivedMsgCount; i++ {
		time.Sleep(time.Second * 1)
	}

	client.Close()

	if receivedMsgCount != exactReceivedMsgCount {
		t.Errorf("Expected %v receivedMsgCount but received %v", exactReceivedMsgCount, receivedMsgCount)
	}
}
