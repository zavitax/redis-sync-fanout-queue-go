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
		RedisKeyPrefix: fmt.Sprintf("test-redis-sync-fanout-queue-room-proxy-mgr::%v", testId),
		Sync:           true,
	}

	return result
}

func createRoomProxyManager(options *redisSyncFanoutQueue.Options) (redisSyncFanoutQueue.RoomProxyManager, error) {
	roomMsgProxyOptions := &redisSyncFanoutQueue.RoomProxyManagerOptions{
		RedisQueueClientProvider: func(ctx context.Context, roomEjectedFunc redisSyncFanoutQueue.HandleRoomEjectedFunc) (redisSyncFanoutQueue.RedisQueueClient, error) {
			opt := *options

			opt.HandleRoomEjected = roomEjectedFunc

			setup(&opt)

			return redisSyncFanoutQueue.NewClient(context.TODO(), &opt)
		},
	}

	return redisSyncFanoutQueue.NewRoomProxyManager(context.TODO(), roomMsgProxyOptions)
}

func TestConnectDisconnect(t *testing.T) {
	manager, err := createRoomProxyManager(createQueueOptions("TestConnectDisconnect"))

	if err != nil {
		t.Error(err)
		return
	}

	manager.Close()
}

func TestRoomsJoinPart(t *testing.T) {
	manager, err := createRoomProxyManager(createQueueOptions("TestRoomsJoinPart"))

	if err != nil {
		t.Error(err)
		return
	}

	clientOptions := &redisSyncFanoutQueue.ClientOptions{
		MessageHandler: func(ctx context.Context, msg *redisSyncFanoutQueue.Message) error {
			return nil
		},
		ClientEjectedHandler: func(ctx context.Context, client *redisSyncFanoutQueue.ClientHandle) error {
			return nil
		},
	}

	clients := []*redisSyncFanoutQueue.ClientHandle{}

	for i := 0; i < 10; i++ {
		if client, err := manager.AddClient(context.TODO(), clientOptions); err != nil {
			t.Error(err)
		} else {
			clients = append(clients, client)
		}
	}

	for roomIndex := 0; roomIndex < len(clients); roomIndex++ {
		roomId := fmt.Sprintf("room-%d", roomIndex+1)

		for _, client := range clients {
			client.AddRoom(context.TODO(), roomId)
		}
	}

	for roomIndex := 0; roomIndex < len(clients); roomIndex++ {
		roomId := fmt.Sprintf("room-%d", roomIndex+1)

		for _, client := range clients {
			client.RemoveRoom(context.TODO(), roomId)
		}
	}

	for _, client := range clients {
		if err := manager.RemoveClient(context.TODO(), client); err != nil {
			t.Error(err)
		}
	}

	manager.Close()
}

func TestRoomsJoinPartSendReceive(t *testing.T) {
	manager, err := createRoomProxyManager(createQueueOptions("TestRoomsJoinPartSendReceive"))

	if err != nil {
		t.Error(err)
		return
	}

	client1_count := int32(0)
	client2_count := int32(0)

	client1, _ := manager.AddClient(context.TODO(), &redisSyncFanoutQueue.ClientOptions{
		MessageHandler: func(ctx context.Context, msg *redisSyncFanoutQueue.Message) error {
			atomic.AddInt32(&client1_count, 1)

			if msg.Ack != nil {
				msg.Ack(ctx)
			}

			return nil
		},
		ClientEjectedHandler: func(ctx context.Context, client *redisSyncFanoutQueue.ClientHandle) error {
			return nil
		},
	})

	client2, _ := manager.AddClient(context.TODO(), &redisSyncFanoutQueue.ClientOptions{
		MessageHandler: func(ctx context.Context, msg *redisSyncFanoutQueue.Message) error {
			atomic.AddInt32(&client2_count, 1)

			if msg.Ack != nil {
				msg.Ack(ctx)
			}

			return nil
		},
		ClientEjectedHandler: func(ctx context.Context, client *redisSyncFanoutQueue.ClientHandle) error {
			return nil
		},
	})

	client1.AddRoom(context.TODO(), "room1")
	client1.AddRoom(context.TODO(), "room2")

	client2.AddRoom(context.TODO(), "room2")

	manager.Send(context.TODO(), "room1", "test message for room 1", 1)
	manager.Send(context.TODO(), "room2", "test message for room 2", 1)

	time.Sleep(time.Second)

	if client1_count != 2 {
		t.Errorf("client1_count = %d, want %d", client1_count, 2)
	}

	if client2_count != 1 {
		t.Errorf("client2_count = %d, want %d", client2_count, 1)
	}

	manager.Close()
}
