package main

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8"

	redisSyncFanoutQueue "github.com/zavitax/redis-sync-fanout-queue-go"
)

var testMessageContent = "test message content"
var testRoomId = "GO-ROOM-TEST"

var numRooms = 1000

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

func createQueueOptions() *redisSyncFanoutQueue.Options {
	result := &redisSyncFanoutQueue.Options{
		RedisOptions:   redisOptions,
		ClientTimeout:  time.Second * 15,
		RedisKeyPrefix: fmt.Sprintf("{test-redis-sync-fanout-queue}::%v", "test"),
		Sync:           true,
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

func sub_multi(sync bool) {
	var receivedMsgCount int64

	handler := func(ctx context.Context, msg *redisSyncFanoutQueue.Message) error {
		atomic.AddInt64(&receivedMsgCount, 1)

		strData := (*msg.Data).(string)

		if receivedMsgCount%10 == 0 {
			fmt.Printf("\rSUB: %s -> %d : %s    ", msg.MessageContext.Room, receivedMsgCount, strData)
		}

		if msg.Ack != nil {
			msg.Ack(ctx)
		}

		return nil
	}

	ejectHandler := func(ctx context.Context, client *redisSyncFanoutQueue.ClientHandle) error {
		fmt.Printf("\n\n**** Client ejected: %s ****\n\n", client.GetClientID())

		return nil
	}

	queueOptions := createQueueOptions()
	queueOptions.Sync = sync

	// setup(queueOptions)

	manager, _ := createRoomProxyManager(queueOptions)

	client1, _ := manager.AddClient(context.TODO(), &redisSyncFanoutQueue.ClientOptions{
		MessageHandler:       handler,
		ClientEjectedHandler: ejectHandler,
	})

	for i := 1; i <= numRooms; i++ {
		room := fmt.Sprintf("room-%d", i)

		client1.AddRoom(context.TODO(), room)
	}

	fmt.Printf("Waiting...\n")

	for {
		time.Sleep(time.Second * 1)
	}

	manager.RemoveClient(context.TODO(), client1)

	manager.Close()
}

func pub_multi(oob bool) {
	queueOptions := createQueueOptions()
	queueOptions.Sync = true

	manager, _ := createRoomProxyManager(queueOptions)

	for loop := 0; loop < 50; loop++ {
		for i := 1; i <= numRooms; i++ {
			room := fmt.Sprintf("room-%d", i)
			msg := room

			if oob {
				manager.SendOutOfBand(context.TODO(), room, msg)
			} else {
				manager.Send(context.TODO(), room, msg, 1)
			}

			if i%100 == 0 {
				fmt.Printf("Pub: %s -> %d\n", room, i)
			}
		}
	}

	manager.Close()
}

func peek() {
	manager, _ := createRoomProxyManager(createQueueOptions())
	defer manager.Close()

	if msgs, err := manager.Peek(context.TODO(), "room-1", 0, 10); err != nil {
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
	case "mpub":
		pub_multi(false)
	case "mpuboob":
		pub_multi(true)
	case "msub":
		sub_multi(true)
	case "msubasync":
		sub_multi(false)
	case "peek":
		peek()
	}
}
