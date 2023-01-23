package redisSyncFanoutQueue

import (
	"context"
	"time"
)

type AckMessageFunc func(ctx context.Context) error

type Message struct {
	Data           *interface{}
	Ack            AckMessageFunc
	MessageContext struct {
		Timestamp time.Time
		Producer  string
		Sequence  int64
		Latency   time.Duration
		Room      string
	}
}

type redisQueueWireMessage struct {
	Timestamp int64       `json:"t"`
	Producer  string      `json:"c"`
	Room      string      `json:"r"`
	Data      interface{} `json:"d"`
}
