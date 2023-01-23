package redisSyncFanoutQueue

import (
	"time"
)

type Message struct {
	Data           *interface{}
	AckToken       *string
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
	AckToken  *string     `json:"a"`
}
