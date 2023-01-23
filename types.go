package redisSyncFanoutQueue

import (
	"time"
)

type Message struct {
	Data           *interface{}
	AckToken       *string
	Room           string
	MessageContext struct {
		Timestamp time.Time
		Producer  string
		Latency   time.Duration
	}
}

type redisQueueWireMessage struct {
	Timestamp int64       `json:"t"`
	Producer  string      `json:"c"`
	Room      string      `json:"r"`
	Data      interface{} `json:"d"`
	AckToken  *string     `json:"a"`
}
