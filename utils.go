package redisSyncFanoutQueue

import (
	"encoding/json"
	"time"
)

func currentTimestamp() int64 {
	return time.Now().UTC().UnixMilli()
}

func parseMsgData(msgData string) (*Message, *redisQueueWireMessage, error) {
	var packet redisQueueWireMessage

	if err := json.Unmarshal([]byte(msgData), &packet); err != nil {
		return nil, nil, err
	}

	var msg Message

	msg.Data = &packet.Data
	msg.AckToken = packet.AckToken
	msg.Room = packet.Room
	msg.MessageContext.Timestamp = time.UnixMilli(packet.Timestamp).UTC()
	msg.MessageContext.Producer = packet.Producer
	msg.MessageContext.Latency = time.Now().UTC().Sub(msg.MessageContext.Timestamp)

	return &msg, &packet, nil
}
