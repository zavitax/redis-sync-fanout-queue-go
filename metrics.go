package redisSyncFanoutQueue

import (
	"time"
)

type Metrics struct {
	KnownRoomsCount int64

	SubscribedRoomsCount  int
	ReceivedMessagesCount int64
	InvalidMessagesCount  int64

	MinLatency time.Duration
	MaxLatency time.Duration
	AvgLatency time.Duration

	TopRooms                             []*RoomMetrics
	TopRoomsPendingMessagesBacklogLength int64
}
