package redisSyncFanoutQueue

import (
	"time"
)

type GetMetricsOptions struct {
	TopRoomsLimit	int
}

type RoomMetrics struct {
	Room string
	PendingMessagesBacklogLength int64
}

type Metrics struct {
	KnownRoomsCount int64

	SubscribedRoomsCount int
	ReceivedMessagesCount int64
	InvalidMessagesCount int64

	MinLatency time.Duration
	MaxLatency time.Duration
	AvgLatency time.Duration

	TopRooms []*RoomMetrics
	TopRoomsPendingMessagesBacklogLength int64
}
