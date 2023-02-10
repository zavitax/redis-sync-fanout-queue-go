package redisSyncFanoutQueue

import "time"

type RoomState struct {
	Room                         string
	KnownClients                 map[string]time.Time
	AcknowledgedClients          map[string]time.Time
	PendingMessagesBacklogLength int64
}
