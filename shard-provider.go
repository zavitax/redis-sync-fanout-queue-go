package redisSyncFanoutQueue

import (
	"context"
)

type RedisQueueShardsProvider interface {
	GetTotalShardsCount(ctx context.Context) (uint32, error)
	GetRoomShardId(ctx context.Context, room string) (uint32, error)
	GetShardOptions(ctx context.Context, shardId uint32) (*Options, error)
}
