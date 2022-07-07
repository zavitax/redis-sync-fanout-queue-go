package redisSyncFanoutQueue

import (
	"context"
	"fmt"
	"hash/crc32"
)

type redisClusterShardProvider struct {
	options          *Options
	totalShardsCount uint32
	crc32Table       *crc32.Table
}

func NewRedisClusterShardProvider(ctx context.Context, options *Options, totalShardsCount uint32) (RedisQueueShardsProvider, error) {
	if err := options.Validate(); err != nil {
		return nil, err
	}

	if totalShardsCount < 1 || totalShardsCount > 16384 {
		return nil, fmt.Errorf("invalid number of shards: %d", totalShardsCount)
	}

	c := &redisClusterShardProvider{}

	c.totalShardsCount = totalShardsCount
	c.options = options
	c.crc32Table = crc32.MakeTable(0xD5828281)

	return c, nil
}

func (c *redisClusterShardProvider) GetTotalShardsCount(ctx context.Context) (uint32, error) {
	return c.totalShardsCount, nil
}

func (c *redisClusterShardProvider) GetRoomShardId(ctx context.Context, room string) (uint32, error) {
	checksum := crc32.Checksum([]byte(room), c.crc32Table)

	return checksum % c.totalShardsCount, nil
}

func (c *redisClusterShardProvider) GetShardOptions(ctx context.Context, shardId uint32) (*Options, error) {
	if shardId >= c.totalShardsCount {
		return nil, fmt.Errorf("shardId %d is out of range", shardId)
	}

	opt := *c.options

	opt.RedisKeyPrefix = fmt.Sprintf("%s::{slot-%d}", c.options.RedisKeyPrefix, shardId)

	return &opt, nil
}
