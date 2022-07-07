package redisSyncFanoutQueue

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

type redisShardedQueue struct {
	mu            sync.RWMutex
	shardProvider RedisQueueShardsProvider
	shards        []RedisQueueClient
}

func NewShardedClient(ctx context.Context, shardProvider RedisQueueShardsProvider) (RedisQueueClient, error) {
	if shardProvider == nil {
		return nil, fmt.Errorf("shardProvider is nil")
	}

	var c = &redisShardedQueue{}

	c.shardProvider = shardProvider

	var lastError error

	if totalShardsCount, err := c.shardProvider.GetTotalShardsCount(ctx); err != nil {
		return nil, err
	} else {
		for shardId := uint32(0); shardId < totalShardsCount; shardId++ {
			if options, err := c.shardProvider.GetShardOptions(ctx, shardId); err != nil {
				lastError = err

				break
			} else {
				if client, err := NewClient(ctx, options); err != nil {
					lastError = err

					break
				} else {
					c.shards = append(c.shards, client)
				}
			}
		}
	}

	if lastError != nil {
		// Close all clients we've opened so far
		for _, client := range c.shards {
			client.Close()
		}

		return nil, lastError
	}

	return c, nil
}

func (c *redisShardedQueue) Subscribe(ctx context.Context, room string, msgHandlerFunc HandleMessageFunc) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if shardId, err := c.shardProvider.GetRoomShardId(ctx, room); err != nil {
		return err
	} else {
		client := c.shards[shardId]

		return client.Subscribe(ctx, room, msgHandlerFunc)
	}
}

func (c *redisShardedQueue) Unsubscribe(ctx context.Context, room string) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if shardId, err := c.shardProvider.GetRoomShardId(ctx, room); err != nil {
		return err
	} else {
		client := c.shards[shardId]

		return client.Unsubscribe(ctx, room)
	}
}

func (c *redisShardedQueue) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, client := range c.shards {
		client.Close()
	}

	c.shards = []RedisQueueClient{}

	return nil
}

func (c *redisShardedQueue) GetMetrics(ctx context.Context, options *GetMetricsOptions) (*Metrics, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := &Metrics{}

	for index, client := range c.shards {
		if met, err := client.GetMetrics(ctx, options); err != nil {
			return nil, err
		} else {
			result.KnownRoomsCount += met.KnownRoomsCount
			result.SubscribedRoomsCount += met.SubscribedRoomsCount
			result.ReceivedMessagesCount += met.ReceivedMessagesCount
			result.InvalidMessagesCount += met.InvalidMessagesCount

			if index > 0 {
				result.MinLatency = minDuration(result.MinLatency, met.MinLatency)
				result.MaxLatency = maxDuration(result.MinLatency, met.MinLatency)
				result.AvgLatency = avgDuration(result.MinLatency, met.MinLatency)
			} else {
				result.MinLatency = met.MinLatency
				result.MaxLatency = met.MinLatency
				result.AvgLatency = met.MinLatency
			}

			result.TopRoomsPendingMessagesBacklogLength += met.TopRoomsPendingMessagesBacklogLength

			for _, topRoom := range met.TopRooms {
				result.TopRooms = append(result.TopRooms, topRoom)
			}

			sort.Slice(result.TopRooms, func(i, j int) bool {
				return result.TopRooms[i].PendingMessagesBacklogLength > result.TopRooms[j].PendingMessagesBacklogLength
			})

			if len(result.TopRooms) > options.TopRoomsLimit {
				result.TopRooms = result.TopRooms[0:options.TopRoomsLimit]
			}
		}
	}

	return result, nil
}

func (c *redisShardedQueue) Send(ctx context.Context, room string, data interface{}, priority int) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if shardId, err := c.shardProvider.GetRoomShardId(ctx, room); err != nil {
		return err
	} else {
		client := c.shards[shardId]

		return client.Send(ctx, room, data, priority)
	}
}

func (c *redisShardedQueue) SendOutOfBand(ctx context.Context, room string, data interface{}) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if shardId, err := c.shardProvider.GetRoomShardId(ctx, room); err != nil {
		return err
	} else {
		client := c.shards[shardId]

		return client.SendOutOfBand(ctx, room, data)
	}
}

func (c *redisShardedQueue) Peek(ctx context.Context, room string, offset int, limit int) ([]*Message, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if shardId, err := c.shardProvider.GetRoomShardId(ctx, room); err != nil {
		return nil, err
	} else {
		client := c.shards[shardId]

		return client.Peek(ctx, room, offset, limit)
	}
}

func minDuration(a time.Duration, b time.Duration) time.Duration {
	if a < b {
		return a
	} else {
		return b
	}
}

func maxDuration(a time.Duration, b time.Duration) time.Duration {
	if a > b {
		return a
	} else {
		return b
	}
}

func avgDuration(a time.Duration, b time.Duration) time.Duration {
	return (a + b) / 2
}
