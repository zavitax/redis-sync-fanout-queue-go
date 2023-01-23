package redisSyncFanoutQueue

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/sahmad98/go-ringbuffer"
	redisLuaScriptUtils "github.com/zavitax/redis-lua-script-utils-go"
)

type GetWorkerMetricsOptions struct {
}

type WorkerMetrics struct {
	ReceivedMessagesCount int64
	InvalidMessagesCount  int64

	MinLatency time.Duration
	MaxLatency time.Duration
	AvgLatency time.Duration
}

type RedisQueueWorkerClient interface {
	Close() error
	GetMetrics(ctx context.Context, options *GetWorkerMetricsOptions) (*WorkerMetrics, error)
}

type redisQueueWorkerClient struct {
	mu sync.RWMutex
	wg sync.WaitGroup

	options                  *WorkerOptions
	redis_subscriber_timeout *redis.Client
	redis_subscriber_message *redis.Client

	redis_context     context.Context
	redis_cancel_func context.CancelFunc

	statLastMessageLatencies *ringbuffer.RingBuffer
	statRecvMsgCount         int64
	statInvalidMsgCount      int64

	keyPubsubAdminEventsRemoveClientTopic string
	keyRoomPubsub                         string

	redisKeys []*redisLuaScriptUtils.RedisKey
}

func (c *redisQueueWorkerClient) keyGlobalLock(tag string) string {
	return fmt.Sprintf("%s::lock::%s", c.options.RedisKeyPrefix, tag)
}

func (c *redisQueueWorkerClient) keyRoomSetOfKnownClients(room string) string {
	return fmt.Sprintf("%s::room::%s::known-clients", c.options.RedisKeyPrefix, room)
}

func NewWorkerClient(ctx context.Context, options *WorkerOptions) (RedisQueueWorkerClient, error) {
	if err := options.Validate(); err != nil {
		return nil, err
	}

	var c = &redisQueueWorkerClient{}

	c.options = options

	c.redis_context, c.redis_cancel_func = context.WithCancel(context.Background())

	c.keyPubsubAdminEventsRemoveClientTopic = fmt.Sprintf("%s::global::pubsub::admin::removed-clients", c.options.RedisKeyPrefix)
	c.keyRoomPubsub = fmt.Sprintf("%s::global::msg-queue", c.options.RedisKeyPrefix)

	c.statLastMessageLatencies = ringbuffer.NewRingBuffer(100)

	c.redis_subscriber_timeout = redis.NewClient(c.options.RedisOptions)
	c.redis_subscriber_message = redis.NewClient(c.options.RedisOptions)

	c.wg.Add(1)
	go (func() {
		defer c.wg.Done()

		// Listen for client timeout messages
		for {
			msgs, err := c.redis_subscriber_timeout.BLPop(c.redis_context, time.Minute, c.keyPubsubAdminEventsRemoveClientTopic).Result()

			if err == nil {
				for _, msg := range msgs {
					c._handleTimeoutMessage(c.redis_context, msg)
				}
			}

			if err == context.Canceled {
				return
			}
		}
	})()

	c.wg.Add(1)
	go (func() {
		defer c.wg.Done()

		// Listen for client timeout messages
		for {
			msgs, err := c.redis_subscriber_message.BLPop(c.redis_context, time.Minute, c.keyRoomPubsub).Result()

			if err == nil {
				for _, msg := range msgs {
					c._handleMessage(c.redis_context, msg)
				}
			} else if err == context.Canceled {
				// Context cancelled, we're done
				return
			}
		}
	})()

	return c, nil
}

func (c *redisQueueWorkerClient) createStdRoomArgs(clientId string, room string) *redisLuaScriptUtils.RedisScriptArguments {
	args := make(redisLuaScriptUtils.RedisScriptArguments)
	args["argClientID"] = clientId
	args["argRoomID"] = room
	args["argCurrentTimestamp"] = currentTimestamp()

	return &args
}

func (c *redisQueueWorkerClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.redis_cancel_func()

	c.wg.Wait()

	return nil
}

func (c *redisQueueWorkerClient) _handleTimeoutMessage(ctx context.Context, message string) {
	parts := strings.SplitN(message, "::", 2)

	clientId := parts[0]
	room := parts[1]

	c.options.HandleRoomClientTimeout(ctx, &clientId, &room)
}

func (c *redisQueueWorkerClient) _handleMessage(ctx context.Context, msgData string) error {
	msg, _, err := parseMsgData(msgData)

	if err != nil {
		atomic.AddInt64(&c.statInvalidMsgCount, 1)

		return err
	}

	c.statLastMessageLatencies.Write(msg.MessageContext.Latency)
	atomic.AddInt64(&c.statRecvMsgCount, 1)

	clientIds, err := c.redis_subscriber_message.ZRangeByScore(ctx, c.keyRoomSetOfKnownClients(msg.Room), &redis.ZRangeBy{
		Min:    "-inf",
		Max:    "+inf",
		Offset: 0,
		Count:  1000000,
	}).Result()

	if err != nil {
		return err
	}

	for _, clientKey := range clientIds {
		parts := strings.SplitN(clientKey, "::", 2)

		clientId := parts[0]
		c.options.HandleMessage(ctx, &clientId, msg)
	}

	return nil
}

func (c *redisQueueWorkerClient) getMetricsParseLatencies(result *WorkerMetrics) {
	latencies := make([]interface{}, c.statLastMessageLatencies.Size)
	copy(latencies, c.statLastMessageLatencies.Container)

	var sumLatencyMs int64
	var minLatencyMs int64 = 0
	var maxLatencyMs int64 = 0
	var numLatencies int64 = 0

	if len(latencies) > 0 && latencies[0] != nil {
		minLatencyMs = latencies[0].(time.Duration).Milliseconds()
	}

	for _, latency := range latencies {
		if latency != nil {
			numLatencies++

			ms := latency.(time.Duration).Milliseconds()
			sumLatencyMs += ms
			if ms < minLatencyMs {
				minLatencyMs = ms
			}
			if ms > maxLatencyMs {
				maxLatencyMs = ms
			}
		}
	}

	result.MinLatency = time.Duration(minLatencyMs) * time.Millisecond
	result.MaxLatency = time.Duration(maxLatencyMs) * time.Millisecond

	if numLatencies > 0 {
		result.AvgLatency = time.Duration(sumLatencyMs/numLatencies) * time.Millisecond
	} else {
		result.AvgLatency = time.Duration(0)
	}
}

func (c *redisQueueWorkerClient) GetMetrics(ctx context.Context, options *GetWorkerMetricsOptions) (*WorkerMetrics, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := &WorkerMetrics{
		ReceivedMessagesCount: c.statRecvMsgCount,
		InvalidMessagesCount:  c.statInvalidMsgCount,
	}

	c.getMetricsParseLatencies(result)

	return result, nil
}
