package redisSyncFanoutQueue

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

type HandleMessageFunc func(ctx context.Context, msg *Message) error
type HandleRoomEjectedFunc func(ctx context.Context, clientId, room *string) error

type Options struct {
	RedisOptions   *redis.Options
	ClientTimeout  time.Duration
	Sync           bool
	RedisKeyPrefix string
	// HandleMessage HandleMessageFunc
	HandleRoomEjected HandleRoomEjectedFunc
}
type ShardingOptions struct {
	TotalShardsCount int
}

var validationError = fmt.Errorf("All Options values must be correctly specified")

func (o *Options) Validate() error {
	if o == nil {
		return validationError
	}

	if o.RedisOptions == nil {
		return validationError
	}

	if o.ClientTimeout < time.Second {
		return validationError
	}

	if len(o.RedisKeyPrefix) < 1 {
		return validationError
	}

	//if (o.HandleMessage == nil) {
	//	return validationError
	//}

	return nil
}

func (o *ShardingOptions) Validate() error {
	if o == nil {
		return validationError
	}

	if o.TotalShardsCount < 1 {
		return validationError
	}

	if o.TotalShardsCount > 1024 {
		return validationError
	}

	return nil
}
