package redisSyncFanoutQueue

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

type HandleMessageFunc func(ctx context.Context, clientId *string, msg *Message) error
type HandleRoomClientTimeoutFunc func(ctx context.Context, clientId, room *string) error

type ApiOptions struct {
	RedisOptions   *redis.Options
	ClientTimeout  time.Duration
	RedisKeyPrefix string
}

type WorkerOptions struct {
	RedisOptions            *redis.Options
	RedisKeyPrefix          string
	HandleMessage           HandleMessageFunc
	HandleRoomClientTimeout HandleRoomClientTimeoutFunc
}

var validationError = fmt.Errorf("All Options values must be correctly specified")

func (o *ApiOptions) Validate() error {
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

	return nil
}

func (o *WorkerOptions) Validate() error {
	if o == nil {
		return validationError
	}

	if o.RedisOptions == nil {
		return validationError
	}

	if len(o.RedisKeyPrefix) < 1 {
		return validationError
	}

	if o.HandleMessage == nil {
		return validationError
	}

	if o.HandleRoomClientTimeout == nil {
		return validationError
	}

	return nil
}
