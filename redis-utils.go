package redisSyncFanoutQueue

import (
	"github.com/go-redis/redis/v8"
	"context"
)

type redisScriptCall func (ctx context.Context, client *redis.Client, args []interface{}, keys []string) (*redis.Cmd);

func newScriptCall (ctx context.Context, client *redis.Client, script string) (redisScriptCall, error) {
	var redisScript = redis.NewScript(script)

	var invoke = func (ctx context.Context, client *redis.Client, args []interface{}, keys []string) (*redis.Cmd) {
		return redisScript.Run(ctx, client, keys, args)
	}

	return invoke, nil
}
