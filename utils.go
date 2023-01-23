package redisSyncFanoutQueue

import "time"

func currentTimestamp() int64 {
	return time.Now().UTC().UnixMilli()
}
