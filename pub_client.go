package redisSyncFanoutQueue

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	redisLuaScriptUtils "github.com/zavitax/redis-lua-script-utils-go"
)

type GetPubMetricsOptions struct {
	TopRoomsLimit int
}

type RoomMetrics struct {
	Room                         string
	PendingMessagesBacklogLength int64
}

type PubMetrics struct {
	KnownRoomsCount int64

	TopRooms                             []*RoomMetrics
	TopRoomsPendingMessagesBacklogLength int64
}

type RedisQueuePubClient interface {
	Close() error
	Send(ctx context.Context, clientId string, room string, data interface{}, priority int) error
	SendOutOfBand(ctx context.Context, clientId string, room string, data interface{}) error
	Peek(ctx context.Context, room string, offset int, limit int) ([]*Message, error)
	GetMetrics(ctx context.Context, options *GetPubMetricsOptions) (*PubMetrics, error)
	Ping(ctx context.Context, clientId string, room string) error
	AckMessage(ctx context.Context, clientId string, ackToken string) error
}

type redisQueuePubClient struct {
	mu sync.RWMutex

	options *Options
	redis   *redis.Client

	housekeep_context    context.Context
	housekeep_cancelFunc context.CancelFunc

	callRemoveSyncClientFromRoom       *redisLuaScriptUtils.CompiledRedisScript
	callPong                           *redisLuaScriptUtils.CompiledRedisScript
	callGetMetrics                     *redisLuaScriptUtils.CompiledRedisScript
	callSend                           *redisLuaScriptUtils.CompiledRedisScript
	callSendOutOfBand                  *redisLuaScriptUtils.CompiledRedisScript
	callAckClientMessage               *redisLuaScriptUtils.CompiledRedisScript
	callConditionalProcessRoomMessages *redisLuaScriptUtils.CompiledRedisScript

	keyLastTimestamp           string
	keyGlobalSetOfKnownClients string
	keyGlobalKnownRooms        string

	redisKeys []*redisLuaScriptUtils.RedisKey
}

func (c *redisQueuePubClient) keyRoomQueue(room string) string {
	return fmt.Sprintf("%s::room::%s::msg-queue", c.options.RedisKeyPrefix, room)
}

func (c *redisQueuePubClient) keyRoomSetOfKnownClients(room string) string {
	return fmt.Sprintf("%s::room::%s::known-clients", c.options.RedisKeyPrefix, room)
}

func (c *redisQueuePubClient) keyRoomSetOfAckedClients(room string) string {
	return fmt.Sprintf("%s::room::%s::acked-clients", c.options.RedisKeyPrefix, room)
}

func (c *redisQueuePubClient) keyGlobalLock(tag string) string {
	return fmt.Sprintf("%s::lock::%s", c.options.RedisKeyPrefix, tag)
}

func NewPubClient(ctx context.Context, options *Options) (RedisQueuePubClient, error) {
	if err := options.Validate(); err != nil {
		return nil, err
	}

	var c = &redisQueuePubClient{}

	c.options = options
	c.redis = redis.NewClient(c.options.RedisOptions)

	c.keyGlobalSetOfKnownClients = fmt.Sprintf("%s::global::known-clients", c.options.RedisKeyPrefix)
	c.keyGlobalKnownRooms = fmt.Sprintf("%s::global::last-client-id-timestamp", c.options.RedisKeyPrefix)

	c.redisKeys = []*redisLuaScriptUtils.RedisKey{
		redisLuaScriptUtils.NewStaticKey("keyLastTimestamp", c.keyGlobalKnownRooms),
		redisLuaScriptUtils.NewStaticKey("keyGlobalSetOfKnownClients", c.keyGlobalSetOfKnownClients),
		redisLuaScriptUtils.NewStaticKey("keyPubsubAdminEventsRemoveClientTopic", fmt.Sprintf("%s::global::pubsub::admin::removed-clients", c.options.RedisKeyPrefix)),
		redisLuaScriptUtils.NewStaticKey("keyGlobalKnownRooms", fmt.Sprintf("%s::global::known-rooms", c.options.RedisKeyPrefix)),
		redisLuaScriptUtils.NewStaticKey("keyGlobalAckedRooms", fmt.Sprintf("%s::global::acked-rooms", c.options.RedisKeyPrefix)),
		redisLuaScriptUtils.NewDynamicKey("keyRoomQueue", func(args *redisLuaScriptUtils.RedisScriptArguments) string {
			return c.keyRoomQueue((*args)["argRoomID"].(string))
		}),
		redisLuaScriptUtils.NewStaticKey("keyRoomPubsub", fmt.Sprintf("%s::global::msg-queue", c.options.RedisKeyPrefix)),
		redisLuaScriptUtils.NewDynamicKey("keyRoomSetOfKnownClients", func(args *redisLuaScriptUtils.RedisScriptArguments) string {
			return c.keyRoomSetOfKnownClients((*args)["argRoomID"].(string))
		}),
		redisLuaScriptUtils.NewDynamicKey("keyRoomSetOfAckedClients", func(args *redisLuaScriptUtils.RedisScriptArguments) string {
			return c.keyRoomSetOfAckedClients((*args)["argRoomID"].(string))
		}),
	}

	var err error

	if c.callRemoveSyncClientFromRoom, err = redisLuaScriptUtils.CompileRedisScripts(
		[]*redisLuaScriptUtils.RedisScript{
			scriptRemoveSyncClientFromRoom,
			scriptConditionalProcessRoomMessages,
		},
		c.redisKeys,
	); err != nil {
		c.redis.Close()
		return nil, err
	}

	if c.callPong, err = redisLuaScriptUtils.CompileRedisScripts(
		[]*redisLuaScriptUtils.RedisScript{
			scriptUpdateClientTimestamp,
		},
		c.redisKeys,
	); err != nil {
		c.redis.Close()
		return nil, err
	}

	if c.callGetMetrics, err = redisLuaScriptUtils.CompileRedisScripts(
		[]*redisLuaScriptUtils.RedisScript{
			scriptGetMetrics,
		},
		c.redisKeys,
	); err != nil {
		c.redis.Close()
		return nil, err
	}

	if c.callSend, err = redisLuaScriptUtils.CompileRedisScripts(
		[]*redisLuaScriptUtils.RedisScript{
			scriptEnqueueRoomMessage,
			scriptConditionalProcessRoomMessages,
		},
		c.redisKeys,
	); err != nil {
		c.redis.Close()
		return nil, err
	}

	if c.callSendOutOfBand, err = redisLuaScriptUtils.CompileRedisScripts(
		[]*redisLuaScriptUtils.RedisScript{
			scriptSendOutOfBandRoomMessage,
			scriptConditionalProcessRoomMessages,
		},
		c.redisKeys,
	); err != nil {
		c.redis.Close()
		return nil, err
	}

	if c.callAckClientMessage, err = redisLuaScriptUtils.CompileRedisScripts(
		[]*redisLuaScriptUtils.RedisScript{
			scriptUpdateClientTimestamp,
			scriptAckClientMessage,
			scriptConditionalProcessRoomMessages,
		},
		c.redisKeys,
	); err != nil {
		c.redis.Close()
		return nil, err
	}

	if c.callConditionalProcessRoomMessages, err = redisLuaScriptUtils.CompileRedisScripts(
		[]*redisLuaScriptUtils.RedisScript{
			scriptConditionalProcessRoomMessages,
		},
		c.redisKeys,
	); err != nil {
		c.redis.Close()
		return nil, err
	}

	c.housekeep_context, c.housekeep_cancelFunc = context.WithCancel(ctx)
	go (func() {
		housekeepingInterval := time.Duration(c.options.ClientTimeout / 2)

		ticker := time.NewTicker(housekeepingInterval)
		defer ticker.Stop()

		for {
			select {
			case <-c.housekeep_context.Done():
				return
			case <-ticker.C:
				c._housekeep(c.housekeep_context)
			}
		}
	})()

	return c, nil
}

func (c *redisQueuePubClient) createStdRoomArgs(room string) *redisLuaScriptUtils.RedisScriptArguments {
	args := make(redisLuaScriptUtils.RedisScriptArguments)
	args["argRoomID"] = room
	args["argCurrentTimestamp"] = currentTimestamp()

	return &args
}

func (c *redisQueuePubClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.housekeep_cancelFunc()

	return c.redis.Close()
}

func (c *redisQueuePubClient) Send(ctx context.Context, clientId string, room string, data interface{}, priority int) error {
	packet := &redisQueueWireMessage{
		Timestamp: currentTimestamp(),
		Producer:  clientId,
		Room:      room,
		Data:      data,
	}

	jsonString, serr := json.Marshal(packet)

	if serr != nil {
		return serr
	}

	args := c.createStdRoomArgs(room)
	(*args)["argPriority"] = priority
	(*args)["argMsg"] = string(jsonString)

	_, err := c.callSend.Run(ctx, c.redis, args).Slice()

	return err
}

func (c *redisQueuePubClient) SendOutOfBand(ctx context.Context, clientId string, room string, data interface{}) error {
	packet := &redisQueueWireMessage{
		Timestamp: currentTimestamp(),
		Producer:  clientId,
		Room:      room,
		Data:      data,
	}

	jsonString, serr := json.Marshal(packet)

	if serr != nil {
		return serr
	}

	args := c.createStdRoomArgs(room)
	(*args)["argMsg"] = string(jsonString)

	_, err := c.callSendOutOfBand.Run(ctx, c.redis, args).Slice()

	return err
}

func (c *redisQueuePubClient) _housekeep(ctx context.Context) error {
	c._removeTimedOutClients(ctx)
	c._conditionalProcessRoomsMessages(ctx)

	return nil
}

func (c *redisQueuePubClient) _lock(ctx context.Context, tag string, timeout time.Duration) error {
	clientId := "PubClient"

	err := c.redis.Do(ctx, "SET", c.keyGlobalLock(tag), clientId, "NX", "PX", timeout.Milliseconds()).Err()

	return err
}

func (c *redisQueuePubClient) _extend_lock(ctx context.Context, tag string, timeout time.Duration) error {
	err := c.redis.Do(ctx, "PEXPIRE", c.keyGlobalLock(tag), timeout.Milliseconds()).Err()

	return err
}

func (c *redisQueuePubClient) _peek(ctx context.Context, room string, offset int, limit int) ([]string, error) {
	return c.redis.Do(ctx, "ZRANGEBYSCORE", c.keyRoomQueue(room), "-inf", "+inf", "LIMIT", offset, limit).StringSlice()
}

func (c *redisQueuePubClient) _ack(ctx context.Context, clientId string, ackToken string) error {
	parts := strings.SplitN(ackToken, "::", 2)

	if len(parts) < 2 {
		return fmt.Errorf("Invalid ackToken %v", ackToken)
	}

	room := parts[1]

	args := c.createStdRoomArgs(room)
	(*args)["argClientID"] = clientId

	return c.callAckClientMessage.Run(ctx, c.redis, args).Err()
}

func (c *redisQueuePubClient) _pong(ctx context.Context, clientId string, room string) error {
	args := c.createStdRoomArgs(room)
	(*args)["argClientID"] = clientId

	if err := c.callPong.Run(ctx, c.redis, args).Err(); err != nil {
		return err
	}

	return nil
}

func (c *redisQueuePubClient) _removeTimedOutClients(ctx context.Context) error {
	lockKey := "_removeTimedOutClients"
	lockTimeout := time.Duration(c.options.ClientTimeout / 2)

	if err := c._lock(ctx, lockKey, lockTimeout); err != nil {
		return err
	}

	argMaxTimestampToRemove := currentTimestamp() - c.options.ClientTimeout.Milliseconds()

	clientRoomIDs, err := c.redis.Do(ctx, "ZRANGEBYSCORE", c.keyGlobalSetOfKnownClients, "-inf", argMaxTimestampToRemove).StringSlice()

	if err != nil {
		return err
	}

	for _, clientRoomID := range clientRoomIDs {
		parts := strings.SplitN(clientRoomID, "::", 2)

		if len(parts) < 2 {
			continue
		}

		clientId := parts[0]
		room := parts[1]

		args := c.createStdRoomArgs(room)
		(*args)["argClientID"] = clientId

		reqErr := c.callRemoveSyncClientFromRoom.Run(ctx, c.redis, args).Err()

		if reqErr != nil {
			return reqErr
		}

		reqErr = c._extend_lock(ctx, lockKey, lockTimeout)

		if reqErr != nil {
			return reqErr
		}
	}

	return nil
}

func (c *redisQueuePubClient) _conditionalProcessRoomsMessages(ctx context.Context) error {
	lockKey := "_conditionalProcessRoomsMessages"
	lockTimeout := time.Duration(c.options.ClientTimeout / 2)

	if err := c._lock(ctx, lockKey, lockTimeout); err != nil {
		return err
	}

	roomIDs, reqErr := c.redis.Do(ctx, "ZRANGEBYSCORE", c.keyGlobalKnownRooms, "-inf", "+inf").StringSlice()

	if reqErr != nil {
		return reqErr
	}

	for _, room := range roomIDs {
		if err := c.callConditionalProcessRoomMessages.Run(ctx, c.redis, c.createStdRoomArgs(room)).Err(); err != nil {
			return err
		}

		if err := c._extend_lock(ctx, lockKey, lockTimeout); err != nil {
			return err
		}
	}

	return nil
}

func (c *redisQueuePubClient) Peek(ctx context.Context, room string, offset int, limit int) ([]*Message, error) {
	if msgDataStrings, err := c._peek(ctx, room, offset, limit); err != nil {
		return nil, err
	} else {
		var result []*Message

		for _, msgData := range msgDataStrings {
			if msg, _, err := c._parseMsgData(msgData); err != nil {
				return nil, err
			} else {
				result = append(result, msg)
			}
		}

		return result, nil
	}
}

func (c *redisQueuePubClient) _parseMsgData(msgData string) (*Message, *redisQueueWireMessage, error) {
	var packet redisQueueWireMessage

	if err := json.Unmarshal([]byte(msgData), &packet); err != nil {
		return nil, nil, err
	}

	var msg Message

	msg.Data = &packet.Data
	msg.MessageContext.Timestamp = time.UnixMilli(packet.Timestamp).UTC()
	msg.MessageContext.Producer = packet.Producer
	msg.MessageContext.Room = packet.Room
	msg.MessageContext.Latency = time.Now().UTC().Sub(msg.MessageContext.Timestamp)

	return &msg, &packet, nil
}

func (c *redisQueuePubClient) getMetricsParseTopRooms(result *PubMetrics, data []interface{}) {
	if list, ok := data[1].([]interface{}); ok {
		for i := 0; i < len(list); i += 2 {
			if backlog, err := strconv.ParseInt(list[i+1].(string), 10, 0); err == nil {
				result.TopRooms = append(result.TopRooms, &RoomMetrics{
					Room:                         list[i].(string),
					PendingMessagesBacklogLength: backlog,
				})

				result.TopRoomsPendingMessagesBacklogLength += backlog
			}
		}
	}
}

func (c *redisQueuePubClient) GetMetrics(ctx context.Context, options *GetPubMetricsOptions) (*PubMetrics, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	args := make(redisLuaScriptUtils.RedisScriptArguments)
	args["argTopRoomsLimit"] = options.TopRoomsLimit
	resultsArray, err := c.callGetMetrics.Run(ctx, c.redis, &args).Slice()

	if err != nil {
		return nil, err
	}

	data := resultsArray[0].([]interface{})

	result := &PubMetrics{
		KnownRoomsCount:                      data[0].(int64),
		TopRoomsPendingMessagesBacklogLength: 0,
	}

	c.getMetricsParseTopRooms(result, data)

	return result, nil
}

func (c *redisQueuePubClient) Ping(ctx context.Context, clientId string, room string) error {
	return c._pong(ctx, clientId, room)
}

func (c *redisQueuePubClient) AckMessage(ctx context.Context, clientId string, ackToken string) error {
	return c._ack(ctx, clientId, ackToken)
}
