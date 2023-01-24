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

type GetApiMetricsOptions struct {
	TopRoomsLimit int
}

type RoomMetrics struct {
	Room                         string
	PendingMessagesBacklogLength int64
}

type ApiMetrics struct {
	KnownRoomsCount int64

	TopRooms                             []*RoomMetrics
	TopRoomsPendingMessagesBacklogLength int64
}

type RedisQueueApiClient interface {
	CreateClientID(ctx context.Context) (string, error)
	Close() error
	Send(ctx context.Context, producerId string, room string, data interface{}, priority int) error
	SendOutOfBand(ctx context.Context, producerId string, room string, data interface{}) error
	Peek(ctx context.Context, room string, offset int, limit int) ([]*Message, error)
	GetMetrics(ctx context.Context, options *GetApiMetricsOptions) (*ApiMetrics, error)
	Ping(ctx context.Context, clientId string) error
	AckMessage(ctx context.Context, clientId string, ackToken *string) error
	Subscribe(ctx context.Context, clientId string, room string) error
	Unsubscribe(ctx context.Context, clientId string, room string) error
}

type redisQueueApiClient struct {
	mu sync.RWMutex

	options *ApiOptions
	redis   *redis.Client

	housekeep_context    context.Context
	housekeep_cancelFunc context.CancelFunc

	callCreateClientID                 *redisLuaScriptUtils.CompiledRedisScript
	callRemoveSyncClientFromRoom       *redisLuaScriptUtils.CompiledRedisScript
	callPong                           *redisLuaScriptUtils.CompiledRedisScript
	callGetMetrics                     *redisLuaScriptUtils.CompiledRedisScript
	callSend                           *redisLuaScriptUtils.CompiledRedisScript
	callSendOutOfBand                  *redisLuaScriptUtils.CompiledRedisScript
	callAckClientMessage               *redisLuaScriptUtils.CompiledRedisScript
	callConditionalProcessRoomMessages *redisLuaScriptUtils.CompiledRedisScript
	callSubscribe                      *redisLuaScriptUtils.CompiledRedisScript
	callUnsubscribe                    *redisLuaScriptUtils.CompiledRedisScript

	keyGlobalSetOfKnownClients string
	keyGlobalKnownRooms        string

	redisKeys []*redisLuaScriptUtils.RedisKey
}

func (c *redisQueueApiClient) keyRoomQueue(room string) string {
	return fmt.Sprintf("%s::room::%s::msg-queue", c.options.RedisKeyPrefix, room)
}

func (c *redisQueueApiClient) keyRoomSetOfKnownClients(room string) string {
	return fmt.Sprintf("%s::room::%s::known-clients", c.options.RedisKeyPrefix, room)
}

func (c *redisQueueApiClient) keyRoomSetOfAckedClients(room string) string {
	return fmt.Sprintf("%s::room::%s::acked-clients", c.options.RedisKeyPrefix, room)
}

func (c *redisQueueApiClient) keyGlobalLock(tag string) string {
	return fmt.Sprintf("%s::lock::%s", c.options.RedisKeyPrefix, tag)
}

func NewApiClient(ctx context.Context, options *ApiOptions) (RedisQueueApiClient, error) {
	if err := options.Validate(); err != nil {
		return nil, err
	}

	var c = &redisQueueApiClient{}

	c.options = options
	c.redis = redis.NewClient(c.options.RedisOptions)

	c.keyGlobalSetOfKnownClients = fmt.Sprintf("%s::global::known-clients", c.options.RedisKeyPrefix)
	c.keyGlobalKnownRooms = fmt.Sprintf("%s::global::known-rooms", c.options.RedisKeyPrefix)

	c.redisKeys = []*redisLuaScriptUtils.RedisKey{
		redisLuaScriptUtils.NewStaticKey("keyClientIDSequence", fmt.Sprintf("%s::global::last-client-id-seq", c.options.RedisKeyPrefix)),
		redisLuaScriptUtils.NewStaticKey("keyLastTimestamp", fmt.Sprintf("%s::global::last-client-id-timestamp", c.options.RedisKeyPrefix)),
		redisLuaScriptUtils.NewStaticKey("keyGlobalSetOfKnownClients", c.keyGlobalSetOfKnownClients),
		redisLuaScriptUtils.NewStaticKey("keyPubsubAdminEventsRemoveClientTopic", fmt.Sprintf("%s::global::pubsub::admin::removed-clients", c.options.RedisKeyPrefix)),
		redisLuaScriptUtils.NewStaticKey("keyGlobalKnownRooms", c.keyGlobalKnownRooms),
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

	if c.callCreateClientID, err = redisLuaScriptUtils.CompileRedisScripts(
		[]*redisLuaScriptUtils.RedisScript{
			scriptCreateClientID,
		}, c.redisKeys); err != nil {
		c.redis.Close()
		return nil, err
	}

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

	if c.callSubscribe, err = redisLuaScriptUtils.CompileRedisScripts(
		[]*redisLuaScriptUtils.RedisScript{
			scriptAddSyncClientToRoom,
		},
		c.redisKeys,
	); err != nil {
		c.redis.Close()
		return nil, err
	}

	if c.callUnsubscribe, err = redisLuaScriptUtils.CompileRedisScripts(
		[]*redisLuaScriptUtils.RedisScript{
			scriptRemoveSyncClientFromRoom,
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

func (c *redisQueueApiClient) Subscribe(ctx context.Context, clientId string, room string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.callSubscribe.Run(ctx, c.redis, c.createStdRoomArgs(clientId, room)).Err()

	return nil
}

func (c *redisQueueApiClient) Unsubscribe(ctx context.Context, clientId string, room string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c._unsubscribe(ctx, clientId, room); err != nil {
		return err
	}

	return nil
}

func (c *redisQueueApiClient) _unsubscribe(ctx context.Context, clientId string, room string) error {
	return c.callUnsubscribe.Run(ctx, c.redis, c.createStdRoomArgs(clientId, room)).Err()
}

func (c *redisQueueApiClient) createStdRoomArgs(clientId string, room string) *redisLuaScriptUtils.RedisScriptArguments {
	args := make(redisLuaScriptUtils.RedisScriptArguments)
	args["argClientID"] = clientId
	args["argRoomID"] = room
	args["argCurrentTimestamp"] = currentTimestamp()

	return &args
}

func (c *redisQueueApiClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.housekeep_cancelFunc()

	return c.redis.Close()
}

func (c *redisQueueApiClient) Send(ctx context.Context, producerId string, room string, data interface{}, priority int) error {
	token := fmt.Sprintf("%s::%s", producerId, room)

	packet := &redisQueueWireMessage{
		Timestamp: currentTimestamp(),
		Producer:  producerId,
		Room:      room,
		Data:      data,
		AckToken:  &token,
	}

	jsonString, serr := json.Marshal(packet)

	if serr != nil {
		return serr
	}

	args := c.createStdRoomArgs("ApiClient", room)
	(*args)["argPriority"] = priority
	(*args)["argMsg"] = string(jsonString)

	_, err := c.callSend.Run(ctx, c.redis, args).Slice()

	return err
}

func (c *redisQueueApiClient) SendOutOfBand(ctx context.Context, producerId string, room string, data interface{}) error {
	packet := &redisQueueWireMessage{
		Timestamp: currentTimestamp(),
		Producer:  producerId,
		Room:      room,
		Data:      data,
		AckToken:  nil,
	}

	jsonString, serr := json.Marshal(packet)

	if serr != nil {
		return serr
	}

	args := c.createStdRoomArgs("ApiClient", room)
	(*args)["argMsg"] = string(jsonString)

	_, err := c.callSendOutOfBand.Run(ctx, c.redis, args).Slice()

	return err
}

func (c *redisQueueApiClient) _housekeep(ctx context.Context) error {
	c._removeTimedOutClients(ctx)
	c._conditionalProcessRoomsMessages(ctx)

	return nil
}

func (c *redisQueueApiClient) _lock(ctx context.Context, tag string, timeout time.Duration) error {
	clientId := "PubClient"

	err := c.redis.Do(ctx, "SET", c.keyGlobalLock(tag), clientId, "NX", "PX", timeout.Milliseconds()).Err()

	return err
}

func (c *redisQueueApiClient) _extend_lock(ctx context.Context, tag string, timeout time.Duration) error {
	err := c.redis.Do(ctx, "PEXPIRE", c.keyGlobalLock(tag), timeout.Milliseconds()).Err()

	return err
}

func (c *redisQueueApiClient) _peek(ctx context.Context, room string, offset int, limit int) ([]string, error) {
	return c.redis.Do(ctx, "ZRANGEBYSCORE", c.keyRoomQueue(room), "-inf", "+inf", "LIMIT", offset, limit).StringSlice()
}

func (c *redisQueueApiClient) _ack(ctx context.Context, clientId string, ackToken *string) error {
	parts := strings.SplitN(*ackToken, "::", 2)

	if len(parts) < 2 {
		return fmt.Errorf("Invalid ackToken %v", ackToken)
	}

	room := parts[1]

	args := c.createStdRoomArgs(clientId, room)

	fmt.Println("callAckClientMessage args: ", args)
	return c.callAckClientMessage.Run(ctx, c.redis, args).Err()
}

func (c *redisQueueApiClient) _pong(ctx context.Context, clientId string, room string) error {
	args := c.createStdRoomArgs(clientId, room)
	(*args)["argClientID"] = clientId

	if err := c.callPong.Run(ctx, c.redis, args).Err(); err != nil {
		return err
	}

	return nil
}

func (c *redisQueueApiClient) _removeTimedOutClients(ctx context.Context) error {
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

		args := c.createStdRoomArgs(clientId, room)
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

func (c *redisQueueApiClient) _conditionalProcessRoomsMessages(ctx context.Context) error {
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
		if err := c.callConditionalProcessRoomMessages.Run(ctx, c.redis, c.createStdRoomArgs("ApiClient", room)).Err(); err != nil {
			return err
		}

		if err := c._extend_lock(ctx, lockKey, lockTimeout); err != nil {
			return err
		}
	}

	return nil
}

func (c *redisQueueApiClient) Peek(ctx context.Context, room string, offset int, limit int) ([]*Message, error) {
	if msgDataStrings, err := c._peek(ctx, room, offset, limit); err != nil {
		return nil, err
	} else {
		var result []*Message

		for _, msgData := range msgDataStrings {
			if msg, _, err := parseMsgData(msgData); err != nil {
				return nil, err
			} else {
				result = append(result, msg)
			}
		}

		return result, nil
	}
}

func (c *redisQueueApiClient) getMetricsParseTopRooms(result *ApiMetrics, data []interface{}) {
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

func (c *redisQueueApiClient) GetMetrics(ctx context.Context, options *GetApiMetricsOptions) (*ApiMetrics, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	args := make(redisLuaScriptUtils.RedisScriptArguments)
	args["argTopRoomsLimit"] = options.TopRoomsLimit
	resultsArray, err := c.callGetMetrics.Run(ctx, c.redis, &args).Slice()

	if err != nil {
		return nil, err
	}

	data := resultsArray[0].([]interface{})

	result := &ApiMetrics{
		KnownRoomsCount:                      data[0].(int64),
		TopRoomsPendingMessagesBacklogLength: 0,
	}

	c.getMetricsParseTopRooms(result, data)

	return result, nil
}

func (c *redisQueueApiClient) Ping(ctx context.Context, clientId string) error {
	if list, _, err := c.redis.ZScan(ctx, c.keyGlobalSetOfKnownClients, 0, fmt.Sprintf("%s::*", clientId), 1000000).Result(); err != nil {
		return err
	} else {
		for index, clientRoomId := range list {
			if index%2 == 0 {
				parts := strings.SplitN(clientRoomId, "::", 2)

				if len(parts) < 2 {
					panic(fmt.Sprintf("Invalid data in Redis: [%v] [%v]", index, clientRoomId))
				}

				clientId := parts[0]
				room := parts[1]

				if err = c._pong(ctx, clientId, room); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (c *redisQueueApiClient) AckMessage(ctx context.Context, clientId string, ackToken *string) error {
	return c._ack(ctx, clientId, ackToken)
}

func (c *redisQueueApiClient) createClientId(ctx context.Context) (string, error) {
	args := make(redisLuaScriptUtils.RedisScriptArguments, 0)
	args["argCurrentTimestamp"] = currentTimestamp()
	if result, err := c.callCreateClientID.Run(ctx, c.redis, &args).StringSlice(); err != nil {
		return "ERROR_OBTAINING_CLIENT_ID", err
	} else {
		return result[0], nil
	}
}

func (c *redisQueueApiClient) CreateClientID(ctx context.Context) (string, error) {
	return c.createClientId(ctx)
}
