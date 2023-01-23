package redisSyncFanoutQueue

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/sahmad98/go-ringbuffer"
	redisLuaScriptUtils "github.com/zavitax/redis-lua-script-utils-go"
)

func currentTimestamp() int64 {
	return time.Now().UTC().UnixMilli()
}

type RedisQueuePubClient interface {
	Subscribe(ctx context.Context, room string, msgHandlerFunc HandleMessageFunc) error
	Unsubscribe(ctx context.Context, room string) error
	Close() error
	GetMetrics(ctx context.Context, options *GetMetricsOptions) (*Metrics, error)
	Send(ctx context.Context, room string, data interface{}, priority int) error
	SendOutOfBand(ctx context.Context, room string, data interface{}) error
	Peek(ctx context.Context, room string, offset int, limit int) ([]*Message, error)
}

type redisQueueWireMessage struct {
	Timestamp int64       `json:"t"`
	Producer  string      `json:"c"`
	Sequence  int64       `json:"s"`
	Data      interface{} `json:"d"`
}

type AckMessageFunc func(ctx context.Context) error

type Message struct {
	Data           *interface{}
	Ack            AckMessageFunc
	MessageContext struct {
		Timestamp time.Time
		Producer  string
		Sequence  int64
		Latency   time.Duration
		Room      string
	}
}

type roomData struct {
	handleMessage HandleMessageFunc
	redisHandle   *redis.PubSub
}

type redisQueueClient struct {
	mu sync.RWMutex

	options                  *Options
	redis                    *redis.Client
	redis_subscriber_timeout *redis.Client
	redis_subscriber_message *redis.Client

	redis_subscriber_timeout_handle      *redis.PubSub
	redis_subscriber_timeout_context     context.Context
	redis_subscriber_timeout_cancel_func context.CancelFunc

	housekeep_context    context.Context
	housekeep_cancelFunc context.CancelFunc

	clientId                  string
	lastMessageSequenceNumber int64

	callCreateClientID                 *redisLuaScriptUtils.CompiledRedisScript
	callSubscribe                      *redisLuaScriptUtils.CompiledRedisScript
	callUnsubscribe                    *redisLuaScriptUtils.CompiledRedisScript
	callRemoveSyncClientFromRoom       *redisLuaScriptUtils.CompiledRedisScript
	callPong                           *redisLuaScriptUtils.CompiledRedisScript
	callGetMetrics                     *redisLuaScriptUtils.CompiledRedisScript
	callSend                           *redisLuaScriptUtils.CompiledRedisScript
	callAckClientMessage               *redisLuaScriptUtils.CompiledRedisScript
	callHousekeep                      *redisLuaScriptUtils.CompiledRedisScript
	callConditionalProcessRoomMessages *redisLuaScriptUtils.CompiledRedisScript

	rooms map[string]*roomData

	keyClientIDSequence                   string
	keyLastTimestamp                      string
	keyGlobalSetOfKnownClients            string
	keyPubsubAdminEventsRemoveClientTopic string
	keyGlobalKnownRooms                   string

	statLastMessageLatencies *ringbuffer.RingBuffer
	statRecvMsgCount         int64
	statInvalidMsgCount      int64

	redisKeys []*redisLuaScriptUtils.RedisKey
}

func (c *redisQueueClient) keyRoomQueue(room string) string {
	return fmt.Sprintf("%s::room::%s::msg-queue", c.options.RedisKeyPrefix, room)
}

func (c *redisQueueClient) keyRoomPubsub(room string) string {
	return fmt.Sprintf("%s::room::%s::msg-pubsub", c.options.RedisKeyPrefix, room)
}

func (c *redisQueueClient) keyRoomSetOfKnownClients(room string) string {
	return fmt.Sprintf("%s::room::%s::known-clients", c.options.RedisKeyPrefix, room)
}

func (c *redisQueueClient) keyRoomSetOfAckedClients(room string) string {
	return fmt.Sprintf("%s::room::%s::acked-clients", c.options.RedisKeyPrefix, room)
}

func (c *redisQueueClient) keyGlobalLock(tag string) string {
	return fmt.Sprintf("%s::lock::%s", c.options.RedisKeyPrefix, tag)
}

func NewClient(ctx context.Context, options *Options) (RedisQueuePubClient, error) {
	if err := options.Validate(); err != nil {
		return nil, err
	}

	var c = &redisQueueClient{}

	c.options = options
	c.redis = redis.NewClient(c.options.RedisOptions)
	//fmt.Printf("FLUSHDB: err: %v\n", c.redis.Do(ctx, "FLUSHDB").Err()) // TODO: Kill
	c.rooms = make(map[string]*roomData)

	c.redisKeys = []*redisLuaScriptUtils.RedisKey{
		redisLuaScriptUtils.NewStaticKey("keyClientIDSequence", fmt.Sprintf("%s::global::last-client-id-seq", c.options.RedisKeyPrefix)),
		redisLuaScriptUtils.NewStaticKey("keyLastTimestamp", fmt.Sprintf("%s::global::last-client-id-timestamp", c.options.RedisKeyPrefix)),
		redisLuaScriptUtils.NewStaticKey("keyGlobalSetOfKnownClients", fmt.Sprintf("%s::global::known-clients", c.options.RedisKeyPrefix)),
		redisLuaScriptUtils.NewStaticKey("keyPubsubAdminEventsRemoveClientTopic", fmt.Sprintf("%s::global::pubsub::admin::removed-clients", c.options.RedisKeyPrefix)),
		redisLuaScriptUtils.NewStaticKey("keyGlobalKnownRooms", fmt.Sprintf("%s::global::known-rooms", c.options.RedisKeyPrefix)),
		redisLuaScriptUtils.NewStaticKey("keyGlobalAckedRooms", fmt.Sprintf("%s::global::acked-rooms", c.options.RedisKeyPrefix)),
		redisLuaScriptUtils.NewDynamicKey("keyRoomQueue", func(args *redisLuaScriptUtils.RedisScriptArguments) string {
			return c.keyRoomQueue((*args)["argRoomID"].(string))
		}),
		redisLuaScriptUtils.NewDynamicKey("keyRoomPubsub", func(args *redisLuaScriptUtils.RedisScriptArguments) string {
			return c.keyRoomPubsub((*args)["argRoomID"].(string))
		}),
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

	if c.callSubscribe, err = redisLuaScriptUtils.CompileRedisScripts(
		[]*redisLuaScriptUtils.RedisScript{
			scriptAddSyncClientToRoom,
			//scriptUpdateClientTimestamp,
			//scriptConditionalProcessRoomMessages,
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
			scriptUpdateClientTimestamp,
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

	if c.callHousekeep, err = redisLuaScriptUtils.CompileRedisScripts(
		[]*redisLuaScriptUtils.RedisScript{
			scriptUpdateClientTimestamp,
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

	c.keyClientIDSequence = fmt.Sprintf("%s::global::last-client-id-seq", c.options.RedisKeyPrefix)
	c.keyLastTimestamp = fmt.Sprintf("%s::global::last-client-id-timestamp", c.options.RedisKeyPrefix)
	c.keyGlobalSetOfKnownClients = fmt.Sprintf("%s::global::known-clients", c.options.RedisKeyPrefix)
	c.keyPubsubAdminEventsRemoveClientTopic = fmt.Sprintf("%s::global::pubsub::admin::removed-clients", c.options.RedisKeyPrefix)
	c.keyGlobalKnownRooms = fmt.Sprintf("%s::global::known-rooms", c.options.RedisKeyPrefix)

	c.lastMessageSequenceNumber = 0

	c.statLastMessageLatencies = ringbuffer.NewRingBuffer(100)

	if c.clientId, err = c.createClientId(ctx); err != nil {
		return nil, err
	}

	c.redis_subscriber_timeout = redis.NewClient(c.options.RedisOptions)
	c.redis_subscriber_message = redis.NewClient(c.options.RedisOptions)

	c.redis_subscriber_timeout_context, c.redis_subscriber_timeout_cancel_func = context.WithCancel(ctx)
	c.redis_subscriber_timeout_handle = c.redis_subscriber_timeout.Subscribe(c.redis_subscriber_timeout_context, c.keyPubsubAdminEventsRemoveClientTopic)

	go (func() {
		// Listen for clietn timeout messages
		for msg := range c.redis_subscriber_timeout_handle.Channel() {
			c._handleTimeoutMessage(c.redis_subscriber_timeout_context, msg.Channel, msg.Payload)
		}
	})()

	//c._housekeep(ctx)

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

func (c *redisQueueClient) Subscribe(ctx context.Context, room string, handleMessage HandleMessageFunc) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	contextWithCancel, contextCancelFunc := context.WithCancel(ctx)

	redisHandle := c.redis_subscriber_message.Subscribe(contextWithCancel, c.keyRoomPubsub(room))

	if c.options.Sync {
		if _, err := c.callSubscribe.Run(ctx, c.redis, c.createStdRoomArgs(room)).Slice(); err != nil {
			redisHandle.Close()
			contextCancelFunc()

			return err
		}
	}

	c.rooms[room] = &roomData{
		handleMessage: handleMessage,
		redisHandle:   redisHandle,
	}

	go (func() {
		for msg := range redisHandle.Channel() {
			// Listen for incoming room messages
			c._handleRoomMessage(contextWithCancel, msg.Channel, msg.Payload)
		}

		contextCancelFunc()
	})()

	return nil
}

func (c *redisQueueClient) createStdRoomArgs(room string) *redisLuaScriptUtils.RedisScriptArguments {
	args := make(redisLuaScriptUtils.RedisScriptArguments)
	args["argClientID"] = c.clientId
	args["argRoomID"] = room
	args["argCurrentTimestamp"] = currentTimestamp()

	return &args
}

func (c *redisQueueClient) Unsubscribe(ctx context.Context, room string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c._unsubscribe(ctx, room); err != nil {
		return err
	}

	return nil
}

func (c *redisQueueClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.housekeep_cancelFunc()

	c.redis_subscriber_timeout_cancel_func()
	c.redis_subscriber_timeout_handle.Close()

	for key := range c.rooms {
		c._unsubscribe(context.Background(), key)
	}

	c.rooms = make(map[string]*roomData)

	return c.redis.Close()
}

func (c *redisQueueClient) Send(ctx context.Context, room string, data interface{}, priority int) error {
	packet := &redisQueueWireMessage{
		Timestamp: currentTimestamp(),
		Producer:  c.clientId,
		Sequence:  atomic.AddInt64(&c.lastMessageSequenceNumber, 1),
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

func (c *redisQueueClient) SendOutOfBand(ctx context.Context, room string, data interface{}) error {
	packet := &redisQueueWireMessage{
		Timestamp: currentTimestamp(),
		Producer:  c.clientId,
		Sequence:  0,
		Data:      data,
	}

	jsonString, serr := json.Marshal(packet)

	if serr != nil {
		return serr
	}

	return c.redis.Do(ctx, "PUBLISH", c.keyRoomPubsub(room), jsonString).Err()
}

func (c *redisQueueClient) _housekeep(ctx context.Context) error {
	c._pong(ctx)
	c._removeTimedOutClients(ctx)
	c._conditionalProcessRoomsMessages(ctx)

	return nil
}

func (c *redisQueueClient) createClientId(ctx context.Context) (string, error) {
	args := make(redisLuaScriptUtils.RedisScriptArguments, 0)
	args["argCurrentTimestamp"] = currentTimestamp()
	if result, err := c.callCreateClientID.Run(ctx, c.redis, &args).StringSlice(); err != nil {
		return "ERROR_OBTAINING_CLIENT_ID", err
	} else {
		return result[0], nil
	}
}

func (c *redisQueueClient) _lock(ctx context.Context, tag string, timeout time.Duration) error {
	err := c.redis.Do(ctx, "SET", c.keyGlobalLock(tag), c.clientId, "NX", "PX", timeout.Milliseconds()).Err()

	return err
}

func (c *redisQueueClient) _extend_lock(ctx context.Context, tag string, timeout time.Duration) error {
	err := c.redis.Do(ctx, "PEXPIRE", c.keyGlobalLock(tag), timeout.Milliseconds()).Err()

	return err
}

func (c *redisQueueClient) _peek(ctx context.Context, room string, offset int, limit int) ([]string, error) {
	return c.redis.Do(ctx, "ZRANGEBYSCORE", c.keyRoomQueue(room), "-inf", "+inf", "LIMIT", offset, limit).StringSlice()
}

func (c *redisQueueClient) _ack(ctx context.Context, ackToken string) error {
	parts := strings.SplitN(ackToken, "::", 2)

	if len(parts) < 2 {
		return fmt.Errorf("Invalid ackToken %v", ackToken)
	}

	room := parts[1]

	return c.callAckClientMessage.Run(ctx, c.redis, c.createStdRoomArgs(room)).Err()
}

func (c *redisQueueClient) _handleTimeoutMessage(ctx context.Context, _channel string, message string) {
	c.mu.Lock()

	parts := strings.SplitN(message, "::", 2)

	clientId := parts[0]

	if clientId != c.clientId {
		c.mu.Unlock()
		return
	}

	roomsCopy := c.rooms
	for _, roomVal := range c.rooms {
		roomVal.redisHandle.Close() // Unsubscribe
	}

	c.rooms = make(map[string]*roomData)
	c.mu.Unlock()

	for room, _ := range roomsCopy {
		if c.options.HandleRoomEjected != nil {
			c.options.HandleRoomEjected(ctx, &room)
		}
	}
}

func (c *redisQueueClient) _handleRoomMessage(ctx context.Context, channel string, message string) {
	parts := strings.Split(channel, "::")

	if parts[len(parts)-1] == "msg-pubsub" {
		room := parts[len(parts)-2]

		c._handleMessage(ctx, room, message)
	}
}

func (c *redisQueueClient) _unsubscribe(ctx context.Context, room string) error {
	roomVal, ok := c.rooms[room]

	if !ok {
		return fmt.Errorf("No such room: %s", room)
	}

	roomVal.redisHandle.Close()

	delete(c.rooms, room)

	if c.options.Sync {
		return c.callUnsubscribe.Run(ctx, c.redis, c.createStdRoomArgs(room)).Err()
	}

	return nil
}

func (c *redisQueueClient) _pong(ctx context.Context) error {
	for room := range c.rooms {
		if err := c.callPong.Run(ctx, c.redis, c.createStdRoomArgs(room)).Err(); err != nil {
			return err
		}
	}

	return nil
}

func (c *redisQueueClient) _removeTimedOutClients(ctx context.Context) error {
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

func (c *redisQueueClient) _conditionalProcessRoomsMessages(ctx context.Context) error {
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

func (c *redisQueueClient) Peek(ctx context.Context, room string, offset int, limit int) ([]*Message, error) {
	if msgDataStrings, err := c._peek(ctx, room, offset, limit); err != nil {
		return nil, err
	} else {
		var result []*Message

		for _, msgData := range msgDataStrings {
			if msg, _, err := c._parseMsgData(msgData, room); err != nil {
				return nil, err
			} else {
				result = append(result, msg)
			}
		}

		return result, nil
	}
}

func (c *redisQueueClient) _parseMsgData(msgData string, room string) (*Message, *redisQueueWireMessage, error) {
	var packet redisQueueWireMessage

	if err := json.Unmarshal([]byte(msgData), &packet); err != nil {
		atomic.AddInt64(&c.statInvalidMsgCount, 1)

		return nil, nil, err
	}

	var msg Message

	msg.Data = &packet.Data
	msg.MessageContext.Timestamp = time.UnixMilli(packet.Timestamp).UTC()
	msg.MessageContext.Producer = packet.Producer
	msg.MessageContext.Sequence = packet.Sequence
	msg.MessageContext.Room = room
	msg.MessageContext.Latency = time.Now().UTC().Sub(msg.MessageContext.Timestamp)

	return &msg, &packet, nil
}

func (c *redisQueueClient) _handleMessage(ctx context.Context, room string, msgData string) error {
	c.mu.RLock()

	if roomVal, ok := c.rooms[room]; !ok {
		c.mu.RUnlock()
		return fmt.Errorf("Invalid room: %v", room)
	} else {
		c.mu.RUnlock()
		msg, packet, err := c._parseMsgData(msgData, room)

		if err != nil {
			atomic.AddInt64(&c.statInvalidMsgCount, 1)

			return err
		}

		if c.options.Sync && packet.Sequence > 0 {
			ackToken := fmt.Sprintf("%s::%s", packet.Producer, room)

			msg.Ack = func(ctx context.Context) error {
				err := c._ack(ctx, ackToken)

				return err
			}
		}

		c.statLastMessageLatencies.Write(msg.MessageContext.Latency)
		atomic.AddInt64(&c.statRecvMsgCount, 1)

		return roomVal.handleMessage(ctx, msg)
	}
}

func (c *redisQueueClient) getMetricsParseTopRooms(result *Metrics, data []interface{}) {
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

func (c *redisQueueClient) getMetricsParseLatencies(result *Metrics) {
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

func (c *redisQueueClient) GetMetrics(ctx context.Context, options *GetMetricsOptions) (*Metrics, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	args := make(redisLuaScriptUtils.RedisScriptArguments)
	args["argTopRoomsLimit"] = options.TopRoomsLimit
	resultsArray, err := c.callGetMetrics.Run(ctx, c.redis, &args).Slice()

	if err != nil {
		return nil, err
	}

	data := resultsArray[0].([]interface{})

	result := &Metrics{
		KnownRoomsCount:                      data[0].(int64),
		SubscribedRoomsCount:                 len(c.rooms),
		ReceivedMessagesCount:                c.statRecvMsgCount,
		InvalidMessagesCount:                 c.statInvalidMsgCount,
		TopRoomsPendingMessagesBacklogLength: 0,
	}

	c.getMetricsParseTopRooms(result, data)
	c.getMetricsParseLatencies(result)

	return result, nil
}
