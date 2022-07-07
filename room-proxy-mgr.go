package redisSyncFanoutQueue

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"
)

type RoomProxyManager interface {
	AddClient(ctx context.Context, options *ClientOptions) (*ClientHandle, error)
	RemoveClient(ctx context.Context, client *ClientHandle) error
	Close() error

	GetMetrics(ctx context.Context, options *GetMetricsOptions) (*Metrics, error)
	Send(ctx context.Context, room string, data interface{}, priority int) error
	SendOutOfBand(ctx context.Context, room string, data interface{}) error
	Peek(ctx context.Context, room string, offset int, limit int) ([]*Message, error)
}

type Room interface {
}

type ClientHandle struct {
	mu           sync.RWMutex
	rooms        map[string]*roomProxy
	clientId     string
	options      *ClientOptions
	roomProxyMgr *roomProxyMgr
}

type roomProxy struct {
	mu                sync.RWMutex
	roomProxyMgr      *roomProxyMgr
	roomId            string
	clients           map[string]*ClientHandle
	ackedClientsCount int32
	lastAck           AckMessageFunc
}

type roomProxyMgr struct {
	mu               sync.RWMutex
	rooms            map[string]*roomProxy
	clients          map[string]*ClientHandle
	redisQueueClient RedisQueueClient
}

type RedisQueueClientProviderFunc func(ctx context.Context, roomEjectedFunc HandleRoomEjectedFunc) (RedisQueueClient, error)

type RoomProxyManagerOptions struct {
	RedisQueueClientProvider RedisQueueClientProviderFunc
}

type ClientOptions struct {
	MessageHandler       HandleMessageFunc
	ClientEjectedHandler func(ctx context.Context, client *ClientHandle) error
}

func (o *ClientOptions) Validate() error {
	if o == nil {
		return fmt.Errorf("ClientOptions is nil")
	}

	if o.MessageHandler == nil {
		return fmt.Errorf("ClientOptions.MessageHandler is nil")
	}

	return nil
}

func (o *RoomProxyManagerOptions) Validate() error {
	if o == nil {
		return fmt.Errorf("RoomProxyOptions is nil")
	}

	if o.RedisQueueClientProvider == nil {
		return fmt.Errorf("RoomProxyOptions.RedisQueueClientProvider is nil")
	}

	return nil
}

func (this *roomProxyMgr) Close() error {
	this.mu.Lock()
	defer this.mu.Unlock()

	for _, client := range this.clients {
		this.removeClient(context.Background(), client)
	}

	return this.redisQueueClient.Close()
}

func NewRoomProxyManager(ctx context.Context, options *RoomProxyManagerOptions) (RoomProxyManager, error) {
	if err := options.Validate(); err != nil {
		return nil, err
	}

	this := &roomProxyMgr{}
	this.rooms = make(map[string]*roomProxy)
	this.clients = make(map[string]*ClientHandle)

	if redisQueueClient, err := options.RedisQueueClientProvider(ctx, this.roomEjectedFunc); err != nil {
		return nil, err
	} else {
		this.redisQueueClient = redisQueueClient

		return this, nil
	}
}

func (this *roomProxyMgr) roomEjectedFunc(ctx context.Context, roomId *string) error {
	room, ok := this.rooms[*roomId]

	if !ok {
		return nil
	}

	room.mu.Lock()
	defer room.mu.Unlock()

	delete(this.rooms, *roomId)

	for _, client := range room.clients {
		room.removeClient(ctx, client)
		this.RemoveClient(ctx, client)

		if client.options.ClientEjectedHandler != nil {
			if err := client.options.ClientEjectedHandler(ctx, client); err != nil {
				// TODO: Log error and ignore
			}
		}
	}

	return nil
}

func (this *roomProxyMgr) removeRoom(ctx context.Context, roomId string) error {
	if err := this.redisQueueClient.Unsubscribe(ctx, roomId); err != nil {
		// TODO: Log and ignore
	}

	delete(this.rooms, roomId)

	return nil
}

func (this *roomProxyMgr) getOrCreateRoom(ctx context.Context, roomId string) (*roomProxy, error) {
	var room *roomProxy = nil

	this.mu.RLock()
	room, ok := this.rooms[roomId]
	this.mu.RUnlock()

	if ok {
		return room, nil
	} else {
		this.mu.Lock()
		defer this.mu.Unlock()

		room, ok = this.rooms[roomId]
		if !ok {
			var err error
			room, err = this._createRoom(ctx, roomId)

			if err != nil {
				return nil, err
			}

			this.rooms[roomId] = room
		}

		return room, nil
	}
}

func (this *roomProxyMgr) AddClient(ctx context.Context, options *ClientOptions) (*ClientHandle, error) {
	if err := options.Validate(); err != nil {
		return nil, err
	}

	this.mu.Lock()
	defer this.mu.Unlock()

	client := &ClientHandle{}
	client.clientId = uuid.New().String()
	client.rooms = make(map[string]*roomProxy)
	client.options = options
	client.roomProxyMgr = this

	this.clients[client.clientId] = client

	return client, nil
}

func (this *roomProxyMgr) RemoveClient(ctx context.Context, client *ClientHandle) error {
	for _, room := range client.rooms {
		room.removeClient(ctx, client)
	}

	this.mu.Lock()
	defer this.mu.Unlock()

	delete(this.clients, client.clientId)

	return nil
}

func (this *roomProxyMgr) removeClient(ctx context.Context, client *ClientHandle) error {
	for _, room := range client.rooms {
		room.removeClient(ctx, client)
	}

	delete(this.clients, client.clientId)

	return nil
}

func (this *roomProxy) checkAck(ctx context.Context, ackedCount int32) error {
	if this.lastAck == nil {
		return nil
	}

	if ackedCount == int32(len(this.clients)) {
		if err := this.lastAck(ctx); err != nil {
			return err
		}

		this.ackedClientsCount = 0
		this.lastAck = nil
	}

	return nil
}

func (this *roomProxy) addClient(ctx context.Context, client *ClientHandle) error {
	if _, ok := this.clients[client.clientId]; ok {
		return nil
	}

	this.clients[client.clientId] = client
	atomic.AddInt32(&this.ackedClientsCount, 1)
	client.rooms[this.roomId] = this

	if len(this.clients) == 1 {
		this.roomProxyMgr.redisQueueClient.Subscribe(
			ctx,
			this.roomId,
			func(ctx context.Context, msg *Message) error {
				this.mu.RLock()
				defer this.mu.RUnlock()

				if msg.Ack != nil {
					this.lastAck = msg.Ack

					msg.Ack = func(ctx context.Context) error {
						this.mu.RLock()
						defer this.mu.RUnlock()

						ackedCount := atomic.AddInt32(&this.ackedClientsCount, 1)

						return this.checkAck(ctx, ackedCount)
					}
				}

				for _, client := range this.clients {
					if err := client._processMsg(ctx, msg); err != nil {
						// TODO: Log error and ignore
					}
				}

				return nil
			})
	}

	return nil
}

func (this *roomProxy) removeClient(ctx context.Context, client *ClientHandle) error {
	if _, ok := this.clients[client.clientId]; !ok {
		return nil
	}

	delete(client.rooms, this.roomId)
	delete(this.clients, client.clientId)
	ackedCount := atomic.AddInt32(&this.ackedClientsCount, -1)

	if len(this.clients) == 0 {
		return this.roomProxyMgr.removeRoom(ctx, this.roomId)
	} else {
		this.checkAck(ctx, ackedCount)
	}

	return nil
}

func (this *roomProxyMgr) _createRoom(ctx context.Context, roomId string) (*roomProxy, error) {
	result := &roomProxy{}

	result.roomProxyMgr = this
	result.roomId = roomId
	result.clients = make(map[string]*ClientHandle)
	result.ackedClientsCount = 0

	return result, nil
}

func (client *ClientHandle) _processMsg(ctx context.Context, msg *Message) error {
	return client.options.MessageHandler(ctx, msg)
}

func (this *ClientHandle) AddRoom(ctx context.Context, roomId string) (Room, error) {
	this.mu.Lock()
	defer this.mu.Unlock()

	if room, ok := this.rooms[roomId]; ok {
		return room, nil
	} else {
		if room, err := this.roomProxyMgr.getOrCreateRoom(ctx, roomId); err != nil {
			return nil, err
		} else {
			if err := room.addClient(ctx, this); err != nil {
				this.roomProxyMgr.removeRoom(ctx, room.roomId)

				return nil, err
			} else {
				return room, nil
			}
		}
	}
}

func (this *ClientHandle) RemoveRoom(ctx context.Context, roomId string) error {
	this.mu.Lock()
	defer this.mu.Unlock()

	if room, ok := this.rooms[roomId]; ok {
		return room.removeClient(ctx, this)
	} else {
		return fmt.Errorf("Room '%s' not found", roomId)
	}
}

func (this *roomProxyMgr) GetMetrics(ctx context.Context, options *GetMetricsOptions) (*Metrics, error) {
	return this.redisQueueClient.GetMetrics(ctx, options)
}

func (this *roomProxyMgr) Send(ctx context.Context, room string, data interface{}, priority int) error {
	return this.redisQueueClient.Send(ctx, room, data, priority)
}

func (this *roomProxyMgr) SendOutOfBand(ctx context.Context, room string, data interface{}) error {
	return this.redisQueueClient.SendOutOfBand(ctx, room, data)
}

func (this *roomProxyMgr) Peek(ctx context.Context, room string, offset int, limit int) ([]*Message, error) {
	return this.redisQueueClient.Peek(ctx, room, offset, limit)
}
