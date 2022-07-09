package redisSyncFanoutQueue

import (
	"context"
	"fmt"
	"sync"
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

func (o *RoomProxyManagerOptions) Validate() error {
	if o == nil {
		return fmt.Errorf("RoomProxyOptions is nil")
	}

	if o.RedisQueueClientProvider == nil {
		return fmt.Errorf("RoomProxyOptions.RedisQueueClientProvider is nil")
	}

	return nil
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

func (this *roomProxyMgr) Close() error {
	this.mu.Lock()
	defer this.mu.Unlock()

	for _, client := range this.clients {
		this.removeClient(context.Background(), client)
	}

	return this.redisQueueClient.Close()
}

func (this *roomProxyMgr) roomEjectedFunc(ctx context.Context, roomId *string) error {
	room, ok := this.rooms[*roomId]

	if !ok {
		return nil
	}

	room.mu.Lock()
	room.mu.Unlock()

	delete(this.rooms, *roomId)

	ejectedClients := []*ClientHandle{}

	for _, client := range room.clients {
		room.removeClient(ctx, client)
		this.removeClient(ctx, client)

		if client.options.ClientEjectedHandler != nil {
			ejectedClients = append(ejectedClients, client)
		}
	}

	for _, client := range ejectedClients {
		if err := client.options.ClientEjectedHandler(ctx, client); err != nil {
			// TODO: Log error and ignore
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
		// Optimistic check succeeded
		return room, nil
	} else {
		// Double-action lock
		this.mu.Lock()
		defer this.mu.Unlock()

		room, ok = this.rooms[roomId]
		if !ok {
			var err error
			room, err = newRoom(ctx, this, roomId)

			if err != nil {
				return nil, err
			}

			this.rooms[roomId] = room
		}

		return room, nil
	}
}

func (this *roomProxyMgr) AddClient(ctx context.Context, options *ClientOptions) (*ClientHandle, error) {
	if client, err := newClientHandle(this, options); err != nil {
		return nil, err
	} else {
		this.mu.Lock()
		defer this.mu.Unlock()

		this.clients[client.clientId] = client

		return client, nil
	}
}

func (this *roomProxyMgr) RemoveClient(ctx context.Context, client *ClientHandle) error {
	this.mu.Lock()
	defer this.mu.Unlock()

	return this.removeClient(ctx, client)
}

func (this *roomProxyMgr) removeClient(ctx context.Context, client *ClientHandle) error {
	for _, room := range client.rooms {
		room.removeClient(ctx, client)
	}

	delete(this.clients, client.clientId)

	return nil
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
