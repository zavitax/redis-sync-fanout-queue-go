package redisSyncFanoutQueue

import (
	"context"
	"fmt"
	"sync"

	"github.com/google/uuid"
)

type ClientHandle struct {
	mu           sync.RWMutex
	rooms        map[string]*roomProxy
	clientId     string
	options      *ClientOptions
	roomProxyMgr *roomProxyMgr
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

func newClientHandle(roomProxyMgr *roomProxyMgr, options *ClientOptions) (*ClientHandle, error) {
	if err := options.Validate(); err != nil {
		return nil, err
	}

	client := &ClientHandle{}
	client.clientId = uuid.New().String()
	client.rooms = make(map[string]*roomProxy)
	client.options = options
	client.roomProxyMgr = roomProxyMgr

	return client, nil
}

func (client *ClientHandle) processMsg(ctx context.Context, msg *Message) error {
	return client.options.MessageHandler(ctx, msg)
}

func (this *ClientHandle) GetClientID() string {
	this.mu.RLock()
	defer this.mu.RUnlock()

	return this.clientId
}

func (this *ClientHandle) GetRoomProxyManager() RoomProxyManager {
	this.mu.RLock()
	defer this.mu.RUnlock()

	return this.roomProxyMgr
}

func (this *ClientHandle) GetRooms() []string {
	this.mu.RLock()
	defer this.mu.RUnlock()

	keys := make([]string, len(this.rooms))

	i := 0
	for k := range this.rooms {
		keys[i] = k
		i++
	}

	return keys
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
