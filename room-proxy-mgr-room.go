package redisSyncFanoutQueue

import (
	"context"
	"sync"
	"sync/atomic"
)

type Room interface {
}

type roomProxy struct {
	mu                sync.RWMutex
	roomProxyMgr      *roomProxyMgr
	roomId            string
	clients           map[string]*ClientHandle
	ackedClientsCount int32
	lastAck           AckMessageFunc
}

func newRoom(ctx context.Context, roomProxyMgr *roomProxyMgr, roomId string) (*roomProxy, error) {
	result := &roomProxy{}

	result.roomProxyMgr = roomProxyMgr
	result.roomId = roomId
	result.clients = make(map[string]*ClientHandle)
	result.ackedClientsCount = 0

	return result, nil
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
					if err := client.processMsg(ctx, msg); err != nil {
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
