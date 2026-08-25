package redis

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"
)

type FailoverOptions struct {
	MasterName    string
	SentinelAddrs []string
	Dialer        func(ctx context.Context, addr string) (net.Conn, error)
	ReadTimeout   time.Duration
	WriteTimeout  time.Duration
}

type FailoverClient struct {
	opt        *FailoverOptions
	mu         sync.RWMutex
	masterAddr string
	pubsubs    []*PubSub
	closed     bool
	ctx        context.Context
	cancel     context.CancelFunc
}

func NewFailoverClient(opt *FailoverOptions) *FailoverClient {
	if opt == nil {
		opt = &FailoverOptions{}
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &FailoverClient{
		opt:     opt,
		pubsubs: make([]*PubSub, 0),
		ctx:     ctx,
		cancel:  cancel,
	}
}

func (f *FailoverClient) SetMasterAddr(addr string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.masterAddr = addr
}

func (f *FailoverClient) GetMasterAddr() string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.masterAddr
}

func (f *FailoverClient) RegisterPubSub(ps *PubSub) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pubsubs = append(f.pubsubs, ps)
}

func (f *FailoverClient) OnMasterSwitch(newAddr string) {
	f.mu.Lock()
	f.masterAddr = newAddr
	activePubsubs := make([]*PubSub, len(f.pubsubs))
	copy(activePubsubs, f.pubsubs)
	f.mu.Unlock()

	for _, ps := range activePubsubs {
		if ps != nil {
			cn, _ := ps.getNetConn()
			if cn != nil {
				ps.flagUnhealthy(cn)
			}
		}
	}
}

func (f *FailoverClient) Close() error {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return errors.New("redis: failover client already closed")
	}
	f.closed = true
	f.cancel()
	pubsubs := f.pubsubs
	f.pubsubs = nil
	f.mu.Unlock()

	for _, ps := range pubsubs {
		if ps != nil {
			_ = ps.Close()
		}
	}
	return nil
}
