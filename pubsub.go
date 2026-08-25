package redis

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrClosed        = errors.New("redis: client is closed")
	ErrPingTimeout   = errors.New("redis: pubsub ping timeout")
	ErrConnectionNil = errors.New("redis: connection is nil")
)

type PubSubOptions struct {
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	PingInterval time.Duration
	Dialer       func(ctx context.Context) (net.Conn, error)
}

type Message struct {
	Channel string
	Pattern string
	Payload string
}

type PubSub struct {
	opt      *PubSubOptions
	mu       sync.Mutex
	cn       net.Conn
	closed   uint32
	channels map[string]struct{}
	patterns map[string]struct{}

	msgCh  chan *Message
	errCh  chan error
	pingCh chan string

	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	reconnSem chan struct{}
}

func NewPubSub(opt *PubSubOptions) *PubSub {
	if opt == nil {
		opt = &PubSubOptions{}
	}
	if opt.ReadTimeout == 0 {
		opt.ReadTimeout = 3 * time.Second
	}
	if opt.WriteTimeout == 0 {
		opt.WriteTimeout = 3 * time.Second
	}
	if opt.PingInterval == 0 {
		opt.PingInterval = 5 * time.Second
	}

	ctx, cancel := context.WithCancel(context.Background())
	ps := &PubSub{
		opt:       opt,
		channels:  make(map[string]struct{}),
		patterns:  make(map[string]struct{}),
		msgCh:     make(chan *Message, 100),
		errCh:     make(chan error, 10),
		pingCh:    make(chan string, 10),
		ctx:       ctx,
		cancel:    cancel,
		reconnSem: make(chan struct{}, 1),
	}
	return ps
}

func (c *PubSub) SetNetConn(cn net.Conn) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cn = cn
}

func (c *PubSub) getNetConn() (net.Conn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.isClosed() {
		return nil, ErrClosed
	}
	if c.cn == nil {
		return nil, ErrConnectionNil
	}
	return c.cn, nil
}

func (c *PubSub) isClosed() bool {
	return atomic.LoadUint32(&c.closed) == 1
}

func (c *PubSub) Ping(ctx context.Context, payload ...string) error {
	if c.isClosed() {
		return ErrClosed
	}

	cn, err := c.getNetConn()
	if err != nil {
		return err
	}

	writeDeadline := time.Now().Add(c.opt.WriteTimeout)
	if d, ok := ctx.Deadline(); ok && d.Before(writeDeadline) {
		writeDeadline = d
	}
	_ = cn.SetWriteDeadline(writeDeadline)

	msg := "PING\r\n"
	if len(payload) > 0 {
		msg = fmt.Sprintf("PING %s\r\n", payload[0])
	}

	_, err = cn.Write([]byte(msg))
	if err != nil {
		c.flagUnhealthy(cn)
		return err
	}

	readDeadline := time.Now().Add(c.opt.ReadTimeout)
	if d, ok := ctx.Deadline(); ok && d.Before(readDeadline) {
		readDeadline = d
	}
	_ = cn.SetReadDeadline(readDeadline)

	buf := make([]byte, 128)
	n, err := cn.Read(buf)
	if err != nil {
		c.flagUnhealthy(cn)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return err
	}

	resp := string(buf[:n])
	if len(resp) == 0 {
		c.flagUnhealthy(cn)
		return ErrPingTimeout
	}
	return nil
}

func (c *PubSub) flagUnhealthy(cn net.Conn) {
	c.mu.Lock()
	if c.cn == cn && cn != nil {
		_ = cn.Close()
		c.cn = nil
	}
	c.mu.Unlock()

	select {
	case c.reconnSem <- struct{}{}:
		go c.reconnectRoutine()
	default:
	}
}

func (c *PubSub) reconnectRoutine() {
	defer func() {
		<-c.reconnSem
	}()

	if c.isClosed() || c.opt.Dialer == nil {
		return
	}

	for !c.isClosed() {
		ctx, cancel := context.WithTimeout(c.ctx, 5*time.Second)
		cn, err := c.opt.Dialer(ctx)
		cancel()
		if err == nil && cn != nil {
			c.mu.Lock()
			c.cn = cn
			c.mu.Unlock()
			return
		}
		select {
		case <-c.ctx.Done():
			return
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func (c *PubSub) StartHealthCheck() {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		ticker := time.NewTicker(c.opt.PingInterval)
		defer ticker.Stop()

		for {
			select {
			case <-c.ctx.Done():
				return
			case <-ticker.C:
				ctx, cancel := context.WithTimeout(c.ctx, c.opt.ReadTimeout)
				_ = c.Ping(ctx)
				cancel()
			}
		}
	}()
}

func (c *PubSub) Close() error {
	if !atomic.CompareAndSwapUint32(&c.closed, 0, 1) {
		return ErrClosed
	}
	c.cancel()

	c.mu.Lock()
	if c.cn != nil {
		_ = c.cn.Close()
		c.cn = nil
	}
	c.mu.Unlock()

	c.wg.Wait()
	return nil
}

func (c *PubSub) Channel() <-chan *Message {
	return c.msgCh
}
