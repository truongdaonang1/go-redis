package redis

import (
	"context"
	"net"
	"testing"
	"time"
)

type mockBlockedConn struct {
	net.Conn
	readBlock chan struct{}
	closed    bool
}

func newMockBlockedConn() *mockBlockedConn {
	return &mockBlockedConn{
		readBlock: make(chan struct{}),
	}
}

func (m *mockBlockedConn) Read(b []byte) (n int, err error) {
	<-m.readBlock
	return 0, net.ErrClosed
}

func (m *mockBlockedConn) Write(b []byte) (n int, err error) {
	return len(b), nil
}

func (m *mockBlockedConn) SetDeadline(t time.Time) error      { return nil }
func (m *mockBlockedConn) SetReadDeadline(t time.Time) error  { return nil }
func (m *mockBlockedConn) SetWriteDeadline(t time.Time) error { return nil }
func (m *mockBlockedConn) Close() error {
	if !m.closed {
		m.closed = true
		close(m.readBlock)
	}
	return nil
}

func TestPubSubPingDeadlineWithContext(t *testing.T) {
	ps := NewPubSub(&PubSubOptions{
		ReadTimeout:  50 * time.Millisecond,
		WriteTimeout: 50 * time.Millisecond,
	})
	defer ps.Close()

	mockConn := newMockBlockedConn()
	ps.SetNetConn(mockConn)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := ps.Ping(ctx)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatalf("expected error on blocked connection, got nil")
	}

	if elapsed > 500*time.Millisecond {
		t.Fatalf("PubSub.Ping blocked for too long: %v", elapsed)
	}
}

func TestSentinelFailoverConnectionTermination(t *testing.T) {
	fc := NewFailoverClient(&FailoverOptions{
		MasterName: "mymaster",
	})
	defer fc.Close()

	ps := NewPubSub(&PubSubOptions{
		ReadTimeout: 100 * time.Millisecond,
	})
	mockConn := newMockBlockedConn()
	ps.SetNetConn(mockConn)
	fc.RegisterPubSub(ps)

	fc.OnMasterSwitch("127.0.0.1:6380")

	if !mockConn.closed {
		t.Fatalf("expected old connection to be terminated on master switch")
	}
}
