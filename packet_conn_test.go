package brutaludp

import (
	"net"
	"sync"
	"testing"
	"time"
)

type mockPacketConn struct {
	net.PacketConn
	writtenPackets int
	closed         sync.Mutex
	isClosed       bool
}

func (m *mockPacketConn) WriteTo(p []byte, addr net.Addr) (int, error) {
	m.closed.Lock()
	defer m.closed.Unlock()
	if m.isClosed {
		return 0, net.ErrClosed
	}
	m.writtenPackets++
	return len(p), nil
}

func (m *mockPacketConn) Close() error {
	m.closed.Lock()
	defer m.closed.Unlock()
	m.isClosed = true
	return nil
}

func TestPacedPacketConnMicroBurstAndWriteTo(t *testing.T) {
	manager := &GroupManager{groups: make(map[uint64]*Group)}
	defer manager.Close()

	g := manager.GetOrCreate(201, 100*1024*1024) // 100 MB/s
	mock := &mockPacketConn{}
	conn := NewPacedPacketConn(mock, g)
	defer conn.Close()

	addr, _ := net.ResolveUDPAddr("udp", "127.0.0.1:12345")
	buf := make([]byte, 1400)

	// Sending small packet should dispatch without noticeable sleep delay (< 1ms threshold)
	start := time.Now()
	n, err := conn.WriteTo(buf, addr)
	elapsed := time.Since(start)

	if err != nil || n != len(buf) {
		t.Fatalf("unexpected WriteTo error: n=%d err=%v", n, err)
	}
	if elapsed >= 10*time.Millisecond {
		t.Fatalf("micro-burst packet experienced excessive sleep: %v", elapsed)
	}
	if mock.writtenPackets != 1 {
		t.Fatalf("expected 1 written packet, got %d", mock.writtenPackets)
	}
}

func TestPacedPacketConnIdempotentClose(t *testing.T) {
	manager := &GroupManager{groups: make(map[uint64]*Group)}
	defer manager.Close()

	g := manager.GetOrCreate(202, 10*1024*1024)
	mock := &mockPacketConn{}
	conn := NewPacedPacketConn(mock, g)

	initialRef := g.refCount.Load()

	// Double Close should only decrement refCount once
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = conn.Close()
		}()
	}
	wg.Wait()

	if g.refCount.Load() != initialRef-1 {
		t.Fatalf("expected refCount to decrease by exactly 1, initial=%d current=%d", initialRef, g.refCount.Load())
	}

	// Writes after close should fail with net.ErrClosed immediately
	addr, _ := net.ResolveUDPAddr("udp", "127.0.0.1:12345")
	_, err := conn.WriteTo([]byte("test"), addr)
	if err != net.ErrClosed {
		t.Fatalf("expected net.ErrClosed, got %v", err)
	}
}

func TestPacedPacketConnTailDropTelemetry(t *testing.T) {
	manager := &GroupManager{groups: make(map[uint64]*Group)}
	defer manager.Close()

	// Low rate (64 KB/s) to quickly saturate forward horizon (100ms = 6.4 KB)
	g := manager.GetOrCreate(203, 64*1024)
	mock := &mockPacketConn{}
	conn := NewPacedPacketConn(mock, g)
	defer conn.Close()

	addr, _ := net.ResolveUDPAddr("udp", "127.0.0.1:12345")
	packet := make([]byte, 1024)

	// Blast concurrently with 20 workers to overwhelm forward horizon
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 5; j++ {
				_, _ = conn.WriteTo(packet, addr)
			}
		}()
	}
	wg.Wait()

	if conn.DroppedPackets() == 0 {
		t.Fatal("expected DroppedPackets() to be > 0 on horizon saturation")
	}
}

func TestPacedPacketConnConcurrentWriteAndClose(t *testing.T) {
	manager := &GroupManager{groups: make(map[uint64]*Group)}
	defer manager.Close()

	g := manager.GetOrCreate(204, 10*1024*1024)
	mock := &mockPacketConn{}
	conn := NewPacedPacketConn(mock, g)

	addr, _ := net.ResolveUDPAddr("udp", "127.0.0.1:12345")
	var wg sync.WaitGroup

	// Concurrently write and close to ensure zero race conditions or nil dereferences
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_, _ = conn.WriteTo([]byte("payload"), addr)
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(1 * time.Millisecond)
		_ = conn.Close()
	}()

	wg.Wait()
}

func TestPacedPacketConnUnwrapAndAlignment(t *testing.T) {
	mock := &mockPacketConn{}
	conn := NewPacedPacketConn(mock, nil)
	defer conn.Close()

	if conn.Upstream() != mock {
		t.Fatal("Upstream() did not return wrapped packet conn")
	}

	// Verify 64-bit atomic droppedPackets operations
	conn.droppedPackets.Store(42)
	if conn.DroppedPackets() != 42 {
		t.Fatalf("expected DroppedPackets() = 42, got %d", conn.DroppedPackets())
	}
}

func TestPacedPacketConnTunableSleepThreshold(t *testing.T) {
	conn := NewPacedPacketConn(&mockPacketConn{}, nil)
	defer conn.Close()

	if conn.MinSleepThreshold() != MinSleepThreshold {
		t.Fatalf("expected default min sleep %v, got %v", MinSleepThreshold, conn.MinSleepThreshold())
	}
	conn.SetMinSleepThreshold(5 * time.Millisecond)
	if conn.MinSleepThreshold() != 5*time.Millisecond {
		t.Fatalf("expected updated min sleep 5ms, got %v", conn.MinSleepThreshold())
	}
}

func TestPacedPacketConnCloseWakesSleepingWriter(t *testing.T) {
	mgr := NewGroupManager()
	defer mgr.Close()

	g := mgr.GetOrCreate(777, BrutalMinRate)
	g.SetMaxHorizon(500 * time.Millisecond)
	mock := &mockPacketConn{}
	conn := NewPacedPacketConn(mock, g)

	addr := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9999}
	// First burst consumes lag credit and schedules next departure ~200ms in the future
	_, _ = conn.WriteTo(make([]byte, 16384), addr)

	done := make(chan error, 1)
	start := time.Now()
	go func() {
		_, err := conn.WriteTo(make([]byte, 8192), addr)
		done <- err
	}()

	time.Sleep(10 * time.Millisecond)
	_ = conn.Close()

	select {
	case err := <-done:
		if err != net.ErrClosed {
			t.Fatalf("expected net.ErrClosed when closing during pacing sleep, got %v", err)
		}
		if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
			t.Fatalf("Close() took too long to wake sleeping writer: %v", elapsed)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("WriteTo remained blocked after Close()")
	}
}

func TestCancelReservationOnAbortedSleep(t *testing.T) {
	mgr := NewGroupManager()
	defer mgr.Close()

	g := mgr.GetOrCreate(888, BrutalMinRate)
	g.SetMaxHorizon(500 * time.Millisecond)
	mock := &mockPacketConn{}
	conn := NewPacedPacketConn(mock, g)

	addr := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9999}
	_, _ = conn.WriteTo(make([]byte, 16384), addr)
	sentBefore := g.SentBytes()

	done := make(chan error, 1)
	go func() {
		_, err := conn.WriteTo(make([]byte, 8192), addr)
		done <- err
	}()

	time.Sleep(10 * time.Millisecond)
	_ = conn.Close()
	<-done

	if g.SentBytes() != sentBefore {
		t.Fatalf("expected aborted WriteTo reservation to be refunded: sentBefore=%d sentAfter=%d", sentBefore, g.SentBytes())
	}
}
