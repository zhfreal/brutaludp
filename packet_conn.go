package brutaludp

import (
	"io"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	MaxBurstPackets                = 10
	MinSleepThreshold              = 1 * time.Millisecond // Micro-burst smoothing threshold to eliminate timer thrashing
	DefaultPooledTimerInitDuration = time.Hour            // Initial standby duration before stopping pooled timer
)

var pacerTimerPool = sync.Pool{
	New: func() any {
		t := time.NewTimer(DefaultPooledTimerInitDuration)
		t.Stop()
		return t
	},
}

// PacedPacketConn wraps net.PacketConn to meter raw UDP datagrams without head-of-line blocking.
type PacedPacketConn struct {
	// 8-byte aligned 64-bit atomics placed first for guaranteed alignment on 32-bit platforms
	droppedPackets        atomic.Uint64
	minSleepThresholdNano atomic.Int64 // Micro-burst smoothing threshold in nanoseconds

	net.PacketConn
	group     *Group
	closeOnce sync.Once
	closeChan chan struct{}
	closed    atomic.Bool
}

// DroppedPackets returns the total count of datagrams tail-dropped due to pacer forward horizon saturation.
func (c *PacedPacketConn) DroppedPackets() uint64 {
	return c.droppedPackets.Load()
}

// SetMinSleepThreshold configures the minimum delay before sleeping, smoothing micro-bursts without scheduler churn.
func (c *PacedPacketConn) SetMinSleepThreshold(d time.Duration) {
	if d <= 0 {
		d = MinSleepThreshold
	}
	c.minSleepThresholdNano.Store(int64(d))
}

// MinSleepThreshold returns the effective minimum sleep threshold.
func (c *PacedPacketConn) MinSleepThreshold() time.Duration {
	d := c.minSleepThresholdNano.Load()
	if d <= 0 {
		return MinSleepThreshold
	}
	return time.Duration(d)
}

func NewPacedPacketConn(conn net.PacketConn, group *Group) *PacedPacketConn {
	c := &PacedPacketConn{
		PacketConn: conn,
		group:      group,
		closeChan:  make(chan struct{}),
	}
	c.minSleepThresholdNano.Store(int64(MinSleepThreshold))
	return c
}

func (c *PacedPacketConn) sleepWithClose(delay time.Duration) error {
	if c.closed.Load() {
		return net.ErrClosed
	}
	t := pacerTimerPool.Get().(*time.Timer)
	t.Reset(delay)
	select {
	case <-t.C:
		pacerTimerPool.Put(t)
		if c.closed.Load() {
			return net.ErrClosed
		}
		return nil
	case <-c.closeChan:
		if !t.Stop() {
			select {
			case <-t.C:
			default:
			}
		}
		pacerTimerPool.Put(t)
		return net.ErrClosed
	}
}

// Upstream exposes the underlying wrapped PacketConn for transparent wrapper inspection.
func (c *PacedPacketConn) Upstream() any {
	return c.PacketConn
}

// SyscallConn exposes the raw socket file descriptor for setsockopt and BPF attachment if supported.
func (c *PacedPacketConn) SyscallConn() (syscall.RawConn, error) {
	if c.PacketConn == nil {
		return nil, syscall.EINVAL
	}
	if sc, ok := c.PacketConn.(syscall.Conn); ok {
		return sc.SyscallConn()
	}
	return nil, syscall.EINVAL
}

func (c *PacedPacketConn) LocalAddr() net.Addr {
	if c.PacketConn == nil {
		return nil
	}
	return c.PacketConn.LocalAddr()
}

func (c *PacedPacketConn) RemoteAddr() net.Addr {
	if c.PacketConn == nil {
		return nil
	}
	if r, ok := c.PacketConn.(interface{ RemoteAddr() net.Addr }); ok {
		return r.RemoteAddr()
	}
	return nil
}

func (c *PacedPacketConn) SetDeadline(t time.Time) error {
	if c.closed.Load() {
		return net.ErrClosed
	}
	if c.PacketConn == nil {
		return net.ErrClosed
	}
	return c.PacketConn.SetDeadline(t)
}

func (c *PacedPacketConn) SetReadDeadline(t time.Time) error {
	if c.closed.Load() {
		return net.ErrClosed
	}
	if c.PacketConn == nil {
		return net.ErrClosed
	}
	return c.PacketConn.SetReadDeadline(t)
}

func (c *PacedPacketConn) SetWriteDeadline(t time.Time) error {
	if c.closed.Load() {
		return net.ErrClosed
	}
	if c.PacketConn == nil {
		return net.ErrClosed
	}
	return c.PacketConn.SetWriteDeadline(t)
}

func (c *PacedPacketConn) WriteTo(p []byte, addr net.Addr) (int, error) {
	if c.closed.Load() {
		return 0, net.ErrClosed
	}
	if c.PacketConn == nil {
		return 0, net.ErrClosed
	}
	if c.group != nil {
		nowNano := monotonicNano()
		delay, ok := c.group.Reserve(nowNano, len(p))
		if !ok {
			// Tail-drop packet when pacer forward horizon is saturated.
			// Prevents unbounded goroutine sleep stack explosion and buffer bloat.
			c.droppedPackets.Add(1)
			return len(p), nil
		}
		minSleep := c.MinSleepThreshold()
		if delay >= minSleep {
			if err := c.sleepWithClose(delay); err != nil {
				c.group.CancelReservation(len(p))
				return 0, err
			}
		}
	}
	if c.closed.Load() {
		if c.group != nil {
			c.group.CancelReservation(len(p))
		}
		return 0, net.ErrClosed
	}
	return c.PacketConn.WriteTo(p, addr)
}

func (c *PacedPacketConn) Write(p []byte) (int, error) {
	if c.closed.Load() {
		return 0, net.ErrClosed
	}
	if c.PacketConn == nil {
		return 0, net.ErrClosed
	}
	if conn, ok := c.PacketConn.(io.Writer); ok {
		if c.group != nil {
			nowNano := monotonicNano()
			delay, ok := c.group.Reserve(nowNano, len(p))
			if !ok {
				c.droppedPackets.Add(1)
				return len(p), nil
			}
			minSleep := c.MinSleepThreshold()
			if delay >= minSleep {
				if err := c.sleepWithClose(delay); err != nil {
					c.group.CancelReservation(len(p))
					return 0, err
				}
			}
		}
		if c.closed.Load() {
			if c.group != nil {
				c.group.CancelReservation(len(p))
			}
			return 0, net.ErrClosed
		}
		return conn.Write(p)
	}
	return 0, syscall.EINVAL
}

func (c *PacedPacketConn) Read(p []byte) (int, error) {
	if c.closed.Load() {
		return 0, net.ErrClosed
	}
	if c.PacketConn == nil {
		return 0, net.ErrClosed
	}
	if len(p) == 0 {
		return 0, nil
	}
	if r, ok := c.PacketConn.(io.Reader); ok {
		return r.Read(p)
	}
	return 0, syscall.EINVAL
}

func (c *PacedPacketConn) ReadFrom(p []byte) (int, net.Addr, error) {
	if c.closed.Load() {
		return 0, nil, net.ErrClosed
	}
	if c.PacketConn == nil {
		return 0, nil, net.ErrClosed
	}
	if len(p) == 0 {
		return 0, nil, nil
	}
	return c.PacketConn.ReadFrom(p)
}

func (c *PacedPacketConn) Close() error {
	var err error
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		if c.closeChan != nil {
			close(c.closeChan)
		}
		if c.group != nil {
			c.group.Release()
		}
		if c.PacketConn != nil {
			err = c.PacketConn.Close()
		}
	})
	return err
}
