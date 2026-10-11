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
	MaxBurstPackets   = 10
	MinSleepThreshold = 1 * time.Millisecond // Micro-burst smoothing threshold to eliminate timer thrashing
)

// PacedPacketConn wraps net.PacketConn to meter raw UDP datagrams without head-of-line blocking.
type PacedPacketConn struct {
	droppedPackets atomic.Uint64 // 64-bit atomic placed first for guaranteed 8-byte alignment on 32-bit platforms
	net.PacketConn
	group          *Group
	closeOnce      sync.Once
	closed         atomic.Bool
}

// DroppedPackets returns the total count of datagrams tail-dropped due to pacer forward horizon saturation.
func (c *PacedPacketConn) DroppedPackets() uint64 {
	return c.droppedPackets.Load()
}

func NewPacedPacketConn(conn net.PacketConn, group *Group) *PacedPacketConn {
	return &PacedPacketConn{
		PacketConn: conn,
		group:      group,
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

func (c *PacedPacketConn) RemoteAddr() net.Addr {
	if c.PacketConn == nil {
		return nil
	}
	if r, ok := c.PacketConn.(interface{ RemoteAddr() net.Addr }); ok {
		return r.RemoteAddr()
	}
	return nil
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
		if delay >= MinSleepThreshold {
			if c.closed.Load() {
				return 0, net.ErrClosed
			}
			time.Sleep(delay)
		}
	}
	if c.closed.Load() {
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
			if delay >= MinSleepThreshold {
				if c.closed.Load() {
					return 0, net.ErrClosed
				}
				time.Sleep(delay)
			}
		}
		if c.closed.Load() {
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
	if r, ok := c.PacketConn.(io.Reader); ok {
		return r.Read(p)
	}
	return 0, syscall.EINVAL
}

func (c *PacedPacketConn) Close() error {
	var err error
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		if c.group != nil {
			c.group.Release()
		}
		if c.PacketConn != nil {
			err = c.PacketConn.Close()
		}
	})
	return err
}
