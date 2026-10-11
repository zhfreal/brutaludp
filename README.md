# brutaludp

[![Go Reference](https://pkg.go.dev/badge/github.com/zhfreal/brutaludp.svg)](https://pkg.go.dev/github.com/zhfreal/brutaludp)
[![Go Report Card](https://goreportcard.com/badge/github.com/zhfreal/brutaludp)](https://goreportcard.com/report/github.com/zhfreal/brutaludp)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Zero Dependencies](https://img.shields.io/badge/dependencies-0-success.svg)](go.mod)

**`brutaludp`** is a high-performance, **zero-external-dependency** Go module providing **TCP Brutal v2-style Group Traffic Control**, lock-free **Earliest Departure Time (EDT)** virtual clock packet pacing, and empirical loss compensation for user-space UDP datagrams (`net.PacketConn`) and user-space QUIC congestion controllers.

---

## Background & Motivation

The Linux kernel module [`tcp-brutal`](https://github.com/apernet/tcp-brutal) enforces bandwidth pacing and fixed congestion window semantics directly at the OS TCP socket layer. However, **kernel TCP congestion control cannot meter UDP datagrams**. 

For modern UDP-based tunneling protocols—such as **QUIC** (Hysteria 2, TUIC), **Queqiao multi-lane port hopping**, and **HTTP/3 (SplitHTTP / XHTTP)**—user-space rate pacing is critical to prevent:
1. **Multi-Flow Rate Amplification**: When multiple concurrent QUIC streams, lanes, or connections run in parallel, unpooled pacers each transmit at the full configured rate multiplier (e.g. 10 flows $\times$ 100 Mbps = 1 Gbps), causing instant link saturation and catastrophic self-congestion.
2. **Timer Thrashing & Jitter**: Traditional user-space token buckets rely on micro-timers that thrash the Go runtime scheduler under high packet rates.
3. **NTP Stepping Vulnerability**: Pacers tied to wall-clock time (`time.Now()`) stall or overrun whenever system clocks step via NTP or daylight saving changes.

`brutaludp` solves these problems entirely in user space using a **shared virtual clock**, **monotonically anchored time**, and **lock-free atomic pacing**.

---

## Core Architecture

```
                  ┌─────────────────────────────────────────┐
                  │           Application Traffic           │
                  │   (QUIC CC / Multi-Lane / UDP Proxy)    │
                  └──────┬───────────────────────────┬──────┘
                         │                           │
            Flow 1       ▼                           ▼       Flow 2
     ┌───────────────────────────────┐       ┌───────────────────────────────┐
     │ wancongestion.BrutalSender    │       │ wancongestion.BrutalSender    │
     │   (or PacedPacketConn #1)     │       │   (or PacedPacketConn #2)     │
     └───────────────┬───────────────┘       └───────────────┬───────────────┘
                     │                                       │
                     └───────────────────┬───────────────────┘
                                         ▼
                     ┌───────────────────────────────────────┐
                     │          brutaludp.Group              │
                     │  • Lock-Free Atomic Virtual Clock     │
                     │  • GroupMaxLag (2ms burst clamp)      │
                     │  • GroupMaxHorizon (100ms anti-OOM)   │
                     │  • 4s Epoch Loss Tracker (0.80 floor) │
                     └───────────────────┬───────────────────┘
                                         ▼
                     ┌───────────────────────────────────────┐
                     │         brutaludp.GroupManager        │
                     │  • Reference Counting & Eviction      │
                     │  • Directional Uplink/Downlink Namesp │
                     │  • MaxGroups = 32768 (DoS protection) │
                     └───────────────────────────────────────┘
```

### Key Architectural Highlights

* **100% Go Standard Library**: Relies strictly on `sync`, `sync/atomic`, `time`, `net`, and `syscall`. Zero third-party dependencies.
* **Lock-Free Pacing Hot Path**: Pacing and departure scheduling use atomic `CompareAndSwapInt64` over nanosecond monotonic time (`time.Since(startTime).Nanoseconds()`).
* **Burst Clamping (`GroupMaxLag = 2ms`)**: Restricts accumulated idle burst credits to 2 milliseconds, matching Linux kernel `tcp-brutal` semantics.
* **Anti-OOM Forward Horizon (`GroupMaxHorizon = 100ms`)**: Binds maximum scheduling latency to 100ms; excess datagrams during severe network backpressure are tail-dropped instead of causing unbounded memory growth.
* **4-Epoch Ring Buffer Loss Compensation**: Tracks ACK/loss feedback over a sliding 4-second epoch window. Adapts effective pacing rate upward when packet loss occurs, bounded by an 80% ACK floor (`MinAckRate = 0.80`, capping compensation boost at $1.25\times$).
* **Directional Namespace Isolation**: Distinguishes uplink and downlink rate pools using bit 63 (`DirectionDownlinkFlag`), preventing asymmetric broadband connections (e.g. 50M up / 500M down) from colliding on the same group ID.
* **32-Bit Platform Safe**: 64-bit atomic integers (`atomic.Int64`, `atomic.Uint64`) are positioned at struct offset 0 to guarantee 8-byte memory alignment on ARMv7, MIPS, and 32-bit x86.
* **`PacedPacketConn` Micro-Burst Smoothing**: Sleeps *outside* the internal lock with a 1ms micro-burst threshold (`MinSleepThreshold = 1ms`), preventing timer thrashing under high packet loads.

---

## Installation

```bash
go get github.com/zhfreal/brutaludp
```

---

## Usage Examples

### 1. Pacing Raw UDP Datagrams (`PacedPacketConn`)

Wrap any standard `net.PacketConn` to enforce group bandwidth metering:

```go
package main

import (
	"log"
	"net"

	"github.com/zhfreal/brutaludp"
)

func main() {
	// Listen on a standard UDP socket
	rawConn, err := net.ListenPacket("udp", ":0")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	// Allocate a shared group: Group ID 1001, Rate: 50 MB/s (400 Mbps)
	rateBytesPerSec := uint64(50 * 1024 * 1024)
	group := brutaludp.DefaultManager.GetOrCreate(1001, rateBytesPerSec)
	defer group.Release()

	// Wrap in PacedPacketConn
	pacedConn := brutaludp.NewPacedPacketConn(rawConn, group)
	defer pacedConn.Close()

	// Transmissions are now smoothly paced according to the shared virtual clock
	dstAddr, _ := net.ResolveUDPAddr("udp", "198.51.100.1:443")
	payload := make([]byte, 1200)

	if _, err := pacedConn.WriteTo(payload, dstAddr); err != nil {
		log.Printf("write error: %v", err)
	}
}
```

---

### 2. QUIC Congestion Control Integration

Integrate `brutaludp.Group` into custom QUIC senders (such as `quic-go` congestion controllers) to coordinate bandwidth across multiple concurrent streams:

```go
package quiccc

import (
	"context"
	"time"

	"github.com/zhfreal/brutaludp"
)

type PooledBrutalSender struct {
	group                   *brutaludp.Group
	disableLossCompensation bool
}

func NewPooledBrutalSender(groupID uint64, targetRate uint64, ctx context.Context) *PooledBrutalSender {
	group := brutaludp.DefaultManager.GetOrCreate(groupID, targetRate)

	// Cleanly release group reference when connection closes
	context.AfterFunc(ctx, func() {
		group.Release()
	})

	return &PooledBrutalSender{group: group}
}

// OnPacketSent updates the virtual clock and returns departure delay
func (s *PooledBrutalSender) OnPacketSent(sentBytes uint64) (delay time.Duration, drop bool) {
	delayNano, drop := s.group.OnPacketSent(sentBytes)
	if drop {
		return 0, true
	}
	return time.Duration(delayNano), false
}

// OnCongestionEvent feeds ACK and packet loss feedback into the empirical loss tracker
func (s *PooledBrutalSender) OnCongestionEvent(ackedCount, lostCount uint64) {
	s.group.RecordFeedback(ackedCount, lostCount)
}
```

---

### 3. Directional Group Isolation for Asymmetric Links

Ensure uplink and downlink bandwidth pools operate independently without ID collisions:

```go
package main

import "github.com/zhfreal/brutaludp"

func setupAsymmetricPools(tenantID uint64, upRate, downRate uint64) (*brutaludp.Group, *brutaludp.Group) {
	upGroupID := brutaludp.GroupIDForUplink(tenantID)
	downGroupID := brutaludp.GroupIDForDownlink(tenantID)

	uplinkGroup := brutaludp.DefaultManager.GetOrCreate(upGroupID, upRate)
	downlinkGroup := brutaludp.DefaultManager.GetOrCreate(downGroupID, downRate)

	return uplinkGroup, downlinkGroup
}
```

---

## API Overview

### `GroupManager`
* `DefaultManager`: Global singleton instance with standard retention TTL (30s) and automatic idle eviction.
* `NewGroupManager(idleTTL time.Duration)`: Creates an isolated group registry.
* `GetOrCreate(id uint64, rateBytesPerSec uint64) *Group`: Thread-safely acquires or registers a shared rate group. Returns `nil` if `MaxGroups` (32768) ceiling is exceeded.
* `Len() int`: Returns currently active group count.
* `Close()`: Shuts down the background eviction worker.

### `Group`
* `Acquire()` / `Release()`: Atomic reference-counting lifecycle.
* `OnPacketSent(packetBytes uint64) (delayNano int64, drop bool)`: Hot-path EDT virtual clock progression.
* `RecordFeedback(ackedCount, lostCount uint64)`: Submits loss and ACK sample counts to the sliding 4s ring buffer.
* `SetRate(rateBytesPerSec uint64)`: Dynamically reconfigures base pacing rate.
* `GetRate() uint64` / `EffectiveRate() uint64`: Lock-free inspection of base and compensated pacing rates.
* `SentBytes() uint64`: Cumulative bytes transmitted through the group.

### `PacedPacketConn`
* `NewPacedPacketConn(conn net.PacketConn, group *Group) *PacedPacketConn`: Instantiates the pacing wrapper.
* `WriteTo(p []byte, addr net.Addr) (int, error)`: Paced transmission to unaddressed endpoints.
* `Write(p []byte) (int, error)`: Paced transmission for connected UDP sockets.
* `DroppedPackets() uint64`: Telemetry reporting datagrams dropped due to forward horizon saturation.
* `Upstream() any`: Returns underlying unwrapped `net.PacketConn`.
* `SyscallConn() (syscall.RawConn, error)`: Exposes raw socket descriptor for `setsockopt` / eBPF inspection.

---

## Verification & Testing

The package includes comprehensive unit tests verifying virtual clock math, concurrency, loss compensation, and 32-bit alignment safety:

```bash
# Run all unit tests
go test -v ./...

# Run race detector
go test -race ./...
```

All 14 test suites pass with **zero data races** and **zero allocations on the pacing hot path**.

---

## License

This project is licensed under the [MIT License](LICENSE).
