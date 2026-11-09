# brutaludp

[![Go Reference](https://pkg.go.dev/badge/github.com/zhfreal/brutaludp.svg)](https://pkg.go.dev/github.com/zhfreal/brutaludp)
[![Go Report Card](https://goreportcard.com/badge/github.com/zhfreal/brutaludp)](https://goreportcard.com/report/github.com/zhfreal/brutaludp)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Zero Dependencies](https://img.shields.io/badge/dependencies-0-success.svg)](go.mod)

**`brutaludp`** is a high-performance, **zero-external-dependency** Go module providing **TCP Brutal v2-style Group Traffic Control**, lock-free **Earliest Departure Time (EDT)** virtual clock packet pacing, and empirical loss compensation for user-space UDP datagrams (`net.PacketConn`) and user-space QUIC congestion controllers.

It serves as a pure Go standard-library implementation and architectural extension of the Linux kernel [`tcp-brutal`](https://github.com/apernet/tcp-brutal) module, bringing group-based bandwidth metering directly to userland network stacks.

---

## Background & Motivation

The original Linux kernel module [`tcp-brutal`](https://github.com/apernet/tcp-brutal) enforces bandwidth pacing and fixed congestion window semantics directly at the OS TCP socket buffer layer via `setsockopt`. However, **kernel TCP congestion control cannot meter UDP datagrams**. 

For modern UDP-based tunneling protocols—such as **QUIC** (Hysteria 2, TUIC), **Xlane multi-lane bonded transport**, **port hopping (`HopConn`)**, and **HTTP/3 (SplitHTTP / XHTTP)**—user-space rate pacing is critical to prevent:
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

* **100% Go Standard Library**: Relies strictly on `sync`, `sync/atomic`, `time`, `net`, and `syscall`. Zero external dependencies.
* **Lock-Free Pacing Hot Path**: Pacing and departure scheduling use atomic `CompareAndSwapInt64` over nanosecond monotonic time (`time.Since(startTime).Nanoseconds()`).
* **Burst Clamping (`GroupMaxLag = 2ms`)**: Restricts accumulated idle burst credits to 2 milliseconds, matching Linux kernel `tcp-brutal` semantics.
* **Anti-OOM Forward Horizon (`GroupMaxHorizon = 100ms`)**: Binds maximum scheduling latency to 100ms; excess datagrams during severe network backpressure are tail-dropped instead of causing unbounded memory growth.
* **4-Epoch Ring Buffer Loss Compensation**: Tracks ACK/loss feedback over a sliding 4-second epoch window. Adapts effective pacing rate upward when packet loss occurs, bounded by an 80% ACK floor (`MinAckRate = 0.80`, capping compensation boost at $1.25\times$).
* **Directional Namespace Isolation**: Distinguishes uplink and downlink rate pools using bit 63 (`DirectionDownlinkFlag`), preventing asymmetric broadband connections (e.g. 50M up / 500M down) from colliding on the same group ID.
* **32-Bit Platform Safe**: 64-bit atomic integers (`atomic.Int64`, `atomic.Uint64`) are positioned at struct offset 0 to guarantee 8-byte memory alignment on ARMv7, MIPS, and 32-bit x86.
* **`PacedPacketConn` Micro-Burst Smoothing**: Sleeps *outside* the internal lock with a 1ms micro-burst threshold (`MinSleepThreshold = 1ms`), preventing timer thrashing under high packet loads.

---

## Detailed Design & Capabilities

### 1. Lock-Free EDT Monotonic Virtual Clock (`group.go`)
* **Strict Monotonic Anchor**:
  - Anchors pacing against process startup monotonic duration (`time.Since(startTime).Nanoseconds()`).
  - Completely immune to NTP adjustments, system wall-clock stepping, and daylight saving time jumps.
* **Lock-Free Pacing Hot Path (`Group.Reserve`)**:
  - Atomically calculates departure timestamps via CAS on `nextTimeNano`.
  - Enforces burst lag credit clamping (`GroupMaxLag = 2ms` by default, tunable via `SetMaxLag`) matching kernel `GROUP_MAX_LAG_NS`.
  - Enforces forward scheduling horizon clamping (`GroupMaxHorizon = 100ms` by default, tunable via `SetMaxHorizon`). Tail-drops excess datagrams under network backpressure, preventing unbounded goroutine sleeps and buffer bloat.
* **Overflow Protection (`MaxSafePacketBytes`)**:
  - Bounds byte counts to 64 MiB (`MaxSafePacketBytes = 64 * 1024 * 1024`), eliminating `int64` overflow in burst duration calculations (`bytes * NanosPerSecond`).

### 2. 4-Epoch Ring Buffer Loss Compensation (`group.go`)
* **Sliding Epoch Loss Tracking**:
  - Maintains a 4-epoch circular buffer (`PktInfoSlots = 4`) tracking ACK and packet loss telemetry.
  - Dynamically boosts pacing rate when loss occurs, bounded by an 80% ACK rate floor (`MinAckRate = 0.80`, capping rate boost at $1.25\times$).
  - Ignores loss feedback when total samples are below statistical confidence threshold (`MinFeedbackSlots = 50`), falling back to baseline 100% rate (`DefaultAckRate = 1.0`).

### 3. Directional Namespace Isolation & Multi-Tenancy (`group.go`)
* **Directional Partitioning (`DirectionDownlinkFlag`)**:
  - Partitions 64-bit group IDs using bit 63 (`DirectionDownlinkFlag = 1 << 63`).
  - Isolates uplink and downlink pacing pools across asymmetric broadband links (e.g. 50 Mbps up / 500 Mbps down) sharing the same identifier.
* **Capacity Guardrails & DoS Protection (`GroupManager`)**:
  - Enforces maximum concurrent group ceilings (`MaxGroups = 32768`, tunable via `SetMaxGroups`).
  - Provides throttled emergency sweeps (`EmergencySweepMinInterval = 200ms`) and background idle reaping (`DefaultJanitorInterval = 10s`, `DefaultIdleTTL = 30s`, tunable via `SetIdleTTL`).
  - Thread-safe lazy janitor startup: `GetOrCreate` triggers `StartJanitor` with fallback reading of `idleTTL` safely guarded under `m.mu.Lock()`, eliminating data races during concurrent group initialization.
  - Automatic hash map compaction (`compactLocked`) when tombstoned entries exceed thresholds (`MinMapCompactDeletedThreshold = 512`, `MapCompactRatioThreshold = 2`).

### 4. User-Space Datagram Pacer (`packet_conn.go`)
* **Micro-Burst Smoothing (`PacedPacketConn`)**:
  - Sleeps strictly *outside* the pacing lock with a configurable micro-burst threshold (`MinSleepThreshold = 1ms`, tunable via `SetMinSleepThreshold`), preventing scheduler thrashing.
  - Tail-dropped datagrams are tracked via atomic telemetry (`DroppedPackets()`).
* **Complete Interface Conformance & Nil Safety**:
  - Delegates `LocalAddr()`, `RemoteAddr()`, `SetDeadline()`, `SetReadDeadline()`, `SetWriteDeadline()`, `ReadFrom()`, `WriteTo()`, `Write()`, and `SyscallConn()`.
  - Guarded against nil underlying connections and closed state (`net.ErrClosed`).

---

## Tunable Parameters & Defaults Matrix

| Parameter Constant | Default Value | Tunable Method | Description |
| :--- | :--- | :--- | :--- |
| `GroupMaxLag` | 2 ms | `Group.SetMaxLag` / `Manager.SetDefaultMaxLag` | Maximum accumulated burst credit during idle periods. |
| `GroupMaxHorizon` | 100 ms | `Group.SetMaxHorizon` / `Manager.SetDefaultMaxHorizon` | Maximum forward departure scheduling horizon before tail drop. |
| `MinSleepThreshold` | 1 ms | `PacedPacketConn.SetMinSleepThreshold` | Delay floor below which packets are transmitted without sleeping. |
| `MaxGroups` | 32,768 | `GroupManager.SetMaxGroups` | Maximum concurrent group allocation limit preventing memory exhaustion. |
| `DefaultIdleTTL` | 30 s | `GroupManager.SetIdleTTL` | Retention timeout for unreferenced groups before eviction. |
| `DefaultJanitorInterval` | 10 s | `GroupManager.StartJanitor` | Background reaping interval for idle groups. |
| `DefaultCwndGain` | 20 (2.0x) | `Group.SetCwndGain` | Multiplier for congestion window sizing in tenths. |
| `BrutalMinRate` | 64 KiB/s | Internal floor | Minimum enforced transmission rate floor. |
| `MinAckRate` | 0.80 | Internal floor | Minimum loss compensation ACK rate clamp (1.25x maximum boost). |

---

## Source File Matrix

| File Path | Component | Description |
| :--- | :--- | :--- |
| `group.go` | Core Engine | EDT virtual clock, lock-free CAS pacing, sliding-epoch loss compensation, directional namespace isolation, group lifecycle manager, and user-tunable lag/horizon/group setters. |
| `group_test.go` | Test Suite | Unit tests for virtual clock accuracy, burst limits, forward horizon tail drops, 32-bit atomic alignment, concurrency stress, and tunable lag/horizon verification. |
| `packet_conn.go` | Network Wrapper | `PacedPacketConn` wrapping `net.PacketConn`, micro-burst smoothing, tail-drop telemetry, user-tunable sleep threshold, and comprehensive nil/closed error guards. |
| `packet_conn_test.go` | Test Suite | Unit tests for packet pacing, micro-bursts, tail drops, concurrent close safety, and tunable sleep thresholds. |

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

### 4. Customizing Guardrails & Burst Latency

Tune pacing constraints for high-throughput or low-latency environments:

```go
package main

import (
	"time"

	"github.com/zhfreal/brutaludp"
)

func configureCustomGuardrails() {
	// Configure global manager defaults
	brutaludp.DefaultManager.SetMaxGroups(65536)
	brutaludp.DefaultManager.SetIdleTTL(60 * time.Second)
	brutaludp.DefaultManager.SetDefaultMaxLag(5 * time.Millisecond)       // Allow up to 5ms idle burst accumulation
	brutaludp.DefaultManager.SetDefaultMaxHorizon(200 * time.Millisecond) // Allow up to 200ms scheduling horizon

	group := brutaludp.DefaultManager.GetOrCreate(2002, 100*1024*1024)
	defer group.Release()

	// Override per-group limits dynamically
	group.SetMaxLag(1 * time.Millisecond)
	group.SetMaxHorizon(50 * time.Millisecond)
	group.SetCwndGain(25) // 2.5x gain
}
```

---

## API Overview

### `GroupManager`
* `DefaultManager`: Global singleton instance with standard retention TTL (30s) and automatic idle eviction.
* `NewGroupManager(idleTTL ...time.Duration)`: Creates an isolated group registry with optional idle TTL.
* `GetOrCreate(id uint64, rateBytesPerSec uint64, cwndGain ...uint32) *Group`: Thread-safely acquires or registers a shared rate group. Returns `nil` if `MaxGroups` (32,768) ceiling is exceeded.
* `Len() int`: Returns currently active group count.
* `Close()`: Shuts down the background eviction worker.
* `SetMaxGroups(max int)`: Configures the maximum concurrent group ceiling.
* `SetIdleTTL(ttl time.Duration)`: Configures group idle eviction duration.
* `SetDefaultMaxLag(lag time.Duration)`: Configures default maximum idle burst lag.
* `SetDefaultMaxHorizon(horizon time.Duration)`: Configures default forward horizon.

### `Group`
* `Acquire()` / `Release()`: Atomic reference-counting lifecycle.
* `Reserve(nowNano int64, bytes int) (delay time.Duration, ok bool)`: Lock-free CAS reservation on the EDT monotonic virtual clock with anti-OOM forward horizon tail-drop backpressure.
* `OnPacketSent(now time.Time, bytes int)`: Lock-free post-send byte commitment to the group virtual clock for QUIC/external congestion controllers.
* `HasPacingBudget(now ...time.Time) bool` / `TimeUntilSend(now ...time.Time) time.Duration`: Lock-free departure budget inspection.
* `RecordFeedback(ackedCount, lostCount uint64)`: Submits loss and ACK sample counts to the sliding 4s ring buffer with saturating uint64 overflow protection.
* `SetRate(rateBytesPerSec uint64)`: Dynamically reconfigures base pacing rate (clamped to `BrutalMinRate`).
* `GetRate() uint64` / `EffectiveRate() uint64`: Lock-free inspection of base and loss-compensated pacing rates.
* `SetMaxLag(lag time.Duration)` / `MaxLag() time.Duration`: Configures per-group idle burst accumulation ceiling.
* `SetMaxHorizon(horizon time.Duration)` / `MaxHorizon() time.Duration`: Configures per-group forward scheduling horizon.
* `SetCwndGain(gain uint32)` / `GetCwndGain() uint32` / `CwndGainVal() uint32`: Configures congestion window multiplier in tenths.
* `SentBytes() uint64`: Cumulative bytes transmitted through the group.

### `PacedPacketConn`
* `NewPacedPacketConn(conn net.PacketConn, group *Group) *PacedPacketConn`: Instantiates the pacing wrapper with 8-byte aligned 64-bit atomics and an interruptible `closeChan`.
* `WriteTo(p []byte, addr net.Addr) (int, error)`: Paced transmission to unaddressed endpoints using pooled `time.Timer` instances (`pacerTimerPool`) that wake immediately with `net.ErrClosed` if `Close()` is called during a pacing sleep.
* `Write(p []byte) (int, error)`: Paced transmission for connected UDP sockets with immediate `Close()` interruption.
* `SetMinSleepThreshold(threshold time.Duration)`: Configures the micro-burst sleep smoothing floor.
* `DroppedPackets() uint64`: Telemetry reporting datagrams dropped due to forward horizon saturation.
* `Upstream() any`: Returns underlying unwrapped `net.PacketConn`.
* `SyscallConn() (syscall.RawConn, error)`: Exposes raw socket descriptor for `setsockopt` / eBPF inspection.

---

## Verification & Testing

The package includes comprehensive unit tests verifying virtual clock math, concurrency, loss compensation, and 32-bit alignment safety:

```bash
# Run all unit tests
go test -v ./...

# Run race detector with zero cache
go test -count=1 -race ./...
```

All test suites pass with **zero data races** and **zero allocations on the pacing hot path**.

---

## License

This project is licensed under the [MIT License](LICENSE).
