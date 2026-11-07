# Architecture & Implementation Statement: brutaludp

**Current Codebase:** `zhfreal/brutaludp` (`main`)  
**Reference Baseline:** User-Space Port & Architectural Extension of TCP Brutal v2 (`apernet/tcp-brutal`)  
**Repository Location:** `./brutaludp` (symlinked as `./brutal-udp`)  
**Status:** Production-Ready, Race-Verified (`go test -race ./...` passing 100%)

---

## 1. Executive Summary

This document specifies the architectural design, algorithmic models, and implementation delta of `brutaludp` relative to the kernel-space reference design of TCP Brutal (`apernet/tcp-brutal`).

While the original Linux kernel `tcp-brutal` module is bound exclusively to TCP sockets at the OS socket buffer layer via `setsockopt`, `brutaludp` is a pure Go standard-library implementation delivering **TCP Brutal v2 group traffic control**, **Earliest Departure Time (EDT)** monotonic virtual-clock packet pacing, and **empirical loss compensation** directly to user-space UDP datagram streams (`net.PacketConn`) and transport multiplexers (`Xlane`, QUIC CC, TUIC, Hysteria 2).

Key design features include:
1. **User-Tunable Pacing & Guardrails**: Elimination of hardcoded numeric figures via named constants (`GroupMaxLag`, `GroupMaxHorizon`, `BrutalMinRate`, `MinAckRate`, `MinFeedbackSlots`, `PktInfoSlots`, `MaxGroups`, `DefaultIdleTTL`, `EmergencySweepMinInterval`, `MaxSafePacketBytes`, `DefaultCwndGain`, `DefaultJanitorInterval`, `MinSleepThreshold`). Full user configurability for burst lag credit (`MaxLag`), scheduling horizon (`MaxHorizon`), group capacity (`MaxGroups`), idle eviction (`IdleTTL`), and micro-burst sleep smoothing (`MinSleepThreshold`).
2. **Lock-Free Pacing Hot Path**: Pacing departure scheduling executed via lock-free atomic `CompareAndSwapInt64` loops over monotonically anchored nanosecond time, completely bypassing runtime locks and scheduler micro-timer churn.
3. **Robust Alignment & Memory Safety**: 64-bit atomic integers strictly positioned at offset 0 of structs, guaranteeing 8-byte memory alignment and preventing runtime panics on 32-bit platforms (ARMv7, MIPS, x86). Zero external dependencies (100% Go standard library).

---

## 2. Architectural Design & Capabilities

### 2.1 Lock-Free EDT Monotonic Virtual Clock (`group.go`)
* **Strict Monotonic Anchor**:
  - Anchors pacing against process startup monotonic duration (`time.Since(startTime).Nanoseconds()`).
  - Completely immune to NTP adjustments, system wall-clock stepping, and daylight saving time jumps.
* **Lock-Free Pacing Hot Path (`Group.Reserve`)**:
  - Atomically calculates departure timestamps via CAS on `nextTimeNano`.
  - Enforces burst lag credit clamping (`GroupMaxLag = 2ms` by default, tunable via `SetMaxLag`) matching kernel `GROUP_MAX_LAG_NS`.
  - Enforces forward scheduling horizon clamping (`GroupMaxHorizon = 100ms` by default, tunable via `SetMaxHorizon`). Tail-drops excess datagrams under network backpressure, preventing unbounded goroutine sleeps and buffer bloat.
* **Overflow Protection (`MaxSafePacketBytes`)**:
  - Bounds byte counts to 64 MiB (`MaxSafePacketBytes = 64 * 1024 * 1024`), eliminating `int64` overflow in burst duration calculations (`bytes * NanosPerSecond`).

### 2.2 4-Epoch Ring Buffer Loss Compensation (`group.go`)
* **Sliding Epoch Loss Tracking**:
  - Maintains a 4-epoch circular buffer (`PktInfoSlots = 4`) tracking ACK and packet loss telemetry.
  - Dynamically boosts pacing rate when loss occurs, bounded by an 80% ACK rate floor (`MinAckRate = 0.80`, capping rate boost at $1.25\times$).
  - Ignores loss feedback when total samples are below statistical confidence threshold (`MinFeedbackSlots = 50`), falling back to baseline 100% rate (`DefaultAckRate = 1.0`).

### 2.3 Directional Namespace Isolation & Multi-Tenancy (`group.go`)
* **Directional Partitioning (`DirectionDownlinkFlag`)**:
  - Partitions 64-bit group IDs using bit 63 (`DirectionDownlinkFlag = 1 << 63`).
  - Isolates uplink and downlink pacing pools across asymmetric broadband links (e.g. 50 Mbps up / 500 Mbps down) sharing the same identifier.
* **Capacity Guardrails & DoS Protection (`GroupManager`)**:
  - Enforces maximum concurrent group ceilings (`MaxGroups = 32768`, tunable via `SetMaxGroups`).
  - Provides throttled emergency sweeps (`EmergencySweepMinInterval = 200ms`) and background idle reaping (`DefaultJanitorInterval = 10s`, `DefaultIdleTTL = 30s`, tunable via `SetIdleTTL`).
  - Thread-safe lazy janitor startup: `GetOrCreate` triggers `StartJanitor` with fallback reading of `idleTTL` safely guarded under `m.mu.Lock()`, eliminating data races during concurrent group initialization.
  - Automatic hash map compaction (`compactLocked`) when tombstoned entries exceed thresholds (`MinMapCompactDeletedThreshold = 512`, `MapCompactRatioThreshold = 2`).

### 2.4 User-Space Datagram Pacer (`packet_conn.go`)
* **Micro-Burst Smoothing (`PacedPacketConn`)**:
  - Sleeps strictly *outside* the pacing lock with a configurable micro-burst threshold (`MinSleepThreshold = 1ms`, tunable via `SetMinSleepThreshold`), preventing scheduler thrashing.
  - Tail-dropped datagrams are tracked via atomic telemetry (`DroppedPackets()`).
* **Complete Interface Conformance & Nil Safety**:
  - Delegates `LocalAddr()`, `RemoteAddr()`, `SetDeadline()`, `SetReadDeadline()`, `SetWriteDeadline()`, `ReadFrom()`, `WriteTo()`, `Write()`, and `SyscallConn()`.
  - Guarded against nil underlying connections and closed state (`net.ErrClosed`).

---

## 3. Tunable Parameters & Defaults Matrix

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

## 4. Source File Matrix

| File Path | Component | Description |
| :--- | :--- | :--- |
| `group.go` | Core Engine | EDT virtual clock, lock-free CAS pacing, sliding-epoch loss compensation, directional namespace isolation, group lifecycle manager, and user-tunable lag/horizon/group setters. |
| `group_test.go` | Test Suite | Unit tests for virtual clock accuracy, burst limits, forward horizon tail drops, 32-bit atomic alignment, concurrency stress, and tunable lag/horizon verification. |
| `packet_conn.go` | Network Wrapper | `PacedPacketConn` wrapping `net.PacketConn`, micro-burst smoothing, tail-drop telemetry, user-tunable sleep threshold, and comprehensive nil/closed error guards. |
| `packet_conn_test.go` | Test Suite | Unit tests for packet pacing, micro-bursts, tail drops, concurrent close safety, and tunable sleep thresholds. |

