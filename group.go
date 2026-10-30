package brutaludp

import (
	"sync"
	"sync/atomic"
	"time"
)

const (
	GroupMaxLag                   = 2 * time.Millisecond   // Equivalent to kernel GROUP_MAX_LAG_NS
	GroupMaxHorizon               = 100 * time.Millisecond // Bounded forward scheduling horizon (anti-OOM backpressure)
	MinAckRate                    = 0.80                   // Equivalent to kernel MIN_ACK_RATE_PERCENT (max 1.25x boost)
	BrutalMinRate                 = 64 * 1024              // 64 KB/s floor
	MinFeedbackSlots              = 50                     // Minimum sample count before loss compensation adapts
	PktInfoSlots                  = 4                      // Kernel-identical 4-second epoch slots
	MaxGroups                     = 32768                  // Maximum concurrent groups ceiling (prevents OOM DoS attacks)
	DefaultIdleTTL                = 30 * time.Second       // Retention TTL for unreferenced groups before eviction
	EmergencySweepMinInterval     = 200 * time.Millisecond // Minimum interval between emergency capacity sweeps (DoS protection)
	MaxSafePacketBytes            = 64 * 1024 * 1024       // 64 MB clamp preventing int64 integer overflow in burst math
	DefaultCwndGain               = 20                     // Default CWND gain in tenths (2.0x)
	DefaultJanitorInterval        = 10 * time.Second       // Default background reaping interval
	MinMapCompactDeletedThreshold = 512                    // Minimum deleted group count before evaluating map compaction
	MapCompactRatioThreshold      = 2                      // Ratio threshold (deleted * ratio >= total) for map compaction
	NanosPerSecond                = 1_000_000_000          // Nanoseconds per second for time conversions
	DefaultAckRate                = 1.0                    // Baseline ACK rate (100%) when sample count is insufficient
)

// DirectionDownlinkFlag partitions group IDs directionally on bit 63 to isolate uplink/downlink pools.
const DirectionDownlinkFlag = uint64(1 << 63)

func GroupIDForUplink(id uint64) uint64   { return id &^ DirectionDownlinkFlag }
func GroupIDForDownlink(id uint64) uint64 { return id | DirectionDownlinkFlag }

var startTime = time.Now()

// monotonicNano returns strictly monotonic nanoseconds from process startup.
// It bypasses wall-clock adjustments and NTP stepping, eliminating stalls or spurious packet drops.
func monotonicNano() int64 {
	return time.Since(startTime).Nanoseconds()
}

type PktSlot struct {
	sec    int64
	acked  uint64
	losses uint64
}

// Group represents a shared bandwidth pool governed by an EDT monotonic virtual clock.
// All 64-bit atomic and integer fields are placed first (offset 0) to guarantee 8-byte
// memory alignment and eliminate crashes on 32-bit architectures (ARMv7, MIPS, x86).
type Group struct {
	// 8-byte aligned 64-bit fields first
	nextTimeNano   atomic.Int64  // Monotonic virtual clock in nanoseconds (lock-free CAS)
	effectiveRate  atomic.Uint64 // Precomputed effective pacing rate in BPS (lock-free reads)
	sentBytes      atomic.Uint64
	lastActiveNano atomic.Int64  // Last transmission activity timestamp for idle sweeping
	rate           atomic.Uint64 // Configured bytes per second (thread-safe lock-free reads)
	maxLagNano     atomic.Int64  // Maximum burst lag credit clamp in nanoseconds (lock-free)
	maxHorizonNano atomic.Int64  // Maximum forward scheduling horizon clamp in nanoseconds (lock-free)
	ID             uint64
	Rate           uint64        // Configured bytes per second (cached field for compatibility)

	// 4-byte and smaller fields
	CwndGain                atomic.Uint32 // CWND gain in tenths (20 = 2.0x, lock-free)
	refCount                atomic.Int32  // Active connection reference counter
	disableLossComp         atomic.Bool   // Thread-safe lock-free toggle for loss compensation
	DisableLossCompensation bool          // Cached field for compatibility

	// Structs, pointers, and mutex (used ONLY for infrequent feedback/rate updates, NEVER on pacing hot path)
	manager    *GroupManager
	mu         sync.Mutex
	slots      [PktInfoSlots]PktSlot
	ackRate    float64
	lastUpdate int64
}

func NewGroup(id uint64, rateBytesPerSec uint64, cwndGain uint32, manager *GroupManager) *Group {
	if rateBytesPerSec < BrutalMinRate {
		rateBytesPerSec = BrutalMinRate
	}
	if cwndGain == 0 {
		cwndGain = DefaultCwndGain
	}
	nowNano := monotonicNano()
	g := &Group{
		ID:         id,
		Rate:       rateBytesPerSec,
		manager:    manager,
		ackRate:    DefaultAckRate,
		lastUpdate: nowNano,
	}
	g.rate.Store(rateBytesPerSec)
	g.refCount.Store(1)
	g.CwndGain.Store(cwndGain)
	g.effectiveRate.Store(rateBytesPerSec)
	g.nextTimeNano.Store(nowNano)
	g.lastActiveNano.Store(nowNano)

	maxLag := int64(GroupMaxLag)
	maxHorizon := int64(GroupMaxHorizon)
	if manager != nil {
		if mLag := manager.defaultMaxLag.Load(); mLag > 0 {
			maxLag = mLag
		}
		if mHorizon := manager.defaultMaxHorizon.Load(); mHorizon > 0 {
			maxHorizon = mHorizon
		}
	}
	g.maxLagNano.Store(maxLag)
	g.maxHorizonNano.Store(maxHorizon)
	return g
}

func (g *Group) Acquire() {
	g.refCount.Add(1)
}

func (g *Group) Release() {
	if g.manager != nil {
		g.manager.release(g)
		return
	}
	for {
		cur := g.refCount.Load()
		if cur <= 0 {
			return
		}
		if g.refCount.CompareAndSwap(cur, cur-1) {
			return
		}
	}
}

// GetCwndGain locklessly returns the configured CWND gain factor.
func (g *Group) GetCwndGain() uint32 {
	return g.CwndGain.Load()
}

// GetRate locklessly returns the configured base rate in BPS.
func (g *Group) GetRate() uint64 {
	return g.rate.Load()
}

// SentBytes locklessly returns the total bytes sent through this group.
func (g *Group) SentBytes() uint64 {
	return g.sentBytes.Load()
}

// SetRate thread-safely updates the configured rate and precomputes effectiveRate.
func (g *Group) SetRate(rateBytesPerSec uint64) {
	if rateBytesPerSec < BrutalMinRate {
		rateBytesPerSec = BrutalMinRate
	}
	g.mu.Lock()
	g.Rate = rateBytesPerSec
	g.rate.Store(rateBytesPerSec)
	ackRate := g.ackRate
	if g.disableLossComp.Load() || ackRate <= 0 {
		ackRate = 1.0
	}
	eff := uint64(float64(rateBytesPerSec) / ackRate)
	if eff < BrutalMinRate {
		eff = BrutalMinRate
	}
	g.effectiveRate.Store(eff)
	g.mu.Unlock()
}

// SetCwndGain locklessly updates the configured CWND gain factor.
func (g *Group) SetCwndGain(gain uint32) {
	if gain == 0 {
		gain = DefaultCwndGain
	}
	g.CwndGain.Store(gain)
}

// SetMaxLag sets the maximum burst lag credit clamp for this group.
func (g *Group) SetMaxLag(lag time.Duration) {
	if lag <= 0 {
		lag = GroupMaxLag
	}
	g.maxLagNano.Store(int64(lag))
}

// MaxLag returns the configured maximum burst lag credit clamp.
func (g *Group) MaxLag() time.Duration {
	lag := g.maxLagNano.Load()
	if lag <= 0 {
		return GroupMaxLag
	}
	return time.Duration(lag)
}

// SetMaxHorizon sets the maximum forward scheduling horizon clamp for this group.
func (g *Group) SetMaxHorizon(horizon time.Duration) {
	if horizon <= 0 {
		horizon = GroupMaxHorizon
	}
	g.maxHorizonNano.Store(int64(horizon))
}

// MaxHorizon returns the configured maximum forward scheduling horizon clamp.
func (g *Group) MaxHorizon() time.Duration {
	horizon := g.maxHorizonNano.Load()
	if horizon <= 0 {
		return GroupMaxHorizon
	}
	return time.Duration(horizon)
}

// SetDisableLossCompensation thread-safely toggles loss compensation.
func (g *Group) SetDisableLossCompensation(disable bool) {
	g.mu.Lock()
	g.DisableLossCompensation = disable
	g.disableLossComp.Store(disable)
	r := g.rate.Load()
	eff := r
	if !disable && g.ackRate > 0 {
		eff = uint64(float64(r) / g.ackRate)
		if eff < BrutalMinRate {
			eff = BrutalMinRate
		}
	}
	g.effectiveRate.Store(eff)
	g.mu.Unlock()
}

// EffectiveRate locklessly returns the precomputed pacing rate without floating-point math or mutex locks.
func (g *Group) EffectiveRate() uint64 {
	return g.effectiveRate.Load()
}

// HasPacingBudget locklessly checks if the group has departure budget at current monotonic time.
// Accepts optional timestamp to avoid redundant time.Now() syscalls on high-frequency packet loops.
func (g *Group) HasPacingBudget(now ...time.Time) bool {
	var nowNano int64
	if len(now) > 0 && !now[0].IsZero() {
		nowNano = now[0].Sub(startTime).Nanoseconds()
	} else {
		nowNano = monotonicNano()
	}
	return nowNano >= g.nextTimeNano.Load()
}

// TimeUntilSend locklessly calculates delay to next departure slot without committing bytes.
// Accepts optional timestamp to avoid redundant time.Now() syscalls on high-frequency packet loops.
func (g *Group) TimeUntilSend(now ...time.Time) time.Duration {
	var nowNano int64
	if len(now) > 0 && !now[0].IsZero() {
		nowNano = now[0].Sub(startTime).Nanoseconds()
	} else {
		nowNano = monotonicNano()
	}
	nextNano := g.nextTimeNano.Load()
	if nowNano >= nextNano {
		return 0
	}
	return time.Duration(nextNano - nowNano)
}

// Reserve atomically and locklessly reserves an Earliest Departure Time (EDT) slot on the virtual clock.
// It executes with zero mutex locking via an atomic CompareAndSwap loop on nextTimeNano.
// If the pacer queue forward horizon is saturated (> GroupMaxHorizon), it returns ok=false (anti-OOM tail drop).
// Physical sleep MUST be performed by the caller OUTSIDE of any lock.
func (g *Group) Reserve(nowNano int64, bytes int) (delay time.Duration, ok bool) {
	if bytes <= 0 {
		return 0, true
	}
	if bytes > MaxSafePacketBytes {
		bytes = MaxSafePacketBytes // Prevent int64 integer overflow in burstDuration calculation
	}
	effBps := g.effectiveRate.Load()
	if effBps < BrutalMinRate {
		effBps = BrutalMinRate
	}
	burstDuration := (int64(bytes) * NanosPerSecond) / int64(effBps)
	lag := g.maxLagNano.Load()
	if lag <= 0 {
		lag = int64(GroupMaxLag)
	}
	horizon := g.maxHorizonNano.Load()
	if horizon <= 0 {
		horizon = int64(GroupMaxHorizon)
	}
	minStart := nowNano - lag
	maxStart := nowNano + horizon

	for {
		cur := g.nextTimeNano.Load()
		start := cur
		if start < minStart {
			start = minStart
		} else if start > maxStart {
			// Anti-OOM Backpressure: forward horizon saturated, tail drop packet
			return 0, false
		}
		departure := start
		if departure < nowNano {
			departure = nowNano
		}
		newNext := start + burstDuration
		if g.nextTimeNano.CompareAndSwap(cur, newNext) {
			g.lastActiveNano.Store(nowNano)
			g.sentBytes.Add(uint64(bytes))
			if departure > nowNano {
				return time.Duration(departure - nowNano), true
			}
			return 0, true
		}
	}
}

// OnPacketSent locklessly commits transmitted bytes to the group virtual clock using an atomic CompareAndSwap loop.
// Note: It respects GroupMaxLag to cap idle burst credits, but does NOT forward-clamp start down to maxStart,
// preventing rate debt forgiveness and unmetered transmission overruns during bursts.
func (g *Group) OnPacketSent(now time.Time, bytes int) {
	if bytes <= 0 {
		return
	}
	if bytes > MaxSafePacketBytes {
		bytes = MaxSafePacketBytes // Prevent int64 integer overflow in burstDuration calculation
	}
	effBps := g.effectiveRate.Load()
	if effBps < BrutalMinRate {
		effBps = BrutalMinRate
	}
	burstDuration := (int64(bytes) * NanosPerSecond) / int64(effBps)
	var nowNano int64
	if !now.IsZero() {
		nowNano = now.Sub(startTime).Nanoseconds()
	} else {
		nowNano = monotonicNano()
	}
	lag := g.maxLagNano.Load()
	if lag <= 0 {
		lag = int64(GroupMaxLag)
	}
	minStartNano := nowNano - lag

	for {
		curNext := g.nextTimeNano.Load()
		start := curNext
		if start < minStartNano {
			start = minStartNano
		}
		newNext := start + burstDuration
		if g.nextTimeNano.CompareAndSwap(curNext, newNext) {
			break
		}
	}
	g.lastActiveNano.Store(nowNano)
	g.sentBytes.Add(uint64(bytes))
}

// RecordFeedback updates the sliding window loss stats safely using a 4-second ring buffer.
func (g *Group) RecordFeedback(acked, lost uint64) {
	if g.disableLossComp.Load() || (acked == 0 && lost == 0) {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.disableLossComp.Load() {
		return
	}

	sec := monotonicNano() / NanosPerSecond
	slotIdx := sec & (PktInfoSlots - 1)

	if g.slots[slotIdx].sec == sec {
		g.slots[slotIdx].acked += acked
		g.slots[slotIdx].losses += lost
	} else {
		g.slots[slotIdx].sec = sec
		g.slots[slotIdx].acked = acked
		g.slots[slotIdx].losses = lost
	}

	var totalAcked, totalLost uint64
	for i := 0; i < PktInfoSlots; i++ {
		if g.slots[i].sec > sec-int64(PktInfoSlots) && g.slots[i].sec <= sec {
			totalAcked += g.slots[i].acked
			totalLost += g.slots[i].losses
		}
	}
	total := totalAcked + totalLost
	if total < MinFeedbackSlots {
		if g.ackRate == DefaultAckRate {
			return
		}
		g.ackRate = DefaultAckRate
	} else {
		rate := float64(totalAcked) / float64(total)
		if rate < MinAckRate {
			rate = MinAckRate
		}
		if rate == g.ackRate {
			return
		}
		g.ackRate = rate
	}
	eff := uint64(float64(g.rate.Load()) / g.ackRate)
	if eff < BrutalMinRate {
		eff = BrutalMinRate
	}
	g.effectiveRate.Store(eff)
	g.lastUpdate = monotonicNano()
}

// GroupManager provides thread-safe group retrieval, active idle cleanup, automated background reaping, and map compaction.
type GroupManager struct {
	mu                     sync.Mutex
	groups                 map[uint64]*Group
	deletedCount           int
	lastEmergencySweepNano int64
	janitorRunning         atomic.Bool
	janitorDone            chan struct{}
	maxGroups              int
	idleTTL                time.Duration
	defaultMaxLag          atomic.Int64
	defaultMaxHorizon      atomic.Int64
}

// SetMaxGroups sets the maximum number of concurrent groups allowed before emergency cleanup.
func (m *GroupManager) SetMaxGroups(max int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.maxGroups = max
}

// SetIdleTTL sets the retention TTL for unreferenced groups before eviction.
func (m *GroupManager) SetIdleTTL(ttl time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.idleTTL = ttl
}

// SetDefaultMaxLag sets the default maximum burst lag for groups created by this manager.
func (m *GroupManager) SetDefaultMaxLag(lag time.Duration) {
	if lag <= 0 {
		lag = GroupMaxLag
	}
	m.defaultMaxLag.Store(int64(lag))
}

// SetDefaultMaxHorizon sets the default forward horizon for groups created by this manager.
func (m *GroupManager) SetDefaultMaxHorizon(horizon time.Duration) {
	if horizon <= 0 {
		horizon = GroupMaxHorizon
	}
	m.defaultMaxHorizon.Store(int64(horizon))
}

var DefaultManager = &GroupManager{
	groups: make(map[uint64]*Group),
}

// NewGroupManager creates an independent GroupManager instance.
func NewGroupManager() *GroupManager {
	return &GroupManager{
		groups: make(map[uint64]*Group),
	}
}

func (m *GroupManager) compactLocked() {
	newMap := make(map[uint64]*Group, len(m.groups))
	for k, v := range m.groups {
		newMap[k] = v
	}
	m.groups = newMap
	m.deletedCount = 0
}

func (m *GroupManager) cleanupIdleLocked(idleLimit int64) int {
	nowNano := monotonicNano()
	evicted := 0
	for id, g := range m.groups {
		if g.refCount.Load() <= 0 && (nowNano-g.lastActiveNano.Load()) >= idleLimit {
			delete(m.groups, id)
			m.deletedCount++
			evicted++
		}
	}
	if evicted > 0 {
		if len(m.groups) == 0 {
			m.groups = make(map[uint64]*Group)
			m.deletedCount = 0
		} else if m.deletedCount >= MinMapCompactDeletedThreshold && (m.deletedCount*MapCompactRatioThreshold >= len(m.groups)) {
			m.compactLocked()
		}
	}
	return evicted
}

func (m *GroupManager) GetOrCreate(id uint64, rate uint64, cwndGain ...uint32) *Group {
	if id == 0 {
		return nil
	}
	// Lazily initialize background janitor on first group creation (fast lock-free check)
	if !m.janitorRunning.Load() {
		idleTTL := m.idleTTL
		if idleTTL <= 0 {
			idleTTL = DefaultIdleTTL
		}
		m.StartJanitor(DefaultJanitorInterval, idleTTL)
	}

	gain := uint32(DefaultCwndGain)
	if len(cwndGain) > 0 && cwndGain[0] > 0 {
		gain = cwndGain[0]
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if g, ok := m.groups[id]; ok {
		g.Acquire()
		g.lastActiveNano.Store(monotonicNano())
		if rate > 0 && rate != g.rate.Load() {
			g.SetRate(rate)
		}
		if len(cwndGain) > 0 && cwndGain[0] > 0 && cwndGain[0] != g.GetCwndGain() {
			g.SetCwndGain(cwndGain[0])
		}
		return g
	}

	// Capacity guardrail: prevent OOM from randomized group ID exhaustion
	maxGroups := m.maxGroups
	if maxGroups <= 0 {
		maxGroups = MaxGroups
	}
	if len(m.groups) >= maxGroups {
		nowNano := monotonicNano()
		if nowNano-m.lastEmergencySweepNano >= int64(EmergencySweepMinInterval) {
			m.lastEmergencySweepNano = nowNano
			m.cleanupIdleLocked(0) // Throttled emergency sweep of unreferenced groups
		}
		if len(m.groups) >= maxGroups {
			return nil // Refuse allocation to protect process memory (O(1) fast rejection under flood)
		}
	}

	g := NewGroup(id, rate, gain, m)
	m.groups[id] = g
	return g
}

func (m *GroupManager) release(g *Group) {
	for {
		cur := g.refCount.Load()
		if cur <= 0 {
			return // Guard against double-release and underflow
		}
		if g.refCount.CompareAndSwap(cur, cur-1) {
			if cur == 1 {
				// Mark last active timestamp. The group lingers during DefaultIdleTTL
				// so rapid reconnects reuse warmed loss stats without reallocation.
				g.lastActiveNano.Store(monotonicNano())
			}
			return
		}
	}
}

// CleanupIdle reclaims memory from dormant groups exceeding maxIdle duration and compacts map memory.
func (m *GroupManager) CleanupIdle(maxIdle time.Duration) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.cleanupIdleLocked(int64(maxIdle))
}

// StartJanitor launches a background goroutine to periodically clean up idle groups.
// It is thread-safe and restartable if StopJanitor was previously called.
func (m *GroupManager) StartJanitor(interval, maxIdle time.Duration) {
	m.mu.Lock()
	if m.janitorRunning.Load() {
		m.mu.Unlock()
		return
	}
	m.janitorRunning.Store(true)
	done := make(chan struct{})
	m.janitorDone = done
	m.mu.Unlock()

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				m.CleanupIdle(maxIdle)
			case <-done:
				return
			}
		}
	}()
}

// StopJanitor stops the background janitor goroutine and allows clean restarts.
func (m *GroupManager) StopJanitor() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.janitorRunning.Load() {
		m.janitorRunning.Store(false)
		if m.janitorDone != nil {
			close(m.janitorDone)
			m.janitorDone = nil
		}
	}
}

// Close cleanly stops the background janitor and frees all retained group memory.
func (m *GroupManager) Close() {
	m.StopJanitor()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.groups = make(map[uint64]*Group)
	m.deletedCount = 0
}
