package brutaludp

import (
	"sync"
	"testing"
	"time"
)

func TestGroupVirtualClockRate(t *testing.T) {
	manager := &GroupManager{groups: make(map[uint64]*Group)}
	defer manager.Close()

	rate := uint64(10 * 1024 * 1024) // 10 MB/s
	g := manager.GetOrCreate(101, rate)
	if g == nil {
		t.Fatal("failed to create group")
	}

	packetSize := 1000
	expectedPktDuration := time.Duration((int64(packetSize) * 1e9) / int64(rate))

	now := monotonicNano()
	delay1, ok1 := g.Reserve(now, packetSize)
	if !ok1 || delay1 != 0 {
		t.Fatalf("first packet should send immediately, got delay=%v ok=%v", delay1, ok1)
	}

	delay2, ok2 := g.Reserve(now, packetSize)
	if !ok2 {
		t.Fatal("second packet reserve failed")
	}
	if delay2 < expectedPktDuration-50*time.Microsecond || delay2 > expectedPktDuration+50*time.Microsecond {
		t.Fatalf("expected delay ~%v, got %v", expectedPktDuration, delay2)
	}
}

func TestGroupMaxLagBurstLimit(t *testing.T) {
	manager := &GroupManager{groups: make(map[uint64]*Group)}
	defer manager.Close()

	rate := uint64(10 * 1024 * 1024) // 10 MB/s
	g := manager.GetOrCreate(102, rate)

	now := monotonicNano()
	// Simulate group idle for 1 hour
	idleNow := now + int64(1*time.Hour)

	// Single 1-byte packet after long idle
	delay, ok := g.Reserve(idleNow, 1000)
	if !ok || delay != 0 {
		t.Fatalf("packet after idle should send immediately, got delay=%v", delay)
	}

	// Clock should have been clamped behind to idleNow - GroupMaxLag
	curNext := g.nextTimeNano.Load()
	minStart := idleNow - int64(GroupMaxLag)
	if curNext < minStart {
		t.Fatalf("virtual clock not clamped to minStart: curNext=%v, minStart=%v", curNext, minStart)
	}
}

func TestGroupForwardHorizonTailDrop(t *testing.T) {
	manager := &GroupManager{groups: make(map[uint64]*Group)}
	defer manager.Close()

	rate := uint64(1024 * 1024) // 1 MB/s
	g := manager.GetOrCreate(103, rate)

	now := monotonicNano()
	// GroupMaxHorizon is 100ms. At 1 MB/s, 100ms corresponds to 102,400 bytes.
	// Reserve 200,000 bytes in a burst
	reserved := 0
	dropped := 0
	for i := 0; i < 200; i++ {
		_, ok := g.Reserve(now, 1000)
		if ok {
			reserved++
		} else {
			dropped++
		}
	}

	if dropped == 0 {
		t.Fatal("expected forward horizon tail drops, but 0 packets dropped")
	}
	if reserved == 0 {
		t.Fatal("expected some packets to be reserved before horizon ceiling")
	}
}

func TestBurstDurationOverflowClamp(t *testing.T) {
	manager := &GroupManager{groups: make(map[uint64]*Group)}
	defer manager.Close()

	g := manager.GetOrCreate(104, 10*1024*1024)
	now := monotonicNano()

	// Pass huge buffer length (e.g. 10 GB) that would overflow int64 if not clamped
	hugeBytes := 10 * 1024 * 1024 * 1024
	delay, ok := g.Reserve(now, hugeBytes)
	// Should either be tail-dropped or clamped with strictly non-negative delay
	if delay < 0 {
		t.Fatalf("integer overflow produced negative delay: %v", delay)
	}
	_ = ok
}

func TestLossCompensationCapAndRecovery(t *testing.T) {
	manager := &GroupManager{groups: make(map[uint64]*Group)}
	defer manager.Close()

	baseRate := uint64(10_000_000) // 10 MB/s
	g := manager.GetOrCreate(105, baseRate)

	// Under 50 samples: no adaptation
	g.RecordFeedback(20, 10)
	if g.EffectiveRate() != baseRate {
		t.Fatalf("expected rate unchanged under sample threshold, got %v", g.EffectiveRate())
	}

	// Reach 60 samples with 50% loss (30 acked, 30 lost)
	g.RecordFeedback(10, 20)
	// Loss is 50%, which drops ACK rate below MinAckRate (0.80).
	// Pacing boost must cap strictly at 1 / 0.80 = 1.25x
	maxEffectiveRate := uint64(float64(baseRate) / 0.80)
	if g.EffectiveRate() != maxEffectiveRate {
		t.Fatalf("expected rate capped at 1.25x (%v), got %v", maxEffectiveRate, g.EffectiveRate())
	}

	// Disable loss compensation test
	g.SetDisableLossCompensation(true)
	if g.EffectiveRate() != baseRate {
		t.Fatalf("expected rate to revert to base rate when disabled, got %v", g.EffectiveRate())
	}
}

func TestDirectionalNamespaceIsolation(t *testing.T) {
	baseID := uint64(12345)
	upID := GroupIDForUplink(baseID)
	downID := GroupIDForDownlink(baseID)

	if upID == downID {
		t.Fatalf("uplink and downlink IDs collided: up=%x down=%x", upID, downID)
	}
	if upID != baseID {
		t.Fatalf("expected uplink ID %x to equal base %x", upID, baseID)
	}
	if (downID & DirectionDownlinkFlag) == 0 {
		t.Fatalf("expected downlink bit flag set on downID: %x", downID)
	}
}

func TestGroupManagerCapacityAndEmergencySweep(t *testing.T) {
	manager := &GroupManager{groups: make(map[uint64]*Group)}
	defer manager.Close()

	// Fill manager to a test capacity
	g1 := manager.GetOrCreate(1, 1024*1024)
	g2 := manager.GetOrCreate(2, 1024*1024)
	if g1 == nil || g2 == nil {
		t.Fatal("failed to create groups")
	}

	g1.Release() // refCount becomes 0

	// Immediate sweep with 0 limit should evict g1
	evicted := manager.cleanupIdleLocked(0)
	if evicted != 1 {
		t.Fatalf("expected 1 group evicted, got %d", evicted)
	}

	manager.mu.Lock()
	_, exists := manager.groups[1]
	manager.mu.Unlock()
	if exists {
		t.Fatal("group 1 should have been deleted")
	}
}

func TestGroupManagerConcurrencyRace(t *testing.T) {
	manager := &GroupManager{groups: make(map[uint64]*Group)}
	defer manager.Close()

	var wg sync.WaitGroup
	workers := 16
	iterations := 200

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				groupID := uint64((workerID%4)*10 + (j % 5) + 1)
				g := manager.GetOrCreate(groupID, 10*1024*1024)
				if g != nil {
					now := monotonicNano()
					g.Reserve(now, 1000)
					g.OnPacketSent(time.Now(), 1000)
					g.HasPacingBudget()
					g.TimeUntilSend()
					g.Release()
				}
			}
		}(i)
	}

	wg.Wait()
}

func TestGroupAlignment32Bit(t *testing.T) {
	g := &Group{}
	// Verify atomic operations on struct fields don't panic
	g.nextTimeNano.Store(100)
	g.effectiveRate.Store(200)
	g.sentBytes.Store(300)
	g.lastActiveNano.Store(400)
	g.CwndGain.Store(20)
	g.refCount.Store(1)

	if g.nextTimeNano.Load() != 100 || g.refCount.Load() != 1 {
		t.Fatal("atomic load value mismatch")
	}
}
