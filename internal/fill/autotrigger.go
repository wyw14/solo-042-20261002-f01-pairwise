package fill

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"

	"github.com/wyw14/cry-98/internal/model"
)

// AutoPolicy configures how low-level samples escalate into an automatic
// fill demand for a single vessel.
type AutoPolicy struct {
	// LowThreshold is the level (mm) below which a sample counts as low.
	LowThreshold float64
	// RecoveryThreshold is the level (mm) at or above which the vessel is
	// considered recovered and the trigger re-arms.
	RecoveryThreshold float64
	// ConsecutiveSamples is the number of consecutive low samples required
	// before a demand is submitted.
	ConsecutiveSamples int
	// Cooldown is the minimum interval between automatic fill demands.
	Cooldown time.Duration
}

func (p AutoPolicy) Validate() error {
	if p.LowThreshold <= 0 {
		return errors.New("low threshold must be positive")
	}
	if p.RecoveryThreshold < p.LowThreshold {
		return errors.New("recovery threshold must not be below the low threshold")
	}
	if p.ConsecutiveSamples < 1 {
		return errors.New("consecutive samples must be at least one")
	}
	if p.Cooldown < 0 {
		return errors.New("cooldown must not be negative")
	}
	return nil
}

// autoVesselState tracks the trigger state of one low-level round.
type autoVesselState struct {
	consecutive     int
	demandSubmitted bool
	cooldownUntil   time.Time
}

// AutoTrigger observes level samples and submits at most one automatic fill
// demand per low-level round, per vessel, according to its AutoPolicy.
type AutoTrigger struct {
	mu       sync.Mutex
	service  *Service
	now      func() time.Time
	policies map[string]AutoPolicy
	states   map[string]*autoVesselState
}

func NewAutoTrigger(service *Service, now func() time.Time) *AutoTrigger {
	if now == nil {
		now = time.Now
	}
	return &AutoTrigger{
		service:  service,
		now:      now,
		policies: make(map[string]AutoPolicy),
		states:   make(map[string]*autoVesselState),
	}
}

// SetPolicy installs or replaces the policy for a vessel and resets its
// trigger state, so a policy change starts a fresh evaluation.
func (t *AutoTrigger) SetPolicy(vesselID string, policy AutoPolicy) error {
	if err := policy.Validate(); err != nil {
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.policies[vesselID] = policy
	t.states[vesselID] = &autoVesselState{}
	return nil
}

// RemovePolicy disables automatic triggering for a vessel.
func (t *AutoTrigger) RemovePolicy(vesselID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.policies, vesselID)
	delete(t.states, vesselID)
}

func (t *AutoTrigger) Policy(vesselID string) (AutoPolicy, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	policy, ok := t.policies[vesselID]
	return policy, ok
}

// Observe implements a telemetry observer for level samples and is safe to
// register with telemetry.Ingestor.AddObserver.
func (t *AutoTrigger) Observe(sample model.Sample) {
	if sample.Kind != model.ProbeLevel {
		return
	}
	if math.IsNaN(sample.Value) || math.IsInf(sample.Value, 0) {
		// Invalid reading: it neither counts as low nor breaks the streak.
		return
	}
	t.mu.Lock()
	policy, ok := t.policies[sample.VesselID]
	if !ok {
		t.mu.Unlock()
		return
	}
	state := t.states[sample.VesselID]
	if state == nil {
		state = &autoVesselState{}
		t.states[sample.VesselID] = state
	}
	switch {
	case sample.Value >= policy.RecoveryThreshold:
		// Readings recovered: the low round ends and the trigger re-arms.
		state.consecutive = 0
		state.demandSubmitted = false
		t.mu.Unlock()
		return
	case sample.Value >= policy.LowThreshold:
		// Hysteresis band: not low, so the streak breaks, but an open
		// round stays open until recovery.
		state.consecutive = 0
		t.mu.Unlock()
		return
	}
	state.consecutive++
	if state.consecutive < policy.ConsecutiveSamples || state.demandSubmitted || t.now().Before(state.cooldownUntil) {
		t.mu.Unlock()
		return
	}
	if t.service.HasActiveSession(sample.VesselID) {
		// A manual fill or another active session owns the vessel; the
		// automatic flow must not preempt it. The round stays open and no
		// demand is consumed.
		t.mu.Unlock()
		return
	}
	t.mu.Unlock()

	_, err := t.service.OnLowLevel(context.Background(), sample.VesselID, sample.ObservedAt)

	t.mu.Lock()
	defer t.mu.Unlock()
	if err != nil {
		// The demand was not submitted (e.g. a session started concurrently
		// or no circuit is free); back off instead of consuming the round.
		state.cooldownUntil = t.now().Add(policy.Cooldown)
		return
	}
	state.demandSubmitted = true
	state.cooldownUntil = t.now().Add(policy.Cooldown)
}
