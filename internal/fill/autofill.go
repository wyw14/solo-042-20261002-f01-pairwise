package fill

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/wyw14/cry-98/internal/model"
)

// AutoFillPolicy configures the automatic fill demand for one vessel.
type AutoFillPolicy struct {
	LowThreshold       float64
	RecoveryThreshold  float64
	ConsecutiveSamples int
	Cooldown           time.Duration
}

func (p AutoFillPolicy) Validate() error {
	if p.LowThreshold <= 0 {
		return errors.New("low threshold must be positive")
	}
	if p.RecoveryThreshold < p.LowThreshold {
		return errors.New("recovery threshold must not be below low threshold")
	}
	if p.ConsecutiveSamples < 1 {
		return errors.New("consecutive samples must be at least one")
	}
	if p.Cooldown < 0 {
		return errors.New("cooldown must not be negative")
	}
	return nil
}

// AutomaticDemand submits one automatic fill demand for a vessel.
type AutomaticDemand func(ctx context.Context, vesselID string, observedAt time.Time) error

type autoVesselState struct {
	consecutive int
	demandOpen  bool
	lastDemand  time.Time
}

// AutoTrigger turns level samples into automatic fill demands according to
// per-vessel policies. A low episode yields at most one demand; the trigger
// rearms only after the level recovers to the recovery threshold.
type AutoTrigger struct {
	mu       sync.Mutex
	policies map[string]AutoFillPolicy
	states   map[string]*autoVesselState
	demand   AutomaticDemand
	now      func() time.Time
}

func NewAutoTrigger(policies map[string]AutoFillPolicy, demand AutomaticDemand, now func() time.Time) (*AutoTrigger, error) {
	if demand == nil {
		return nil, errors.New("automatic demand handler is required")
	}
	if now == nil {
		now = time.Now
	}
	trigger := &AutoTrigger{
		policies: make(map[string]AutoFillPolicy, len(policies)),
		states:   make(map[string]*autoVesselState),
		demand:   demand,
		now:      now,
	}
	for vesselID, policy := range policies {
		if err := trigger.SetPolicy(vesselID, policy); err != nil {
			return nil, err
		}
	}
	return trigger, nil
}

func (t *AutoTrigger) SetPolicy(vesselID string, policy AutoFillPolicy) error {
	if vesselID == "" {
		return errors.New("vessel id is required")
	}
	if err := policy.Validate(); err != nil {
		return err
	}
	t.mu.Lock()
	t.policies[vesselID] = policy
	t.mu.Unlock()
	return nil
}

func (t *AutoTrigger) Policy(vesselID string) (AutoFillPolicy, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	policy, ok := t.policies[vesselID]
	return policy, ok
}

// Observe processes one sample and submits a demand when the vessel's policy
// requires it. It returns nil when the sample does not lead to a demand.
func (t *AutoTrigger) Observe(ctx context.Context, sample model.Sample) error {
	if sample.Kind != model.ProbeLevel {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	policy, ok := t.policies[sample.VesselID]
	if !ok {
		return nil
	}
	state := t.states[sample.VesselID]
	if state == nil {
		state = &autoVesselState{}
		t.states[sample.VesselID] = state
	}
	switch {
	case sample.Value >= policy.RecoveryThreshold:
		state.consecutive = 0
		state.demandOpen = false
		return nil
	case sample.Value >= policy.LowThreshold:
		state.consecutive = 0
		return nil
	}
	state.consecutive++
	if state.demandOpen || state.consecutive < policy.ConsecutiveSamples {
		return nil
	}
	now := t.now()
	if !state.lastDemand.IsZero() && now.Sub(state.lastDemand) < policy.Cooldown {
		return nil
	}
	// A rejected demand (e.g. an active manual session, which the coordinator
	// never lets the automatic flow preempt) does not consume the episode's
	// single demand; the next valid low sample retries.
	if err := t.demand(ctx, sample.VesselID, sample.ObservedAt); err != nil {
		return err
	}
	state.demandOpen = true
	state.lastDemand = now
	return nil
}
