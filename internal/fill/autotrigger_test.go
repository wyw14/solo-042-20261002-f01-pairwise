package fill

import (
	"context"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/wyw14/cry-98/internal/circuit"
	"github.com/wyw14/cry-98/internal/model"
	"github.com/wyw14/cry-98/internal/valve"
	"github.com/wyw14/cry-98/internal/vessel"
)

type mutableClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *mutableClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *mutableClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func newAutoTriggerStack(t *testing.T, now func() time.Time) (*Service, *AutoTrigger) {
	t.Helper()
	vessels := vessel.NewRegistry()
	vesselState, err := model.NewVessel("V-01", "Vessel 01", "north", now())
	if err != nil {
		t.Fatal(err)
	}
	if err := vessels.Register(vesselState); err != nil {
		t.Fatal(err)
	}
	stateService := vessel.NewStateService(vessels, now)
	circuitList := circuit.NewRegistry(circuit.Circuit{ID: "C-A", Zone: "north", ValveID: "VALVE-A", Enabled: true})
	circuits := circuit.NewManager("C-A")
	controller, err := valve.NewController(valve.NewMemoryTransport(now), now)
	if err != nil {
		t.Fatal(err)
	}
	routes := valve.NewRouteService(valve.NewCommandService(controller, now))
	coordinator := NewCoordinator(circuits, circuitList, routes, stateService, now)
	completion := NewCompletionService(coordinator, nil, nil, now)
	aborts := NewAbortService(coordinator, now)
	service, err := NewService(coordinator, completion, aborts, 2.1)
	if err != nil {
		t.Fatal(err)
	}
	return service, NewAutoTrigger(service, now)
}

func levelSample(vesselID string, value float64, at time.Time) model.Sample {
	return model.Sample{
		ProbeID:    "P-L01",
		VesselID:   vesselID,
		Kind:       model.ProbeLevel,
		Value:      value,
		ObservedAt: at,
	}
}

func automaticSessions(service *Service) int {
	count := 0
	for _, session := range service.List() {
		if session.Origin == model.FillAutomatic {
			count++
		}
	}
	return count
}

func activeSession(t *testing.T, service *Service, vesselID string) model.FillSession {
	t.Helper()
	for _, session := range service.List() {
		if session.VesselID == vesselID && session.Active() {
			return session
		}
	}
	t.Fatalf("no active session for vessel %s", vesselID)
	return model.FillSession{}
}

var testPolicy = AutoPolicy{
	LowThreshold:       20,
	RecoveryThreshold:  28,
	ConsecutiveSamples: 3,
	Cooldown:           0,
}

func TestAutoTriggerRequiresConsecutiveLowSamples(t *testing.T) {
	clock := &mutableClock{now: time.Now()}
	service, trigger := newAutoTriggerStack(t, clock.Now)
	if err := trigger.SetPolicy("V-01", testPolicy); err != nil {
		t.Fatal(err)
	}
	trigger.Observe(levelSample("V-01", 19, clock.Now()))
	trigger.Observe(levelSample("V-01", 18, clock.Now()))
	if service.HasActiveSession("V-01") {
		t.Fatal("demand submitted before enough consecutive low samples")
	}
	trigger.Observe(levelSample("V-01", 17, clock.Now()))
	if !service.HasActiveSession("V-01") {
		t.Fatal("no demand submitted after enough consecutive low samples")
	}
	if got := automaticSessions(service); got != 1 {
		t.Fatalf("expected one automatic session, got %d", got)
	}
}

func TestAutoTriggerSubmitsOnlyOncePerLowRound(t *testing.T) {
	clock := &mutableClock{now: time.Now()}
	service, trigger := newAutoTriggerStack(t, clock.Now)
	if err := trigger.SetPolicy("V-01", testPolicy); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		trigger.Observe(levelSample("V-01", 15, clock.Now()))
	}
	session := activeSession(t, service, "V-01")
	if _, err := service.Abort(context.Background(), session.ID, "test abort"); err != nil {
		t.Fatal(err)
	}
	// The level is still low, but the round already consumed its one demand.
	trigger.Observe(levelSample("V-01", 14, clock.Now()))
	trigger.Observe(levelSample("V-01", 13, clock.Now()))
	trigger.Observe(levelSample("V-01", 12, clock.Now()))
	if service.HasActiveSession("V-01") {
		t.Fatal("second demand submitted in the same low round")
	}
	if got := automaticSessions(service); got != 1 {
		t.Fatalf("expected one automatic session, got %d", got)
	}
}

func TestAutoTriggerRearmsAfterRecovery(t *testing.T) {
	clock := &mutableClock{now: time.Now()}
	service, trigger := newAutoTriggerStack(t, clock.Now)
	if err := trigger.SetPolicy("V-01", testPolicy); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		trigger.Observe(levelSample("V-01", 15, clock.Now()))
	}
	session := activeSession(t, service, "V-01")
	if _, err := service.Abort(context.Background(), session.ID, "test abort"); err != nil {
		t.Fatal(err)
	}
	trigger.Observe(levelSample("V-01", 30, clock.Now()))
	for i := 0; i < 3; i++ {
		trigger.Observe(levelSample("V-01", 15, clock.Now()))
	}
	if !service.HasActiveSession("V-01") {
		t.Fatal("trigger did not re-arm after readings recovered")
	}
	if got := automaticSessions(service); got != 2 {
		t.Fatalf("expected two automatic sessions, got %d", got)
	}
}

func TestAutoTriggerHysteresisBand(t *testing.T) {
	clock := &mutableClock{now: time.Now()}
	service, trigger := newAutoTriggerStack(t, clock.Now)
	if err := trigger.SetPolicy("V-01", testPolicy); err != nil {
		t.Fatal(err)
	}
	// A reading in the hysteresis band breaks the low streak.
	trigger.Observe(levelSample("V-01", 19, clock.Now()))
	trigger.Observe(levelSample("V-01", 18, clock.Now()))
	trigger.Observe(levelSample("V-01", 24, clock.Now()))
	trigger.Observe(levelSample("V-01", 19, clock.Now()))
	trigger.Observe(levelSample("V-01", 18, clock.Now()))
	if service.HasActiveSession("V-01") {
		t.Fatal("band reading did not break the consecutive low streak")
	}
	trigger.Observe(levelSample("V-01", 17, clock.Now()))
	session := activeSession(t, service, "V-01")
	if _, err := service.Abort(context.Background(), session.ID, "test abort"); err != nil {
		t.Fatal(err)
	}
	// A band reading must not end the round: only recovery re-arms.
	trigger.Observe(levelSample("V-01", 24, clock.Now()))
	for i := 0; i < 3; i++ {
		trigger.Observe(levelSample("V-01", 15, clock.Now()))
	}
	if service.HasActiveSession("V-01") {
		t.Fatal("band reading re-armed the trigger before recovery")
	}
	trigger.Observe(levelSample("V-01", 28, clock.Now()))
	for i := 0; i < 3; i++ {
		trigger.Observe(levelSample("V-01", 15, clock.Now()))
	}
	if !service.HasActiveSession("V-01") {
		t.Fatal("trigger did not re-arm once readings reached the recovery threshold")
	}
}

func TestAutoTriggerCooldown(t *testing.T) {
	clock := &mutableClock{now: time.Now()}
	service, trigger := newAutoTriggerStack(t, clock.Now)
	policy := AutoPolicy{LowThreshold: 20, RecoveryThreshold: 28, ConsecutiveSamples: 1, Cooldown: 5 * time.Minute}
	if err := trigger.SetPolicy("V-01", policy); err != nil {
		t.Fatal(err)
	}
	trigger.Observe(levelSample("V-01", 15, clock.Now()))
	session := activeSession(t, service, "V-01")
	if _, err := service.Abort(context.Background(), session.ID, "test abort"); err != nil {
		t.Fatal(err)
	}
	trigger.Observe(levelSample("V-01", 30, clock.Now()))
	clock.Advance(time.Minute)
	trigger.Observe(levelSample("V-01", 15, clock.Now()))
	if service.HasActiveSession("V-01") {
		t.Fatal("demand submitted during cooldown")
	}
	clock.Advance(5 * time.Minute)
	trigger.Observe(levelSample("V-01", 15, clock.Now()))
	if !service.HasActiveSession("V-01") {
		t.Fatal("demand not submitted after cooldown elapsed")
	}
}

func TestAutoTriggerDoesNotPreemptManualSession(t *testing.T) {
	clock := &mutableClock{now: time.Now()}
	service, trigger := newAutoTriggerStack(t, clock.Now)
	if err := trigger.SetPolicy("V-01", testPolicy); err != nil {
		t.Fatal(err)
	}
	manual, err := service.StartManual(context.Background(), "V-01", 0)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		trigger.Observe(levelSample("V-01", 15, clock.Now()))
	}
	if got := automaticSessions(service); got != 0 {
		t.Fatalf("automatic flow preempted a manual session (%d automatic sessions)", got)
	}
	if !service.HasActiveSession("V-01") {
		t.Fatal("manual session was displaced by the automatic flow")
	}
	if _, err := service.Abort(context.Background(), manual.ID, "test abort"); err != nil {
		t.Fatal(err)
	}
	// The blocked round did not consume its demand: it fires once free.
	trigger.Observe(levelSample("V-01", 15, clock.Now()))
	if got := automaticSessions(service); got != 1 {
		t.Fatalf("expected the pending demand to fire after the manual session, got %d automatic sessions", got)
	}
}

func TestAutoTriggerIgnoresInvalidSamples(t *testing.T) {
	clock := &mutableClock{now: time.Now()}
	service, trigger := newAutoTriggerStack(t, clock.Now)
	policy := AutoPolicy{LowThreshold: 20, RecoveryThreshold: 28, ConsecutiveSamples: 2, Cooldown: 0}
	if err := trigger.SetPolicy("V-01", policy); err != nil {
		t.Fatal(err)
	}
	trigger.Observe(levelSample("V-01", 19, clock.Now()))
	trigger.Observe(levelSample("V-01", math.NaN(), clock.Now()))
	trigger.Observe(levelSample("V-01", math.Inf(1), clock.Now()))
	if service.HasActiveSession("V-01") {
		t.Fatal("invalid sample counted toward the low streak")
	}
	trigger.Observe(levelSample("V-01", 18, clock.Now()))
	if !service.HasActiveSession("V-01") {
		t.Fatal("invalid sample broke the consecutive low streak")
	}
}

func TestAutoTriggerIgnoresUnconfiguredVessel(t *testing.T) {
	clock := &mutableClock{now: time.Now()}
	service, trigger := newAutoTriggerStack(t, clock.Now)
	for i := 0; i < 5; i++ {
		trigger.Observe(levelSample("V-01", 15, clock.Now()))
		trigger.Observe(levelSample("V-99", 15, clock.Now()))
	}
	if service.HasActiveSession("V-01") {
		t.Fatal("demand submitted for a vessel without a policy")
	}
}

func TestAutoTriggerSetPolicyResetsState(t *testing.T) {
	clock := &mutableClock{now: time.Now()}
	service, trigger := newAutoTriggerStack(t, clock.Now)
	policy := AutoPolicy{LowThreshold: 20, RecoveryThreshold: 28, ConsecutiveSamples: 1, Cooldown: 0}
	if err := trigger.SetPolicy("V-01", policy); err != nil {
		t.Fatal(err)
	}
	trigger.Observe(levelSample("V-01", 15, clock.Now()))
	session := activeSession(t, service, "V-01")
	if _, err := service.Abort(context.Background(), session.ID, "test abort"); err != nil {
		t.Fatal(err)
	}
	if err := trigger.SetPolicy("V-01", policy); err != nil {
		t.Fatal(err)
	}
	trigger.Observe(levelSample("V-01", 15, clock.Now()))
	if got := automaticSessions(service); got != 2 {
		t.Fatalf("expected policy update to reset the round, got %d automatic sessions", got)
	}
}

func TestAutoPolicyValidation(t *testing.T) {
	valid := AutoPolicy{LowThreshold: 20, RecoveryThreshold: 28, ConsecutiveSamples: 3, Cooldown: time.Minute}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid policy rejected: %v", err)
	}
	equal := AutoPolicy{LowThreshold: 20, RecoveryThreshold: 20, ConsecutiveSamples: 1, Cooldown: 0}
	if err := equal.Validate(); err != nil {
		t.Fatalf("equal thresholds should be allowed: %v", err)
	}
	cases := []AutoPolicy{
		{LowThreshold: 0, RecoveryThreshold: 28, ConsecutiveSamples: 3, Cooldown: time.Minute},
		{LowThreshold: -1, RecoveryThreshold: 28, ConsecutiveSamples: 3, Cooldown: time.Minute},
		{LowThreshold: 20, RecoveryThreshold: 10, ConsecutiveSamples: 3, Cooldown: time.Minute},
		{LowThreshold: 20, RecoveryThreshold: 28, ConsecutiveSamples: 0, Cooldown: time.Minute},
		{LowThreshold: 20, RecoveryThreshold: 28, ConsecutiveSamples: 3, Cooldown: -time.Second},
	}
	for i, policy := range cases {
		if err := policy.Validate(); err == nil {
			t.Fatalf("invalid policy %d was accepted", i)
		}
	}
}
