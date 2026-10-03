package fill

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/wyw14/cry-98/internal/model"
)

func testPolicy() AutoFillPolicy {
	return AutoFillPolicy{
		LowThreshold:       20,
		RecoveryThreshold:  25,
		ConsecutiveSamples: 3,
		Cooldown:           10 * time.Minute,
	}
}

func levelSample(vesselID string, value float64) model.Sample {
	return model.Sample{ProbeID: "P-L01", VesselID: vesselID, Kind: model.ProbeLevel, Value: value, ObservedAt: time.Now()}
}

func newTestTrigger(t *testing.T, now func() time.Time, demand AutomaticDemand) *AutoTrigger {
	t.Helper()
	trigger, err := NewAutoTrigger(map[string]AutoFillPolicy{"V-01": testPolicy()}, demand, now)
	if err != nil {
		t.Fatal(err)
	}
	return trigger
}

func TestAutoTriggerRequiresConsecutiveLowSamples(t *testing.T) {
	var demands int
	trigger := newTestTrigger(t, time.Now, func(context.Context, string, time.Time) error {
		demands++
		return nil
	})
	for i := 0; i < 2; i++ {
		if err := trigger.Observe(context.Background(), levelSample("V-01", 15)); err != nil {
			t.Fatal(err)
		}
	}
	if demands != 0 {
		t.Fatalf("demand submitted before %d consecutive low samples", testPolicy().ConsecutiveSamples)
	}
	// A reading above the low threshold breaks the streak.
	if err := trigger.Observe(context.Background(), levelSample("V-01", 22)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := trigger.Observe(context.Background(), levelSample("V-01", 15)); err != nil {
			t.Fatal(err)
		}
	}
	if demands != 0 {
		t.Fatal("streak was not reset by a non-low sample")
	}
	if err := trigger.Observe(context.Background(), levelSample("V-01", 15)); err != nil {
		t.Fatal(err)
	}
	if demands != 1 {
		t.Fatalf("expected one demand after the streak completed, got %d", demands)
	}
}

func TestAutoTriggerSubmitsOncePerLowEpisode(t *testing.T) {
	var demands int
	trigger := newTestTrigger(t, time.Now, func(context.Context, string, time.Time) error {
		demands++
		return nil
	})
	for i := 0; i < 3; i++ {
		if err := trigger.Observe(context.Background(), levelSample("V-01", 15)); err != nil {
			t.Fatal(err)
		}
	}
	if demands != 1 {
		t.Fatalf("expected one demand, got %d", demands)
	}
	for i := 0; i < 5; i++ {
		if err := trigger.Observe(context.Background(), levelSample("V-01", 10)); err != nil {
			t.Fatal(err)
		}
	}
	if demands != 1 {
		t.Fatalf("low episode submitted more than one demand: %d", demands)
	}
	// A reading in the hysteresis band must not rearm the trigger.
	if err := trigger.Observe(context.Background(), levelSample("V-01", 22)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := trigger.Observe(context.Background(), levelSample("V-01", 15)); err != nil {
			t.Fatal(err)
		}
	}
	if demands != 1 {
		t.Fatalf("hysteresis-band reading rearmed the trigger: %d demands", demands)
	}
}

func TestAutoTriggerRearmsAfterRecovery(t *testing.T) {
	var demands int
	current := time.Now()
	trigger := newTestTrigger(t, func() time.Time { return current }, func(context.Context, string, time.Time) error {
		demands++
		return nil
	})
	for i := 0; i < 3; i++ {
		if err := trigger.Observe(context.Background(), levelSample("V-01", 15)); err != nil {
			t.Fatal(err)
		}
	}
	if demands != 1 {
		t.Fatalf("expected one demand, got %d", demands)
	}
	if err := trigger.Observe(context.Background(), levelSample("V-01", 30)); err != nil {
		t.Fatal(err)
	}
	// Recovered, but still inside the cooldown window.
	for i := 0; i < 3; i++ {
		if err := trigger.Observe(context.Background(), levelSample("V-01", 15)); err != nil {
			t.Fatal(err)
		}
	}
	if demands != 1 {
		t.Fatalf("cooldown did not suppress the demand: %d", demands)
	}
	current = current.Add(11 * time.Minute)
	for i := 0; i < 3; i++ {
		if err := trigger.Observe(context.Background(), levelSample("V-01", 15)); err != nil {
			t.Fatal(err)
		}
	}
	if demands != 2 {
		t.Fatalf("expected a second demand after recovery and cooldown, got %d", demands)
	}
}

func TestAutoTriggerRejectedDemandDoesNotLatch(t *testing.T) {
	var demands int
	reject := true
	trigger := newTestTrigger(t, time.Now, func(context.Context, string, time.Time) error {
		demands++
		if reject {
			return ErrActiveSession
		}
		return nil
	})
	for i := 0; i < 2; i++ {
		if err := trigger.Observe(context.Background(), levelSample("V-01", 15)); err != nil {
			t.Fatal(err)
		}
	}
	// The streak completes while a manual session is active; the demand is
	// rejected and must not latch the episode.
	for i := 0; i < 2; i++ {
		if err := trigger.Observe(context.Background(), levelSample("V-01", 15)); !errors.Is(err, ErrActiveSession) {
			t.Fatalf("expected ErrActiveSession, got %v", err)
		}
	}
	if demands != 2 {
		t.Fatalf("expected two demand attempts, got %d", demands)
	}
	// The manual session ends; the next low sample must retry the demand.
	reject = false
	if err := trigger.Observe(context.Background(), levelSample("V-01", 15)); err != nil {
		t.Fatal(err)
	}
	if demands != 3 {
		t.Fatalf("rejected demand consumed the episode: %d attempts", demands)
	}
}

func TestAutoTriggerIgnoresUnconfiguredVessels(t *testing.T) {
	var demands int
	trigger := newTestTrigger(t, time.Now, func(context.Context, string, time.Time) error {
		demands++
		return nil
	})
	for i := 0; i < 10; i++ {
		if err := trigger.Observe(context.Background(), levelSample("V-99", 1)); err != nil {
			t.Fatal(err)
		}
	}
	if demands != 0 {
		t.Fatalf("unconfigured vessel produced %d demands", demands)
	}
}

func TestAutoTriggerPerVesselPolicies(t *testing.T) {
	var demands []string
	trigger, err := NewAutoTrigger(map[string]AutoFillPolicy{
		"V-01": testPolicy(),
		"V-02": {LowThreshold: 50, RecoveryThreshold: 60, ConsecutiveSamples: 1, Cooldown: 0},
	}, func(_ context.Context, vesselID string, _ time.Time) error {
		demands = append(demands, vesselID)
		return nil
	}, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if err := trigger.Observe(context.Background(), levelSample("V-01", 30)); err != nil {
		t.Fatal(err)
	}
	if err := trigger.Observe(context.Background(), levelSample("V-02", 30)); err != nil {
		t.Fatal(err)
	}
	if len(demands) != 1 || demands[0] != "V-02" {
		t.Fatalf("per-vessel thresholds not applied: %v", demands)
	}
}

func TestAutoFillPolicyValidation(t *testing.T) {
	valid := testPolicy()
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	cases := []AutoFillPolicy{
		{LowThreshold: 0, RecoveryThreshold: 25, ConsecutiveSamples: 3},
		{LowThreshold: 20, RecoveryThreshold: 19, ConsecutiveSamples: 3},
		{LowThreshold: 20, RecoveryThreshold: 25, ConsecutiveSamples: 0},
		{LowThreshold: 20, RecoveryThreshold: 25, ConsecutiveSamples: 3, Cooldown: -time.Second},
	}
	for i, policy := range cases {
		if err := policy.Validate(); err == nil {
			t.Fatalf("case %d: invalid policy accepted", i)
		}
	}
	trigger, err := NewAutoTrigger(map[string]AutoFillPolicy{"V-01": cases[0]}, func(context.Context, string, time.Time) error { return nil }, nil)
	if err == nil {
		t.Fatal("invalid seed policy accepted")
	}
	if trigger != nil {
		t.Fatal("trigger returned with invalid policy")
	}
}
