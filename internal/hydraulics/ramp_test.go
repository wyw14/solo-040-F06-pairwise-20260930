package hydraulics

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/gate"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
)

func newTestRamp(interval time.Duration) (*Ramp, *clock) {
	clk := &clock{}
	return NewRamp(interval, clk.now), clk
}

type clock struct{ t time.Time }

func (c *clock) now() time.Time {
	c.t = c.t.Add(time.Second)
	return c.t
}

// scriptedIssuer records every opening and blocks on releases per issue, so a
// test can hold the ramp mid-issue at a deterministic safe point.
func scriptedIssuer(releases chan struct{}, recorded *[]float64, mu *sync.Mutex, failOn map[float64]bool) StepIssuer {
	return func(_ context.Context, target float64) error {
		mu.Lock()
		*recorded = append(*recorded, target)
		mu.Unlock()
		if releases != nil {
			<-releases
		}
		if failOn[target] {
			return errors.New("field bus offline")
		}
		return nil
	}
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("condition not met before deadline")
}

func TestRampIssuesEveryStepOnce(t *testing.T) {
	ramp, _ := newTestRamp(2 * time.Millisecond)
	var mu sync.Mutex
	var issued []float64
	targets := []float64{0.1, 0.2, 0.4, 0.7}
	view, err := ramp.Start("c1", uuid.New(), 1, targets, scriptedIssuer(nil, &issued, &mu, nil))
	if err != nil {
		t.Fatal(err)
	}
	if view.Status != RampRunning || view.Completed != 0 {
		t.Fatalf("unexpected initial view: %+v", view)
	}
	waitFor(t, func() bool {
		v, _ := ramp.Status("c1")
		return v.Status == RampCompleted
	})
	final, _ := ramp.Status("c1")
	if final.Completed != len(targets) {
		t.Fatalf("completed = %d, want %d", final.Completed, len(targets))
	}
	mu.Lock()
	defer mu.Unlock()
	if len(issued) != len(targets) {
		t.Fatalf("issued %v, want %v", issued, targets)
	}
	for i := range targets {
		if issued[i] != targets[i] {
			t.Fatalf("issued %v, want %v", issued, targets)
		}
	}
}

func TestPauseThenResumeNeverRepeatsAnOpening(t *testing.T) {
	ramp, _ := newTestRamp(5 * time.Millisecond)
	var mu sync.Mutex
	var issued []float64
	releases := make(chan struct{})
	targets := []float64{0.1, 0.2, 0.4, 0.7}
	cycleID := uuid.New()
	if _, err := ramp.Start("c1", cycleID, 3, targets, scriptedIssuer(releases, &issued, &mu, nil)); err != nil {
		t.Fatal(err)
	}
	// Wait until the first opening is in flight, then pause and release it. The
	// pause flag is set before the issue returns, so the run halts at the very
	// next safe point with exactly one step committed.
	waitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(issued) == 1
	})
	paused := make(chan RampRunView, 1)
	go func() {
		view, err := ramp.Pause("c1", "operator review")
		if err != nil {
			t.Errorf("pause: %v", err)
		}
		paused <- view
	}()
	time.Sleep(20 * time.Millisecond) // let Pause close the stop gate
	releases <- struct{}{}            // allow the in-flight first opening to settle
	view := <-paused
	if view.Status != RampPaused || view.Completed != 1 || !view.Resumable {
		t.Fatalf("unexpected paused view: %+v", view)
	}
	if view.NextIndex == nil || *view.NextIndex != 1 {
		t.Fatalf("next index = %v, want 1 (second step)", view.NextIndex)
	}

	// Resume from the second step; release the three remaining openings.
	view, err := ramp.Resume("c1", cycleID, 3, scriptedIssuer(releases, &issued, &mu, nil), "ops-leader")
	if err != nil {
		t.Fatal(err)
	}
	if view.Completed != 1 || view.Status != RampRunning {
		t.Fatalf("resume must continue from step 2, got %+v", view)
	}
	for i := 0; i < 3; i++ {
		releases <- struct{}{}
	}
	waitFor(t, func() bool {
		v, _ := ramp.Status("c1")
		return v.Status == RampCompleted
	})

	mu.Lock()
	defer mu.Unlock()
	want := []float64{0.1, 0.2, 0.4, 0.7}
	if len(issued) != len(want) {
		t.Fatalf("issued %d openings %v, want exactly %d with no repeats", len(issued), issued, len(want))
	}
	for i := range want {
		if issued[i] != want[i] {
			t.Fatalf("issued %v, want %v (an opening was repeated or skipped)", issued, want)
		}
	}
}

func TestResumeRejectedForAnotherGeneration(t *testing.T) {
	ramp, _ := newTestRamp(time.Hour)
	var mu sync.Mutex
	var issued []float64
	releases := make(chan struct{})
	cycleID := uuid.New()
	if _, err := ramp.Start("c1", cycleID, 2, []float64{0.1, 0.2}, scriptedIssuer(releases, &issued, &mu, nil)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(issued) == 1
	})
	go func() { _, _ = ramp.Pause("c1", "review") }()
	time.Sleep(20 * time.Millisecond)
	releases <- struct{}{}
	waitFor(t, func() bool {
		v, _ := ramp.Status("c1")
		return v.Status == RampPaused
	})

	// Newer generation (e.g. a replacement transit) must not resume the ramp.
	if _, err := ramp.Resume("c1", cycleID, 3, scriptedIssuer(nil, &issued, &mu, nil), "ops"); !errors.Is(err, ErrRampGeneration) {
		t.Fatalf("generation guard err = %v, want ErrRampGeneration", err)
	}
	// A different cycle id at the same generation number is rejected too.
	if _, err := ramp.Resume("c1", uuid.New(), 2, scriptedIssuer(nil, &issued, &mu, nil), "ops"); !errors.Is(err, ErrRampGeneration) {
		t.Fatalf("cycle identity guard err = %v, want ErrRampGeneration", err)
	}
}

func TestAbortForbidsResumeEvenWhileIssueInFlight(t *testing.T) {
	ramp, _ := newTestRamp(time.Hour)
	var mu sync.Mutex
	var issued []float64
	releases := make(chan struct{})
	cycleID := uuid.New()
	if _, err := ramp.Start("c1", cycleID, 1, []float64{0.1, 0.2, 0.4}, scriptedIssuer(releases, &issued, &mu, nil)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(issued) == 1
	})
	// Abort is the emergency/cancellation path: it must return immediately even
	// though the first command is still in flight.
	view, err := ramp.Abort("c1", "transit emergency")
	if err != nil || view.Status != RampAborted {
		t.Fatalf("abort view = %+v, err = %v", view, err)
	}
	releases <- struct{}{} // let the in-flight opening settle afterwards
	waitFor(t, func() bool {
		v, _ := ramp.Status("c1")
		return v.Completed == 1
	})

	if _, err := ramp.Resume("c1", cycleID, 1, scriptedIssuer(nil, &issued, &mu, nil), "ops"); err == nil {
		t.Fatal("resume after abort must fail")
	}
	status, _ := ramp.Status("c1")
	if status.Resumable || status.Completed != 1 {
		t.Fatalf("aborted ramp must keep its one issued step but never resume: %+v", status)
	}
}

func TestAbortIfGenerationLeavesSupersededRampAlone(t *testing.T) {
	ramp, _ := newTestRamp(5 * time.Millisecond)
	var mu sync.Mutex
	var issued []float64
	if _, err := ramp.Start("c1", uuid.New(), 2, []float64{0.1}, scriptedIssuer(nil, &issued, &mu, nil)); err != nil {
		t.Fatal(err)
	}
	// An event for the previous generation must not touch this ramp.
	if _, matched, err := ramp.AbortIfGeneration("c1", uuid.New(), 1, "stale cancel"); err != nil || matched {
		t.Fatalf("matched = %v, err = %v, want false/nil", matched, err)
	}
	waitFor(t, func() bool {
		v, _ := ramp.Status("c1")
		return v.Status == RampCompleted
	})
}

func TestStartRejectedWhileRampActive(t *testing.T) {
	ramp, _ := newTestRamp(time.Hour)
	var mu sync.Mutex
	var issued []float64
	releases := make(chan struct{})
	cycleID := uuid.New()
	if _, err := ramp.Start("c1", cycleID, 1, []float64{0.1, 0.2}, scriptedIssuer(releases, &issued, &mu, nil)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(issued) == 1
	})
	if _, err := ramp.Start("c1", uuid.New(), 2, []float64{0.9}, scriptedIssuer(nil, &issued, &mu, nil)); !errors.Is(err, ErrRampActive) {
		t.Fatalf("start guard err = %v, want ErrRampActive", err)
	}
	releases <- struct{}{}
}

func TestIssuerFailureParksRamp(t *testing.T) {
	ramp, _ := newTestRamp(2 * time.Millisecond)
	var mu sync.Mutex
	var issued []float64
	cycleID := uuid.New()
	if _, err := ramp.Start("c1", cycleID, 1, []float64{0.1, 0.2},
		scriptedIssuer(nil, &issued, &mu, map[float64]bool{0.2: true})); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		v, _ := ramp.Status("c1")
		return v.Status == RampPaused
	})
	view, _ := ramp.Status("c1")
	if view.Completed != 1 || view.PauseReason == "" {
		t.Fatalf("failed step must park after the last committed step: %+v", view)
	}
}

// Controller-level integration: step issues become queued valve commands, and
// every lifecycle transition lands in the journal with generation stamped.
func TestControllerRampCommandsAndJournal(t *testing.T) {
	store := journal.NewMemoryStore()
	recorder := journal.NewRecorder(store, nil)
	commands := gate.NewCommands()
	clk := &clock{}
	ramp := NewRamp(2*time.Millisecond, clk.now)
	controller := NewController(commands, recorder, ramp, clk.now)

	cycleValue, err := model.NewTransitCycle("c1", uuid.New(), model.DirectionUpstream, 1, clk.now())
	if err != nil {
		t.Fatal(err)
	}
	view, err := controller.StartRamp(cycleValue, []float64{0.1, -0.3, 0.7})
	if err != nil {
		t.Fatal(err)
	}
	if view.Status != RampRunning {
		t.Fatalf("status = %s", view.Status)
	}
	waitFor(t, func() bool {
		v, _ := controller.RampStatus("c1")
		return v.Status == RampCompleted
	})
	// The completed audit event is written just after the state flips; wait for
	// it in the journal rather than racing the transition hook.
	waitFor(t, func() bool {
		events, err := store.Events(context.Background(), "c1", 0)
		if err != nil {
			return false
		}
		for _, event := range events {
			if event.Type == model.EventRampCompleted {
				return true
			}
		}
		return false
	})

	pending := commands.Pending("c1")
	if len(pending) != 3 {
		t.Fatalf("queued commands = %d, want 3", len(pending))
	}
	if pending[0].Device != model.DeviceFillValve || pending[1].Device != model.DeviceDrainValve {
		t.Fatalf("negative targets must drive the drain valve: %+v", pending)
	}
	events, err := store.Events(context.Background(), "c1", 0)
	if err != nil {
		t.Fatal(err)
	}
	var kinds []model.EventType
	for _, event := range events {
		kinds = append(kinds, event.Type)
	}
	assertContainsEvent(t, kinds, model.EventRampStarted)
	assertContainsEvent(t, kinds, model.EventRampCompleted)
	assertContainsEvent(t, kinds, model.EventCommandQueued)
}

func assertContainsEvent(t *testing.T, kinds []model.EventType, want model.EventType) {
	t.Helper()
	for _, kind := range kinds {
		if kind == want {
			return
		}
	}
	t.Errorf("journal events %v do not contain %s", kinds, want)
}
