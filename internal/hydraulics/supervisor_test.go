package hydraulics

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
)

type fakeIssuer struct {
	mu      sync.Mutex
	issued  []float64
	entered chan int
	gates   []chan struct{}
	fail    bool
}

func (f *fakeIssuer) IssueStep(_ context.Context, cycle model.TransitCycle, target float64) (model.DeviceCommand, error) {
	f.mu.Lock()
	index := len(f.issued)
	f.issued = append(f.issued, target)
	f.mu.Unlock()
	if f.entered != nil {
		f.entered <- index
	}
	if f.gates != nil && index < len(f.gates) && f.gates[index] != nil {
		<-f.gates[index]
	}
	return model.DeviceCommand{ID: uuid.New(), CycleID: cycle.ID, ChamberID: cycle.ChamberID, Generation: cycle.Generation, Target: target}, nil
}

func (f *fakeIssuer) targets() []float64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]float64(nil), f.issued...)
}

type harness struct {
	t        *testing.T
	issuer   *fakeIssuer
	recorder *journal.Recorder
	store    *journal.MemoryStore
	super    *Supervisor
	cycle    model.TransitCycle
	emg      bool
}

func newHarness(t *testing.T, steps int, interval time.Duration) *harness {
	t.Helper()
	cycleValue, err := model.NewTransitCycle("chamber-1", uuid.New(), model.DirectionUpstream, 1, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	store := journal.NewMemoryStore()
	recorder := journal.NewRecorder(store, time.Now)
	issuer := &fakeIssuer{entered: make(chan int, steps*2+4)}
	gates := make([]chan struct{}, steps)
	for i := range gates {
		gates[i] = make(chan struct{})
	}
	issuer.gates = gates
	h := &harness{t: t, issuer: issuer, recorder: recorder, store: store, cycle: cycleValue}
	h.super = newSupervisor(issuer, recorder, interval, time.Now)
	h.super.BindGuards(func(string) (model.TransitCycle, bool) {
		return h.cycle, true
	}, func(string) bool { return h.emg })
	return h
}

func (h *harness) release(step int) { close(h.issuer.gates[step]) }

func waitFor(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("timed out waiting for " + msg)
}

func TestRampRunsAllStepsOnce(t *testing.T) {
	h := newHarness(t, 4, time.Millisecond)
	targets := []float64{10, 20, 30, 40}
	if _, err := h.super.Start(context.Background(), h.cycle, targets); err != nil {
		t.Fatal(err)
	}
	for i := range targets {
		<-h.issuer.entered
		h.release(i)
	}
	waitFor(t, func() bool {
		plan, _ := h.super.Current("chamber-1")
		return plan.Status == RampCompleted
	}, "completion")
	got := h.issuer.targets()
	if len(got) != len(targets) {
		t.Fatalf("issued %v, want %v", got, targets)
	}
	for i := range targets {
		if got[i] != targets[i] {
			t.Fatalf("step %d = %v, want %v", i, got[i], targets[i])
		}
	}
	plan, _ := h.super.Current("chamber-1")
	if plan.NextIndex != len(targets) {
		t.Fatalf("next index = %d", plan.NextIndex)
	}
	if len(plan.CompletedSteps()) != len(targets) || len(plan.PendingSteps()) != 0 {
		t.Fatalf("completed=%d pending=%d", len(plan.CompletedSteps()), len(plan.PendingSteps()))
	}
}

func TestRampPauseAndResumeContinuesFromNextStep(t *testing.T) {
	h := newHarness(t, 5, time.Millisecond)
	targets := []float64{10, 20, 30, 40, 50}
	if _, err := h.super.Start(context.Background(), h.cycle, targets); err != nil {
		t.Fatal(err)
	}
	// Let steps 0 and 1 finish; pause while step 2 is in flight.
	<-h.issuer.entered
	h.release(0)
	<-h.issuer.entered
	h.release(1)
	<-h.issuer.entered
	if _, err := h.super.Pause(context.Background(), "chamber-1", "operator check"); err != nil {
		t.Fatal(err)
	}
	h.release(2)

	waitFor(t, func() bool {
		plan, _ := h.super.Current("chamber-1")
		return plan.Status == RampPaused && plan.NextIndex == 3
	}, "pause after step 2")

	// Resume without a confirmed safety review must be rejected.
	if _, err := h.super.Resume(context.Background(), "chamber-1", SafetyReview{Reviewer: "ops"}); err != ErrSafetyReview {
		t.Fatalf("got %v, want ErrSafetyReview", err)
	}
	if got := h.issuer.targets(); len(got) != 3 {
		t.Fatalf("steps issued while paused = %v", got)
	}

	if _, err := h.super.Resume(context.Background(), "chamber-1", SafetyReview{
		Reviewer: "ops-lead", Confirmed: true, Note: "level stable",
	}); err != nil {
		t.Fatal(err)
	}
	// Remaining steps 3 and 4 dispatch exactly once.
	select {
	case i := <-h.issuer.entered:
		if i != 3 {
			t.Fatalf("resume resumed at step %d, want 3", i)
		}
		h.release(3)
	case <-time.After(time.Second):
		t.Fatal("step 3 never issued after resume")
	}
	select {
	case i := <-h.issuer.entered:
		if i != 4 {
			t.Fatalf("got step %d, want 4", i)
		}
		h.release(4)
	case <-time.After(time.Second):
		t.Fatal("step 4 never issued after resume")
	}
	waitFor(t, func() bool {
		plan, _ := h.super.Current("chamber-1")
		return plan.Status == RampCompleted
	}, "completion after resume")
	got := h.issuer.targets()
	if len(got) != 5 {
		t.Fatalf("issued %v, want all 5 steps once", got)
	}
}

func TestRampRejectsResumeWhenCycleCancelled(t *testing.T) {
	h := newHarness(t, 4, time.Millisecond)
	h.startAndPauseAt(t, []float64{10, 20, 30, 40}, 2)

	// Simulate the cycle moving to a terminal (cancelled) stage.
	h.cycle.Stage = model.StageCancelled
	_, err := h.super.Resume(context.Background(), "chamber-1", SafetyReview{Reviewer: "ops", Confirmed: true})
	if err != ErrCycleTerminal {
		t.Fatalf("got %v, want ErrCycleTerminal", err)
	}
	plan, _ := h.super.Current("chamber-1")
	if plan.Status != RampAborted {
		t.Fatalf("status = %s, want aborted", plan.Status)
	}
	for _, step := range plan.PendingSteps() {
		t.Fatalf("step %d still pending after abort", step.Index)
	}
	if got := h.issuer.targets(); len(got) != 3 {
		t.Fatalf("issued %v after cancelled resume, want no extra steps", got)
	}
	// An aborted ramp can never be resumed.
	if _, err := h.super.Resume(context.Background(), "chamber-1", SafetyReview{Reviewer: "ops", Confirmed: true}); err != ErrRampNotResumable {
		t.Fatalf("got %v, want ErrRampNotResumable", err)
	}
}

func TestRampRejectsResumeOnGenerationChange(t *testing.T) {
	h := newHarness(t, 4, time.Millisecond)
	h.startAndPauseAt(t, []float64{10, 20, 30, 40}, 2)

	h.cycle.Generation++
	if _, err := h.super.Resume(context.Background(), "chamber-1", SafetyReview{Reviewer: "ops", Confirmed: true}); err != ErrCycleNotCurrent {
		t.Fatalf("got %v, want ErrCycleNotCurrent", err)
	}
	plan, _ := h.super.Current("chamber-1")
	if plan.Status != RampAborted {
		t.Fatalf("status = %s, want aborted", plan.Status)
	}
}

func TestRampRejectsResumeDuringEmergency(t *testing.T) {
	h := newHarness(t, 4, time.Millisecond)
	h.startAndPauseAt(t, []float64{10, 20, 30, 40}, 2)

	h.emg = true
	if _, err := h.super.Resume(context.Background(), "chamber-1", SafetyReview{Reviewer: "ops", Confirmed: true}); err != ErrCycleTerminal {
		t.Fatalf("got %v, want ErrCycleTerminal", err)
	}
}

func TestRampAbortStopsDispatchAndAllowsNoResume(t *testing.T) {
	h := newHarness(t, 4, time.Millisecond)
	h.startAndPauseAt(t, []float64{10, 20, 30, 40}, 2)

	if _, ok := h.super.AbortIfIdentity(context.Background(), "chamber-1", h.cycle.ID, h.cycle.Generation, "emergency"); !ok {
		t.Fatal("expected ramp abort to match the generation")
	}
	// An abort for a different generation must not touch this ramp after it is
	// already terminal.
	plan, _ := h.super.Current("chamber-1")
	if plan.Status != RampAborted {
		t.Fatalf("status = %s, want aborted", plan.Status)
	}
	if got := h.issuer.targets(); len(got) != 3 {
		t.Fatalf("issued %v after abort", got)
	}
}

func TestRampReconstructsFromJournal(t *testing.T) {
	h := newHarness(t, 5, time.Millisecond)
	h.startAndPauseAt(t, []float64{10, 20, 30, 40, 50}, 3)

	// Wait until the in-flight step's issuance event is durable before replay.
	waitFor(t, func() bool {
		events, err := h.store.Events(context.Background(), "chamber-1", 0)
		if err != nil {
			return false
		}
		var issued int
		for _, event := range events {
			if event.Type == model.EventRampStepIssued {
				issued++
			}
		}
		return issued == 4
	}, "four step events persisted")

	events, err := h.store.Events(context.Background(), "chamber-1", 0)
	if err != nil {
		t.Fatal(err)
	}
	plans := ReconstructPlans(events)
	if len(plans) != 1 {
		t.Fatalf("reconstructed %d plans", len(plans))
	}
	plan := plans[0]
	if plan.Status != RampPaused || plan.NextIndex != 4 {
		t.Fatalf("reconstructed status=%s next=%d", plan.Status, plan.NextIndex)
	}
	if len(plan.CompletedSteps()) != 4 || len(plan.PendingSteps()) != 1 {
		t.Fatalf("completed=%d pending=%d", len(plan.CompletedSteps()), len(plan.PendingSteps()))
	}
	for _, step := range plan.CompletedSteps() {
		if step.CommandID == "" {
			t.Fatalf("step %d missing command id", step.Index)
		}
	}

	// A ramp left "running" by a crashed process must restore as paused so a
	// field actuator is never driven without a fresh safety review.
	store2 := journal.NewMemoryStore()
	rec2 := journal.NewRecorder(store2, time.Now)
	cycleValue := h.cycle
	live := RampPlan{
		PlanID: uuid.NewString(), ChamberID: "chamber-1", CycleID: cycleValue.ID.String(),
		Generation: cycleValue.Generation, Status: RampRunning,
		Steps: []RampStep{{Index: 0, Target: 10, State: StepPending}},
	}
	if _, err := rec2.Record(context.Background(), "chamber-1", model.EventRampCreated, cycleValue.ID, cycleValue.Generation, live); err != nil {
		t.Fatal(err)
	}
	events2, _ := store2.Events(context.Background(), "chamber-1", 0)
	restored := ReconstructPlans(events2)
	if restored[0].Status != RampPaused {
		t.Fatalf("running plan restored as %s, want paused", restored[0].Status)
	}
}

func TestRampRejectsSecondActivePlan(t *testing.T) {
	h := newHarness(t, 4, time.Millisecond)
	if _, err := h.super.Start(context.Background(), h.cycle, []float64{10, 20}); err != nil {
		t.Fatal(err)
	}
	<-h.issuer.entered // step in flight, ramp still running
	if _, err := h.super.Start(context.Background(), h.cycle, []float64{30}); err != ErrRampActive {
		t.Fatalf("got %v, want ErrRampActive", err)
	}
}

// startAndPauseAt runs steps [0, pauseStep) to completion and pauses while
// step pauseStep is in flight, then releases it so it is recorded truthfully.
func (h *harness) startAndPauseAt(t *testing.T, targets []float64, pauseStep int) {
	t.Helper()
	if _, err := h.super.Start(context.Background(), h.cycle, targets); err != nil {
		t.Fatal(err)
	}
	for i := 0; i <= pauseStep; i++ {
		<-h.issuer.entered
		if i < pauseStep {
			h.release(i)
		}
	}
	if _, err := h.super.Pause(context.Background(), "chamber-1", "test pause"); err != nil {
		t.Fatal(err)
	}
	h.release(pauseStep)
	waitFor(t, func() bool {
		plan, _ := h.super.Current("chamber-1")
		return plan.Status == RampPaused && plan.NextIndex == pauseStep+1
	}, "paused state")
}
