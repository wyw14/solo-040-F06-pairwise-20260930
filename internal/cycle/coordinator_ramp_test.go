package cycle

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/gate"
	"github.com/wyw14/cry-96/internal/hydraulics"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
)

type rampRig struct {
	coord *Coordinator
	hyd   *hydraulics.Controller
	plan  model.ConvoyPlan
}

func newRampRig(t *testing.T) rampRig {
	t.Helper()
	chamberID := "chamber-1"
	store := journal.NewMemoryStore()
	now := time.Now
	recorder := journal.NewRecorder(store, now)
	state := gate.NewState(chamberID)
	commands := gate.NewCommands()
	gates := gate.NewController(state, commands, recorder, now)
	hyd := hydraulics.NewController(commands, recorder, time.Millisecond, now)
	coord := NewCoordinator(gates, hyd, recorder, now)
	hyd.Supervisor().BindGuards(coord.Current, func(string) bool { return false })

	plan := model.ConvoyPlan{
		ID: uuid.New(), ChamberID: chamberID,
		Direction: model.DirectionUpstream,
		Vessels:   []model.Vessel{{ID: uuid.New(), Name: "River Star", CallSign: "RS-1", LengthM: 80}},
		CreatedAt: now(),
	}
	if _, err := coord.Start(context.Background(), plan); err != nil {
		t.Fatal(err)
	}
	return rampRig{coord: coord, hyd: hyd, plan: plan}
}

func waitRamp(t *testing.T, rig rampRig, cond func(hydraulics.RampPlan) bool, msg string) hydraulics.RampPlan {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		plan, ok := rig.hyd.Ramp(rig.plan.ChamberID)
		if ok && cond(plan) {
			return plan
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("timed out waiting for " + msg)
	return hydraulics.RampPlan{}
}

func TestCycleCancelAbortsRampAndBlocksResume(t *testing.T) {
	rig := newRampRig(t)
	cycleValue, _ := rig.coord.Current(rig.plan.ChamberID)
	targets := []float64{10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120}
	if _, err := rig.hyd.StartRamp(context.Background(), cycleValue, targets); err != nil {
		t.Fatal(err)
	}
	waitRamp(t, rig, func(p hydraulics.RampPlan) bool { return p.NextIndex >= 2 && p.Status == hydraulics.RampRunning }, "two steps issued")
	if _, err := rig.hyd.PauseRamp(context.Background(), rig.plan.ChamberID, "operator pause"); err != nil {
		t.Fatal(err)
	}
	waitRamp(t, rig, func(p hydraulics.RampPlan) bool { return p.Status == hydraulics.RampPaused }, "pause")

	cancelled, err := rig.coord.Cancel(context.Background(), rig.plan.ChamberID, "operator cancellation")
	if err != nil {
		t.Fatal(err)
	}
	if cancelled.Stage != model.StageCancelled {
		t.Fatalf("stage = %s", cancelled.Stage)
	}
	plan := waitRamp(t, rig, func(p hydraulics.RampPlan) bool { return p.Status == hydraulics.RampAborted }, "abort on cancel")
	if len(plan.CompletedSteps()) < 2 || len(plan.PendingSteps()) != 0 {
		t.Fatalf("completed=%d pending=%d", len(plan.CompletedSteps()), len(plan.PendingSteps()))
	}

	if _, err := rig.hyd.ResumeRamp(context.Background(), rig.plan.ChamberID, hydraulics.SafetyReview{
		Reviewer: "ops", Confirmed: true,
	}); err == nil {
		t.Fatal("resume after cancellation must fail")
	}
}

func TestEmergencyAbortsRamp(t *testing.T) {
	rig := newRampRig(t)
	cycleValue, _ := rig.coord.Current(rig.plan.ChamberID)
	targets := []float64{10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120}
	if _, err := rig.hyd.StartRamp(context.Background(), cycleValue, targets); err != nil {
		t.Fatal(err)
	}
	waitRamp(t, rig, func(p hydraulics.RampPlan) bool { return p.NextIndex >= 1 && p.Status == hydraulics.RampRunning }, "one step issued")
	if _, err := rig.hyd.PauseRamp(context.Background(), rig.plan.ChamberID, "hold before emergency drill"); err != nil {
		t.Fatal(err)
	}
	waitRamp(t, rig, func(p hydraulics.RampPlan) bool { return p.Status == hydraulics.RampPaused }, "pause")
	if _, err := rig.coord.Cancel(context.Background(), rig.plan.ChamberID, "emergency"); err != nil {
		t.Fatal(err)
	}
	plan := waitRamp(t, rig, func(p hydraulics.RampPlan) bool { return p.Status == hydraulics.RampAborted }, "abort on emergency")
	if plan.AbortReason == "" {
		t.Fatal("expected abort reason referencing emergency")
	}
}

func TestNewGenerationDoesNotResumeOldRamp(t *testing.T) {
	rig := newRampRig(t)
	cycleValue, _ := rig.coord.Current(rig.plan.ChamberID)
	targets := []float64{10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120}
	if _, err := rig.hyd.StartRamp(context.Background(), cycleValue, targets); err != nil {
		t.Fatal(err)
	}
	waitRamp(t, rig, func(p hydraulics.RampPlan) bool { return p.NextIndex >= 1 && p.Status == hydraulics.RampRunning }, "one step issued")
	if _, err := rig.hyd.PauseRamp(context.Background(), rig.plan.ChamberID, "pause"); err != nil {
		t.Fatal(err)
	}
	waitRamp(t, rig, func(p hydraulics.RampPlan) bool { return p.Status == hydraulics.RampPaused }, "pause")
	if _, err := rig.coord.Cancel(context.Background(), rig.plan.ChamberID, "operator cancellation"); err != nil {
		t.Fatal(err)
	}

	next := model.ConvoyPlan{
		ID: uuid.New(), ChamberID: rig.plan.ChamberID,
		Direction: model.DirectionDownstream,
		Vessels:   []model.Vessel{{ID: uuid.New(), Name: "Sea Bird", CallSign: "SB-9", LengthM: 70}},
		CreatedAt: time.Now(),
	}
	newCycle, err := rig.coord.Start(context.Background(), next)
	if err != nil {
		t.Fatal(err)
	}
	if newCycle.Generation != cycleValue.Generation+1 {
		t.Fatalf("generation = %d, want %d", newCycle.Generation, cycleValue.Generation+1)
	}
	// The old plan remains aborted; a new ramp starts against the new generation.
	old, _ := rig.hyd.Ramp(rig.plan.ChamberID)
	oldStatus := old.Status
	if _, err := rig.hyd.StartRamp(context.Background(), newCycle, []float64{50}); err != nil {
		t.Fatalf("new generation ramp: %v", err)
	}
	fresh := waitRamp(t, rig, func(p hydraulics.RampPlan) bool {
		return p.Generation == newCycle.Generation && p.Status == hydraulics.RampCompleted
	}, "new generation ramp completes")
	if fresh.CycleID != newCycle.ID.String() || oldStatus != hydraulics.RampAborted {
		t.Fatalf("old=%s fresh cycle=%s", oldStatus, fresh.CycleID)
	}
}
