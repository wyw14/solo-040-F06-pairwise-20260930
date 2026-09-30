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

type harness struct {
	coordinator *Coordinator
	hydraulics  *hydraulics.Controller
	ramp        *hydraulics.Ramp
	now         *fakeNow
}

type fakeNow struct{ t time.Time }

func (f *fakeNow) now() time.Time {
	f.t = f.t.Add(time.Second)
	return f.t
}

func newHarness() *harness {
	store := journal.NewMemoryStore()
	clk := &fakeNow{}
	recorder := journal.NewRecorder(store, clk.now)
	commands := gate.NewCommands()
	gateState := gate.NewState("c1")
	gates := gate.NewController(gateState, commands, recorder, clk.now)
	ramp := hydraulics.NewRamp(time.Hour, clk.now)
	hydraulicsController := hydraulics.NewController(commands, recorder, ramp, clk.now)
	coordinator := NewCoordinator(gates, hydraulicsController, recorder, clk.now)
	return &harness{coordinator: coordinator, hydraulics: hydraulicsController, ramp: ramp, now: clk}
}

func planFor(chamberID string) model.ConvoyPlan {
	return model.ConvoyPlan{
		ID: uuid.New(), ChamberID: chamberID, Direction: model.DirectionUpstream,
		Vessels: []model.Vessel{{ID: uuid.New(), Name: "v", CallSign: "cs", LengthM: 10}},
	}
}

func TestCancelAbortsRunningRampForSameGeneration(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	plan := planFor("c1")
	cycleValue, err := h.coordinator.Start(ctx, plan)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.hydraulics.StartRamp(cycleValue, []float64{0.1, 0.2, 0.3}); err != nil {
		t.Fatal(err)
	}
	// Cancel the transit: the ramp must be aborted and unresumable even though
	// it may be sitting between steps.
	cancelled, err := h.coordinator.Cancel(ctx, "c1", "operator cancellation")
	if err != nil {
		t.Fatal(err)
	}
	if cancelled.Stage != model.StageCancelled {
		t.Fatalf("stage = %s", cancelled.Stage)
	}
	view, ok := h.hydraulics.RampStatus("c1")
	if !ok {
		t.Fatal("ramp missing")
	}
	if view.Status != hydraulics.RampAborted || view.Resumable {
		t.Fatalf("cancelled transit ramp = %+v, want aborted/non-resumable", view)
	}
	if view.CycleID != cycleValue.ID {
		t.Fatal("aborted ramp identity mismatch")
	}
}

func TestNewGenerationAbortsLeftoverRamp(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	first, err := h.coordinator.Start(ctx, planFor("c1"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.hydraulics.StartRamp(first, []float64{0.1, 0.2}); err != nil {
		t.Fatal(err)
	}
	// Cancel first transit, then start a replacement generation.
	if _, err := h.coordinator.Cancel(ctx, "c1", "operator cancellation"); err != nil {
		t.Fatal(err)
	}
	second, err := h.coordinator.Start(ctx, planFor("c1"))
	if err != nil {
		t.Fatal(err)
	}
	if second.Generation != first.Generation+1 {
		t.Fatalf("generation = %d, want %d", second.Generation, first.Generation+1)
	}
	view, _ := h.hydraulics.RampStatus("c1")
	if view.Status != hydraulics.RampAborted {
		t.Fatalf("leftover ramp status = %s, want aborted", view.Status)
	}
	// Resuming the old ramp against the new generation must be refused.
	if _, err := h.hydraulics.ResumeRamp(second, "ops"); err == nil {
		t.Fatal("resume of a superseded ramp must fail")
	}
	// A fresh ramp for the new generation is allowed after the abort.
	if _, err := h.hydraulics.StartRamp(second, []float64{0.5}); err != nil {
		t.Fatalf("fresh ramp for new generation: %v", err)
	}
}

func TestEmergencyStageAbortsRamp(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	cycleValue, err := h.coordinator.Start(ctx, planFor("c1"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.hydraulics.StartRamp(cycleValue, []float64{0.1}); err != nil {
		t.Fatal(err)
	}
	emergency, err := h.coordinator.Cancel(ctx, "c1", "emergency")
	if err != nil {
		t.Fatal(err)
	}
	if emergency.Stage != model.StageEmergency {
		t.Fatalf("stage = %s, want emergency", emergency.Stage)
	}
	view, _ := h.hydraulics.RampStatus("c1")
	if view.Status != hydraulics.RampAborted || view.AbortReason == "" {
		t.Fatalf("emergency ramp = %+v", view)
	}
}
