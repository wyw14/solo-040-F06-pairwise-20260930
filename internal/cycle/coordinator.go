package cycle

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/gate"
	"github.com/wyw14/cry-96/internal/hydraulics"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
)

// Coordinator is the authoritative owner of active transit generations.
type Coordinator struct {
	mu          sync.RWMutex
	current     map[string]model.TransitCycle
	generations map[string]uint64
	gates       *gate.Controller
	hydraulics  *hydraulics.Controller
	recorder    *journal.Recorder
	now         func() time.Time
}

func NewCoordinator(gates *gate.Controller, hydraulicsController *hydraulics.Controller, recorder *journal.Recorder, now func() time.Time) *Coordinator {
	if now == nil {
		now = time.Now
	}
	return &Coordinator{
		current: make(map[string]model.TransitCycle), generations: make(map[string]uint64),
		gates: gates, hydraulics: hydraulicsController, recorder: recorder, now: now,
	}
}

func (c *Coordinator) Start(ctx context.Context, plan model.ConvoyPlan) (model.TransitCycle, error) {
	c.mu.Lock()
	if active, ok := c.current[plan.ChamberID]; ok && !active.Stage.Terminal() {
		c.mu.Unlock()
		return model.TransitCycle{}, errors.New("chamber already has an active transit")
	}
	c.generations[plan.ChamberID]++
	cycleValue, err := model.NewTransitCycle(plan.ChamberID, plan.ID, plan.Direction, c.generations[plan.ChamberID], c.now())
	if err == nil {
		c.current[plan.ChamberID] = cycleValue
	}
	c.mu.Unlock()
	if err != nil {
		return model.TransitCycle{}, err
	}
	if _, err := c.recorder.Record(ctx, plan.ChamberID, model.EventCycleCreated, cycleValue.ID, cycleValue.Generation, cycleValue); err != nil {
		return model.TransitCycle{}, err
	}
	// A fresh generation supersedes any ramp left behind by the previous one.
	c.hydraulics.AbortRamp(ctx, plan.ChamberID, "new transit generation started")
	return cycleValue, nil
}

func (c *Coordinator) Current(chamberID string) (model.TransitCycle, bool) {
	c.mu.RLock()
	cycleValue, ok := c.current[chamberID]
	c.mu.RUnlock()
	return cycleValue.Clone(), ok
}

func (c *Coordinator) Cancel(ctx context.Context, chamberID, reason string) (model.TransitCycle, error) {
	c.mu.Lock()
	cycleValue, ok := c.current[chamberID]
	if !ok || cycleValue.Stage.Terminal() {
		c.mu.Unlock()
		return model.TransitCycle{}, errors.New("no cancellable transit")
	}
	if err := cycleValue.Advance(terminalStage(reason), reason, c.now()); err != nil {
		c.mu.Unlock()
		return model.TransitCycle{}, err
	}
	c.current[chamberID] = cycleValue
	c.mu.Unlock()
	if _, err := c.recorder.Record(ctx, chamberID, model.EventCycleAdvanced, cycleValue.ID, cycleValue.Generation, cycleValue); err != nil {
		return cycleValue, err
	}
	// A cancelled or emergency transit must never be resumed: invalidate any
	// in-flight ramp bound to this exact generation.
	c.hydraulics.AbortRampForCycle(ctx, chamberID, cycleValue.ID, cycleValue.Generation, "transit "+string(cycleValue.Stage))
	return cycleValue, nil
}

func (c *Coordinator) ApplyGateAck(ctx context.Context, ack model.DeviceAck) error {
	command, err := c.gates.Confirm(ctx, ack)
	if err != nil {
		return err
	}
	c.mu.Lock()
	cycleValue, ok := c.current[ack.ChamberID]
	if !ok || cycleValue.Stage.Terminal() || !cycleValue.IdentityMatches(command.CycleID, command.Generation) {
		c.mu.Unlock()
		return errors.New("device acknowledgement belongs to an inactive transit generation")
	}
	next, err := nextAfterGate(cycleValue.Stage)
	if err == nil {
		err = cycleValue.Advance(next, "matching gate command settled", c.now())
	}
	if err == nil {
		c.current[ack.ChamberID] = cycleValue
	}
	c.mu.Unlock()
	if err != nil {
		return err
	}
	if next == model.StageLeveling {
		if err := c.hydraulics.BeginLeveling(cycleValue); err != nil {
			return err
		}
	}
	_, err = c.recorder.Record(ctx, ack.ChamberID, model.EventCycleAdvanced, cycleValue.ID, cycleValue.Generation, cycleValue)
	return err
}

func (c *Coordinator) ApplySignalHandoff(ctx context.Context, cycleID uuid.UUID, generation uint64) error {
	c.mu.Lock()
	var chamberID string
	var cycleValue model.TransitCycle
	for id, candidate := range c.current {
		if candidate.IdentityMatches(cycleID, generation) {
			chamberID, cycleValue = id, candidate
			break
		}
	}
	if chamberID == "" {
		c.mu.Unlock()
		return errors.New("signal handoff transit is not current")
	}
	if err := applySignalTransition(&cycleValue); err != nil {
		c.mu.Unlock()
		return err
	}
	cycleValue.UpdatedAt = c.now().UTC()
	c.current[chamberID] = cycleValue
	c.mu.Unlock()
	_, err := c.recorder.Record(ctx, chamberID, model.EventCycleAdvanced, cycleValue.ID, generation, cycleValue)
	return err
}

func (c *Coordinator) Restore(cycleValue model.TransitCycle) {
	c.mu.Lock()
	c.current[cycleValue.ChamberID] = cycleValue
	if c.generations[cycleValue.ChamberID] < cycleValue.Generation {
		c.generations[cycleValue.ChamberID] = cycleValue.Generation
	}
	c.mu.Unlock()
}
