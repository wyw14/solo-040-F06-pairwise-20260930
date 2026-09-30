package hydraulics

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/gate"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
)

// Controller coordinates valve commands, level windows and ramp operations.
type Controller struct {
	mu         sync.RWMutex
	commands   *gate.Commands
	recorder   *journal.Recorder
	windows    map[string]*LevelWindow
	supervisor *Supervisor
	now        func() time.Time
}

func NewController(commands *gate.Commands, recorder *journal.Recorder, interval time.Duration, now func() time.Time) *Controller {
	if now == nil {
		now = time.Now
	}
	supervisor := newSupervisor(commandsAdapter{commands: commands, recorder: recorder, now: now}, recorder, interval, now)
	return &Controller{
		commands: commands, recorder: recorder, windows: make(map[string]*LevelWindow),
		supervisor: supervisor, now: now,
	}
}

// Supervisor exposes the managed ramp lifecycle.
func (c *Controller) Supervisor() *Supervisor { return c.supervisor }

func (c *Controller) window(chamberID string) (*LevelWindow, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	window := c.windows[chamberID]
	if window != nil {
		return window, nil
	}
	created, err := NewLevelWindow(4)
	if err != nil {
		return nil, err
	}
	c.windows[chamberID] = created
	return created, nil
}

func (c *Controller) BeginLeveling(cycle model.TransitCycle) error {
	window, err := c.window(cycle.ChamberID)
	if err != nil {
		return err
	}
	window.Begin(cycle.ID, cycle.Generation)
	return nil
}

func (c *Controller) Observe(sample model.LevelSample) (bool, error) {
	window, err := c.windowForCycle(sample.CycleID, sample.Generation)
	if err != nil {
		return false, err
	}
	if err := window.Add(sample); err != nil {
		return false, err
	}
	return window.Stable(0.05), nil
}

func (c *Controller) windowForCycle(cycleID interface{ String() string }, generation uint64) (*LevelWindow, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, window := range c.windows {
		id, currentGeneration := window.Identity()
		if id.String() == cycleID.String() && currentGeneration == generation {
			return window, nil
		}
	}
	return nil, errors.New("no active level window for transit")
}

// commandsAdapter adapts the gate command queue to the supervisor's issuer.
type commandsAdapter struct {
	commands *gate.Commands
	recorder *journal.Recorder
	now      func() time.Time
}

// IssueStep dispatches one valve opening for the current generation and
// journals the queued command.
func (a commandsAdapter) IssueStep(ctx context.Context, cycle model.TransitCycle, target float64) (model.DeviceCommand, error) {
	device := model.DeviceFillValve
	if target < 0 {
		device = model.DeviceDrainValve
		target = -target
	}
	command, err := a.commands.Issue(cycle, device, "set_opening", target, a.now())
	if err != nil {
		return model.DeviceCommand{}, err
	}
	if _, err := a.recorder.Record(ctx, cycle.ChamberID, model.EventCommandQueued, cycle.ID, cycle.Generation, command); err != nil {
		return model.DeviceCommand{}, err
	}
	return command, nil
}

// StartRamp begins a managed, resumable ramp bound to the cycle generation.
func (c *Controller) StartRamp(ctx context.Context, cycle model.TransitCycle, targets []float64) (RampPlan, error) {
	return c.supervisor.Start(ctx, cycle, targets)
}

func (c *Controller) PauseRamp(ctx context.Context, chamberID, note string) (RampPlan, error) {
	return c.supervisor.Pause(ctx, chamberID, note)
}

func (c *Controller) ResumeRamp(ctx context.Context, chamberID string, review SafetyReview) (RampPlan, error) {
	return c.supervisor.Resume(ctx, chamberID, review)
}

func (c *Controller) Ramp(chamberID string) (RampPlan, bool) {
	return c.supervisor.Current(chamberID)
}

// AbortRampForCycle invalidates the ramp only when it belongs to the given
// transit generation (cancel / emergency).
func (c *Controller) AbortRampForCycle(ctx context.Context, chamberID string, cycleID uuid.UUID, generation uint64, reason string) {
	c.supervisor.AbortIfIdentity(ctx, chamberID, cycleID, generation, reason)
}

// AbortRamp invalidates any ramp for the chamber (new generation started).
func (c *Controller) AbortRamp(ctx context.Context, chamberID, reason string) {
	c.supervisor.AbortForChamber(ctx, chamberID, reason)
}

// RestoreRamps rebuilds ramp plans after journal replay.
func (c *Controller) RestoreRamps(plans []RampPlan) { c.supervisor.RestorePlans(plans) }

func (c *Controller) WindowSamples(chamberID string) []model.LevelSample {
	c.mu.RLock()
	window := c.windows[chamberID]
	c.mu.RUnlock()
	if window == nil {
		return nil
	}
	return window.Samples()
}
