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
	mu       sync.RWMutex
	commands *gate.Commands
	recorder *journal.Recorder
	windows  map[string]*LevelWindow
	ramp     *Ramp
	now      func() time.Time
}

func NewController(commands *gate.Commands, recorder *journal.Recorder, ramp *Ramp, now func() time.Time) *Controller {
	if now == nil {
		now = time.Now
	}
	controller := &Controller{
		commands: commands, recorder: recorder, windows: make(map[string]*LevelWindow),
		ramp: ramp, now: now,
	}
	ramp.OnChange(controller.recordTransition)
	return controller
}

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

func (c *Controller) IssueValve(ctx context.Context, cycle model.TransitCycle, target float64) error {
	device := model.DeviceFillValve
	if target < 0 {
		device = model.DeviceDrainValve
		target = -target
	}
	command, err := c.commands.Issue(cycle, device, "set_opening", target, c.now())
	if err != nil {
		return err
	}
	_, err = c.recorder.Record(ctx, cycle.ChamberID, model.EventCommandQueued, cycle.ID, cycle.Generation, command)
	return err
}

// issueFor adapts a cycle-bound valve issue to the ramp's context/target
// signature. The ramp deliberately owns pausing at safe points, so the issued
// command always runs under a non-cancellable context once started.
func (c *Controller) issueFor(cycle model.TransitCycle) StepIssuer {
	return func(_ context.Context, target float64) error {
		return c.IssueValve(context.Background(), cycle, target)
	}
}

// StartRamp begins a resumable, server-side ramp for one transit generation.
// The ramp.started audit event is emitted from the lifecycle hook before any
// step is issued. The run outlives the originating request; operators halt it
// explicitly via PauseRamp, or it is aborted when the transit terminates.
func (c *Controller) StartRamp(cycle model.TransitCycle, targets []float64) (RampRunView, error) {
	return c.ramp.Start(cycle.ChamberID, cycle.ID, cycle.Generation, targets, c.issueFor(cycle))
}

func (c *Controller) PauseRamp(chamberID, note string) (RampRunView, error) {
	return c.ramp.Pause(chamberID, note)
}

// ResumeRamp continues a paused ramp from its next step. The reviewer identity
// is recorded on the ramp and with ramp.resumed as safety-review evidence.
// Resumption is rejected unless the same transit generation is still current.
func (c *Controller) ResumeRamp(cycle model.TransitCycle, reviewer string) (RampRunView, error) {
	return c.ramp.Resume(cycle.ChamberID, cycle.ID, cycle.Generation, c.issueFor(cycle), reviewer)
}

// AbortRamp irreversibly terminates the chamber's ramp (cycle cancellation,
// emergency or a newer generation).
func (c *Controller) AbortRamp(chamberID, note string) (RampRunView, error) {
	return c.ramp.Abort(chamberID, note)
}

// AbortRampForCycle terminates the ramp only when it still belongs to the
// given transit generation; a ramp already superseded by a newer generation is
// left untouched.
func (c *Controller) AbortRampForCycle(chamberID string, cycleID uuid.UUID, generation uint64, note string) bool {
	_, matched, err := c.ramp.AbortIfGeneration(chamberID, cycleID, generation, note)
	return matched && err == nil
}

func (c *Controller) RampStatus(chamberID string) (RampRunView, bool) {
	return c.ramp.Status(chamberID)
}

func (c *Controller) WindowSamples(chamberID string) []model.LevelSample {
	c.mu.RLock()
	window := c.windows[chamberID]
	c.mu.RUnlock()
	if window == nil {
		return nil
	}
	return window.Samples()
}

// recordTransition writes the audit event for a ramp lifecycle change.
func (c *Controller) recordTransition(view RampRunView, previous RampStatus) {
	var kind model.EventType
	switch view.Status {
	case RampRunning:
		if previous == "" {
			kind = model.EventRampStarted
		} else if previous == RampPaused {
			kind = model.EventRampResumed
		}
	case RampPaused:
		kind = model.EventRampPaused
	case RampAborted:
		kind = model.EventRampAborted
	case RampCompleted:
		kind = model.EventRampCompleted
	}
	if kind == "" {
		return
	}
	_, _ = c.recorder.Record(context.Background(), view.ChamberID, kind, view.CycleID, view.Generation, view)
}
