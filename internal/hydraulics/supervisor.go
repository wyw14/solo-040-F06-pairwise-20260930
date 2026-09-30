package hydraulics

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
)

var (
	ErrRampActive       = errors.New("a valve ramp is already active for this chamber")
	ErrRampNotFound     = errors.New("no valve ramp for this chamber")
	ErrRampNotPaused    = errors.New("valve ramp is not paused")
	ErrRampNotResumable = errors.New("valve ramp cannot be resumed")
	ErrSafetyReview     = errors.New("resume requires a confirmed safety review")
	ErrCycleNotCurrent  = errors.New("ramp transit generation is no longer current")
	ErrCycleTerminal    = errors.New("transit is cancelled, completed or in emergency")
)

// SafetyReview is the operator acknowledgement required before a paused ramp
// is allowed to continue.
type SafetyReview struct {
	Reviewer  string `json:"reviewer"`
	Confirmed bool   `json:"confirmed"`
	Note      string `json:"note"`
}

func (r SafetyReview) valid() bool { return r.Confirmed && r.Reviewer != "" }

// rampRun is one live or restored ramp plan plus its execution primitives.
type rampRun struct {
	mu       sync.Mutex
	plan     RampPlan
	resumeCh chan struct{}
	ctx      context.Context
	cancel   context.CancelFunc
	live     bool
}

// Supervisor owns the current ramp plan for each chamber and runs it
// independently of any single HTTP request.
type Supervisor struct {
	mu       sync.Mutex
	runs     map[string]*rampRun
	commands commandIssuer
	recorder *journal.Recorder
	interval time.Duration
	now      func() time.Time

	currentCycle func(string) (model.TransitCycle, bool)
	emergency    func(string) bool
}

type commandIssuer interface {
	IssueStep(context.Context, model.TransitCycle, float64) (model.DeviceCommand, error)
}

func newSupervisor(commands commandIssuer, recorder *journal.Recorder, interval time.Duration, now func() time.Time) *Supervisor {
	if interval <= 0 {
		interval = 20 * time.Millisecond
	}
	if now == nil {
		now = time.Now
	}
	return &Supervisor{
		runs: make(map[string]*rampRun), commands: commands, recorder: recorder,
		interval: interval, now: now,
	}
}

// BindGuards wires the live cycle/emergency lookups. Call once after the
// safety and cycle components have been constructed.
func (s *Supervisor) BindGuards(currentCycle func(string) (model.TransitCycle, bool), emergency func(string) bool) {
	s.mu.Lock()
	s.currentCycle = currentCycle
	s.emergency = emergency
	s.mu.Unlock()
}

// Start installs a new running ramp for the given cycle generation.
func (s *Supervisor) Start(ctx context.Context, cycle model.TransitCycle, targets []float64) (RampPlan, error) {
	if len(targets) == 0 {
		return RampPlan{}, errors.New("ramp requires at least one target")
	}
	if existing, ok := s.latest(cycle.ChamberID); ok {
		existing.mu.Lock()
		active := !existing.plan.Status.Terminal()
		existing.mu.Unlock()
		if active {
			return RampPlan{}, ErrRampActive
		}
	}
	now := s.now().UTC()
	steps := make([]RampStep, len(targets))
	for i, target := range targets {
		steps[i] = RampStep{Index: i, Target: target, State: StepPending}
	}
	run := &rampRun{plan: RampPlan{
		PlanID: uuid.NewString(), ChamberID: cycle.ChamberID,
		CycleID: cycle.ID.String(), Generation: cycle.Generation,
		Status: RampRunning, Steps: steps, CreatedAt: now, UpdatedAt: now,
	}}
	run.ctx, run.cancel = context.WithCancel(context.Background())
	s.mu.Lock()
	s.runs[cycle.ChamberID] = run
	s.mu.Unlock()
	if err := s.persist(ctx, run, model.EventRampCreated, s.snapshot(run)); err != nil {
		s.remove(cycle.ChamberID, run)
		return RampPlan{}, err
	}
	s.spawn(run)
	return s.snapshot(run), nil
}

// Pause requests a cooperative stop before the next step is dispatched.
func (s *Supervisor) Pause(ctx context.Context, chamberID, note string) (RampPlan, error) {
	run, err := s.require(chamberID)
	if err != nil {
		return RampPlan{}, err
	}
	run.mu.Lock()
	switch run.plan.Status {
	case RampPaused:
		run.mu.Unlock()
		return s.snapshot(run), nil
	case RampRunning:
	default:
		run.mu.Unlock()
		return RampPlan{}, ErrRampNotResumable
	}
	run.plan.Status = RampPaused
	run.plan.PausedAt = s.now().UTC()
	run.plan.UpdatedAt = run.plan.PausedAt
	run.resumeCh = make(chan struct{})
	run.mu.Unlock()
	if err := s.persist(ctx, run, model.EventRampPaused, map[string]any{"plan_id": run.plan.PlanID, "note": note}); err != nil {
		return RampPlan{}, err
	}
	return s.snapshot(run), nil
}

// Resume continues a paused ramp from the next pending step after a confirmed
// safety review. It refuses when the cycle was cancelled, went to emergency or
// was replaced by another generation.
func (s *Supervisor) Resume(ctx context.Context, chamberID string, review SafetyReview) (RampPlan, error) {
	if !review.valid() {
		return RampPlan{}, ErrSafetyReview
	}
	run, err := s.require(chamberID)
	if err != nil {
		return RampPlan{}, err
	}
	run.mu.Lock()
	if run.plan.Status != RampPaused {
		status := run.plan.Status
		run.mu.Unlock()
		if status.Terminal() {
			return RampPlan{}, ErrRampNotResumable
		}
		return RampPlan{}, ErrRampNotPaused
	}
	wasLive := run.live
	run.mu.Unlock()

	if err := s.guard(run); err != nil {
		_ = s.abort(ctx, run, err.Error())
		return RampPlan{}, err
	}

	run.mu.Lock()
	if run.plan.Status != RampPaused {
		status := run.plan.Status
		run.mu.Unlock()
		if status.Terminal() {
			return RampPlan{}, ErrRampNotResumable
		}
		return RampPlan{}, ErrRampActive
	}
	ch := run.resumeCh
	run.plan.Status = RampRunning
	run.plan.ResumeNote = review.Reviewer + ": " + review.Note
	run.plan.UpdatedAt = s.now().UTC()
	run.resumeCh = nil
	if !wasLive {
		run.ctx, run.cancel = context.WithCancel(context.Background())
	}
	run.mu.Unlock()

	if err := s.persist(ctx, run, model.EventRampResumed, map[string]any{
		"plan_id": run.plan.PlanID, "reviewer": review.Reviewer,
		"confirmed": review.Confirmed, "note": review.Note,
	}); err != nil {
		return RampPlan{}, err
	}
	if !wasLive {
		s.spawn(run)
	} else if ch != nil {
		close(ch)
	}
	return s.snapshot(run), nil
}

// AbortIfIdentity stops the ramp only when it still belongs to the given
// transit generation. Used by cycle cancellation and emergency shutdown.
func (s *Supervisor) AbortIfIdentity(ctx context.Context, chamberID string, cycleID uuid.UUID, generation uint64, reason string) (RampPlan, bool) {
	run, ok := s.latest(chamberID)
	if !ok {
		return RampPlan{}, false
	}
	run.mu.Lock()
	match := run.plan.CycleID == cycleID.String() && run.plan.Generation == generation
	run.mu.Unlock()
	if !match {
		return RampPlan{}, false
	}
	if err := s.abort(ctx, run, reason); err != nil {
		return RampPlan{}, false
	}
	return s.snapshot(run), true
}

// AbortForChamber invalidates any ramp for a chamber regardless of its
// generation, e.g. when a new transit generation starts.
func (s *Supervisor) AbortForChamber(ctx context.Context, chamberID, reason string) {
	if run, ok := s.latest(chamberID); ok {
		_ = s.abort(ctx, run, reason)
	}
}

// Current returns the latest ramp plan for the chamber, if any.
func (s *Supervisor) Current(chamberID string) (RampPlan, bool) {
	run, ok := s.latest(chamberID)
	if !ok {
		return RampPlan{}, false
	}
	return s.snapshot(run), true
}

func (s *Supervisor) require(chamberID string) (*rampRun, error) {
	run, ok := s.latest(chamberID)
	if !ok {
		return nil, ErrRampNotFound
	}
	return run, nil
}

func (s *Supervisor) latest(chamberID string) (*rampRun, bool) {
	s.mu.Lock()
	run, ok := s.runs[chamberID]
	s.mu.Unlock()
	return run, ok
}

func (s *Supervisor) remove(chamberID string, run *rampRun) {
	s.mu.Lock()
	if s.runs[chamberID] == run {
		delete(s.runs, chamberID)
	}
	s.mu.Unlock()
}

// guard verifies the ramp's transit generation is still live and safe.
func (s *Supervisor) guard(run *rampRun) error {
	chamberID := run.plan.ChamberID
	s.mu.Lock()
	currentCycle, emergency := s.currentCycle, s.emergency
	s.mu.Unlock()
	if currentCycle == nil {
		return errors.New("cycle guard is not configured")
	}
	cycle, ok := currentCycle(chamberID)
	if !ok {
		return ErrCycleNotCurrent
	}
	if cycle.ID.String() != run.plan.CycleID || cycle.Generation != run.plan.Generation {
		return ErrCycleNotCurrent
	}
	if cycle.Stage.Terminal() {
		return ErrCycleTerminal
	}
	if emergency != nil && emergency(chamberID) {
		return ErrCycleTerminal
	}
	return nil
}

func (s *Supervisor) lookupCycle(chamberID string) (model.TransitCycle, bool) {
	s.mu.Lock()
	currentCycle := s.currentCycle
	s.mu.Unlock()
	if currentCycle == nil {
		return model.TransitCycle{}, false
	}
	return currentCycle(chamberID)
}

func (s *Supervisor) abort(ctx context.Context, run *rampRun, reason string) error {
	run.mu.Lock()
	if run.plan.Status.Terminal() {
		run.mu.Unlock()
		return nil
	}
	for i := range run.plan.Steps {
		if run.plan.Steps[i].State == StepPending {
			run.plan.Steps[i].State = StepCancelled
		}
	}
	run.plan.Status = RampAborted
	run.plan.AbortReason = reason
	run.plan.UpdatedAt = s.now().UTC()
	if run.cancel != nil {
		run.cancel()
	}
	if run.resumeCh != nil {
		close(run.resumeCh)
		run.resumeCh = nil
	}
	run.mu.Unlock()
	return s.persist(ctx, run, model.EventRampAborted, map[string]any{
		"plan_id": run.plan.PlanID, "reason": reason,
	})
}

// spawn starts the single goroutine that serves a plan for its whole life.
func (s *Supervisor) spawn(run *rampRun) {
	run.mu.Lock()
	if run.live {
		run.mu.Unlock()
		return
	}
	run.live = true
	run.mu.Unlock()
	go s.execute(run)
}

func (s *Supervisor) execute(run *rampRun) {
	defer func() {
		run.mu.Lock()
		run.live = false
		run.mu.Unlock()
	}()
	for {
		run.mu.Lock()
		status := run.plan.Status
		index := run.plan.NextIndex
		total := len(run.plan.Steps)
		chamberID := run.plan.ChamberID
		wait := run.resumeCh
		ctx := run.ctx
		run.mu.Unlock()

		switch status {
		case RampAborted, RampCompleted:
			return
		case RampPaused:
			select {
			case <-wait:
				continue
			case <-ctx.Done():
				return
			}
		}
		if index >= total {
			s.finish(run)
			return
		}
		if err := s.guard(run); err != nil {
			_ = s.abort(context.Background(), run, err.Error())
			return
		}

		cycle, ok := s.lookupCycle(chamberID)
		if !ok {
			_ = s.abort(context.Background(), run, ErrCycleNotCurrent.Error())
			return
		}
		target := run.stepTarget(index)
		command, err := s.commands.IssueStep(ctx, cycle, target)
		if err != nil {
			if errors.Is(ctx.Err(), context.Canceled) {
				return
			}
			_ = s.abort(context.Background(), run, "command issuance failed: "+err.Error())
			return
		}

		now := s.now().UTC()
		run.mu.Lock()
		if run.plan.Status != RampRunning {
			// The command was already queued before a pause/abort landed.
			// Record it truthfully so it can never be issued again on resume;
			// the plan stays in its terminal/paused state.
			run.plan.Steps[index] = RampStep{
				Index: index, Target: target, State: StepIssued,
				IssuedAt: now, CommandID: command.ID.String(),
			}
			if run.plan.NextIndex <= index {
				run.plan.NextIndex = index + 1
			}
			run.plan.UpdatedAt = now
			run.mu.Unlock()
			_ = s.persist(context.Background(), run, model.EventRampStepIssued, map[string]any{
				"plan_id": run.plan.PlanID, "index": index, "target": target, "command_id": command.ID.String(),
			})
			return
		}
		run.plan.Steps[index] = RampStep{
			Index: index, Target: target, State: StepIssued,
			IssuedAt: now, CommandID: command.ID.String(),
		}
		run.plan.NextIndex = index + 1
		run.plan.UpdatedAt = now
		isLast := index+1 >= len(run.plan.Steps)
		run.mu.Unlock()
		if err := s.persist(context.Background(), run, model.EventRampStepIssued, map[string]any{
			"plan_id": run.plan.PlanID, "index": index, "target": target, "command_id": command.ID.String(),
		}); err != nil {
			_ = s.abort(context.Background(), run, "journal failure: "+err.Error())
			return
		}
		if isLast {
			s.finish(run)
			return
		}
		// Pause is cooperative and observed at the next step boundary; the
		// interval is short, so it lands at most one interval after the request
		// and never reuses the resume signal channel.
		timer := time.NewTimer(s.interval)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return
		}
	}
}

func (s *Supervisor) finish(run *rampRun) {
	run.mu.Lock()
	if run.plan.Status != RampRunning {
		run.mu.Unlock()
		return
	}
	run.plan.Status = RampCompleted
	run.plan.NextIndex = len(run.plan.Steps)
	run.plan.UpdatedAt = s.now().UTC()
	for i := range run.plan.Steps {
		if run.plan.Steps[i].State == StepPending {
			run.plan.Steps[i].State = StepCancelled
		}
	}
	run.mu.Unlock()
	_ = s.persist(context.Background(), run, model.EventRampCompleted, map[string]any{"plan_id": run.plan.PlanID})
}

func (s *Supervisor) snapshot(run *rampRun) RampPlan {
	run.mu.Lock()
	defer run.mu.Unlock()
	return run.plan.clone()
}

func (run *rampRun) stepTarget(index int) float64 {
	run.mu.Lock()
	defer run.mu.Unlock()
	if index < 0 || index >= len(run.plan.Steps) {
		return 0
	}
	return run.plan.Steps[index].Target
}

func (s *Supervisor) persist(ctx context.Context, run *rampRun, kind model.EventType, payload any) error {
	cycleID, _ := uuid.Parse(run.plan.CycleID)
	_, err := s.recorder.Record(ctx, run.plan.ChamberID, kind, cycleID, run.plan.Generation, payload)
	return err
}

// RestorePlans rebuilds plans from journal replay. Restored plans never run on
// their own: anything that was running when the process stopped is returned to
// the operator paused, awaiting a fresh safety review before resume.
func (s *Supervisor) RestorePlans(plans []RampPlan) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, plan := range plans {
		copy := plan.clone()
		s.runs[plan.ChamberID] = &rampRun{plan: copy}
	}
}

// ReconstructPlans folds the ramp event stream into the latest plan per chamber.
func ReconstructPlans(events []model.Event) []RampPlan {
	byPlan := make(map[string]*RampPlan)
	var order []string
	for _, event := range events {
		switch event.Type {
		case model.EventRampCreated:
			var plan RampPlan
			if event.Decode(&plan) != nil {
				continue
			}
			if _, exists := byPlan[plan.PlanID]; !exists {
				order = append(order, plan.PlanID)
			}
			copy := plan.clone()
			byPlan[plan.PlanID] = &copy
		case model.EventRampStepIssued:
			var payload struct {
				PlanID    string  `json:"plan_id"`
				Index     int     `json:"index"`
				Target    float64 `json:"target"`
				CommandID string  `json:"command_id"`
			}
			if event.Decode(&payload) != nil {
				continue
			}
			plan, ok := byPlan[payload.PlanID]
			if !ok || payload.Index < 0 || payload.Index >= len(plan.Steps) {
				continue
			}
			plan.Steps[payload.Index] = RampStep{
				Index: payload.Index, Target: payload.Target, State: StepIssued,
				IssuedAt: event.OccurredAt, CommandID: payload.CommandID,
			}
			if plan.NextIndex <= payload.Index {
				plan.NextIndex = payload.Index + 1
			}
			plan.UpdatedAt = event.OccurredAt
		case model.EventRampPaused:
			setStatus(byPlan, event, RampPaused)
		case model.EventRampResumed:
			setStatus(byPlan, event, RampRunning)
		case model.EventRampCompleted:
			setStatus(byPlan, event, RampCompleted)
		case model.EventRampAborted:
			var payload struct {
				PlanID string `json:"plan_id"`
				Reason string `json:"reason"`
			}
			if event.Decode(&payload) != nil {
				continue
			}
			if plan, ok := byPlan[payload.PlanID]; ok {
				plan.Status = RampAborted
				plan.AbortReason = payload.Reason
				plan.UpdatedAt = event.OccurredAt
				for i := range plan.Steps {
					if plan.Steps[i].State == StepPending {
						plan.Steps[i].State = StepCancelled
					}
				}
			}
		}
	}
	result := make([]RampPlan, 0, len(order))
	for _, id := range order {
		plan := byPlan[id]
		if plan.Status == RampRunning {
			// The process died mid-ramp: never auto-resume a field actuator.
			plan.Status = RampPaused
			plan.ResumeNote = "restored after restart; safety review required"
		}
		result = append(result, plan.clone())
	}
	return result
}

func setStatus(byPlan map[string]*RampPlan, event model.Event, status RampStatus) {
	var payload struct {
		PlanID string `json:"plan_id"`
	}
	if event.Decode(&payload) != nil {
		return
	}
	if plan, ok := byPlan[payload.PlanID]; ok {
		plan.Status = status
		plan.UpdatedAt = event.OccurredAt
	}
}
