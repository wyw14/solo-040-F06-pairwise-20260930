package hydraulics

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
)

// RampStatus is the externally visible lifecycle state of one valve ramp.
type RampStatus string

const (
	// RampRunning steps are being issued.
	RampRunning RampStatus = "running"
	// RampPaused the operator (or a dropped request) halted the run at a safe
	// point; resumable only after a safety review for the same generation.
	RampPaused RampStatus = "paused"
	// RampCompleted every step has been issued.
	RampCompleted RampStatus = "completed"
	// RampAborted the transit was cancelled, moved into emergency or superseded
	// by a new generation; the ramp can never be resumed.
	RampAborted RampStatus = "aborted"
)

func (s RampStatus) resumable() bool { return s == RampPaused }

// RampStepView describes one planned opening for operators.
type RampStepView struct {
	Index     int        `json:"index"`
	Target    float64    `json:"target"`
	IssuedAt  *time.Time `json:"issued_at,omitempty"`
	Completed bool       `json:"completed"`
}

// RampRunView is the full operator-visible state of a ramp.
type RampRunView struct {
	ChamberID   string         `json:"chamber_id"`
	CycleID     uuid.UUID      `json:"cycle_id"`
	Generation  uint64         `json:"generation"`
	Status      RampStatus     `json:"status"`
	Steps       []RampStepView `json:"steps"`
	Completed   int            `json:"completed"`
	Total       int            `json:"total"`
	NextIndex   *int           `json:"next_index,omitempty"`
	Resumable   bool           `json:"resumable"`
	PausedAt    *time.Time     `json:"paused_at,omitempty"`
	PauseReason string         `json:"pause_reason,omitempty"`
	Reviewer    string         `json:"reviewer,omitempty"`
	AbortedAt   *time.Time     `json:"aborted_at,omitempty"`
	AbortReason string         `json:"abort_reason,omitempty"`
	CompletedAt *time.Time     `json:"completed_at,omitempty"`
	StartedAt   time.Time      `json:"started_at"`
}

// StepIssuer issues one valve opening. It is always allowed to finish once
// started: pausing only takes effect at the safe points around an issue, so an
// opening is never committed twice on resume.
type StepIssuer func(context.Context, float64) error

// rampRun tracks one ramp: its identity, the fixed step plan and how many
// steps have completed. The run survives its issuing goroutine and its
// originating request, so a cancelled HTTP request leaves an inspectable,
// pausable ramp rather than lost progress.
type rampRun struct {
	chamberID  string
	cycleID    uuid.UUID
	generation uint64
	targets    []float64
	issuedAt   []time.Time
	completed  int
	status     RampStatus
	startedAt  time.Time
	pausedAt   time.Time
	pauseNote  string
	reviewer   string
	abortedAt  time.Time
	abortNote  string
	endedAt    time.Time
	// stop is closed to ask the issuing goroutine to halt at the next safe
	// point. aborting decides whether that halt is final.
	stop     chan struct{}
	aborting bool
	done     chan struct{}
}

func (run *rampRun) identity(cycleID uuid.UUID, generation uint64) bool {
	return run.cycleID == cycleID && run.generation == generation
}

func (run *rampRun) nextIndex() *int {
	if run.completed >= len(run.targets) {
		return nil
	}
	index := run.completed
	return &index
}

func (run *rampRun) snapshot() RampRunView {
	view := RampRunView{
		ChamberID: run.chamberID, CycleID: run.cycleID, Generation: run.generation,
		Status: run.status, Completed: run.completed, Total: len(run.targets),
		NextIndex: run.nextIndex(), Resumable: run.status.resumable(),
		StartedAt: run.startedAt, Steps: make([]RampStepView, len(run.targets)),
	}
	for index, target := range run.targets {
		step := RampStepView{Index: index + 1, Target: target, Completed: index < run.completed}
		if index < run.completed {
			issuedAt := run.issuedAt[index]
			step.IssuedAt = &issuedAt
		}
		view.Steps[index] = step
	}
	if !run.pausedAt.IsZero() {
		pausedAt := run.pausedAt
		view.PausedAt = &pausedAt
		view.PauseReason = run.pauseNote
	}
	if run.reviewer != "" {
		view.Reviewer = run.reviewer
	}
	if !run.abortedAt.IsZero() {
		abortedAt := run.abortedAt
		view.AbortedAt = &abortedAt
		view.AbortReason = run.abortNote
	}
	if !run.endedAt.IsZero() && run.status == RampCompleted {
		completedAt := run.endedAt
		view.CompletedAt = &completedAt
	}
	return view
}

// Ramp owns the resumable valve ramps of all chambers.
type Ramp struct {
	mu       sync.Mutex
	runs     map[string]*rampRun
	interval time.Duration
	now      func() time.Time
	// onChange, when set, is invoked once per lifecycle transition, never while
	// holding the ramp lock.
	onChange func(view RampRunView, previous RampStatus)
}

// NewRamp builds a Ramp. interval is the dwell between step issues.
func NewRamp(interval time.Duration, now func() time.Time) *Ramp {
	if interval <= 0 {
		interval = 20 * time.Millisecond
	}
	if now == nil {
		now = time.Now
	}
	return &Ramp{runs: make(map[string]*rampRun), interval: interval, now: now}
}

// OnChange installs the lifecycle hook (used to write audit events).
func (r *Ramp) OnChange(hook func(view RampRunView, previous RampStatus)) {
	r.mu.Lock()
	r.onChange = hook
	r.mu.Unlock()
}

var (
	// ErrRampActive a running or paused ramp already occupies the chamber.
	ErrRampActive = errRampActive
	// ErrRampNotFound the chamber has no ramp.
	ErrRampNotFound = errRampNotFound
	// ErrRampNotPaused resume requires a paused ramp.
	ErrRampNotPaused = errRampNotPaused
	// ErrRampGeneration the ramp belongs to a different transit generation.
	ErrRampGeneration = errRampGeneration
	errRampActive     = errors.New("a ramp is already active for this chamber; resume or abort it first")
	errRampNotFound   = errors.New("no ramp found for this chamber")
	errRampNotPaused  = errors.New("ramp is not paused")
	errRampGeneration = errors.New("ramp belongs to another transit generation")
)

// Start begins a fresh ramp for a transit generation. Any prior ramp for the
// chamber must have finished (completed or aborted) first. The run is a
// server-side operation: it is not tied to the request that started it, so a
// dropped connection never truncates the step sequence. Operators halt it
// explicitly with Pause or Abort.
func (r *Ramp) Start(chamberID string, cycleID uuid.UUID, generation uint64, targets []float64, issue StepIssuer) (RampRunView, error) {
	if chamberID == "" || cycleID == uuid.Nil || generation == 0 {
		return RampRunView{}, errors.New("ramp requires a chamber and transit generation")
	}
	if len(targets) == 0 || len(targets) > 12 {
		return RampRunView{}, errors.New("ramp requires 1 to 12 targets")
	}
	if issue == nil {
		return RampRunView{}, errors.New("ramp requires a step issuer")
	}
	r.mu.Lock()
	if current, ok := r.runs[chamberID]; ok && (current.status == RampRunning || current.status == RampPaused) {
		r.mu.Unlock()
		return RampRunView{}, errRampActive
	}
	run := &rampRun{
		chamberID: chamberID, cycleID: cycleID, generation: generation,
		targets: append([]float64(nil), targets...), issuedAt: make([]time.Time, len(targets)),
		status: RampRunning, startedAt: r.now().UTC(),
		stop: make(chan struct{}), done: make(chan struct{}),
	}
	r.runs[chamberID] = run
	view := run.snapshot()
	r.mu.Unlock()
	r.notify(view, "")

	go r.execute(run, issue)
	return view, nil
}

// execute drives a run from its completed-offset. A step is committed
// immediately after its issue succeeds, so resume always starts at the next
// opening and never repeats one.
func (r *Ramp) execute(run *rampRun, issue StepIssuer) {
	defer close(run.done)
	// Detach valve work from the originating request: request cancellation
	// must park the ramp at a safe point, never interrupt an in-flight opening.
	issueCtx := context.Background()
	for {
		target, ok := r.prepare(run)
		if !ok {
			r.completeIfRunning(run)
			return
		}
		if err := issue(issueCtx, target); err != nil {
			r.park(run, "valve command failed: "+err.Error())
			return
		}
		if !r.commit(run) {
			return
		}
		if !r.dwell(run) {
			return
		}
	}
}

// prepare returns the next target. False means the run must stop (halted
// externally) or that every step is done.
func (r *Ramp) prepare(run *rampRun) (float64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if run.status != RampRunning || run.completed >= len(run.targets) {
		return 0, false
	}
	return run.targets[run.completed], true
}

// commit stamps the step that just finished issuing and reports whether the
// goroutine may continue. An abort landing during the issue still records the
// opening (it happened) but ends the goroutine with the ramp aborted.
func (r *Ramp) commit(run *rampRun) bool {
	r.mu.Lock()
	index := run.completed
	if index < len(run.targets) {
		run.issuedAt[index] = r.now().UTC()
		run.completed++
	}
	aborting := run.aborting
	keepRunning := run.status == RampRunning && !aborting
	transitionedHere := false
	if aborting && run.status == RampRunning {
		run.status = RampAborted
		run.abortedAt = r.now().UTC()
		run.endedAt = run.abortedAt
		transitionedHere = true
	}
	view := run.snapshot()
	r.mu.Unlock()
	if transitionedHere {
		r.notify(view, RampRunning)
	}
	return keepRunning
}

// dwell waits the inter-step interval and halts when pause/abort is requested.
func (r *Ramp) dwell(run *rampRun) bool {
	timer := time.NewTimer(r.interval)
	defer timer.Stop()
	select {
	case <-run.stop:
		r.halt(run)
		return false
	case <-timer.C:
		return true
	}
}

// halt applies the paused/aborted transition requested through stop.
func (r *Ramp) halt(run *rampRun) {
	r.mu.Lock()
	if run.status != RampRunning {
		r.mu.Unlock()
		return
	}
	next := RampPaused
	if run.aborting {
		next = RampAborted
		run.abortedAt = r.now().UTC()
		run.endedAt = run.abortedAt
	} else {
		run.pausedAt = r.now().UTC()
	}
	run.status = next
	view := run.snapshot()
	r.mu.Unlock()
	r.notify(view, RampRunning)
}

// park freezes a run whose issuer failed; the operator reviews before resume.
func (r *Ramp) park(run *rampRun, note string) {
	r.mu.Lock()
	if run.status != RampRunning {
		r.mu.Unlock()
		return
	}
	run.status = RampPaused
	run.pausedAt = r.now().UTC()
	run.pauseNote = note
	view := run.snapshot()
	r.mu.Unlock()
	r.notify(view, RampRunning)
}

// completeIfRunning marks the ramp completed once the last step is committed.
func (r *Ramp) completeIfRunning(run *rampRun) {
	r.mu.Lock()
	if run.status != RampRunning {
		r.mu.Unlock()
		return
	}
	run.status = RampCompleted
	run.completed = len(run.targets)
	run.endedAt = r.now().UTC()
	view := run.snapshot()
	r.mu.Unlock()
	r.notify(view, RampRunning)
}

// Pause requests a running ramp to halt at the next safe point. It stays
// resumable. Pausing an already-paused ramp is idempotent.
func (r *Ramp) Pause(chamberID string, note string) (RampRunView, error) {
	r.mu.Lock()
	run, ok := r.runs[chamberID]
	if !ok {
		r.mu.Unlock()
		return RampRunView{}, errRampNotFound
	}
	if note != "" && run.status == RampRunning {
		run.pauseNote = note
	}
	switch run.status {
	case RampPaused:
		view := run.snapshot()
		r.mu.Unlock()
		return view, nil
	case RampRunning:
		select {
		case <-run.stop:
		default:
			close(run.stop)
		}
		r.mu.Unlock()
		<-run.done
	default:
		status := run.status
		r.mu.Unlock()
		return RampRunView{}, errors.New("ramp is " + string(status) + " and cannot be paused")
	}
	r.mu.Lock()
	view := run.snapshot()
	r.mu.Unlock()
	return view, nil
}

// Resume continues a paused ramp from the next step, but only for the same
// transit generation. A cancelled, emergency or newer generation is rejected.
// reviewer identifies the operator who performed the safety review. The
// resumed run remains server-side and outlives the resuming request.
func (r *Ramp) Resume(chamberID string, cycleID uuid.UUID, generation uint64, issue StepIssuer, reviewer string) (RampRunView, error) {
	if issue == nil {
		return RampRunView{}, errors.New("ramp requires a step issuer")
	}
	if reviewer == "" {
		return RampRunView{}, errors.New("safety review reviewer is required")
	}
	r.mu.Lock()
	run, ok := r.runs[chamberID]
	if !ok {
		r.mu.Unlock()
		return RampRunView{}, errRampNotFound
	}
	if !run.identity(cycleID, generation) {
		r.mu.Unlock()
		return RampRunView{}, errRampGeneration
	}
	if !run.status.resumable() {
		status := run.status
		r.mu.Unlock()
		return RampRunView{}, errors.New("ramp is " + string(status) + " and cannot be resumed")
	}
	run.status = RampRunning
	// Fresh signalling channels: the previous execute goroutine closed the old
	// done channel when it halted.
	run.stop = make(chan struct{})
	run.done = make(chan struct{})
	run.aborting = false
	run.reviewer = reviewer
	run.pausedAt = time.Time{}
	run.pauseNote = ""
	view := run.snapshot()
	r.mu.Unlock()
	r.notify(view, RampPaused)

	go r.execute(run, issue)
	return view, nil
}

// Abort terminates a ramp irreversibly (cancellation, emergency, new
// generation). A completed ramp is reported as-is.
func (r *Ramp) Abort(chamberID string, note string) (RampRunView, error) {
	r.mu.Lock()
	run, ok := r.runs[chamberID]
	if !ok {
		r.mu.Unlock()
		return RampRunView{}, errRampNotFound
	}
	switch run.status {
	case RampRunning:
		run.aborting = true
		run.abortNote = note
		select {
		case <-run.stop:
		default:
			close(run.stop)
		}
		// The goroutine may be inside a long issue with no safe point; mark
		// terminal immediately so concurrent status/resume see the truth.
		changed := false
		if run.status == RampRunning {
			run.status = RampAborted
			run.abortedAt = r.now().UTC()
			run.endedAt = run.abortedAt
			changed = true
		}
		view := run.snapshot()
		r.mu.Unlock()
		if changed {
			r.notify(view, RampRunning)
		}
		// Do not wait for the issuing goroutine: an in-flight command is allowed
		// to finish, but emergency/cancellation must not block on it. It ends
		// itself at the next safe point (commit/dwell) and emits nothing more.
		return view, nil
	case RampPaused:
		run.status = RampAborted
		run.abortedAt = r.now().UTC()
		run.endedAt = run.abortedAt
		run.abortNote = note
		view := run.snapshot()
		r.mu.Unlock()
		r.notify(view, RampPaused)
		return view, nil
	default:
		view := run.snapshot()
		r.mu.Unlock()
		return view, nil
	}
}

// AbortIfGeneration stops the ramp only when it belongs to the named transit
// generation, e.g. when that transit is cancelled or enters emergency.
func (r *Ramp) AbortIfGeneration(chamberID string, cycleID uuid.UUID, generation uint64, note string) (RampRunView, bool, error) {
	r.mu.Lock()
	run, ok := r.runs[chamberID]
	if !ok || !run.identity(cycleID, generation) {
		r.mu.Unlock()
		return RampRunView{}, false, nil
	}
	r.mu.Unlock()
	view, err := r.Abort(chamberID, note)
	return view, true, err
}

// Status returns the current ramp for a chamber, if any.
func (r *Ramp) Status(chamberID string) (RampRunView, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	run, ok := r.runs[chamberID]
	if !ok {
		return RampRunView{}, false
	}
	return run.snapshot(), true
}

// notify fires the lifecycle hook exactly once per transition; never called
// while holding r.mu.
func (r *Ramp) notify(view RampRunView, previous RampStatus) {
	r.mu.Lock()
	hook := r.onChange
	r.mu.Unlock()
	if hook != nil {
		hook(view, previous)
	}
}
