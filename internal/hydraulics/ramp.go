package hydraulics

import "time"

// RampStepState is the lifecycle state of a single valve opening step.
type RampStepState string

const (
	StepPending   RampStepState = "pending"
	StepIssued    RampStepState = "issued"
	StepCancelled RampStepState = "cancelled"
)

func (s RampStepState) terminal() bool { return s == StepIssued || s == StepCancelled }

// RampStatus is the operator-visible lifecycle of a transit ramp plan.
type RampStatus string

const (
	RampRunning   RampStatus = "running"
	RampPaused    RampStatus = "paused"
	RampCompleted RampStatus = "completed"
	RampAborted   RampStatus = "aborted"
)

func (s RampStatus) Terminal() bool { return s == RampCompleted || s == RampAborted }

// RampStep describes one opening target and its dispatch state.
type RampStep struct {
	Index     int           `json:"index"`
	Target    float64       `json:"target"`
	State     RampStepState `json:"state"`
	IssuedAt  time.Time     `json:"issued_at,omitempty"`
	CommandID string        `json:"command_id,omitempty"`
}

// RampPlan is the durable, operator-visible view of one cycle's valve ramp.
// It is bound to a transit cycle identity (id + generation); resume is only
// ever accepted for that exact generation, so a newer transit can never reuse
// a stale plan and re-issue the same opening.
type RampPlan struct {
	PlanID      string     `json:"plan_id"`
	ChamberID   string     `json:"chamber_id"`
	CycleID     string     `json:"cycle_id"`
	Generation  uint64     `json:"generation"`
	Status      RampStatus `json:"status"`
	Steps       []RampStep `json:"steps"`
	NextIndex   int        `json:"next_index"`
	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
	PausedAt    time.Time  `json:"paused_at,omitempty"`
	ResumeNote  string     `json:"resume_note,omitempty"`
	AbortReason string     `json:"abort_reason,omitempty"`
}

// CompletedSteps returns the already dispatched steps in order.
func (p RampPlan) CompletedSteps() []RampStep {
	return p.stepsInState(StepIssued)
}

// PendingSteps returns the steps still waiting to be dispatched.
func (p RampPlan) PendingSteps() []RampStep {
	return p.stepsInState(StepPending)
}

// MarkAborted invalidates a restored plan that can no longer be resumed.
func (p *RampPlan) MarkAborted(reason string) {
	p.Status = RampAborted
	p.AbortReason = reason
	for i := range p.Steps {
		if p.Steps[i].State == StepPending {
			p.Steps[i].State = StepCancelled
		}
	}
}

func (p RampPlan) stepsInState(want RampStepState) []RampStep {
	result := make([]RampStep, 0, len(p.Steps))
	for _, step := range p.Steps {
		if step.State == want {
			result = append(result, step)
		}
	}
	return result
}

func (p RampPlan) clone() RampPlan {
	plan := p
	plan.Steps = append([]RampStep(nil), p.Steps...)
	return plan
}
