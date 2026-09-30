package model

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
)

type EventType string

const (
	EventPlanCreated      EventType = "plan.created"
	EventPlanCancelled    EventType = "plan.cancelled"
	EventCycleCreated     EventType = "cycle.created"
	EventCycleAdvanced    EventType = "cycle.advanced"
	EventCommandQueued    EventType = "command.queued"
	EventCommandSettled   EventType = "command.settled"
	EventSignalChanged    EventType = "signal.changed"
	EventLeaseChanged     EventType = "lease.changed"
	EventEmergencyStarted EventType = "emergency.started"
	EventEmergencySettled EventType = "emergency.settled"
	EventRampCreated      EventType = "ramp.created"
	EventRampStepIssued   EventType = "ramp.step_issued"
	EventRampPaused       EventType = "ramp.paused"
	EventRampResumed      EventType = "ramp.resumed"
	EventRampCompleted    EventType = "ramp.completed"
	EventRampAborted      EventType = "ramp.aborted"
	EventReplayWarning    EventType = "replay.warning"
)

type Event struct {
	ID         uuid.UUID       `json:"id"`
	ChamberID  string          `json:"chamber_id"`
	Sequence   uint64          `json:"sequence"`
	Type       EventType       `json:"type"`
	CycleID    uuid.UUID       `json:"cycle_id,omitempty"`
	Generation uint64          `json:"generation,omitempty"`
	OccurredAt time.Time       `json:"occurred_at"`
	Payload    json.RawMessage `json:"payload,omitempty"`
}

func NewEvent(chamberID string, kind EventType, cycleID uuid.UUID, generation uint64, payload any, now time.Time) (Event, error) {
	if chamberID == "" || kind == "" {
		return Event{}, errors.New("event chamber and type are required")
	}
	var encoded json.RawMessage
	if payload != nil {
		data, err := json.Marshal(payload)
		if err != nil {
			return Event{}, err
		}
		encoded = data
	}
	return Event{
		ID: uuid.New(), ChamberID: chamberID, Type: kind,
		CycleID: cycleID, Generation: generation,
		OccurredAt: now.UTC(), Payload: encoded,
	}, nil
}

func (e Event) Decode(target any) error {
	if len(e.Payload) == 0 {
		return errors.New("event has no payload")
	}
	return json.Unmarshal(e.Payload, target)
}

type ChamberSnapshot struct {
	ChamberID string                     `json:"chamber_id"`
	Sequence  uint64                     `json:"sequence"`
	Cycle     *TransitCycle              `json:"cycle,omitempty"`
	Gates     map[Direction]GatePosition `json:"gates"`
	LevelM    float64                    `json:"level_m"`
	Signals   []SignalState              `json:"signals"`
	SavedAt   time.Time                  `json:"saved_at"`
}

func (s ChamberSnapshot) Clone() ChamberSnapshot {
	if s.Cycle != nil {
		cycle := s.Cycle.Clone()
		s.Cycle = &cycle
	}
	s.Gates = map[Direction]GatePosition{
		DirectionUpstream:   s.Gates[DirectionUpstream],
		DirectionDownstream: s.Gates[DirectionDownstream],
	}
	s.Signals = append([]SignalState(nil), s.Signals...)
	return s
}
