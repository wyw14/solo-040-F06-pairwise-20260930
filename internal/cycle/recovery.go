package cycle

import (
	"context"
	"fmt"

	"github.com/wyw14/cry-96/internal/hydraulics"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
)

// Recovery rebuilds coordinator state from partition-safe replay results.
type Recovery struct {
	replayer    *journal.Replayer
	coordinator *Coordinator
	hydraulics  *hydraulics.Controller
}

func NewRecovery(replayer *journal.Replayer, coordinator *Coordinator, hydraulicsController *hydraulics.Controller) *Recovery {
	return &Recovery{replayer: replayer, coordinator: coordinator, hydraulics: hydraulicsController}
}

func (r *Recovery) RestoreAll(ctx context.Context) (map[string]model.TransitCycle, error) {
	states, err := r.replayer.ReplayAll(ctx)
	if err != nil {
		return nil, err
	}
	restored := make(map[string]model.TransitCycle)
	var rampEvents []model.Event
	for chamberID, state := range states {
		cycleValue := state.Snapshot.Cycle
		for _, event := range state.Applied {
			if event.ChamberID != chamberID {
				return nil, fmt.Errorf("event partition mismatch chamber=%s event=%s", chamberID, event.ChamberID)
			}
			if isRampEvent(event.Type) {
				rampEvents = append(rampEvents, event)
			}
			if event.Type == model.EventCycleCreated || event.Type == model.EventCycleAdvanced {
				var decoded model.TransitCycle
				if err := event.Decode(&decoded); err != nil {
					return nil, err
				}
				if decoded.ChamberID != chamberID {
					return nil, fmt.Errorf("cycle partition mismatch chamber=%s cycle=%s", chamberID, decoded.ChamberID)
				}
				cycleCopy := decoded
				cycleValue = &cycleCopy
			}
		}
		if cycleValue != nil {
			r.coordinator.Restore(*cycleValue)
			restored[chamberID] = cycleValue.Clone()
		}
	}
	if r.hydraulics != nil && len(rampEvents) > 0 {
		plans := hydraulics.ReconstructPlans(rampEvents)
		for i := range plans {
			cycleValue, ok := restored[plans[i].ChamberID]
			if plans[i].Status.Terminal() {
				continue
			}
			// A terminal cycle or a replaced generation makes the plan
			// unresumable; never present it as a pause the operator can resume.
			if !ok || cycleValue.Stage.Terminal() ||
				cycleValue.ID.String() != plans[i].CycleID || cycleValue.Generation != plans[i].Generation {
				plans[i].MarkAborted("transit " + string(cycleValue.Stage) + " at restart")
			}
		}
		r.hydraulics.RestoreRamps(plans)
	}
	return restored, nil
}

func isRampEvent(kind model.EventType) bool {
	switch kind {
	case model.EventRampCreated, model.EventRampStepIssued, model.EventRampPaused,
		model.EventRampResumed, model.EventRampCompleted, model.EventRampAborted:
		return true
	}
	return false
}
