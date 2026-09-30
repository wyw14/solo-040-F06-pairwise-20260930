package api

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/wyw14/cry-96/internal/hydraulics"
	"github.com/wyw14/cry-96/internal/model"
)

type rampInput struct {
	Targets []float64 `json:"targets"`
}

type rampPauseInput struct {
	Note string `json:"note"`
}

type rampResumeInput struct {
	// Confirmed and Reviewer are the safety-review evidence required before a
	// paused ramp is allowed to continue.
	Confirmed bool   `json:"confirmed"`
	Reviewer  string `json:"reviewer"`
}

// rampStatusResponse augments the stored ramp with the live transit checks
// that decide whether resumption is currently permitted.
type rampStatusResponse struct {
	hydraulics.RampRunView
	Stage         model.TransitStage `json:"stage"`
	CanResume     bool               `json:"can_resume"`
	BlockedReason string             `json:"blocked_reason,omitempty"`
}

func valveRampStartHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var input rampInput
		if err := decodeJSON(r, &input); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		if len(input.Targets) == 0 || len(input.Targets) > 12 {
			writeError(w, http.StatusBadRequest, errors.New("targets must contain 1 to 12 steps"))
			return
		}
		chamberID := chi.URLParam(r, "chamberID")
		cycleValue, ok := runtime.Cycles.Current(chamberID)
		if !ok {
			writeError(w, http.StatusConflict, errors.New("active transit not found"))
			return
		}
		if cycleValue.Stage.Terminal() {
			writeError(w, http.StatusConflict, errors.New("transit is "+string(cycleValue.Stage)+"; ramp is locked"))
			return
		}
		// The ramp is a server-side run and deliberately outlives this request:
		// dropping the HTTP connection must not truncate the step sequence.
		view, err := runtime.Hydraulics.StartRamp(cycleValue, input.Targets)
		if err != nil {
			writeRampError(w, err)
			return
		}
		writeJSON(w, http.StatusAccepted, view)
	}
}

func valveRampStatusHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		chamberID := chi.URLParam(r, "chamberID")
		view, ok := runtime.Hydraulics.RampStatus(chamberID)
		if !ok {
			writeError(w, http.StatusNotFound, errors.New("no ramp found for this chamber"))
			return
		}
		response := rampStatusResponse{RampRunView: view}
		if cycleValue, current := runtime.Cycles.Current(chamberID); current {
			response.Stage = cycleValue.Stage
			response.CanResume, response.BlockedReason = resumePermitted(view, cycleValue)
		} else {
			response.Stage = model.TransitStage("idle")
			response.BlockedReason = "no active transit"
		}
		writeJSON(w, http.StatusOK, response)
	}
}

func valveRampPauseHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var input rampPauseInput
		if r.ContentLength != 0 {
			if err := decodeJSON(r, &input); err != nil {
				writeError(w, http.StatusBadRequest, err)
				return
			}
		}
		if input.Note == "" {
			input.Note = "operator pause"
		}
		view, err := runtime.Hydraulics.PauseRamp(chi.URLParam(r, "chamberID"), input.Note)
		if err != nil {
			writeRampError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, view)
	}
}

func valveRampResumeHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var input rampResumeInput
		if err := decodeJSON(r, &input); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		if !input.Confirmed || input.Reviewer == "" {
			writeError(w, http.StatusBadRequest, errors.New("resume requires a completed safety review: confirmed=true and reviewer name"))
			return
		}
		chamberID := chi.URLParam(r, "chamberID")
		cycleValue, ok := runtime.Cycles.Current(chamberID)
		if !ok {
			writeError(w, http.StatusConflict, errors.New("active transit not found"))
			return
		}
		if cycleValue.Stage.Terminal() {
			writeError(w, http.StatusConflict, errors.New("transit is "+string(cycleValue.Stage)+"; resume is forbidden"))
			return
		}
		view, found := runtime.Hydraulics.RampStatus(chamberID)
		if !found {
			writeError(w, http.StatusNotFound, errors.New("no ramp found for this chamber"))
			return
		}
		if permitted, reason := resumePermitted(view, cycleValue); !permitted {
			writeError(w, http.StatusConflict, errors.New(reason))
			return
		}
		result, err := runtime.Hydraulics.ResumeRamp(cycleValue, input.Reviewer)
		if err != nil {
			writeRampError(w, err)
			return
		}
		writeJSON(w, http.StatusAccepted, result)
	}
}

// resumePermitted centralises the resume guards: the ramp must be paused, the
// current transit must be the exact generation that owns it, and that transit
// must not be terminal (cancelled or emergency).
func resumePermitted(view hydraulics.RampRunView, cycleValue model.TransitCycle) (bool, string) {
	if !view.Resumable {
		switch view.Status {
		case hydraulics.RampRunning:
			return false, "ramp is already running"
		case hydraulics.RampAborted:
			return false, "ramp was aborted: " + view.AbortReason
		case hydraulics.RampCompleted:
			return false, "ramp is already complete"
		default:
			return false, "ramp is " + string(view.Status)
		}
	}
	if cycleValue.Stage.Terminal() {
		return false, "transit is " + string(cycleValue.Stage) + "; resume is forbidden"
	}
	if view.CycleID != cycleValue.ID || view.Generation != cycleValue.Generation {
		return false, "transit generation changed; resume is forbidden"
	}
	return true, ""
}

func writeRampError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, hydraulics.ErrRampNotFound):
		writeError(w, http.StatusNotFound, err)
	case errors.Is(err, hydraulics.ErrRampActive),
		errors.Is(err, hydraulics.ErrRampGeneration),
		errors.Is(err, hydraulics.ErrRampNotPaused):
		writeError(w, http.StatusConflict, err)
	default:
		writeError(w, http.StatusBadRequest, err)
	}
}
