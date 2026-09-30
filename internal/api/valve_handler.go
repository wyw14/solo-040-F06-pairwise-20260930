package api

import (
	"errors"
	"io"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/wyw14/cry-96/internal/hydraulics"
)

type rampInput struct {
	Targets []float64 `json:"targets"`
}

type rampPauseInput struct {
	Note string `json:"note"`
}

type rampResumeInput struct {
	Reviewer  string `json:"reviewer"`
	Confirmed bool   `json:"confirmed"`
	Note      string `json:"note"`
}

// rampErrorStatus maps ramp lifecycle errors to the appropriate HTTP status.
func rampErrorStatus(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, hydraulics.ErrRampNotFound):
		writeError(w, http.StatusNotFound, err)
	case errors.Is(err, hydraulics.ErrSafetyReview):
		writeError(w, http.StatusBadRequest, err)
	default:
		writeError(w, http.StatusConflict, err)
	}
}

// startValveRampHandler begins a managed ramp and returns immediately; the
// ramp continues even if this client disconnects.
func startValveRampHandler(runtime *Runtime) http.HandlerFunc {
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
			writeError(w, http.StatusConflict, errors.New("active transit is terminal"))
			return
		}
		plan, err := runtime.Hydraulics.StartRamp(r.Context(), cycleValue, input.Targets)
		if err != nil {
			rampErrorStatus(w, err)
			return
		}
		writeJSON(w, http.StatusAccepted, plan)
	}
}

// valveRampHandler returns the current ramp with completed and pending steps.
func valveRampHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		chamberID := chi.URLParam(r, "chamberID")
		plan, ok := runtime.Hydraulics.Ramp(chamberID)
		if !ok {
			writeError(w, http.StatusNotFound, hydraulics.ErrRampNotFound)
			return
		}
		writeJSON(w, http.StatusOK, plan)
	}
}

func pauseValveRampHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var input rampPauseInput
		if err := decodeJSON(r, &input); err != nil && !errors.Is(err, io.EOF) {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		plan, err := runtime.Hydraulics.PauseRamp(r.Context(), chi.URLParam(r, "chamberID"), input.Note)
		if err != nil {
			rampErrorStatus(w, err)
			return
		}
		writeJSON(w, http.StatusOK, plan)
	}
}

// resumeValveRampHandler continues from the next step only after an explicit
// safety review. Cancelled, emergency or superseded generations are rejected.
func resumeValveRampHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var input rampResumeInput
		if err := decodeJSON(r, &input); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		plan, err := runtime.Hydraulics.ResumeRamp(r.Context(), chi.URLParam(r, "chamberID"), hydraulics.SafetyReview{
			Reviewer: input.Reviewer, Confirmed: input.Confirmed, Note: input.Note,
		})
		if err != nil {
			rampErrorStatus(w, err)
			return
		}
		writeJSON(w, http.StatusOK, plan)
	}
}
