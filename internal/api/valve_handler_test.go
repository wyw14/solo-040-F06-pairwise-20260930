package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func setupRampServer(t *testing.T) (*Runtime, http.Handler) {
	t.Helper()
	previous := defaultRampInterval
	defaultRampInterval = time.Hour // stretch dwell so a running ramp pauses deterministically
	t.Cleanup(func() { defaultRampInterval = previous })

	runtime, err := NewRuntime(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	return runtime, Router(runtime, "../..")
}

func doJSON(t *testing.T, handler http.Handler, method, path string, body any) (int, map[string]any) {
	t.Helper()
	var reader *bytes.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		reader = bytes.NewReader(data)
	} else {
		reader = bytes.NewReader(nil)
	}
	request := httptest.NewRequest(method, path, reader)
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	decoded := map[string]any{}
	if response.Body.Len() > 0 {
		_ = json.Unmarshal(response.Body.Bytes(), &decoded)
	}
	return response.Code, decoded
}

func createCycle(t *testing.T, handler http.Handler, chamberID string) map[string]any {
	t.Helper()
	status, body := doJSON(t, handler, http.MethodPost, "/api/plans", map[string]any{
		"chamber_id": chamberID,
		"direction":  "upstream",
		"vessels":    []map[string]any{{"name": "Vessel", "call_sign": "CS-1", "length_m": 12}},
	})
	if status != http.StatusCreated {
		t.Fatalf("create plan status = %d body = %v", status, body)
	}
	return body
}

// Full lifecycle: start, observe steps, pause, resume requires review evidence,
// and resumption is forbidden after the transit is cancelled.
func TestRampPauseReviewResumeAndCancelLifecycle(t *testing.T) {
	runtime, handler := setupRampServer(t)
	const chamber = "chamber-1"
	created := createCycle(t, handler, chamber)
	plan := created["plan"].(map[string]any)

	// Start a four-step ramp; it runs server-side (202).
	status, ramp := doJSON(t, handler, http.MethodPost, "/api/chambers/"+chamber+"/valves/ramp",
		map[string]any{"targets": []float64{0.1, 0.2, 0.4, 0.7}})
	if status != http.StatusAccepted || ramp["status"] != "running" {
		t.Fatalf("start status = %d ramp = %v", status, ramp)
	}

	// Exactly one step should be issued before the long dwell parks it.
	waitRamp(t, handler, chamber, func(view map[string]any) bool {
		return number(view["completed"]) == 1
	})

	// Operator can inspect completed vs pending steps.
	status, view := doJSON(t, handler, http.MethodGet, "/api/chambers/"+chamber+"/valves/ramp", nil)
	if status != http.StatusOK {
		t.Fatalf("status code = %d", status)
	}
	steps := view["steps"].([]any)
	if len(steps) != 4 || view["total"].(float64) != 4 || view["completed"].(float64) != 1 {
		t.Fatalf("unexpected step view: %v", view)
	}
	if view["can_resume"].(bool) {
		t.Fatal("a running ramp must not advertise resume")
	}

	// Pause: halts at the safe point with one step committed.
	status, paused := doJSON(t, handler, http.MethodPost, "/api/chambers/"+chamber+"/valves/ramp/pause",
		map[string]any{"note": "operator review"})
	if status != http.StatusOK || paused["status"] != "paused" || paused["completed"].(float64) != 1 {
		t.Fatalf("pause status = %d body = %v", status, paused)
	}

	status, view = doJSON(t, handler, http.MethodGet, "/api/chambers/"+chamber+"/valves/ramp", nil)
	if !view["can_resume"].(bool) || textOf(view["blocked_reason"]) != "" {
		t.Fatalf("paused same-generation ramp should be resumable: %v", view)
	}

	// Resume without safety-review evidence is rejected.
	status, body := doJSON(t, handler, http.MethodPost, "/api/chambers/"+chamber+"/valves/ramp/resume",
		map[string]any{"confirmed": false, "reviewer": ""})
	if status != http.StatusBadRequest {
		t.Fatalf("resume without review status = %d body = %v", status, body)
	}

	// Resume with review continues from step 2 (no repeat of step 1).
	status, resumed := doJSON(t, handler, http.MethodPost, "/api/chambers/"+chamber+"/valves/ramp/resume",
		map[string]any{"confirmed": true, "reviewer": "alice"})
	if status != http.StatusAccepted || resumed["status"] != "running" || resumed["completed"].(float64) != 1 {
		t.Fatalf("resume status = %d body = %v", status, resumed)
	}
	waitRamp(t, handler, chamber, func(v map[string]any) bool { return number(v["completed"]) == 2 })
	if count := len(runtime.GateCommands.Pending(chamber)); count != 2 {
		t.Fatalf("issued commands = %d, want exactly 2 (step 1 must not be re-issued)", count)
	}

	// Cancel the transit: resume becomes forbidden (409).
	status, _ = doJSON(t, handler, http.MethodPost, "/api/plans/"+plan["id"].(string)+"/cancel", map[string]any{})
	if status != http.StatusOK {
		t.Fatalf("cancel plan status = %d", status)
	}
	waitRamp(t, handler, chamber, func(v map[string]any) bool { return v["status"] == "aborted" })
	status, body = doJSON(t, handler, http.MethodPost, "/api/chambers/"+chamber+"/valves/ramp/resume",
		map[string]any{"confirmed": true, "reviewer": "alice"})
	if status != http.StatusConflict {
		t.Fatalf("resume after cancel status = %d body = %v, want 409", status, body)
	}
	status, view = doJSON(t, handler, http.MethodGet, "/api/chambers/"+chamber+"/valves/ramp", nil)
	if view["can_resume"].(bool) || view["status"] != "aborted" {
		t.Fatalf("aborted ramp must never be resumable: %v", view)
	}
}

// After the generation changes (new transit), the old ramp can neither be
// resumed nor hide the block from the operator.
func TestRampResumeForbiddenAfterGenerationChange(t *testing.T) {
	_, handler := setupRampServer(t)
	const chamber = "chamber-2"
	createCycle(t, handler, chamber)
	doJSON(t, handler, http.MethodPost, "/api/chambers/"+chamber+"/valves/ramp",
		map[string]any{"targets": []float64{0.1, 0.2, 0.4}})
	waitRamp(t, handler, chamber, func(v map[string]any) bool { return number(v["completed"]) == 1 })
	doJSON(t, handler, http.MethodPost, "/api/chambers/"+chamber+"/valves/ramp/pause", map[string]any{})
	waitRamp(t, handler, chamber, func(v map[string]any) bool { return v["status"] == "paused" })

	// An emergency terminates the transit and must forbid resuming the ramp.
	status, _ := doJSON(t, handler, http.MethodPost, "/api/chambers/"+chamber+"/emergency", nil)
	if status != http.StatusOK {
		t.Fatalf("emergency status = %d", status)
	}
	waitRamp(t, handler, chamber, func(v map[string]any) bool { return v["status"] == "aborted" })
	status, body := doJSON(t, handler, http.MethodPost, "/api/chambers/"+chamber+"/valves/ramp/resume",
		map[string]any{"confirmed": true, "reviewer": "bob"})
	if status != http.StatusConflict {
		t.Fatalf("resume after emergency status = %d body = %v, want 409", status, body)
	}
}

func number(value any) float64 {
	if n, ok := value.(float64); ok {
		return n
	}
	return -1
}

func textOf(value any) string {
	if s, ok := value.(string); ok {
		return s
	}
	return ""
}

func waitRamp(t *testing.T, handler http.Handler, chamber string, condition func(map[string]any) bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		status, view := doJSON(t, handler, http.MethodGet, "/api/chambers/"+chamber+"/valves/ramp", nil)
		if status == http.StatusOK && condition(view) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("ramp condition not met before deadline")
}
