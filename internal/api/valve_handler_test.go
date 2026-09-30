package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/hydraulics"
	"github.com/wyw14/cry-96/internal/model"
)

type rampAPIRig struct {
	t       *testing.T
	server  *httptest.Server
	runtime *Runtime
	chamber string
}

func newRampAPIRig(t *testing.T) rampAPIRig {
	t.Helper()
	runtime, err := NewRuntime(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(Router(runtime, t.TempDir()))
	t.Cleanup(server.Close)
	return rampAPIRig{t: t, server: server, runtime: runtime, chamber: "chamber-1"}
}

func (r rampAPIRig) startCycle() model.TransitCycle {
	r.t.Helper()
	plan := model.ConvoyPlan{
		ID: uuid.New(), ChamberID: r.chamber, Direction: model.DirectionUpstream,
		Vessels:   []model.Vessel{{ID: uuid.New(), Name: "River Star", CallSign: "RS-1", LengthM: 80}},
		CreatedAt: time.Now(),
	}
	cycleValue, err := r.runtime.Cycles.Start(context.Background(), plan)
	if err != nil {
		r.t.Fatal(err)
	}
	return cycleValue
}

func (r rampAPIRig) do(method, path string, body any) (*http.Response, map[string]any) {
	r.t.Helper()
	var reader io.Reader
	if body != nil {
		data, _ := json.Marshal(body)
		reader = bytes.NewBuffer(data)
	}
	request, _ := http.NewRequest(method, r.server.URL+path, reader)
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		r.t.Fatal(err)
	}
	defer response.Body.Close()
	var decoded map[string]any
	_ = json.NewDecoder(response.Body).Decode(&decoded)
	return response, decoded
}

func TestValveRampHTTPLifecycle(t *testing.T) {
	rig := newRampAPIRig(t)
	rig.startCycle()

	response, _ := rig.do(http.MethodGet, "/api/chambers/"+rig.chamber+"/valves/ramp", nil)
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("GET empty ramp = %d, want 404", response.StatusCode)
	}

	response, plan := rig.do(http.MethodPost, "/api/chambers/"+rig.chamber+"/valves/ramp",
		map[string]any{"targets": []float64{10, 20, 30, 40}})
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("POST ramp = %d body=%v, want 202", response.StatusCode, plan)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		response, current := rig.do(http.MethodGet, "/api/chambers/"+rig.chamber+"/valves/ramp", nil)
		if response.StatusCode == http.StatusOK && current["status"] == string(hydraulics.RampCompleted) {
			steps := current["steps"].([]any)
			if len(steps) != 4 {
				t.Fatalf("steps = %v, want 4", steps)
			}
			if next, _ := current["next_index"].(float64); next != 4 {
				t.Fatalf("next_index = %v, want 4", current["next_index"])
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("ramp never completed")
}

func TestValveRampHTTPPauseResumeAndCancelGuard(t *testing.T) {
	rig := newRampAPIRig(t)
	rig.startCycle()

	// 12 targets widen the window so the pause lands while steps remain.
	targets := []float64{10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120}
	if response, body := rig.do(http.MethodPost, "/api/chambers/"+rig.chamber+"/valves/ramp",
		map[string]any{"targets": targets}); response.StatusCode != http.StatusAccepted {
		t.Fatalf("POST ramp = %d %v", response.StatusCode, body)
	}

	deadline := time.Now().Add(2 * time.Second)
	var paused bool
	for time.Now().Before(deadline) && !paused {
		response, _ := rig.do(http.MethodPost, "/api/chambers/"+rig.chamber+"/valves/ramp/pause",
			map[string]any{"note": "operator check"})
		if response.StatusCode == http.StatusOK {
			paused = true
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if !paused {
		t.Fatal("ramp never paused")
	}

	response, current := rig.do(http.MethodGet, "/api/chambers/"+rig.chamber+"/valves/ramp", nil)
	if response.StatusCode != http.StatusOK || current["status"] != string(hydraulics.RampPaused) {
		t.Fatalf("after pause status=%d plan=%v", response.StatusCode, current)
	}
	if next, _ := current["next_index"].(float64); next >= float64(len(targets)) {
		t.Fatalf("ramp finished before pause: next=%v", next)
	}

	// Resume without confirmed safety review -> 400.
	if response, _ := rig.do(http.MethodPost, "/api/chambers/"+rig.chamber+"/valves/ramp/resume",
		map[string]any{"reviewer": "ops"}); response.StatusCode != http.StatusBadRequest {
		t.Fatalf("resume without review = %d, want 400", response.StatusCode)
	}

	// Cancel the cycle; resume is rejected (409) and the ramp aborts.
	if _, err := rig.runtime.Cycles.Cancel(context.Background(), rig.chamber, "operator cancellation"); err != nil {
		t.Fatal(err)
	}
	if response, _ := rig.do(http.MethodPost, "/api/chambers/"+rig.chamber+"/valves/ramp/resume",
		map[string]any{"reviewer": "ops", "confirmed": true}); response.StatusCode != http.StatusConflict {
		t.Fatalf("resume after cancel = %d, want 409", response.StatusCode)
	}

	response, plan := rig.do(http.MethodGet, "/api/chambers/"+rig.chamber+"/valves/ramp", nil)
	if response.StatusCode != http.StatusOK || plan["status"] != string(hydraulics.RampAborted) {
		t.Fatalf("after cancel status=%d plan=%v", response.StatusCode, plan)
	}
	if pending := len(plan["steps"].([]any)); pending == 0 {
		t.Fatal("expected recorded steps")
	}
}
