package api

import (
	"context"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"
)

type requestIDKey struct{}

func Router(runtime *Runtime, webDirectory string) http.Handler {
	router := chi.NewRouter()
	router.Use(middleware.RealIP)
	router.Use(middleware.Recoverer)
	router.Use(middleware.Timeout(15 * time.Second))
	router.Use(withRequestID)
	router.MethodNotAllowed(methodNotAllowed)
	router.Get("/healthz", healthHandler(runtime))
	router.Get("/", pageHandler(webDirectory, "operations.html"))
	router.Get("/operations", pageHandler(webDirectory, "operations.html"))
	router.Get("/chamber", pageHandler(webDirectory, "chamber.html"))
	router.Get("/interlocks", pageHandler(webDirectory, "interlocks.html"))
	router.Get("/incidents", pageHandler(webDirectory, "incidents.html"))
	router.Handle("/assets/*", http.StripPrefix("/assets/", http.FileServer(http.Dir(webDirectory))))
	router.Route("/api", func(api chi.Router) {
		api.Get("/chambers", chambersHandler(runtime))
		api.Get("/interlocks", interlocksHandler(runtime))
		api.Get("/incidents", incidentsHandler(runtime))
		api.Get("/plans", plansHandler(runtime))
		api.Post("/plans", createPlanHandler(runtime))
		api.Post("/plans/{planID}/signals", publishSignalsHandler(runtime))
		api.Post("/plans/{planID}/cancel", cancelPlanHandler(runtime))
		api.Post("/chambers/{chamberID}/gates/{direction}/open", openGateHandler(runtime))
		api.Post("/chambers/{chamberID}/gates/{direction}/close", closeGateHandler(runtime))
		api.Post("/chambers/{chamberID}/gates/{direction}/observe", gateObservationHandler(runtime))
		api.Post("/chambers/{chamberID}/reservation/release", releaseReservationHandler(runtime))
		api.Post("/chambers/{chamberID}/commands/{commandID}/ack", commandAckHandler(runtime))
		api.Post("/chambers/{chamberID}/valves/ramp", startValveRampHandler(runtime))
		api.Get("/chambers/{chamberID}/valves/ramp", valveRampHandler(runtime))
		api.Post("/chambers/{chamberID}/valves/ramp/pause", pauseValveRampHandler(runtime))
		api.Post("/chambers/{chamberID}/valves/ramp/resume", resumeValveRampHandler(runtime))
		api.Post("/chambers/{chamberID}/emergency", emergencyHandler(runtime))
		api.Post("/chambers/{chamberID}/equipment/pending", equipmentPendingHandler(runtime))
		api.Post("/chambers/{chamberID}/equipment/settled", equipmentSettledHandler(runtime))
		api.Post("/chambers/{chamberID}/signals/handoff", signalHandoffHandler(runtime))
		api.Post("/leases", assignLeaseHandler(runtime))
		api.Get("/leases/expired", expiredLeasesHandler(runtime))
		api.Post("/leases/{vesselID}/heartbeat", heartbeatHandler(runtime))
		api.Post("/leases/{vesselID}/reassign", reassignLeaseHandler(runtime))
		api.Delete("/leases/{vesselID}", releaseLeaseHandler(runtime))
	})
	return router
}

func withRequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := uuid.NewString()
		w.Header().Set("X-Request-ID", requestID)
		ctx := context.WithValue(r.Context(), requestIDKey{}, requestID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func requestID(ctx context.Context) string {
	value, _ := ctx.Value(requestIDKey{}).(string)
	return value
}
