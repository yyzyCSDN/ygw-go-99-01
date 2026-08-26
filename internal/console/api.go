package console

import (
	"encoding/json"
	"net/http"
	"time"
)

type API struct {
	con *Console
}

func NewAPI(con *Console) *API {
	return &API{con: con}
}

type commandRequest struct {
	ID        string  `json:"id"`
	Value     float64 `json:"value"`
	TimeoutMS int64   `json:"timeout_ms"`
	WindowMS  int64   `json:"window_ms"`
}

type commandResponse struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

func (a *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/api/console/summary":
		a.handleSummary(w, r)
	case r.Method == http.MethodGet && r.URL.Path == "/api/console/stream":
		a.handleStream(w, r)
	case r.Method == http.MethodGet && r.URL.Path == "/api/console/events":
		a.handleEvents(w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/api/console/start-fan":
		a.handleCommand(w, r, a.commandStartFan)
	case r.Method == http.MethodPost && r.URL.Path == "/api/console/trigger-trip":
		a.handleCommand(w, r, a.commandTriggerTrip)
	case r.Method == http.MethodPost && r.URL.Path == "/api/console/reset-trip":
		a.handleCommand(w, r, a.commandResetTrip)
	case r.Method == http.MethodPost && r.URL.Path == "/api/console/restore":
		a.handleCommand(w, r, a.commandRestore)
	case r.Method == http.MethodPost && r.URL.Path == "/api/console/stop-after-stable":
		a.handleCommand(w, r, a.commandStopAfterStable)
	case r.Method == http.MethodPost && r.URL.Path == "/api/console/calibrate":
		a.handleCommand(w, r, a.commandCalibrate)
	default:
		http.NotFound(w, r)
	}
}

func (a *API) handleSummary(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, a.con.Summary())
}

func (a *API) handleStream(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	writeJSON(w, http.StatusOK, a.con.Summary())
}

func (a *API) handleEvents(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, a.con.RecentEvents())
}

func (a *API) handleCommand(w http.ResponseWriter, r *http.Request, run func(commandRequest) error) {
	var req commandRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, commandResponse{OK: false, Error: err.Error()})
		return
	}
	if err := run(req); err != nil {
		writeJSON(w, http.StatusOK, commandResponse{OK: false, Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, commandResponse{OK: true})
}

func (a *API) commandStartFan(req commandRequest) error {
	return a.con.CommandStartFan(req.ID, duration(req.TimeoutMS, 5*time.Second))
}

func (a *API) commandTriggerTrip(req commandRequest) error {
	return a.con.CommandTriggerTrip(req.ID, duration(req.TimeoutMS, 5*time.Second))
}

func (a *API) commandResetTrip(req commandRequest) error {
	return a.con.CommandResetTrip(req.ID)
}

func (a *API) commandRestore(req commandRequest) error {
	return a.con.CommandRestore(req.ID)
}

func (a *API) commandStopAfterStable(req commandRequest) error {
	return a.con.CommandStopAfterStable(req.ID, duration(req.WindowMS, 30*time.Second))
}

func (a *API) commandCalibrate(req commandRequest) error {
	return a.con.CommandCalibrate(req.ID, req.Value)
}

func duration(millis int64, fallback time.Duration) time.Duration {
	if millis <= 0 {
		return fallback
	}
	return time.Duration(millis) * time.Millisecond
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
