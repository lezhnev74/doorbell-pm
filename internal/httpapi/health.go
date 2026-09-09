package httpapi

import (
	"net/http"
	"time"

	"doorbell-pm/internal/pool"
)

// healthResponse is the GET <health_path> body. Timestamps are RFC 3339 and
// null when the event has not happened yet.
type healthResponse struct {
	Status string                `json:"status"`
	Pools  map[string]poolHealth `json:"pools"`
}

type poolHealth struct {
	Running      int           `json:"running"`
	Concurrency  int           `json:"concurrency"`
	LastHint     *time.Time    `json:"last_hint"`
	LastExit     *time.Time    `json:"last_exit"`
	LastExitCode *int          `json:"last_exit_code"`
	LastTasks    *int          `json:"last_tasks"`
	Breaker      breakerHealth `json:"breaker"`
}

type breakerHealth struct {
	OpenUntil *time.Time `json:"open_until"`
	Level     int        `json:"level"`
}

func (s *Server) healthHandler(w http.ResponseWriter, _ *http.Request) {
	stats := s.stats()
	resp := healthResponse{Status: "ok", Pools: make(map[string]poolHealth, len(stats))}
	for name, st := range stats {
		resp.Pools[name] = toPoolHealth(st)
	}
	writeJSON(w, http.StatusOK, resp)
}

func toPoolHealth(st pool.Stats) poolHealth {
	h := poolHealth{
		Running:     st.Running,
		Concurrency: st.Concurrency,
		LastHint:    optTime(st.LastHint),
		LastExit:    optTime(st.LastExit),
		Breaker:     breakerHealth{OpenUntil: optTime(st.BreakerOpenUntil), Level: st.BreakerLevel},
	}
	if h.LastExit != nil {
		code := st.LastExitCode
		h.LastExitCode = &code
	}
	if st.HasLastTasks {
		tasks := st.LastTasks
		h.LastTasks = &tasks
	}
	return h
}

// optTime maps the zero time to null.
func optTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}
