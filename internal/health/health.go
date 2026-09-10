// Package health answers the two questions an orchestrator asks.
//
// It exists because the chart could only probe TCP, and said so honestly: the
// API has no unauthenticated call, so a probe could not ask it a question. A
// listener that is up is not a plane that works — a control plane whose
// database or bus has gone stays bound to its port, keeps passing its probe,
// and fails every request it is given, which is the shape of an outage nobody
// is paged for.
//
// The endpoints are unauthenticated because a probe has no credential and
// arranging one for it would put a secret in a liveness check. They are
// therefore deliberately mute: "ok", or the names of the dependencies that are
// down. Never a driver error, a DSN, or an address — a readiness endpoint is
// reachable by anything that can reach the port, and the difference between
// "the database is unreachable" and "the database is at 10.0.3.4:5432 and
// refused the password" is the difference between a status and a leak.
package health

import (
	"context"
	"net/http"
	"sort"
	"strings"
	"time"
)

// Check reports whether one dependency is usable right now.
type Check func(ctx context.Context) error

// probeTimeout bounds every check together. A probe that hangs is a probe that
// fails: kubelet gives up on its own schedule and reads the timeout as a
// failure anyway, so answering "not ready" quickly is strictly better than
// answering nothing slowly.
const probeTimeout = 3 * time.Second

// Handler mounts /healthz and /readyz on mux.
//
// Liveness and readiness are different questions and the difference is
// load-bearing. /healthz says the process is running and is answered without
// touching anything: a plane whose database is down must NOT be killed and
// restarted, because restarting it does not bring the database back and a
// crash-loop makes the outage worse. /readyz says it can do work, and is what
// takes a plane out of a Service until its dependencies return.
func Handler(mux *http.ServeMux, checks map[string]Check) {
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		write(w, http.StatusOK, "ok")
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), probeTimeout)
		defer cancel()

		var down []string
		for name, check := range checks {
			if check == nil {
				continue
			}
			if err := check(ctx); err != nil {
				down = append(down, name)
			}
		}
		if len(down) == 0 {
			write(w, http.StatusOK, "ok")
			return
		}
		// Sorted so a flapping probe reads as one repeated line rather than a
		// different one each time the map is ranged.
		sort.Strings(down)
		write(w, http.StatusServiceUnavailable, "not ready: "+strings.Join(down, ", "))
	})
}

func write(w http.ResponseWriter, code int, body string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	_, _ = w.Write([]byte(body + "\n"))
}
