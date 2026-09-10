package health_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/azrtydxb/dhole/internal/health"
)

func serve(t *testing.T, checks map[string]health.Check) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	health.Handler(mux, checks)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func get(t *testing.T, srv *httptest.Server, path string) (int, string) {
	t.Helper()
	resp, err := srv.Client().Get(srv.URL + path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	buf := make([]byte, 512)
	n, _ := resp.Body.Read(buf)
	return resp.StatusCode, string(buf[:n])
}

// A probe carries no credential — arranging one would put a secret in a
// liveness check — so both endpoints must answer without authentication.
func TestBothProbesAnswerWithoutACredential(t *testing.T) {
	srv := serve(t, nil)

	for _, path := range []string{"/healthz", "/readyz"} {
		if code, _ := get(t, srv, path); code != http.StatusOK {
			t.Errorf("%s answered %d to an unauthenticated probe", path, code)
		}
	}
}

// The whole point: a plane whose dependency is gone must stop saying it can
// work. The chart could only probe TCP, so a control plane with no database
// kept passing its probe and failing every request it was given.
func TestReadinessFailsWhenADependencyIsDown(t *testing.T) {
	srv := serve(t, map[string]health.Check{
		"store": func(context.Context) error { return errors.New("dial tcp 10.0.3.4:5432: connection refused") },
		"bus":   func(context.Context) error { return nil },
	})

	code, body := get(t, srv, "/readyz")
	if code != http.StatusServiceUnavailable {
		t.Fatalf("readiness answered %d with the store down", code)
	}
	if !strings.Contains(body, "store") {
		t.Errorf("the response does not say which dependency is down: %q", body)
	}
	if strings.Contains(body, "bus") {
		t.Errorf("a healthy dependency was reported as down: %q", body)
	}
}

// Unauthenticated and reachable by anything that can reach the port, so it says
// WHAT is down and never anything about where it is or why.
func TestReadinessNamesTheDependencyWithoutLeakingItsDetails(t *testing.T) {
	srv := serve(t, map[string]health.Check{
		"store": func(context.Context) error {
			return errors.New(`dial tcp 10.0.3.4:5432: password authentication failed for user "dhole"`)
		},
	})

	_, body := get(t, srv, "/readyz")
	for _, secret := range []string{"10.0.3.4", "5432", "password", "dhole"} {
		if strings.Contains(body, secret) {
			t.Errorf("the readiness body leaks %q: %q", secret, body)
		}
	}
}

// Liveness must NOT depend on anything external. A plane whose database is
// down is not a plane worth killing: restarting it does not bring the database
// back, and a crash-loop turns one outage into two.
func TestLivenessIgnoresDependencies(t *testing.T) {
	srv := serve(t, map[string]health.Check{
		"store": func(context.Context) error { return errors.New("gone") },
		"bus":   func(context.Context) error { return errors.New("gone") },
	})

	if code, _ := get(t, srv, "/healthz"); code != http.StatusOK {
		t.Errorf("liveness answered %d with dependencies down; the pod would crash-loop through an outage", code)
	}
}

// A check that hangs must not hang the probe. kubelet gives up on its own
// schedule and reads that as a failure anyway, so failing fast is strictly
// better than answering slowly.
func TestAHangingCheckStillAnswers(t *testing.T) {
	srv := serve(t, map[string]health.Check{
		"store": func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		},
	})

	// A client deadline well past the probe's own bound. Without it a failing
	// assertion here leaves the request in flight, and httptest's Close waits
	// for outstanding requests — so a broken timeout would hang the package
	// instead of failing it, which is the least useful way for a test to be
	// right.
	client := &http.Client{Timeout: 20 * time.Second}
	started := time.Now()
	resp, err := client.Get(srv.URL + "/readyz")
	if err != nil {
		t.Fatalf("the readiness probe never answered within %s: %v", time.Since(started), err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("a hanging dependency answered %d", resp.StatusCode)
	}
	if elapsed := time.Since(started); elapsed > 10*time.Second {
		t.Errorf("the probe took %s to give up on a hanging dependency", elapsed)
	}
}
