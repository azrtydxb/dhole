package server_test

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The chart could only probe TCP, and said so honestly: the API has no
// unauthenticated call, so a probe could not ask it a question. A listener that
// is up is not a plane that works — one whose store or bus has gone keeps
// passing a TCP probe while failing every request it is given.
//
// A probe carries no credential, so both endpoints must answer without one.
func TestTheProbesAnswerOnTheAPIListenerWithoutACredential(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	srv := startWithAPI(ctx, t)
	client := &http.Client{Timeout: 30 * time.Second}

	for _, path := range []string{"/healthz", "/readyz"} {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+srv.APIAddr()+path, nil)
		require.NoError(t, err)

		resp, err := client.Do(req)
		require.NoError(t, err, "%s is not served on the API listener", path)
		body, _ := io.ReadAll(resp.Body)
		require.NoError(t, resp.Body.Close())

		require.Equal(t, http.StatusOK, resp.StatusCode,
			"%s answered %d on a healthy plane: %s", path, resp.StatusCode, body)
		require.Contains(t, string(body), "ok")
	}
}

// The probes must not have displaced the contract. Mounting anything at the
// root is exactly how an API stops answering on a path nobody re-tested.
func TestMountingTheProbesLeavesEveryAPIPathServed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	srv := startWithAPI(ctx, t)
	client := apiClient(t, srv)
	token := srv.BootstrapToken()

	created := createPipeline(ctx, t, client, token, "still-served-after-probes")
	require.NotNil(t, created)

	// And the SSE half, which lives under /v1/ rather than under Connect.
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		"http://"+srv.APIAddr()+"/v1/runs/run_does_not_exist/events", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := (&http.Client{Timeout: 30 * time.Second}).Do(req)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.NotEqual(t, http.StatusNotImplemented, resp.StatusCode,
		"the SSE endpoint is no longer reached; something else claimed /v1/")
	require.NotContains(t, resp.Header.Get("Content-Type"), "text/html",
		"the web app answered an API path")
}

// An unknown path is the app's, so a deep link opens the run it names. In a
// build with no app it must say that plainly rather than looking like a
// missing API route.
func TestAnUnknownPathBelongsToTheAppRatherThanTheAPI(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	srv := startWithAPI(ctx, t)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		"http://"+srv.APIAddr()+"/runs/run_123", nil)
	require.NoError(t, err)

	resp, err := (&http.Client{Timeout: 30 * time.Second}).Do(req)
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	require.NoError(t, resp.Body.Close())

	if resp.StatusCode == http.StatusNotFound {
		require.Contains(t, strings.ToLower(string(body)), "without the web app",
			"a deep link 404'd without saying the build has no GUI")
		return
	}
	require.Equal(t, http.StatusOK, resp.StatusCode,
		"a deep link answered %d; a shared run link opens a blank page", resp.StatusCode)
}
