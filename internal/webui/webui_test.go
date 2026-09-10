package webui_test

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/azrtydxb/dhole/internal/webui"
)

// A checkout with no `make web-build` behind it has no GUI, and that must be a
// clean absence rather than a compile error or a handler serving nothing. The
// Go build cannot be made to depend on a working npm install.
func TestABuildWithoutTheAppSaysSoRatherThanServingNothing(t *testing.T) {
	h, ok := webui.Handler()
	if !ok {
		if h != nil {
			t.Error("Handler reported no app but returned one anyway")
		}
		return
	}
	// The app IS embedded (someone ran make web-build before the test), so
	// the rest of this file's properties are the ones that matter.
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("the embedded app does not serve its own index: %d", rec.Code)
	}
}

// The property a shared run link depends on. The app routes client-side, so
// the server is asked for /runs/run_123 — a path no file has — and a 404 there
// makes every link anyone sends a colleague open a blank page.
func TestADeepLinkFallsBackToTheApp(t *testing.T) {
	h := handlerOrSkip(t)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/runs/run_123", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("a client-side route answered %d; a shared run link opens a blank page", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct == "" {
		t.Error("the fallback served no content type")
	}
}

// The exception, and the reason the fallback cannot be unconditional. A hashed
// bundle that is missing means the deployment is broken; answering it with the
// HTML page turns a clear 404 into "unexpected token '<'" in a console.
func TestAMissingAssetIsA404AndNotTheAppItself(t *testing.T) {
	h := handlerOrSkip(t)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/assets/index-DOESNOTEXIST.js", nil))

	if rec.Code != http.StatusNotFound {
		t.Errorf("a missing asset answered %d; a broken deployment must not look like a routing quirk", rec.Code)
	}
}

func handlerOrSkip(t *testing.T) http.Handler {
	t.Helper()
	h, ok := webui.Handler()
	if !ok {
		t.Skip("no web app is embedded: run `make web-build` to exercise this")
	}
	return h
}

// The embed directive covers `all:dist`, which is what makes Vite's output
// reachable — the default embed pattern skips files beginning with a dot or an
// underscore, and a build that emits one would be embedded with a hole in it
// that only shows up in a browser.
func TestTheEmbedDirectiveDoesNotSkipDotFiles(t *testing.T) {
	src, err := os.ReadFile(filepath.Join(".", "webui.go"))
	if err != nil {
		t.Fatal(err)
	}
	if !contains(string(src), "go:embed all:dist") {
		t.Error("the embed directive is not `all:dist`, so parts of a build would be silently omitted")
	}
}

func contains(haystack, needle string) bool {
	return len(haystack) >= len(needle) && (func() bool {
		for i := 0; i+len(needle) <= len(haystack); i++ {
			if haystack[i:i+len(needle)] == needle {
				return true
			}
		}
		return false
	})()
}
