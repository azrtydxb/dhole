// Package webui serves the built web app from the control plane's own binary.
//
// Same origin, deliberately. The GUI speaks to the API over Connect and holds
// two SSE streams open, and every one of those is a cross-origin request when
// the app is served from somewhere else — so a separately hosted GUI does not
// work at all until someone sets controlPlane.allowedOrigins, and silently
// fails in the browser until they realise that is what is wrong. Serving it
// from the plane means one origin, one port, one ingress, and no CORS.
//
// It is embedded rather than mounted from disk because the deployment is a
// distroless image with no web server in it and no volume to put a build on.
package webui

import (
	"embed"
	"io/fs"
	"net/http"
	"strings"
)

// dist holds the built app. It is EMPTY in a normal checkout: `make web-build`
// copies web/dist into it, and a binary built without that step simply has no
// GUI rather than failing to compile. That is the honest arrangement — the Go
// build must not depend on a working npm install.
//
//go:embed all:dist
var dist embed.FS

// Handler returns the app, and whether there is one to serve.
//
// The false case is not an error. A control plane built without the GUI is a
// perfectly good control plane: the CLI and every agent use the same API, and
// ADR 0013's whole point is that the GUI is one client among them.
func Handler() (http.Handler, bool) {
	sub, err := fs.Sub(dist, "dist")
	if err != nil {
		return nil, false
	}
	if _, err := fs.Stat(sub, "index.html"); err != nil {
		return nil, false
	}
	return &spa{files: http.FileServer(http.FS(sub)), fsys: sub}, true
}

// spa serves the built files, falling back to index.html.
//
// The fallback is what makes a deep link work. The app routes client-side, so
// a browser asked to open /runs/run_123 requests exactly that path from the
// server, which has no such file — and a 404 there means a shared link to a
// run opens a blank page, which is the one thing people do with a run URL.
type spa struct {
	files http.Handler
	fsys  fs.FS
}

func (s *spa) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "the web app serves GET and HEAD", http.StatusMethodNotAllowed)
		return
	}

	name := strings.TrimPrefix(r.URL.Path, "/")
	if name == "" {
		s.files.ServeHTTP(w, r)
		return
	}
	// A real file wins. Anything else is a client-side route.
	if _, err := fs.Stat(s.fsys, name); err == nil {
		s.files.ServeHTTP(w, r)
		return
	}
	// Except an asset: a missing hashed bundle is a broken deployment, and
	// answering it with the HTML page turns that into a console error about
	// unexpected token '<' rather than the 404 it is.
	if strings.HasPrefix(name, "assets/") {
		http.NotFound(w, r)
		return
	}
	r2 := r.Clone(r.Context())
	r2.URL.Path = "/"
	s.files.ServeHTTP(w, r2)
}
