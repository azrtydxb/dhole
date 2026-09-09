// Package docs holds the documentation, and the tests that keep it true.
//
// Documentation that is wrong is worse than documentation that is missing,
// because somebody acts on it. So the two tests here are not about prose
// style: one derives the list of things that MUST have a page from the code
// itself, and the other runs the quickstart.
//
// TestEveryStepTypeAndTriggerIsDocumented deliberately reads the source tree
// rather than a list kept beside it. A hand-written list is a second place to
// remember, and the failure mode of forgetting is exactly the one this test
// exists to catch: a step type added next month, shipped, and undocumented.
// Discovery is by parsing, not by importing, because a package that cannot be
// linked into a test binary — a build-tagged backend, one that needs cgo —
// still ships and still needs its page.
package docs

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// extensionPoint is one family of pluggable thing that a reader has to be able
// to look up: where its implementations live, and where their pages go.
type extensionPoint struct {
	// what a reader calls one of these, used in every failure message.
	what string
	// srcDir is the directory whose subdirectories are the implementations,
	// relative to the repository root.
	srcDir string
	// docsDir is the directory under docs/ that must hold one page each.
	docsDir string
	// byKindMethod restricts discovery to packages that actually implement
	// the extension point's interface — the ones declaring `Kind() string`.
	// Steps have no such interface: every package under internal/steps is a
	// step type, so it is false there and the directory name is the identity.
	byKindMethod bool
}

var extensionPoints = []extensionPoint{
	{what: "step type", srcDir: "internal/steps", docsDir: "steps"},
	{what: "trigger kind", srcDir: "internal/trigger", docsDir: "triggers", byKindMethod: true},
	{what: "executor kind", srcDir: "internal/executor", docsDir: "executors", byKindMethod: true},
}

func TestEveryStepTypeAndTriggerIsDocumented(t *testing.T) {
	root := repoRoot(t)

	for _, point := range extensionPoints {
		t.Run(point.docsDir, func(t *testing.T) {
			names := discover(t, root, point)
			require.NotEmpty(t, names,
				"discovered no %s under %s: the test that guards the documentation "+
					"has stopped looking at the code", point.what, point.srcDir)

			var undocumented []string
			for _, name := range names {
				page := filepath.Join(root, "docs", point.docsDir, name+".md")
				body, err := os.ReadFile(page) //nolint:gosec // the path is derived from the source tree
				switch {
				case err != nil:
					undocumented = append(undocumented,
						fmt.Sprintf("%s %q: %v", point.what, name, err))
				case !strings.Contains(string(body), name):
					undocumented = append(undocumented, fmt.Sprintf(
						"%s %q: docs/%s/%s.md never names it",
						point.what, name, point.docsDir, name))
				}
			}
			require.Empty(t, undocumented, "undocumented %ss:\n  %s",
				point.what, strings.Join(undocumented, "\n  "))
		})
	}
}

// discover reads the source tree and returns the names one page each is owed.
//
// The name is the one the CODE uses, not the directory: a trigger's or an
// executor's `Kind` constant is what an operator writes in configuration, and
// a builtin step type's `PluginRef` is what a pipeline carries. A package that
// implements the interface but hides its kind behind something this cannot
// read fails here rather than being skipped — silently skipping is how a
// backend ends up undocumented.
func discover(t *testing.T, root string, point extensionPoint) []string {
	t.Helper()

	dir := filepath.Join(root, point.srcDir)
	entries, err := os.ReadDir(dir)
	require.NoError(t, err, "reading %s", point.srcDir)

	var names []string
	for _, entry := range entries {
		if !entry.IsDir() || strings.HasSuffix(entry.Name(), "test") {
			continue
		}
		pkg := parsePackage(t, filepath.Join(dir, entry.Name()))
		if pkg == nil {
			continue
		}
		if point.byKindMethod {
			if !declaresKindMethod(pkg) {
				continue
			}
			kind, ok := stringConst(pkg, "Kind")
			require.True(t, ok, "%s/%s implements Kind() string but declares no "+
				"exported `const Kind = \"...\"`; the documentation gate cannot name it",
				point.srcDir, entry.Name())
			names = append(names, kind)
			continue
		}
		if ref, ok := stringConst(pkg, "PluginRef"); ok {
			names = append(names, strings.TrimPrefix(ref, "builtin:"))
			continue
		}
		names = append(names, entry.Name())
	}
	sort.Strings(names)
	return names
}

// parsePackage parses the non-test Go files of one directory. It returns nil
// for a directory that holds none.
func parsePackage(t *testing.T, dir string) *ast.Package { //nolint:staticcheck // ast.Package is the shape this needs
	t.Helper()

	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, dir, func(fi os.FileInfo) bool { //nolint:staticcheck // ParseDir is exactly the job
		return !strings.HasSuffix(fi.Name(), "_test.go")
	}, 0)
	require.NoError(t, err, "parsing %s", dir)
	for _, pkg := range pkgs {
		return pkg
	}
	return nil
}

// declaresKindMethod reports whether the package declares a method
// `Kind() string` — the signature the Trigger and Executor interfaces require.
func declaresKindMethod(pkg *ast.Package) bool { //nolint:staticcheck // see parsePackage
	found := false
	forEachDecl(pkg, func(decl ast.Decl) {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Recv == nil || fn.Name.Name != "Kind" {
			return
		}
		results := fn.Type.Results
		if results == nil || len(results.List) != 1 {
			return
		}
		if ident, ok := results.List[0].Type.(*ast.Ident); ok && ident.Name == "string" {
			found = true
		}
	})
	return found
}

// stringConst returns the value of a package-level untyped string constant.
func stringConst(pkg *ast.Package, name string) (string, bool) { //nolint:staticcheck // see parsePackage
	value, found := "", false
	forEachDecl(pkg, func(decl ast.Decl) {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.CONST {
			return
		}
		for _, spec := range gen.Specs {
			vs, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}
			for i, ident := range vs.Names {
				if ident.Name != name || i >= len(vs.Values) {
					continue
				}
				lit, ok := vs.Values[i].(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					continue
				}
				unquoted, err := strconv.Unquote(lit.Value)
				if err != nil {
					continue
				}
				value, found = unquoted, true
			}
		}
	})
	return value, found
}

func forEachDecl(pkg *ast.Package, fn func(ast.Decl)) { //nolint:staticcheck // see parsePackage
	files := make([]string, 0, len(pkg.Files))
	for name := range pkg.Files {
		files = append(files, name)
	}
	sort.Strings(files)
	for _, name := range files {
		for _, decl := range pkg.Files[name].Decls {
			fn(decl)
		}
	}
}

// repoRoot is the directory holding go.mod: this package sits one level under
// it, and every path here is resolved from it so the tests do not depend on
// where `go test` was invoked from.
func repoRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs("..")
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(root, "go.mod"))
	require.NoError(t, err, "docs tests must run from the docs/ directory of a checkout")
	return root
}
