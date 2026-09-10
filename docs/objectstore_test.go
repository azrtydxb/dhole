package docs

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// storeConfigSource is the one file that decides how a Dhole process reaches
// the object store. Every environment variable it reads is configuration a
// deployment sets, so every one of them is part of what a third-party engine
// has to be told.
const storeConfigSource = "internal/blobstore/fromenv.go"

// TestEveryObjectStoreVariableTheCodeReadsIsInTheWireContract keeps the store
// protocol from being documented in one place and read in another.
//
// This repository has been bitten twice by exactly that shape: the conformance
// suite and the shipped engine spelled the bus URL, the tier and the slot
// count differently, so `dhole-engine` could not be run through the suite
// built to test engines. The object store was the same drift one layer down —
// docs/writing-an-engine.md described the variables for `dhole-engine` alone,
// docs/wire-contract.md named none of them, and DHOLE_S3_SESSION_TOKEN was
// read by the code and documented nowhere at all.
//
// The list is derived from the source rather than kept beside it, because a
// hand-written list is a second place to remember and forgetting is the whole
// failure being guarded against.
func TestEveryObjectStoreVariableTheCodeReadsIsInTheWireContract(t *testing.T) {
	root := repoRoot(t)
	vars := objectStoreVariables(t, filepath.Join(root, storeConfigSource))
	require.NotEmpty(t, vars,
		"read no DHOLE_* variables out of %s: the test that guards the object "+
			"store's documentation has stopped looking at the code", storeConfigSource)

	contract, err := os.ReadFile(filepath.Join(root, "docs", "wire-contract.md"))
	require.NoError(t, err)

	var missing []string
	for _, name := range vars {
		if !strings.Contains(string(contract), name) {
			missing = append(missing, name)
		}
	}
	require.Empty(t, missing,
		"docs/wire-contract.md is the contract every engine is written against, and "+
			"%s reads these variables it never names:\n  %s",
		storeConfigSource, strings.Join(missing, "\n  "))
}

// TestTheEngineGuideDoesNotRestateTheObjectStoreContract keeps the two
// documents from disagreeing by keeping only one of them normative.
//
// docs/writing-an-engine.md is about running `dhole-engine`; the store
// protocol is about reaching a deployment's store in any language. When the
// guide carried its own table of S3 variables, the contract and the guide were
// two sources for one fact, and the drift was invisible until an engine author
// followed the wrong one.
func TestTheEngineGuideDoesNotRestateTheObjectStoreContract(t *testing.T) {
	root := repoRoot(t)
	guide, err := os.ReadFile(filepath.Join(root, "docs", "writing-an-engine.md"))
	require.NoError(t, err)

	require.Contains(t, string(guide), "wire-contract.md#reaching-the-object-store",
		"the engine guide must point at the contract's object store section rather "+
			"than describing the store itself")

	vars := objectStoreVariables(t, filepath.Join(root, storeConfigSource))
	var restated []string
	for _, name := range vars {
		if strings.Contains(string(guide), name) {
			restated = append(restated, name)
		}
	}
	require.Empty(t, restated,
		"docs/writing-an-engine.md restates object store variables the contract "+
			"already defines, which is a second place for them to drift:\n  %s",
		strings.Join(restated, "\n  "))
}

var envVarPattern = regexp.MustCompile(`^DHOLE_[A-Z0-9_]+$`)

// objectStoreVariables returns every DHOLE_* environment variable named as a
// string literal in the given file, sorted. It reads literals rather than
// resolving os.Getenv calls: a variable that only appears in an error message
// telling an operator to set it is still one an engine author must be told
// about.
func objectStoreVariables(t *testing.T, path string) []string {
	t.Helper()

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, nil, 0)
	require.NoError(t, err, "parsing %s", path)

	seen := map[string]bool{}
	ast.Inspect(file, func(n ast.Node) bool {
		lit, ok := n.(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			return true
		}
		value, err := strconv.Unquote(lit.Value)
		if err != nil {
			return true
		}
		for _, word := range strings.FieldsFunc(value, func(r rune) bool {
			return r != '_' && (r < 'A' || r > 'Z') && (r < '0' || r > '9')
		}) {
			if envVarPattern.MatchString(word) {
				seen[word] = true
			}
		}
		return true
	})

	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
