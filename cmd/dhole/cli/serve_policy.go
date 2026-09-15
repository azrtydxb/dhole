package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/azrtydxb/dhole/internal/policy"
)

// policyReloadInterval is how often a running plane re-reads its policy file.
// A poll rather than a signal, because the file an operator edits in a cluster
// is a mounted ConfigMap, which Kubernetes rewrites in place and signals nobody
// about.
const policyReloadInterval = 10 * time.Second

// loadPolicyFile reads the dispatch policy `dhole serve --policy` names, or
// returns nil — the built-in default — for no path.
//
// The revision the plane installs it under is the author's with a hash of the
// file's content appended. The engine caches compiled programs under the
// revision, so an edited rule reloaded under an unchanged revision string would
// otherwise go on being evaluated from the program compiled for the rule it
// replaced — the one mistake an author editing a policy is certain to make.
func loadPolicyFile(path string) (*policy.TierPolicy, error) {
	if strings.TrimSpace(path) == "" {
		return nil, nil
	}
	raw, err := os.ReadFile(path) //nolint:gosec // the operator names this file
	if err != nil {
		return nil, fmt.Errorf("--policy %s: %w", path, err)
	}
	p, err := parsePolicyFile(path, raw)
	if err != nil {
		return nil, err
	}
	return &p, nil
}

func parsePolicyFile(path string, raw []byte) (policy.TierPolicy, error) {
	doc, err := policy.ParseDocument(raw)
	if err != nil {
		return policy.TierPolicy{}, fmt.Errorf("--policy %s: %w", path, err)
	}
	p, err := doc.TierPolicy()
	if err != nil {
		return policy.TierPolicy{}, fmt.Errorf("--policy %s: %w", path, err)
	}
	sum := sha256.Sum256(raw)
	p.Revision = p.Revision + "@" + hex.EncodeToString(sum[:])[:12]
	return p, nil
}

// policySetter is the running plane's half of a reload.
type policySetter interface {
	SetPolicy(policy.TierPolicy) error
}

// watchPolicyFile re-reads path every interval until ctx ends, and installs it
// whenever its content has changed from what is in force.
//
// A file that cannot be read or cannot work is reported to report and NOT
// installed: the policy in force stays. Refusing start-up is right for a plane
// that has no policy yet; for one that is running, turning a typo in a reload
// into a plane that refuses every step would make editing policy the riskiest
// thing an operator does. The same broken content is reported once, not every
// interval.
func watchPolicyFile(
	ctx context.Context, path string, interval time.Duration, current *policy.TierPolicy,
	plane policySetter, report io.Writer,
) {
	inForce := ""
	if current != nil {
		inForce = current.Revision
	}
	var reported []byte
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		raw, err := os.ReadFile(path) //nolint:gosec // the operator names this file
		if err != nil {
			if reported == nil || !bytes.Equal(reported, []byte(err.Error())) {
				reported = []byte(err.Error())
				_, _ = fmt.Fprintf(report, "dhole: policy reload: %v; the policy in force stays\n", err)
			}
			continue
		}
		p, err := parsePolicyFile(path, raw)
		if err != nil {
			if !bytes.Equal(reported, raw) {
				reported = raw
				_, _ = fmt.Fprintf(report, "dhole: policy reload: %v; the policy in force stays\n", err)
			}
			continue
		}
		if p.Revision == inForce {
			reported = nil
			continue
		}
		if err := plane.SetPolicy(p); err != nil {
			_, _ = fmt.Fprintf(report, "dhole: policy reload: %v; the policy in force stays\n", err)
			continue
		}
		inForce, reported = p.Revision, nil
		_, _ = fmt.Fprintf(report, "dhole: dispatch policy reloaded from %s (revision %s)\n", path, p.Revision)
	}
}
