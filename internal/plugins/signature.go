package plugins

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/runstore"
)

// signatureTimeFormat matches the run store and the catalog: SQLite has no
// time type, and RFC3339 with nanoseconds in UTC sorts lexicographically in
// chronological order.
const signatureTimeFormat = time.RFC3339Nano

// Source is where a signature record came from, and therefore how its payload
// is checked.
//
// It is stored verbatim, so the values are a persistence contract: add new
// ones, never rename an existing one.
type Source string

const (
	// SourceCosign is a cosign bundle, re-verified from its certificate on
	// every read. See cosign.go for exactly what that does and does not check.
	SourceCosign Source = "cosign"

	// SourceManual is an operator's own attestation, for an artifact signed
	// out of band. Its payload is recorded and shown; nothing cryptographic is
	// checked, because there is nothing here to check it against. Trusting one
	// is trusting whoever could write the row.
	SourceManual Source = "manual"
)

// Signature is one detached statement that a principal vouched for some bytes.
//
// Identity and Issuer together name the principal — the same identity string
// from a different issuer is a different principal — and Payload is the
// evidence, not a summary of it.
type Signature struct {
	// Identity is the signer, exactly as the certificate asserted it.
	Identity string
	// Issuer is the OIDC issuer that asserted Identity.
	Issuer string
	// Payload is the cosign bundle, or the operator's attestation document.
	Payload []byte
	// Source says how Payload is to be checked.
	Source Source
}

// ErrVerificationFailed is the single refusal every denial reaches the caller
// as: no record, no allowed signer, a disallowed one, a broken payload, an
// unknown source, or a store that could not answer. Callers match it with
// errors.Is; the message says which. One sentinel rather than several because
// a caller must not be able to accidentally treat one flavour of denial as
// benign.
var ErrVerificationFailed = errors.New("plugins: signature verification failed")

// Signatures is the durable, tenant-scoped store of detached signature records
// and the verification built on it.
//
// Records are keyed on the artifact DIGEST rather than on a reference, which
// is what makes verification identical for an `oci://` and a `cas://`
// artifact and what stops a moved tag from carrying a signature over to
// different content.
//
// Nothing stored here is authorisation. A record says who signed; the allowed
// list passed to Verify says whose word this tenant accepts.
type Signatures interface {
	// Record stores a signature for d. A cosign signature is verified before
	// it is stored, and re-recording the same signer's attestation for the
	// same digest is idempotent.
	Record(ctx context.Context, tenantID string, d *dholev1.Digest, sig Signature) error

	// Verify reports whether this tenant holds a signature for d from one of
	// the allowed principals. It FAILS CLOSED: an empty allowed list allows
	// nothing, and a storage error, a malformed payload or an unknown source
	// all deny.
	//
	// Each allowed entry names a principal as "<issuer> <identity>". An entry
	// naming only an identity is refused, because it would admit that identity
	// from any issuer.
	Verify(ctx context.Context, tenantID string, d *dholev1.Digest, allowed []string) error

	// Remove withdraws every signature this tenant holds for d. This is the
	// revocation path: after a key compromise or a bad release, the artifact
	// must stop dispatching immediately.
	Remove(ctx context.Context, tenantID string, d *dholev1.Digest) error

	// Close releases the store's resources.
	Close() error
}

// sqlSignatures is the signature store over the same database as the run event
// log, exactly as the catalog and the cache are: the DDL is embedded with the
// run store's and applied by the same migration runner, so the schema has one
// definition.
type sqlSignatures struct {
	db      *sql.DB
	dialect runstore.Dialect
	// ownsDB is true only when this store opened the handle itself, which is
	// what makes Close safe: a store handed a shared handle must not close the
	// run store's database out from under it.
	ownsDB bool
}

var _ Signatures = (*sqlSignatures)(nil)

// NewSignatures returns the signature store over an already-open handle
// speaking dialect. The handle is the caller's, and its schema is expected to
// carry the migrations the run store applies.
func NewSignatures(db *sql.DB, dialect runstore.Dialect) Signatures {
	return &sqlSignatures{db: db, dialect: dialect}
}

// NewSQLiteSignatures is the single-file convenience: it opens the SQLite
// database at path, applies the embedded migrations, and hands back a store
// that owns and closes that handle. It is a shorthand for NewSignatures over
// runstore.OpenSQLite, not the only way in — a Postgres deployment passes its
// own handle to NewSignatures.
func NewSQLiteSignatures(path string) (Signatures, error) {
	db, err := runstore.OpenSQLite(path)
	if err != nil {
		return nil, fmt.Errorf("plugins: open sqlite: %w", err)
	}
	return &sqlSignatures{db: db, dialect: runstore.DialectSQLite, ownsDB: true}, nil
}

// Close releases the database handle, and only if this store opened it. A
// handle passed to NewSignatures belongs to whoever opened it.
func (s *sqlSignatures) Close() error {
	if !s.ownsDB {
		return nil
	}
	return s.db.Close()
}

// Record validates the evidence before storing it.
func (s *sqlSignatures) Record(ctx context.Context, tenantID string, d *dholev1.Digest, sig Signature) error {
	if err := requireTenant(tenantID); err != nil {
		return err
	}
	text, err := digestText(d)
	if err != nil {
		return err
	}
	if err := sig.validate(); err != nil {
		return err
	}
	// A cosign record's identity and issuer must be the ones its own
	// certificate asserts. Storing a caller-supplied pair beside somebody
	// else's bundle would file a genuine signature under a name of the
	// caller's choosing.
	if sig.Source == SourceCosign {
		identity, issuer, err := verifyCosignBundle(d, sig.Payload)
		if err != nil {
			return err
		}
		if identity != sig.Identity || issuer != sig.Issuer {
			return fmt.Errorf("%w: bundle asserts %q from %q, not %q from %q",
				ErrMalformedSignature, identity, issuer, sig.Identity, sig.Issuer)
		}
	}

	const q = `INSERT INTO artifact_signatures
		(tenant_id, digest, identity, issuer, payload, source, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (tenant_id, digest, identity, issuer)
		DO UPDATE SET payload = excluded.payload, source = excluded.source`
	if _, err := s.db.ExecContext(ctx, s.dialect.Rebind(q),
		tenantID, text, sig.Identity, sig.Issuer, sig.Payload, string(sig.Source),
		time.Now().UTC().Format(signatureTimeFormat),
	); err != nil {
		return fmt.Errorf("plugins: record signature for %s: %w", text, err)
	}
	return nil
}

// Remove withdraws every record this tenant holds for the digest.
func (s *sqlSignatures) Remove(ctx context.Context, tenantID string, d *dholev1.Digest) error {
	if err := requireTenant(tenantID); err != nil {
		return err
	}
	text, err := digestText(d)
	if err != nil {
		return err
	}
	const q = `DELETE FROM artifact_signatures WHERE tenant_id = ? AND digest = ?`
	if _, err := s.db.ExecContext(ctx, s.dialect.Rebind(q), tenantID, text); err != nil {
		return fmt.Errorf("plugins: remove signatures for %s: %w", text, err)
	}
	return nil
}

// Verify answers the only question dispatch may ask: does this tenant hold a
// signature over these exact bytes from a principal it named in advance?
//
// Every path out of this function that is not an explicit match is a denial.
// That is the whole design: this check only ever runs on the way to executing
// somebody else's code, so "I could not tell" and "no" must be the same
// answer.
func (s *sqlSignatures) Verify(ctx context.Context, tenantID string, d *dholev1.Digest, allowed []string) error {
	if err := requireTenant(tenantID); err != nil {
		return err
	}
	text, err := digestText(d)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrVerificationFailed, err)
	}

	principals, err := parseAllowed(allowed)
	if err != nil {
		return err
	}

	const q = `SELECT identity, issuer, payload, source
		FROM artifact_signatures WHERE tenant_id = ? AND digest = ?`
	rows, err := s.db.QueryContext(ctx, s.dialect.Rebind(q), tenantID, text)
	if err != nil {
		// A store that cannot answer has not said the artifact is signed; it
		// has said nothing. Returning nil here would turn a database outage
		// into a global authorisation bypass.
		return fmt.Errorf("%w: cannot read signature records for %s: %w", ErrVerificationFailed, text, err)
	}
	defer func() { _ = rows.Close() }()

	var (
		seen     int
		rejected []string
	)
	for rows.Next() {
		var (
			identity, issuer, source string
			payload                  []byte
		)
		if err := rows.Scan(&identity, &issuer, &payload, &source); err != nil {
			return fmt.Errorf("%w: cannot read signature records for %s: %w", ErrVerificationFailed, text, err)
		}
		seen++

		// The allowed check first, because it is the cheap one and because a
		// record from an unnamed principal is not evidence of anything
		// regardless of how well formed it is.
		if !principals[principal{issuer: issuer, identity: identity}] {
			rejected = append(rejected, fmt.Sprintf("%q from %q is not an allowed signer", identity, issuer))
			continue
		}
		if err := checkEvidence(d, Signature{
			Identity: identity, Issuer: issuer, Payload: payload, Source: Source(source),
		}); err != nil {
			rejected = append(rejected, err.Error())
			continue
		}
		return nil
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("%w: cannot read signature records for %s: %w", ErrVerificationFailed, text, err)
	}

	if seen == 0 {
		return fmt.Errorf("%w: no signature is recorded for %s", ErrVerificationFailed, text)
	}
	return fmt.Errorf("%w: %d recorded signature(s) for %s, none accepted: %s",
		ErrVerificationFailed, seen, text, strings.Join(rejected, "; "))
}

// checkEvidence re-checks a stored record's payload against the record's own
// columns. The columns are an index, not the evidence: a cosign bundle is
// re-verified on every read so that anyone who can write a row still cannot
// grant themselves an identity.
func checkEvidence(d *dholev1.Digest, sig Signature) error {
	switch sig.Source {
	case SourceCosign:
		identity, issuer, err := verifyCosignBundle(d, sig.Payload)
		if err != nil {
			return err
		}
		if identity != sig.Identity || issuer != sig.Issuer {
			return fmt.Errorf("%w: stored record claims %q from %q, its bundle asserts %q from %q",
				ErrMalformedSignature, sig.Identity, sig.Issuer, identity, issuer)
		}
		return nil
	case SourceManual:
		if len(sig.Payload) == 0 {
			return fmt.Errorf("%w: manual record for %q carries no attestation", ErrMalformedSignature, sig.Identity)
		}
		return nil
	default:
		// A source this build does not understand is a record whose payload it
		// cannot check. "I do not know how to verify this" is not "verified":
		// the alternative turns a forward-compatibility shortcut into a
		// bypass anyone who can write a row can use.
		return fmt.Errorf("%w: unknown signature source %q, which this build cannot check",
			ErrMalformedSignature, sig.Source)
	}
}

// validate refuses a record that could never be checked.
func (sig Signature) validate() error {
	if strings.TrimSpace(sig.Identity) == "" {
		return fmt.Errorf("%w: a signature must name a signer identity", ErrMalformedSignature)
	}
	// Half a principal is not a principal: without an issuer the record can
	// only ever be compared on the identity, which is exactly the comparison
	// the issuer exists to prevent.
	if strings.TrimSpace(sig.Issuer) == "" {
		return fmt.Errorf("%w: a signature must name the issuer that asserted %q", ErrMalformedSignature, sig.Identity)
	}
	if len(sig.Payload) == 0 {
		return fmt.Errorf("%w: a signature must carry its evidence", ErrMalformedSignature)
	}
	switch sig.Source {
	case SourceCosign, SourceManual:
		return nil
	default:
		return fmt.Errorf("%w: unknown signature source %q, expected %q or %q",
			ErrMalformedSignature, sig.Source, SourceCosign, SourceManual)
	}
}

// principal is the pair that identifies a signer. Both halves are the key
// because the same identity string from a different issuer is a different
// principal.
type principal struct {
	issuer   string
	identity string
}

// parseAllowed turns the tier's allowed-signer list into a set.
//
// An entry is "<issuer> <identity>". Both halves are required: an entry naming
// only an identity would admit it from ANY issuer, so anyone able to make any
// issuer assert that address could sign as it.
//
// An empty list allows NOTHING. That is the direction this has to fail in: an
// empty list is the shape a misconfigured tier arrives in — a missing policy
// key, a nil after unmarshalling — and reading it as "no restriction" makes
// the one deployment that forgot to configure trust the one that trusts
// everybody.
func parseAllowed(allowed []string) (map[principal]bool, error) {
	if len(allowed) == 0 {
		return nil, fmt.Errorf("%w: no allowed signer identities are configured, so nothing is trusted", ErrVerificationFailed)
	}
	out := make(map[principal]bool, len(allowed))
	for _, entry := range allowed {
		fields := strings.Fields(entry)
		// A malformed entry denies rather than being skipped: an entry an
		// operator believes is in force, silently ignored, is worse than a
		// refusal they can see.
		if len(fields) != 2 {
			return nil, fmt.Errorf(`%w: malformed allowed signer entry %q, expected "<issuer> <identity>"`,
				ErrVerificationFailed, entry)
		}
		out[principal{issuer: fields[0], identity: fields[1]}] = true
	}
	return out, nil
}

// digestText renders a digest in the "<algo>:<hex>" form every table stores.
// A nil or half-populated digest is refused rather than rendered as ":" — a
// key that would match every other half-populated digest.
func digestText(d *dholev1.Digest) (string, error) {
	if d == nil || d.GetAlgo() == "" || d.GetHex() == "" {
		return "", fmt.Errorf("%w: a signature is keyed on a complete digest", ErrMalformedSignature)
	}
	return d.GetAlgo() + ":" + d.GetHex(), nil
}

// TrustMarker is how a failed verification reaches the layer that decides what
// a tenant may run. A refusal that only the caller sees is a refusal the next
// caller repeats from scratch; marking the catalog entry untrusted is what
// makes it durable and visible.
type TrustMarker interface {
	// MarkUntrusted records that the artifact with this digest must no longer
	// be treated as trusted for this tenant, with the reason it failed.
	MarkUntrusted(ctx context.Context, tenantID string, d *dholev1.Digest, reason string) error
}

// Dispatcher fetches an artifact only after re-verifying its signature.
//
// The property it exists for: a successful resolution is NOT standing evidence
// of trust. Task 32 pins a reference to a digest exactly once and everything
// afterwards routes on that recorded digest, which makes it tempting to treat
// the pinned artifact as vetted. It is not — resolution is evidence about
// where the bytes are, not about who vouched for them. A signature can be
// withdrawn after a key compromise, and the artifact that dispatched an hour
// ago must stop dispatching now. Trust is re-checked, never remembered.
type Dispatcher struct {
	resolver Resolver
	sigs     Signatures
	allowed  []string
	marker   TrustMarker
}

// DispatchOption configures a Dispatcher.
type DispatchOption func(*Dispatcher)

// WithTrustMarker routes verification failures to the catalog, so a plugin
// that stops verifying is marked untrusted rather than merely refused here.
func WithTrustMarker(m TrustMarker) DispatchOption {
	return func(d *Dispatcher) { d.marker = m }
}

// NewDispatcher returns a Dispatcher that admits only artifacts signed by one
// of the allowed principals, each written as "<issuer> <identity>".
func NewDispatcher(r Resolver, sigs Signatures, allowed []string, opts ...DispatchOption) *Dispatcher {
	d := &Dispatcher{resolver: r, sigs: sigs, allowed: allowed}
	for _, opt := range opts {
		opt(d)
	}
	return d
}

// Dispatch verifies the artifact's signature and then opens its bytes. The
// verification happens on every call, against the artifact's digest — never
// against a cached verdict and never against its reference.
func (d *Dispatcher) Dispatch(ctx context.Context, tenantID string, a Artifact) (io.ReadCloser, error) {
	if err := requireTenant(tenantID); err != nil {
		return nil, err
	}
	if err := d.sigs.Verify(ctx, tenantID, a.Digest, d.allowed); err != nil {
		if d.marker != nil {
			if markErr := d.marker.MarkUntrusted(ctx, tenantID, a.Digest, err.Error()); markErr != nil {
				return nil, errors.Join(fmt.Errorf("plugins: refusing to dispatch %q: %w", a.Ref, err), markErr)
			}
		}
		return nil, fmt.Errorf("plugins: refusing to dispatch %q: %w", a.Ref, err)
	}
	return d.resolver.Fetch(ctx, tenantID, a)
}
