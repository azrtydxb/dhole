package policy_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/azrtydxb/dhole/internal/policy"
	"github.com/azrtydxb/dhole/internal/runstore"
)

// uniqueTenant keeps every dialect run in its own tenant: SQLite gets a fresh
// file per test, but Postgres is shared and persistent, so an assertion that
// only holds because an earlier run's rows were absent is not an assertion.
var (
	tenantSeq atomic.Uint64
	tenantRun = strconv.FormatInt(time.Now().UnixNano(), 36)
)

func uniqueTenant(t *testing.T) string {
	t.Helper()
	name := strings.Map(func(r rune) rune {
		if r == '/' || r == ' ' {
			return '-'
		}
		return r
	}, t.Name())
	return fmt.Sprintf("%s-%s-%d", name, tenantRun, tenantSeq.Add(1))
}

// postgresDB opens a migrated Postgres handle, or skips with a reason. An
// integration test that silently degrades to nothing reports green while
// testing nothing.
func postgresDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("DHOLE_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("DHOLE_TEST_POSTGRES_DSN not set: this test needs a live Postgres")
	}
	db, err := runstore.OpenPostgres(context.Background(), dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// TestSQLiteSatisfiesAuditContract and its Postgres twin hold both dialects to
// one contract. ADR 0012 asks for a single trail behind a single evaluation
// point; a trail that only records on the development store is not one.
func TestSQLiteSatisfiesAuditContract(t *testing.T) {
	auditContract(t, policy.NewSQLAudit(openDB(t)))
}

func TestPostgresSatisfiesAuditContract(t *testing.T) {
	auditContract(t, policy.NewSQLAuditWithDialect(postgresDB(t), runstore.DialectPostgres))
}

// auditContract is the behaviour the audit trail must show on both dialects.
func auditContract(t *testing.T, audit *policy.SQLAudit) {
	t.Helper()

	t.Run("RecordsAllowsAsFaithfullyAsDenials", func(t *testing.T) {
		ctx := context.Background()
		scope := uniqueTenant(t)
		at := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)

		require.NoError(t, audit.Record(ctx, policy.AuditRecord{
			TenantID: scope, Tier: "production", Subject: "pipe-1",
			Rule: "no-privileged", Reason: "denied", PluginRef: "oci://example/a",
			Allow: false, At: at,
		}))
		require.NoError(t, audit.Record(ctx, policy.AuditRecord{
			TenantID: scope, Tier: "production", Subject: "pipe-2",
			Rule: "no-privileged", Reason: "allowed", PluginRef: "oci://example/b",
			Allow: true, At: at.Add(time.Second),
		}))

		rows, err := audit.Records(ctx, scope, 10)
		require.NoError(t, err)
		require.Len(t, rows, 2)

		require.Equal(t, "pipe-1", rows[0].Subject)
		require.False(t, rows[0].Allow, "the boolean must survive the round trip")
		require.Equal(t, "no-privileged", rows[0].Rule)
		require.Equal(t, "denied", rows[0].Reason)
		require.Equal(t, "oci://example/a", rows[0].PluginRef)
		require.Equal(t, "production", rows[0].Tier)
		require.True(t, at.Equal(rows[0].At))

		require.Equal(t, "pipe-2", rows[1].Subject)
		require.True(t, rows[1].Allow, "an allow is recorded as faithfully as a denial")
	})

	t.Run("RecordsAreScopedToTheirTenantAndCapped", func(t *testing.T) {
		ctx := context.Background()
		mine := uniqueTenant(t)
		theirs := uniqueTenant(t)
		at := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)

		for i := range 3 {
			require.NoError(t, audit.Record(ctx, policy.AuditRecord{
				TenantID: mine, Tier: "production", Subject: fmt.Sprintf("pipe-%d", i),
				Rule: "r", Reason: "why", Allow: true, At: at.Add(time.Duration(i) * time.Second),
			}))
		}
		require.NoError(t, audit.Record(ctx, policy.AuditRecord{
			TenantID: theirs, Tier: "production", Subject: "not-mine",
			Rule: "r", Reason: "why", Allow: true, At: at,
		}))

		rows, err := audit.Records(ctx, mine, 2)
		require.NoError(t, err)
		require.Len(t, rows, 2, "the limit must reach the database, not be ignored")
		require.Equal(t, "pipe-0", rows[0].Subject, "oldest first")
		require.Equal(t, "pipe-1", rows[1].Subject)

		rows, err = audit.Records(ctx, theirs, 10)
		require.NoError(t, err)
		require.Len(t, rows, 1, "one tenant's trail must never carry another's decisions")
		require.Equal(t, "not-mine", rows[0].Subject)
	})

	t.Run("AnUnscopedRecordIsRefused", func(t *testing.T) {
		ctx := context.Background()
		require.ErrorIs(t, audit.Record(ctx, policy.AuditRecord{Subject: "pipe-1"}), policy.ErrTenantRequired)
		_, err := audit.Records(ctx, "", 10)
		require.ErrorIs(t, err, policy.ErrTenantRequired)
	})
}
