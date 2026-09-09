package cli

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/azrtydxb/dhole/internal/identity"
	"github.com/azrtydxb/dhole/internal/runstore"
)

// defaultTokenTTL is how long an issued service token lives when nobody said.
// Thirty days: long enough that automation is not re-provisioned weekly, short
// enough that a token leaked into a log stops working while the incident is
// still recent.
const defaultTokenTTL = 30 * 24 * time.Hour

// tokenCmd mints credentials for a plane's own database.
//
// It is a LOCAL ADMINISTRATIVE command, in the same family as `dhole serve`:
// it takes --store-dsn rather than --server, and it runs where the control
// plane's database is. That is deliberate and it is a compromise, so it is
// worth saying which one.
//
// The contract has no identity service. Adding one is not a CLI-shaped
// decision — who may mint a credential, for which tenants, and with which
// scopes is an authorisation model, and inventing it here would be exactly the
// GUI-only-endpoint mistake of ADR 0013 with the CLI in the privileged seat.
// Until that RPC exists, the honest place for token issuance is beside the
// database, where an operator with shell access already has everything anyway.
//
// The bootstrap credential `dhole serve` prints solves a different problem: it
// is what makes a FRESH plane reachable at all. This is the repeatable path —
// a second tenant, a CI account, a replacement for a token somebody leaked.
func tokenCmd(o *options) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "token",
		Short: "issue credentials against a control plane's own database",
		Long: "The API has no unauthenticated call and no identity RPC yet, so a\n" +
			"credential is minted where the database is rather than over the wire.\n" +
			"Run this on the control plane's host, against its --store-dsn.",
		Args: noArgs,
		RunE: func(cmd *cobra.Command, _ []string) error { return cmd.Help() },
	}
	cmd.AddCommand(tokenIssueCmd(o))
	return cmd
}

func tokenIssueCmd(o *options) *cobra.Command {
	var storeDSN, tenant, subject string
	var scopes []string
	var ttl time.Duration
	cmd := &cobra.Command{
		Use:   "issue",
		Short: "mint a service token and print it once",
		Long: "The token is printed here and stored only as a SHA-256, so it cannot be\n" +
			"recovered from the database later — only replaced.",
		Args: noArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if subject == "" {
				return &usageError{cmd: cmd, err: errors.New(
					"--subject is required: a token nobody is attributed to cannot be revoked or audited")}
			}
			if tenant == "" {
				return &usageError{cmd: cmd, err: errors.New(
					"--tenant is required: there is no unscoped credential in this system")}
			}

			ctx, cancel := o.context(cmd)
			defer cancel()

			db, dialect, err := openPlaneDB(ctx, storeDSN)
			if err != nil {
				return err
			}
			defer func() { _ = db.Close() }()

			local := identity.NewLocal(identity.NewSQLStoreWithDialect(db, dialect))
			token, err := local.IssueToken(ctx, identity.Principal{
				TenantID: tenant,
				Subject:  subject,
				Scopes:   scopes,
				Kind:     identity.PrincipalService,
			}, ttl)
			if err != nil {
				return fmt.Errorf("issue token: %w", err)
			}
			_, _ = fmt.Fprintf(o.env.Stdout, "%s\n", token)
			return nil
		},
	}
	flags := cmd.Flags()
	flags.StringVar(&storeDSN, "store-dsn", filepath.Join(defaultStateDir(), "dhole.db"),
		"the plane's Postgres DSN, or a SQLite file path for anything else")
	flags.StringVar(&tenant, "tenant", "default", "the tenant this credential is scoped to")
	flags.StringVar(&subject, "subject", "", "who the token belongs to; recorded on everything it does")
	flags.StringArrayVar(&scopes, "scope", nil, "a scope to grant; repeatable")
	flags.DurationVar(&ttl, "ttl", defaultTokenTTL, "how long the token stays valid")
	return cmd
}

// openPlaneDB opens the plane's database and reports which SQL it speaks. The
// dialect comes from the DSN and not from a build tag: the credential tables
// are the one place where "works only on the development store" locks every
// operator out of the deployment that matters.
func openPlaneDB(ctx context.Context, dsn string) (*sql.DB, runstore.Dialect, error) {
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		db, err := runstore.OpenPostgres(ctx, dsn)
		if err != nil {
			return nil, "", err
		}
		return db, runstore.DialectPostgres, nil
	}
	db, err := runstore.OpenSQLite(dsn)
	if err != nil {
		return nil, "", err
	}
	return db, runstore.DialectSQLite, nil
}
