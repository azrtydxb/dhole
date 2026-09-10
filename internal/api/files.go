package api

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"connectrpc.com/connect"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/tenancy"
)

// FileStore is the content-addressed store the files a definition carries live
// in, narrowed to the two things this package does with it: put the bytes
// somebody uploads, and answer whether the bytes a definition names are
// actually there.
//
// It is narrowed rather than taking cas.Store whole because there is no read
// path here and there must never be a delete one. A file an edit detaches is
// NOT deleted (ADR 0023): it becomes unreferenced, and the collector reclaims
// it when nothing points at it — which is also what makes the inverse of a
// detachment work, because the bytes the inverse names are still there.
//
// *cas.filesystem and the quota-guarded wrapper tenancy.GuardCAS returns both
// satisfy it. A deployment MUST pass the guarded one: uploads are charged
// against the tenant's max_cas_bytes, which is the only ceiling on how large a
// definition may become.
type FileStore interface {
	Put(ctx context.Context, tenantID string, r io.Reader) (*dholev1.Digest, error)
	Has(ctx context.Context, tenantID string, d *dholev1.Digest) (bool, error)
}

// PutDefinitionFile stores the bytes of a file a definition will carry and
// returns the File that names them.
//
// The upload is separate from the edit that declares the file because the two
// are different kinds of thing. The bytes are content-addressed and shared
// between revisions; the declaration is a revision-making operation that has
// to stay small enough to invert. Uploading bytes nothing ends up naming costs
// only the quota they are charged against.
func (s *Server) PutDefinitionFile(
	ctx context.Context, req *connect.Request[dholev1.PutDefinitionFileRequest],
) (*connect.Response[dholev1.PutDefinitionFileResponse], error) {
	p, err := s.principal(ctx, req.Header())
	if err != nil {
		return nil, err
	}
	if s.files == nil {
		// Said rather than faked: a server with no store would otherwise
		// answer with a digest whose bytes nothing holds, and the failure
		// would land on an engine fetching an input minutes later.
		return nil, connect.NewError(connect.CodeUnimplemented,
			errors.New("api: this control plane has no file store configured, "+
				"so a definition cannot carry files"))
	}
	if req.Msg.GetPath() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			errors.New("api: a path is required: it is the name a step binds the file by"))
	}

	digest, err := s.files.Put(ctx, p.TenantID, bytes.NewReader(req.Msg.GetContent()))
	if err != nil {
		if errors.Is(err, tenancy.ErrQuotaExceeded) {
			// The ceiling on a definition's size is the tenant's storage
			// quota and not a constant invented for files (ADR 0023), so the
			// refusal is the quota's own.
			return nil, connect.NewError(connect.CodeResourceExhausted,
				fmt.Errorf("api: store definition file: %w", err))
		}
		return nil, connect.NewError(connect.CodeInternal,
			fmt.Errorf("api: store definition file: %w", err))
	}

	return connect.NewResponse(&dholev1.PutDefinitionFileResponse{
		File: &dholev1.File{
			Path:      req.Msg.GetPath(),
			Digest:    digest,
			SizeBytes: uint64(len(req.Msg.GetContent())),
			MediaType: req.Msg.GetMediaType(),
		},
	}), nil
}

// checkDefinitionFiles refuses a definition whose files do not hold together:
// a file whose bytes nobody uploaded, or a step binding a port to a path the
// definition does not carry.
//
// It runs before the revision is saved, on the way in, because both faults are
// otherwise discovered on an ENGINE — one as an input that cannot be fetched
// and the other as a port with nothing on it — minutes into a run and a long
// way from the edit that caused them.
//
// A server with no file store checks nothing and says so by accepting: it
// cannot serve PutDefinitionFile either, so a definition reaching it with
// files was written against a different plane and refusing it here would be
// this plane deciding about bytes it has no way to see.
func (s *Server) checkDefinitionFiles(ctx context.Context, tenantID string, p *dholev1.Pipeline) error {
	declared := make(map[string]bool, len(p.GetFiles()))
	for _, f := range p.GetFiles() {
		if f.GetPath() == "" {
			return connect.NewError(connect.CodeInvalidArgument,
				errors.New("api: a file the definition carries needs a path"))
		}
		if declared[f.GetPath()] {
			// Two files at one path make the binding ambiguous, and which one
			// a step got would depend on iteration order.
			return connect.NewError(connect.CodeInvalidArgument,
				fmt.Errorf("api: the definition carries two files at %q", f.GetPath()))
		}
		declared[f.GetPath()] = true
		if f.GetDigest().GetHex() == "" {
			return connect.NewError(connect.CodeInvalidArgument,
				fmt.Errorf("api: file %q has no digest: a revision pins the bytes it runs", f.GetPath()))
		}
		if s.files == nil {
			continue
		}
		switch has, err := s.files.Has(ctx, tenantID, f.GetDigest()); {
		case err != nil:
			return connect.NewError(connect.CodeInternal,
				fmt.Errorf("api: checking the bytes of file %q: %w", f.GetPath(), err))
		case !has:
			return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
				"api: file %q names %s:%s, which is not stored for this tenant; "+
					"upload it with PutDefinitionFile first",
				f.GetPath(), f.GetDigest().GetAlgo(), f.GetDigest().GetHex()))
		}
	}

	for _, step := range p.GetSteps() {
		for _, in := range step.GetFileInputs() {
			if !declared[in.GetPath()] {
				return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf(
					"api: step %q reads file %q on port %q, and the definition carries no such file",
					step.GetId(), in.GetPath(), in.GetPort()))
			}
			if findPort(step.GetInputs(), in.GetPort()) == nil {
				return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf(
					"api: step %q binds file %q to input port %q, which it does not declare",
					step.GetId(), in.GetPath(), in.GetPort()))
			}
		}
	}
	return nil
}
