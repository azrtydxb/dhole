package api

import (
	"context"
	"errors"
	"fmt"

	"connectrpc.com/connect"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/catalog"
)

// GetPlugin returns what one published plugin declares.
//
// It exists because "what does this plugin take?" had no answer on the
// contract. The properties panel therefore rendered the schema the DEFINITION
// carried inline on a step's structured input port — a copy of the plugin's
// declaration, made whenever the step was authored and free to be stale, and
// absent entirely for a step whose ports carry no schema. Plugin schemas drive
// the panel, request validation, agent tool discovery and editor autocomplete
// from ONE source (ADR 0012), and a source only the control plane can read is
// not that.
func (s *Server) GetPlugin(
	ctx context.Context, req *connect.Request[dholev1.GetPluginRequest],
) (*connect.Response[dholev1.GetPluginResponse], error) {
	p, err := s.principal(ctx, req.Header())
	if err != nil {
		return nil, err
	}
	ref := req.Msg.GetPluginRef()
	if ref == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("api: a plugin_ref is required"))
	}
	if s.cat == nil {
		return nil, connect.NewError(connect.CodeUnimplemented,
			errors.New("api: this server was built without a catalog"))
	}

	entry, err := s.cat.Resolve(ctx, p.TenantID, ref)
	switch {
	case errors.Is(err, catalog.ErrMalformedRef):
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("api: %w", err))
	case errors.Is(err, catalog.ErrNotFound):
		// Another tenant's plugin is indistinguishable from one that was
		// never published, exactly as another tenant's revision is.
		return nil, connect.NewError(connect.CodeNotFound,
			fmt.Errorf("api: no plugin %q is published", ref))
	case err != nil:
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("api: resolve plugin: %w", err))
	}
	return connect.NewResponse(&dholev1.GetPluginResponse{Plugin: wirePlugin(entry.Manifest)}), nil
}

// PublishPlugin records a type's declaration in the caller's catalog.
//
// It exists because nothing published. catalog.Publish had no caller outside
// tests — not the API, not the CLI, not the git mirror — so a declaration
// could be read through GetPlugin and the only way to put one there was to
// open the control plane's own database, which is a capability the GUI and an
// agent cannot have (ADR 0013). The Playwright suite's seeder grew a /plugin
// route for exactly that reason.
//
// The tenant is the credential's. A caller says what it publishes and never
// whose catalog it lands in, so a publish cannot put a declaration in front of
// another tenant's steps.
func (s *Server) PublishPlugin(
	ctx context.Context, req *connect.Request[dholev1.PublishPluginRequest],
) (*connect.Response[dholev1.PublishPluginResponse], error) {
	p, err := s.principal(ctx, req.Header())
	if err != nil {
		return nil, err
	}
	if s.catWriter == nil {
		return nil, connect.NewError(connect.CodeUnimplemented,
			errors.New("api: this server was built without a writable catalog"))
	}
	plugin := req.Msg.GetPlugin()
	if plugin == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("api: a plugin is required"))
	}

	manifest, err := manifestOf(plugin)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	switch err := s.catWriter.Publish(ctx, p.TenantID, manifest); {
	case errors.Is(err, catalog.ErrVersionExists):
		// A version is immutable, and this is the ONLY refusal a publisher can
		// act on by publishing a new version — so it is its own code rather
		// than folded into the invalid-argument case below.
		return nil, connect.NewError(connect.CodeAlreadyExists, fmt.Errorf("api: %w", err))
	case err != nil:
		// Everything else catalog.Publish refuses is a malformed declaration:
		// a missing digest, an unknown kind, an unspecified effect class, a
		// schema that is not a schema. They are the caller's to fix, and the
		// catalog's message already names which.
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("api: publish plugin: %w", err))
	}
	return connect.NewResponse(&dholev1.PublishPluginResponse{
		Plugin: wirePlugin(manifest),
	}), nil
}

// manifestOf is the wire declaration as the catalog stores it.
//
// The ref the caller sent is IGNORED: identity is namespace/name@version, and
// honouring a ref that disagreed with those three would give one record two
// contradicting names — one it is stored under and one it is addressed by.
func manifestOf(p *dholev1.Plugin) (catalog.Manifest, error) {
	kind := catalog.Kind(p.GetKind())
	if p.GetKind() == "" {
		return catalog.Manifest{}, fmt.Errorf(
			"api: a plugin kind is required: one of %q, %q or %q",
			catalog.KindStep, catalog.KindTrigger, catalog.KindEngine)
	}
	return catalog.Manifest{
		Namespace:    p.GetNamespace(),
		Name:         p.GetName(),
		Version:      p.GetVersion(),
		Digest:       p.GetDigest(),
		Kind:         kind,
		EffectClass:  p.GetEffectClass(),
		Capabilities: p.GetCapabilities(),
		InputSchema:  []byte(p.GetInputSchema()),
		OutputSchema: []byte(p.GetOutputSchema()),
		EngineTypes:  p.GetEngineTypes(),
	}, nil
}

// wirePlugin is one manifest as the wire carries it.
func wirePlugin(m catalog.Manifest) *dholev1.Plugin {
	return &dholev1.Plugin{
		Ref:          m.Ref(),
		Namespace:    m.Namespace,
		Name:         m.Name,
		Version:      m.Version,
		Digest:       m.Digest,
		Kind:         string(m.Kind),
		EffectClass:  m.EffectClass,
		Capabilities: m.Capabilities,
		InputSchema:  string(m.InputSchema),
		OutputSchema: string(m.OutputSchema),
		EngineTypes:  m.EngineTypes,
	}
}
