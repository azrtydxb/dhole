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
