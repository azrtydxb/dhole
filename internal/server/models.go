package server

import (
	"context"
	"fmt"
	"strings"

	"github.com/azrtydxb/go-ai-sdk/provider"
	"github.com/azrtydxb/go-ai-sdk/providers/anthropic"
	"github.com/azrtydxb/go-ai-sdk/providers/openai"
)

// DefaultModels is the model factory the shipped binary uses: the providers
// this build knows how to talk to, each constructed with the credential the
// plane redeemed for THIS call (ADR 0024).
//
// It is small on purpose. A deployment reaching its models through a gateway,
// a proxy or a local runtime supplies its own on server.Config.Models and is
// told exactly that when this one does not recognise a name.
//
// The credential is REQUIRED, and that is the whole reason this function
// exists rather than the provider constructors being called directly. Every
// provider library in the SDK defaults its API key to os.Getenv, so a factory
// that passed the key through without checking would fall back to the
// operator's ambient environment variable whenever a step named no secret —
// silently, and only on the deployments that happen to have one set. That is
// the ambient credential this system refuses; there is one way a credential
// reaches a running thing here, and it is a redemption.
func DefaultModels(_ context.Context, req ModelRequest) (provider.LanguageModel, error) {
	if strings.TrimSpace(req.APIKey) == "" {
		return nil, fmt.Errorf(
			"the model of provider %q needs a credential: name one in the step's `api_key_secret` "+
				"and configure it on this plane; this factory will not read one out of the environment",
			req.Provider)
	}
	switch strings.ToLower(strings.TrimSpace(req.Provider)) {
	case "anthropic":
		return anthropic.New(anthropic.WithAPIKey(req.APIKey)).Model(req.Model), nil
	case "openai":
		return openai.New(openai.WithAPIKey(req.APIKey)).Model(req.Model), nil
	default:
		return nil, fmt.Errorf(
			"no provider named %q is built into this binary; a deployment that reaches its models "+
				"another way supplies its own factory on server.Config.Models", req.Provider)
	}
}
