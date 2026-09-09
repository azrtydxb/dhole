package llm

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash"

	"github.com/azrtydxb/go-ai-sdk/provider"
)

// fingerprintSchema domain-separates this digest from every other use of
// SHA-256 here, and gives a future encoding change somewhere to announce
// itself: bump it and every old fingerprint stops matching rather than
// silently meaning something else.
const fingerprintSchema = "dhole.llm.fingerprint/v1"

// Fingerprint identifies the model that ANSWERED, from the response it gave.
//
// This is the load-bearing property of the whole package, and it is why the
// argument is the response rather than the configuration. A pipeline asks for
// a model by name, and the name it asks for is very often an alias — "latest",
// a channel, a floating tag. An alias repoints without warning: the same
// string names one model on Monday and a different one on Friday. So the
// question "which model produced this output" can only be answered by the
// output, and Fingerprint deliberately has no way to see what was asked for.
//
// Folding the alias in instead is the specific bug this exists to prevent.
// The step's cache key is content-addressed over its environment identity
// (ADR 0009), and the model IS that environment: a key folded over "latest"
// stays identical across a repoint, so the cache serves the old model's answer
// as the new model's answer, and nothing in the run says anything changed.
//
// A response that does not name its model returns the empty string. There is
// no fallback, because every candidate fallback is the alias, and answering
// "the model was whatever we asked for" is precisely the false claim above.
// The caller treats an empty fingerprint as a failure: a call nobody can
// attribute cannot be cached, and should not be recorded as if it could.
//
// The parameter is a value rather than a pointer because it is read, not kept,
// and provider.Response holds no lock — go vet's copylocks has nothing to
// object to. (The SDK at v0.4.1 has no `ai.Response`; the response type it
// returns from a language model call is provider.Response.)
func Fingerprint(resp provider.Response) string {
	model, system := resolvedModel(resp)
	if model == "" {
		return ""
	}
	h := sha256.New()
	writeField(h, []byte(fingerprintSchema))
	writeField(h, []byte(model))
	// The provider's own system fingerprint, where it publishes one: two
	// serving stacks behind the same model id are two environments, and
	// OpenAI says so in system_fingerprint. Framed separately so a model id
	// ending in a stack name cannot collide with the pair.
	writeField(h, []byte(system))
	return fmt.Sprintf("sha256:%x", h.Sum(nil))
}

// resolvedModel digs the answering model's identity out of a response.
//
// Providers report it in two places, so both are read. ProviderMetadata is the
// SDK's structured channel, namespaced by provider name; Raw is the response
// body as it arrived, where every provider this SDK speaks to puts a "model"
// field. Metadata wins when both are present: it is the SDK's considered
// answer, and Raw is whatever the wire happened to carry.
func resolvedModel(resp provider.Response) (model, system string) {
	model, system = fromMetadata(resp.ProviderMetadata)
	if model != "" {
		return model, system
	}
	return fromRaw(resp.Raw)
}

// fromMetadata reads ProviderMetadata, accepting both the flat shape and the
// provider-namespaced one the SDK documents ({"anthropic": {...}}).
func fromMetadata(meta map[string]any) (model, system string) {
	if len(meta) == 0 {
		return "", ""
	}
	if m, s, ok := modelFields(meta); ok {
		return m, s
	}
	// Namespaced. Map order is undefined, so only a namespace that actually
	// names a model is taken, and the first such wins; a response carrying two
	// namespaces that disagree about the model is malformed either way.
	for _, v := range meta {
		nested, ok := v.(map[string]any)
		if !ok {
			continue
		}
		if m, s, ok := modelFields(nested); ok {
			return m, s
		}
	}
	return "", ""
}

func modelFields(m map[string]any) (model, system string, ok bool) {
	model, _ = m["model"].(string)
	system, _ = m["system_fingerprint"].(string)
	return model, system, model != ""
}

// fromRaw reads the provider's response body. Only the two fields are decoded:
// the body holds the completion itself, and unmarshalling all of it to reach a
// name would pull the model's output into a function whose result is written
// to a cache key.
func fromRaw(raw json.RawMessage) (model, system string) {
	if len(raw) == 0 {
		return "", ""
	}
	var body struct {
		Model             string `json:"model"`
		SystemFingerprint string `json:"system_fingerprint"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		// A body that will not parse names no model, which is exactly the
		// empty answer. There is nobody to report the error to that would not
		// be the caller deciding to fall back to the alias.
		return "", ""
	}
	return body.Model, body.SystemFingerprint
}

// writeField frames one variable-length field with its length, so the hashed
// material can only be read apart one way — model "ab"/stack "c" must not hash
// the same as model "a"/stack "bc".
func writeField(h hash.Hash, b []byte) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(b)))
	_, _ = h.Write(length[:])
	_, _ = h.Write(b)
}
