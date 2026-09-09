// ADR 0013: one API contract serves the GUI, the CLI and agents equally. The
// Go side keeps that honest with a descriptor-driven coverage test over the
// CLI (cmd/dhole/cli/coverage_test.go). This is its web-side twin.
//
// The failure mode it exists to catch is drift by hand-writing: somebody adds
// a convenient `client.savePipeline()` the contract does not have, or hand-
// rolls the eight methods of today and never notices the ninth RPC. Neither
// is caught by a test that names RPCs, so this file names none: the expected
// set comes from the GENERATED service descriptor, and missingMethods() below
// is the shared helper both the real check and its own self-test run through.
import {
  create,
  createFileRegistry,
  type DescService,
} from "@bufbuild/protobuf";
import { FileDescriptorProtoSchema } from "@bufbuild/protobuf/wkt";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { PipelineService } from "../gen/dhole/v1/api_pb.js";
import { clearToken, pipelineClient, setToken } from "./client.js";

/** missingMethods names every RPC of `service` the client cannot call. */
function missingMethods(service: DescService, client: object): string[] {
  const methods = client as Record<string, unknown>;
  return service.methods
    .filter((m) => typeof methods[m.localName] !== "function")
    .map((m) => `${service.typeName}.${m.name}`)
    .sort();
}

describe("the generated Connect client", () => {
  it("exposes a method for every RPC in the generated service descriptor", () => {
    expect(PipelineService.methods.length).toBeGreaterThan(0);
    expect(
      missingMethods(PipelineService, pipelineClient),
      "these RPCs have no client method, so the web app cannot reach a corner " +
        "of the contract the CLI and agents can (ADR 0013)",
    ).toEqual([]);
  });

  it("carries no method the contract does not declare", () => {
    const declared = new Set(PipelineService.methods.map((m) => m.localName));
    expect(
      Object.keys(pipelineClient).filter((k) => !declared.has(k)),
      "these client methods are in no dhole.v1 service, so they were " +
        "hand-written rather than generated (ADR 0013)",
    ).toEqual([]);
  });

  // The self-test, mirroring TestCoverageIsDescriptorDrivenNotAList on the Go
  // side: a service that exists nowhere in the tree is fed to the same helper
  // the real check uses. If the check were ever reduced to a list of today's
  // RPCs it would keep passing above and fail here — which is why it is here.
  it("reports an RPC it has never heard of as missing", () => {
    expect(missingMethods(inventedService(), pipelineClient)).toEqual([
      "dhole.v1.InventedService.InventedRPC",
    ]);
  });
});

/** inventedService compiles a descriptor for a service nobody generated. */
function inventedService(): DescService {
  const proto = create(FileDescriptorProtoSchema, {
    name: "dhole/v1/invented_test.proto",
    package: "dhole.v1",
    syntax: "proto3",
    messageType: [{ name: "InventedRequest" }, { name: "InventedResponse" }],
    service: [
      {
        name: "InventedService",
        method: [
          {
            name: "InventedRPC",
            inputType: ".dhole.v1.InventedRequest",
            outputType: ".dhole.v1.InventedResponse",
          },
        ],
      },
    ],
  });
  const service = createFileRegistry(proto, () => undefined).getService(
    "dhole.v1.InventedService",
  );
  expect(service, "the synthetic descriptor failed to compile").toBeDefined();
  return service!;
}

// authorizationOf reads the Authorization header off a fetch call however the
// transport chose to make it — fetch(Request) or fetch(url, init) — so this
// test asserts on the header that goes on the wire, not on a call shape.
function authorizationOf(call: Parameters<typeof fetch>): string | null {
  const [input, init] = call;
  if (input instanceof Request) {
    return input.headers.get("Authorization");
  }
  return new Headers(init?.headers).get("Authorization");
}

describe("authorization", () => {
  const fetchMock = vi.fn<typeof fetch>();

  beforeEach(() => {
    vi.stubGlobal("fetch", fetchMock);
    fetchMock.mockReset();
  });

  afterEach(() => {
    clearToken();
    vi.unstubAllGlobals();
  });

  it("refuses to send a request when no token is stored", async () => {
    clearToken();
    await expect(
      pipelineClient.getPipeline({ pipelineId: "p1" }),
    ).rejects.toThrow(/not signed in/i);
    expect(
      fetchMock,
      "an unauthenticated request must never reach the network",
    ).not.toHaveBeenCalled();
  });

  it("sends the stored token as a bearer Authorization header", async () => {
    setToken("tok-abc");
    fetchMock.mockRejectedValue(new Error("stop after headers"));

    await expect(
      pipelineClient.getPipeline({ pipelineId: "p1" }),
    ).rejects.toThrow();

    expect(fetchMock).toHaveBeenCalledTimes(1);
    const call = fetchMock.mock.calls[0];
    expect(call).toBeDefined();
    expect(authorizationOf(call!)).toBe("Bearer tok-abc");
  });
});
