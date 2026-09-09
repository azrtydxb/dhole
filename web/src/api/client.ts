// The web app's only door to the control plane.
//
// ADR 0013: the GUI is one API client among the CLI and agents, with no
// privileged endpoints. That is why nothing here is hand-written: the client
// is built by createClient() from the GENERATED service descriptor, so it has
// exactly the contract's RPCs — no convenience method the CLI lacks, and no
// missing RPC nobody noticed. src/api/client.test.ts fails if that changes.
//
// There is no RunService: StartRun and WatchRun are RPCs of PipelineService,
// so `pipelineClient` is the whole contract and a separate `runClient` would
// be a fiction. Add one here the day the proto grows one.
import {
  Code,
  ConnectError,
  createClient,
  type Client,
  type Interceptor,
} from "@connectrpc/connect";
import { createConnectTransport } from "@connectrpc/connect-web";

import { PipelineService } from "../gen/dhole/v1/api_pb.js";

const tokenStorageKey = "dhole.token";

/** The one place the bearer token lives. */
let token: string | null = readStoredToken();

function readStoredToken(): string | null {
  try {
    return globalThis.localStorage?.getItem(tokenStorageKey) ?? null;
  } catch {
    // A browser with site data blocked, or a test environment without
    // localStorage: the session is simply not persisted.
    return null;
  }
}

function writeStoredToken(value: string | null): void {
  try {
    if (value === null) {
      globalThis.localStorage?.removeItem(tokenStorageKey);
    } else {
      globalThis.localStorage?.setItem(tokenStorageKey, value);
    }
  } catch {
    // Same as above — an unpersisted token still works for this page.
  }
}

/** setToken records the token every subsequent request will carry. */
export function setToken(value: string): void {
  token = value;
  writeStoredToken(value);
}

/** clearToken signs out: requests after it fail rather than going out bare. */
export function clearToken(): void {
  token = null;
  writeStoredToken(null);
}

/** getToken reports the stored token, or null when signed out. */
export function getToken(): string | null {
  return token;
}

// authorize is the single place the Authorization header is set. A request
// with no token is REFUSED here rather than sent unauthenticated: an
// anonymous call would come back as a server-side 401 that is easy to mistake
// for an expired session, and on a control plane configured without auth it
// would quietly succeed as nobody. Failing before the fetch keeps "signed
// out" a client-side fact.
const authorize: Interceptor = (next) => (req) => {
  if (token === null) {
    throw new ConnectError(
      "not signed in: no API token is stored, so the request was not sent",
      Code.Unauthenticated,
    );
  }
  req.header.set("Authorization", `Bearer ${token}`);
  return next(req);
};

function baseUrl(): string {
  const configured = import.meta.env.VITE_DHOLE_API_URL;
  if (configured !== undefined && configured !== "") {
    return configured;
  }
  return globalThis.location?.origin ?? "http://127.0.0.1:8080";
}

export const transport = createConnectTransport({
  baseUrl: baseUrl(),
  interceptors: [authorize],
});

/** pipelineClient is the generated client for the whole dhole.v1 contract. */
export const pipelineClient: Client<typeof PipelineService> = createClient(
  PipelineService,
  transport,
);
