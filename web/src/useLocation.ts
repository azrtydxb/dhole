/**
 * The current URL, as something React re-renders on.
 *
 * `globalThis.location.hash` read during render is a value React cannot know
 * has changed: nothing schedules an update, so navigating from one run to
 * another left the previous run on screen — same graph, same logs, same
 * everything, addressed by a URL that said otherwise. A person following a
 * link to a second run while the app was open was quietly shown the first.
 *
 * useSyncExternalStore rather than an effect with setState: the location is an
 * external mutable source, and this is the API that exists for reading one
 * without tearing under concurrent rendering.
 */
import { useSyncExternalStore } from "react";

function subscribe(onChange: () => void): () => void {
  // Both events, because the two ways to move are different: hashchange fires
  // for `#/runs/a` -> `#/runs/b`, and popstate for back and forward.
  globalThis.addEventListener?.("hashchange", onChange);
  globalThis.addEventListener?.("popstate", onChange);
  return () => {
    globalThis.removeEventListener?.("hashchange", onChange);
    globalThis.removeEventListener?.("popstate", onChange);
  };
}

/**
 * locationKey is the part of a URL this app routes on, as a string.
 *
 * A STRING, and that is the whole subtlety. React compares snapshots by
 * identity, and `globalThis.location` is a single object mutated in place — it
 * is always identical to itself, so returning it would report "unchanged" for
 * every navigation there has ever been. Exported so that property has a test
 * without needing a DOM.
 */
export function locationKey(
  l: { search: string; hash: string } | undefined,
): string {
  return l === undefined ? "" : l.search + l.hash;
}

function getSnapshot(): string {
  return locationKey(globalThis.location);
}

/** Server-side and test renders with no window have no location to read. */
function getServerSnapshot(): string {
  return "";
}

export function useLocationKey(): string {
  return useSyncExternalStore(subscribe, getSnapshot, getServerSnapshot);
}
