/**
 * The app read the URL during render and subscribed to nothing, so moving from
 * one run to another left the previous run on screen — same graph, same logs,
 * under a URL that said otherwise.
 */
import { describe, expect, it } from "vitest";

import { locationKey } from "./useLocation.js";

describe("locationKey", () => {
  it("is a string, so React can tell two locations apart", () => {
    // The bug this guards: `location` is ONE object mutated in place. Anything
    // that returns it is always identical to itself, and useSyncExternalStore
    // compares by identity — so every navigation reports as no change.
    const live = { search: "", hash: "#/runs/a" };
    const before = locationKey(live);

    live.hash = "#/runs/b";
    const after = locationKey(live);

    expect(typeof before).toBe("string");
    expect(after).not.toEqual(before);
  });

  it("covers both halves of the URL the app routes on", () => {
    expect(locationKey({ search: "?pipeline=p", hash: "" })).toContain(
      "pipeline=p",
    );
    expect(locationKey({ search: "", hash: "#/runs/r1" })).toContain("runs/r1");
  });

  it("has an answer where there is no location at all", () => {
    expect(locationKey(undefined)).toEqual("");
  });
});
