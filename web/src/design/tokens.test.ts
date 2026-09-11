/**
 * The two places a colour lives must agree.
 *
 * tokens.ts exists because React Flow cannot take `var()`; theme.css exists
 * because everything else can. Neither can be deleted, so the risk is that one
 * is edited and the other is not — and the failure is quiet: a grid drawn in
 * the dark palette on a light canvas, which reads as "slightly wrong" rather
 * than as a bug with a name.
 *
 * This parses the stylesheet and compares, so the drift is loud instead.
 */
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

import { svgTokens } from "./tokens.js";

const css = readFileSync(
  fileURLToPath(new URL("./theme.css", import.meta.url)),
  "utf8",
);

/** blockFor returns the declarations of the rule that defines a theme:
 * `:root` for dark, `:root[data-theme="light"]` for light. */
function blockFor(theme: "dark" | "light"): string {
  const selector = theme === "dark" ? ":root {" : ':root[data-theme="light"] {';
  const start = css.indexOf(selector);
  expect(start, `theme.css has no ${selector} rule`).toBeGreaterThan(-1);
  const end = css.indexOf("}", start);
  return css.slice(start, end);
}

/** declared reads one custom property out of a block. */
function declared(block: string, name: string): string {
  const match = new RegExp(`--${name}:\\s*([^;]+);`).exec(block);
  expect(match, `theme.css declares no --${name}`).not.toBeNull();
  return (match?.[1] ?? "").trim();
}

describe.each(["dark", "light"] as const)(
  "the %s palette in tokens.ts matches theme.css",
  (theme) => {
    const block = blockFor(theme);

    it("agrees on the grid colour", () => {
      expect(declared(block, "grid")).toBe(svgTokens[theme].grid);
    });

    it("agrees on ink3", () => {
      expect(declared(block, "ink3")).toBe(svgTokens[theme].ink3);
    });

    it("agrees on the line colour", () => {
      expect(declared(block, "line")).toBe(svgTokens[theme].line);
    });

    it("agrees on panel2", () => {
      expect(declared(block, "panel2")).toBe(svgTokens[theme].panel2);
    });
  },
);
