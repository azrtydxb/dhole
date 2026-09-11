/**
 * The handful of palette values that have to exist in TypeScript as well as in
 * CSS, and the reason they do.
 *
 * Almost everything in this editor takes `var(--token)` and follows the theme
 * for free. React Flow does not: it paints the background grid and the minimap
 * by writing colours into SVG attributes, where an unresolved custom property
 * is not a fallback but an INVALID COLOUR — it paints nothing. That is how the
 * canvas ended up with no grid while the stylesheet was perfectly correct, and
 * how the minimap stayed empty.
 *
 * Reading the computed style instead would work, but only a frame late: the
 * theme is an attribute set on the document, so the first render of a fresh
 * theme reads the old palette. These are constants, so there is no frame to be
 * late by.
 *
 * The obvious cost is two places to change a colour. `tokens.test.ts` closes
 * it: it parses theme.css and fails if either theme drifts from this table.
 */
import type { Theme } from "./useTheme.js";

/** SvgTokens are the tokens an SVG-painting consumer needs resolved. */
export type SvgTokens = {
  readonly grid: string;
  readonly ink3: string;
  readonly line: string;
  readonly panel2: string;
};

export const svgTokens: Record<Theme, SvgTokens> = {
  dark: {
    grid: "rgba(140, 160, 190, 0.06)",
    ink3: "#5d6b80",
    line: "#262f3d",
    panel2: "#0d1117",
  },
  light: {
    grid: "rgba(30, 50, 80, 0.09)",
    ink3: "#5d6878",
    line: "#aab0ba",
    panel2: "#d3d6dc",
  },
};
