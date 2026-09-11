/**
 * Which theme the editor is wearing, and where that choice survives.
 *
 * The value is written to the document element rather than held in React
 * state alone, because the canvas, the panels and the port glyphs all read it
 * through CSS variables — passing it down as a prop would mean every one of
 * them re-rendering to change a colour that CSS can change on its own.
 *
 * The choice is remembered per browser. An editor that reset to dark on every
 * reload would be asking the same question forever.
 */
import { useCallback, useEffect, useState } from "react";

/** Theme is the two the design defines. There is deliberately no "system":
 * the editor is a tool somebody sits in front of for an hour, and following
 * the OS into light at sunset mid-edit is a change nobody asked for. */
export type Theme = "dark" | "light";

const storageKey = "dhole.theme";

/** readStored returns the remembered theme, or dark when there is none.
 *
 * Every access is guarded: a private window, cleared site data, or a browser
 * configured to refuse storage all throw rather than return null, and a
 * throwing preference read is not a reason to fail to render an editor. */
function readStored(): Theme {
  try {
    return globalThis.localStorage?.getItem(storageKey) === "light"
      ? "light"
      : "dark";
  } catch {
    return "dark";
  }
}

/** useTheme returns the current theme and a toggle that persists it. */
export function useTheme(): { theme: Theme; toggleTheme: () => void } {
  const [theme, setTheme] = useState<Theme>(readStored);

  useEffect(() => {
    globalThis.document?.documentElement.setAttribute("data-theme", theme);
    try {
      globalThis.localStorage?.setItem(storageKey, theme);
    } catch {
      // An unpersisted theme still works for this tab, which is the whole of
      // what the user asked for by clicking. Failing here would be louder than
      // the problem.
    }
  }, [theme]);

  const toggleTheme = useCallback(() => {
    setTheme((current) => (current === "dark" ? "light" : "dark"));
  }, []);

  return { theme, toggleTheme };
}
