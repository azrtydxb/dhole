/**
 * A port's type, drawn.
 *
 * A type is a COLOUR AND A SHAPE, and the shape is not decoration. Roughly one
 * man in twelve cannot separate the red report triangle from the green of a
 * cached step by hue, and this editor's whole claim is that a mistyped wire is
 * obvious before it is attempted — so the distinction has to survive being
 * printed in grey. Diamond, circle, square, triangle and ring do; five dots in
 * five colours do not.
 *
 * The same glyph is used on the node, in the legend and in the inspector, from
 * this one file, so a type that gains a colour gains it everywhere at once.
 */

/** PortTypeName is the vocabulary the editor draws. `unknown` is not a
 * failure: a port whose type the plane has not resolved yet is still a port,
 * and drawing nothing there would lose it. */
export type PortTypeName =
  "git-tree" | "blob" | "oci-image" | "report" | "approval" | "unknown";

type Shape = "diamond" | "circle" | "square" | "triangle" | "ring";

const shapes: Record<PortTypeName, { shape: Shape; token: string }> = {
  "git-tree": { shape: "diamond", token: "var(--type-git-tree)" },
  blob: { shape: "circle", token: "var(--type-blob)" },
  "oci-image": { shape: "square", token: "var(--type-oci-image)" },
  report: { shape: "triangle", token: "var(--type-report)" },
  approval: { shape: "ring", token: "var(--type-approval)" },
  unknown: { shape: "ring", token: "var(--ink3)" },
};

/** portTypeColour is the token an edge takes from the port it leaves, so a
 * wire's colour says what flows along it rather than where it happens to go. */
export function portTypeColour(type: PortTypeName): string {
  return shapes[type].token;
}

/** asPortTypeName maps whatever the API called a type onto the drawn
 * vocabulary. An unrecognised name is `unknown` rather than a thrown error:
 * the editor must keep drawing a pipeline that uses a type it has not been
 * taught, and the ring says plainly that it does not know this one. */
export function asPortTypeName(raw: string | undefined): PortTypeName {
  return raw !== undefined && raw in shapes ? (raw as PortTypeName) : "unknown";
}

/** PortGlyph draws one port type at the size used on a node. */
export function PortGlyph({
  type,
  size = 9,
  title,
}: {
  readonly type: PortTypeName;
  readonly size?: number;
  readonly title?: string;
}) {
  const { shape, token } = shapes[type];
  const base: React.CSSProperties = {
    display: "inline-block",
    boxSizing: "border-box",
    flex: "none",
    width: size,
    height: size,
  };

  const style: React.CSSProperties =
    shape === "diamond"
      ? {
          ...base,
          width: size - 1,
          height: size - 1,
          background: token,
          transform: "rotate(45deg)",
        }
      : shape === "circle"
        ? { ...base, background: token, borderRadius: "50%" }
        : shape === "square"
          ? { ...base, width: size - 1, height: size - 1, background: token }
          : shape === "triangle"
            ? {
                ...base,
                background: token,
                clipPath: "polygon(50% 0, 100% 100%, 0 100%)",
              }
            : {
                ...base,
                border: `2px solid ${token}`,
                borderRadius: "50%",
                background: "var(--panel)",
              };

  // The title carries the type name for a screen reader and for a hover, so
  // the shape is a shorthand rather than the only way to know.
  return (
    <span
      aria-hidden={title === undefined}
      title={title ?? type}
      style={style}
    />
  );
}
