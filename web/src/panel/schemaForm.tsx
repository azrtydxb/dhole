/**
 * JSON Schema (draft 2020-12) rendered as form controls.
 *
 * A plugin declares its inputs and the editor renders that declaration
 * (ADR 0012, ADR 0013). Nothing here knows a field name: hand-writing a form
 * per plugin would mean every new plugin needs a web release and would make
 * the plugin's own schema stop being the truth.
 *
 * THE SUBSET THIS FILE RENDERS, and only this:
 *
 *   - a root schema with `"type": "object"` and a `properties` object;
 *   - a property of type "string"      -> a text input, or a <select> when it
 *                                         declares a string `enum`;
 *   - a property of type "integer" or "number" -> a number input;
 *   - a property of type "boolean"     -> a checkbox;
 *   - `title`, `description`, `default` and the root's `required` list.
 *
 * ANYTHING ELSE IS RENDERED, NOT DROPPED. A property whose schema uses a
 * combinator ($ref, oneOf, anyOf, allOf, not, if), declares no type, declares
 * several, or declares a type this file has no control for, becomes a
 * read-only control carrying the reason and naming the construct. Silently
 * skipping it is the one unacceptable option: a dropped field is a value the
 * user can neither set nor see is missing, and the pipeline runs with whatever
 * the plugin defaults it to. A root schema that is not an object schema is
 * reported the same way, in place of the whole form.
 *
 * VALIDATION IS NOT THIS SUBSET. It is Ajv over the WHOLE declaration, so
 * `pattern`, `minimum`, `const`, `dependentRequired` and every other keyword
 * are enforced exactly as written even where no control renders them, and the
 * message the user reads is derived from the schema rather than written here.
 * Ajv is pinned (8.17.1) because a validator that changes its verdicts between
 * installs would make "the schema said so" unfalsifiable.
 */
import { Ajv2020 } from "ajv/dist/2020.js";
import type { ErrorObject } from "ajv";

/** JsonValue is a parsed JSON document — the shape a schema and a value both
 * have. It exists so nothing in this file needs `any`. */
export type JsonValue =
  | string
  | number
  | boolean
  | null
  | readonly JsonValue[]
  | { readonly [key: string]: JsonValue };

/** SchemaObject is a JSON Schema document as parsed, not as understood. */
export type SchemaObject = { readonly [key: string]: JsonValue };

/** FormValues is what the controls hold. A number control keeps the raw
 * string while it is unparseable, so the schema — not this file — gets to say
 * "must be integer". */
export type FormValues = Readonly<Record<string, string | number | boolean>>;

/** Control is how one property is edited, including the case where it cannot
 * be: `unsupported` is a rendered control, never an omission. */
export type Control =
  | { readonly kind: "text" }
  | { readonly kind: "number"; readonly integer: boolean }
  | { readonly kind: "checkbox" }
  | { readonly kind: "select"; readonly options: readonly string[] }
  | { readonly kind: "unsupported"; readonly why: string };

/** Field is one property of the schema, ready to render. */
export interface Field {
  readonly name: string;
  readonly title: string;
  readonly description: string;
  readonly required: boolean;
  readonly control: Control;
  /** The schema's declared default, as text, or "" when it declares none. */
  readonly fallback: string;
}

/** The keywords this renderer refuses to guess at. Each is legal JSON Schema
 * and each changes what a value may be, so a control drawn from the rest of
 * the property would be a control for a different schema. */
const combinators = [
  "$ref",
  "$dynamicRef",
  "oneOf",
  "anyOf",
  "allOf",
  "not",
  "if",
];

function isObject(value: JsonValue | undefined): value is {
  readonly [key: string]: JsonValue;
} {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function asString(value: JsonValue | undefined): string {
  switch (typeof value) {
    case "string":
      return value;
    case "number":
    case "boolean":
      return String(value);
    default:
      return "";
  }
}

/** parseSchema reads a declaration the control plane carried inline. An
 * unparseable one is an error the panel shows: a plugin whose declaration
 * cannot be read must not look like a plugin that declares nothing. */
export function parseSchema(
  source: string,
): { readonly schema: SchemaObject } | { readonly error: string } {
  let parsed: unknown;
  try {
    parsed = JSON.parse(source);
  } catch (cause) {
    return { error: `the declaration is not valid JSON: ${String(cause)}` };
  }
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    return { error: "the declaration is not a JSON Schema object" };
  }
  return { schema: parsed as SchemaObject };
}

/** controlFor picks the control one property's schema calls for, or says why
 * it cannot. */
function controlFor(property: SchemaObject): Control {
  const used = combinators.find((keyword) => property[keyword] !== undefined);
  if (used !== undefined) {
    return {
      kind: "unsupported",
      why: `its schema uses "${used}", which this editor does not render`,
    };
  }

  const declared = property["type"];
  if (Array.isArray(declared)) {
    return {
      kind: "unsupported",
      why: "its schema declares several types, so there is no one control for it",
    };
  }
  if (typeof declared !== "string") {
    return {
      kind: "unsupported",
      why: 'its schema declares no "type"',
    };
  }

  switch (declared) {
    case "string": {
      const options = property["enum"];
      if (
        Array.isArray(options) &&
        options.every((v) => typeof v === "string")
      ) {
        return { kind: "select", options: [...options] };
      }
      if (options !== undefined) {
        return {
          kind: "unsupported",
          why: 'its "enum" is not a list of strings',
        };
      }
      return { kind: "text" };
    }
    case "integer":
      return { kind: "number", integer: true };
    case "number":
      return { kind: "number", integer: false };
    case "boolean":
      return { kind: "checkbox" };
    default:
      return {
        kind: "unsupported",
        why: `this editor has no control for a field of type "${declared}"`,
      };
  }
}

/** fieldsOf turns a declaration into the controls that stand for it, or says
 * why the whole declaration cannot be rendered. */
export function fieldsOf(
  schema: SchemaObject,
): { readonly fields: readonly Field[] } | { readonly error: string } {
  if (schema["type"] !== "object") {
    return {
      error:
        "this editor renders an object schema; this declaration is of type " +
        `"${asString(schema["type"])}"`,
    };
  }
  const properties = schema["properties"];
  if (!isObject(properties)) {
    return { error: 'this declaration has no "properties" object' };
  }
  const requiredList = schema["required"];
  const required = new Set(
    Array.isArray(requiredList)
      ? requiredList.filter((v): v is string => typeof v === "string")
      : [],
  );

  const fields: Field[] = [];
  for (const [name, declaration] of Object.entries(properties)) {
    if (!isObject(declaration)) {
      fields.push({
        name,
        title: name,
        description: "",
        required: required.has(name),
        fallback: "",
        control: {
          kind: "unsupported",
          why: "its schema is not an object, so nothing declares what it holds",
        },
      });
      continue;
    }
    fields.push({
      name,
      title:
        asString(declaration["title"]) === ""
          ? name
          : asString(declaration["title"]),
      description: asString(declaration["description"]),
      required: required.has(name),
      fallback: asString(declaration["default"]),
      control: controlFor(declaration),
    });
  }
  return { fields };
}

/** coerce turns what a control holds into what the schema will be asked
 * about. A number that has not been typed yet is left as its text, so the
 * schema reports "must be integer" instead of this file guessing. */
function coerce(
  field: Field,
  value: string | number | boolean,
): JsonValue | undefined {
  if (field.control.kind === "checkbox") {
    return typeof value === "boolean" ? value : value === "true";
  }
  if (field.control.kind === "number") {
    if (typeof value === "number") {
      return value;
    }
    const text = String(value).trim();
    if (text === "") {
      // Absent rather than empty: "" is not a number, and reporting it as one
      // would answer a question the user has not been asked yet. A schema
      // that requires the property still says so.
      return undefined;
    }
    const parsed = Number(text);
    return Number.isFinite(parsed) ? parsed : text;
  }
  return String(value);
}

/** validateValues asks the SCHEMA whether these values are acceptable, and
 * returns the schema's own message per field.
 *
 * The empty-string key carries anything the schema said about the document as
 * a whole, which a per-field panel would otherwise swallow.
 */
export function validateValues(
  schema: SchemaObject,
  fields: readonly Field[],
  values: FormValues,
): Readonly<Record<string, string>> {
  const document: Record<string, JsonValue> = {};
  for (const field of fields) {
    const held = values[field.name];
    if (held === undefined) {
      continue;
    }
    const value = coerce(field, held);
    if (value !== undefined) {
      document[field.name] = value;
    }
  }

  const ajv = new Ajv2020({ allErrors: true, strict: false });
  let validate;
  try {
    validate = ajv.compile(schema);
  } catch (cause) {
    // A declaration Ajv refuses is reported as a whole-form problem. It is
    // never treated as "no constraints": accepting everything because the
    // schema could not be read is the silent acceptance this panel refuses.
    return {
      "": `this plugin's declaration could not be compiled: ${String(cause)}`,
    };
  }
  if (validate(document)) {
    return {};
  }

  const messages: Record<string, string> = {};
  for (const error of validate.errors ?? []) {
    const name = fieldOf(error);
    const text = error.message ?? "is not valid";
    const detail = detailOf(error);
    const full = `${text}${detail}`;
    const existing = messages[name];
    messages[name] = existing === undefined ? full : `${existing}; ${full}`;
  }
  return messages;
}

/** fieldOf names the property one Ajv error is about. */
function fieldOf(error: ErrorObject): string {
  const missing = (error.params as { missingProperty?: unknown })
    .missingProperty;
  if (typeof missing === "string") {
    return missing;
  }
  const path = error.instancePath;
  if (path.startsWith("/")) {
    const [first] = path.slice(1).split("/");
    return (first ?? "").replaceAll("~1", "/").replaceAll("~0", "~");
  }
  return "";
}

/** detailOf quotes the schema's own parameter back, because "must match
 * pattern" without the pattern tells the user nothing they can act on. */
function detailOf(error: ErrorObject): string {
  const params = error.params as Record<string, unknown>;
  const quoted =
    params["pattern"] ?? params["allowedValues"] ?? params["const"];
  if (typeof quoted === "string") {
    return ` "${quoted}"`;
  }
  return "";
}

/** SchemaFieldProps is one control and everything visible about it. */
export interface SchemaFieldProps {
  readonly field: Field;
  readonly value: string | number | boolean | undefined;
  /** Why this field cannot be edited, or undefined when it can. Rendering it
   * disabled with the reason is the alternative to hiding it. */
  readonly readOnlyBecause?: string;
  readonly error?: string;
  readonly onChange: (value: string | boolean) => void;
}

/** SchemaField renders one property of the declaration. */
export function SchemaField({
  field,
  value,
  readOnlyBecause,
  error,
  onChange,
}: SchemaFieldProps) {
  const held = value === undefined ? "" : String(value);
  const disabled = readOnlyBecause !== undefined;
  const control = field.control;

  return (
    <div data-testid={`field-row-${field.name}`} style={{ marginBottom: 12 }}>
      <label htmlFor={`field-${field.name}`} style={{ display: "block" }}>
        {field.title}
        {field.required && <span aria-hidden> *</span>}
      </label>

      {control.kind === "select" && (
        <select
          id={`field-${field.name}`}
          name={field.name}
          data-testid={`field-${field.name}`}
          value={held}
          disabled={disabled}
          onChange={(event) => onChange(event.target.value)}
        >
          {!control.options.includes(held) && (
            <option value={held}>{held}</option>
          )}
          {control.options.map((option) => (
            <option key={option} value={option}>
              {option}
            </option>
          ))}
        </select>
      )}

      {control.kind === "checkbox" && (
        <input
          id={`field-${field.name}`}
          name={field.name}
          data-testid={`field-${field.name}`}
          type="checkbox"
          checked={value === true || value === "true"}
          disabled={disabled}
          onChange={(event) => onChange(event.target.checked)}
        />
      )}

      {(control.kind === "text" || control.kind === "number") && (
        <input
          id={`field-${field.name}`}
          name={field.name}
          data-testid={`field-${field.name}`}
          type={control.kind === "number" ? "number" : "text"}
          {...(control.kind === "number" && control.integer ? { step: 1 } : {})}
          value={held}
          disabled={disabled}
          onChange={(event) => onChange(event.target.value)}
        />
      )}

      {control.kind === "unsupported" && (
        <>
          <input
            id={`field-${field.name}`}
            name={field.name}
            data-testid={`field-${field.name}`}
            type="text"
            value={held}
            readOnly
            disabled
          />
          <p
            data-testid={`field-unsupported-${field.name}`}
            role="note"
            style={{ color: "#975a16", margin: "2px 0" }}
          >
            {field.name}: {control.why}. Its value is shown as the plugin
            declares it and can only be set through the API.
          </p>
        </>
      )}

      {field.description !== "" && (
        <p style={{ color: "#718096", margin: "2px 0" }}>{field.description}</p>
      )}

      {readOnlyBecause !== undefined && control.kind !== "unsupported" && (
        <p
          data-testid={`field-readonly-${field.name}`}
          role="note"
          style={{ color: "#975a16", margin: "2px 0" }}
        >
          {readOnlyBecause}
        </p>
      )}

      {error !== undefined && (
        <p
          data-testid={`field-error-${field.name}`}
          role="alert"
          style={{ color: "#c53030", margin: "2px 0" }}
        >
          {error}
        </p>
      )}
    </div>
  );
}
