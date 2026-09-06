// Client-side mirror of the server's identifier policy for namespace, table,
// view and column names (hoglake.yaml: violations are 422):
//   ^[A-Za-z_][A-Za-z0-9_-]{0,127}$

export const IDENTIFIER_PATTERN = "^[A-Za-z_][A-Za-z0-9_-]{0,127}$";

const IDENTIFIER_RE = new RegExp(IDENTIFIER_PATTERN);

/**
 * Returns an inline-form error message for an invalid identifier, or null
 * when the name is valid. Empty input returns null so `required` handles
 * the blank case without a premature error.
 */
export function identifierError(name: string): string | null {
  if (name === "" || IDENTIFIER_RE.test(name)) return null;
  if (name.length > 128) return "name is longer than 128 characters";
  return "name must start with a letter or underscore and use only letters, digits, underscore, or hyphen";
}
