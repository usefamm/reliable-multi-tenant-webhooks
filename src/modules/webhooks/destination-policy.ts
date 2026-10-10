/**
 * Outbound network boundary for webhook destinations (SSRF guard).
 * Pure functions and one error type; no I/O, so it is unit-testable and reusable
 * by anything that must decide "may a worker contact this URL?".
 */
/**
 * Error raised when a destination fails the deployment's network boundary check.
 * Treated as NON_RETRYABLE by the worker: misconfiguration must not burn the
 * retry budget, and must never reach the network.
 */
export class DestinationNotAllowedError extends Error {
  constructor(url: string) {
    super(`Destination host is not in the webhook allowlist: ${redactUrl(url)}`);
    this.name = 'DestinationNotAllowedError';
  }
}

/**
 * SSRF guard (PDF section 18): only contact configured destinations.
 *
 * The destination URL is always server-side data from the endpoints table -
 * the event API can never supply or replace a URL. When
 * WEBHOOK_ALLOWED_HOSTS is configured, dispatch is additionally pinned to that
 * explicit host(:port) allowlist, so even a compromised endpoints row cannot
 * point workers at arbitrary internal addresses.
 */
export function isAllowedDestination(url: string, allowedHostsRaw: string): boolean {
  const hosts = allowedHostsRaw
    .split(',')
    .map((h) => h.trim().toLowerCase())
    .filter(Boolean);
  if (hosts.length === 0) return true; // no allowlist configured
  try {
    const u = new URL(url);
    const hostPort = `${u.hostname.toLowerCase()}:${u.port || defaultPort(u.protocol)}`;
    return hosts.includes(hostPort) || hosts.includes(u.hostname.toLowerCase());
  } catch {
    return false;
  }
}

function defaultPort(protocol: string): string {
  return protocol === 'https:' ? '443' : '80';
}

/** Strip any userinfo/query before putting a URL in an error message. */
function redactUrl(url: string): string {
  try {
    const u = new URL(url);
    return `${u.protocol}//${u.hostname}${u.port ? `:${u.port}` : ''}`;
  } catch {
    return '[unparseable]';
  }
}
