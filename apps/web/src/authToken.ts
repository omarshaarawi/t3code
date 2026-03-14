const STORAGE_KEY = "t3code_auth_token";

/**
 * On first load, check for a `?token=` query parameter. If present, persist it
 * to sessionStorage and strip it from the visible URL (avoids leaking tokens
 * in browser history / shared screenshots).
 *
 * Subsequent calls return the cached value.
 */
export function captureAuthToken(): string | null {
  // Already persisted from a previous page load in this session?
  const stored = sessionStorage.getItem(STORAGE_KEY);
  if (stored) return stored;

  const params = new URLSearchParams(window.location.search);
  const token = params.get("token");
  if (!token) return null;

  sessionStorage.setItem(STORAGE_KEY, token);

  // Strip the token from the visible URL so it doesn't leak into browser
  // history, link shares, or screenshots.
  params.delete("token");
  const cleaned = params.toString();
  const newUrl = `${window.location.pathname}${cleaned ? `?${cleaned}` : ""}${window.location.hash}`;
  window.history.replaceState(null, "", newUrl);

  return token;
}

export function getAuthToken(): string | null {
  return sessionStorage.getItem(STORAGE_KEY);
}

export function setAuthToken(token: string): void {
  sessionStorage.setItem(STORAGE_KEY, token);
}

export function clearAuthToken(): void {
  sessionStorage.removeItem(STORAGE_KEY);
}
