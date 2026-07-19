export type Appearance = "auto" | "light" | "dark";

const STORAGE_KEY = "neon-localhost-appearance";

export function storedAppearance(): Appearance {
  try {
    const value = localStorage.getItem(STORAGE_KEY);
    if (value === "light" || value === "dark") return value;
  } catch {
    // Storage can be unavailable in hardened or preview webviews.
  }
  return "auto";
}

export function applyAppearance(appearance: Appearance) {
  const dark = appearance === "dark"
    || (appearance === "auto" && window.matchMedia("(prefers-color-scheme: dark)").matches);
  document.documentElement.dataset.theme = dark ? "dark" : "light";
}

export function saveAppearance(appearance: Appearance) {
  try {
    if (appearance === "auto") localStorage.removeItem(STORAGE_KEY);
    else localStorage.setItem(STORAGE_KEY, appearance);
  } catch {
    // The selected appearance still applies for the current session.
  }
  applyAppearance(appearance);
}
