export const AUTO_REFRESH_MS = 10_000;

export function startAutoRefresh(
  onRefresh: () => void,
  intervalMs = AUTO_REFRESH_MS,
  isVisible: () => boolean = () => true,
): () => void {
  if (!Number.isFinite(intervalMs) || intervalMs < 1_000) {
    throw new Error("Auto-refresh interval must be at least one second.");
  }
  const timer = globalThis.setInterval(() => {
    if (isVisible()) onRefresh();
  }, intervalMs);
  return () => globalThis.clearInterval(timer);
}
