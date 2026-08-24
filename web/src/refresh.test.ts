import { afterEach, describe, expect, it, vi } from "vitest";
import { AUTO_REFRESH_MS, startAutoRefresh } from "./refresh";

afterEach(() => {
  vi.useRealTimers();
});

describe("startAutoRefresh", () => {
  it("refreshes on schedule only while the dashboard is visible", () => {
    vi.useFakeTimers();
    const refresh = vi.fn();
    let visible = true;
    const stop = startAutoRefresh(refresh, AUTO_REFRESH_MS, () => visible);

    vi.advanceTimersByTime(AUTO_REFRESH_MS);
    expect(refresh).toHaveBeenCalledTimes(1);

    visible = false;
    vi.advanceTimersByTime(AUTO_REFRESH_MS * 2);
    expect(refresh).toHaveBeenCalledTimes(1);

    stop();
    visible = true;
    vi.advanceTimersByTime(AUTO_REFRESH_MS);
    expect(refresh).toHaveBeenCalledTimes(1);
  });

  it("rejects an interval that could overload the API", () => {
    expect(() => startAutoRefresh(() => undefined, 999)).toThrow(
      "Auto-refresh interval must be at least one second.",
    );
  });
});
