import { useEffect, useRef, useState } from "react";
import type { PowerChartPoint } from "../components/charts/PowerChart";
import type { PortfolioLive } from "../api/types";

// There is no portfolio history endpoint in the API contract (only
// /portfolio/live and /portfolio/daily) — the chart is built by accumulating
// real polled snapshots client-side, not by fabricating a series. Capped to
// keep the buffer bounded across a long-running demo.
const MAX_POINTS = 120;

export function useLivePortfolioPowerBuffer(live: PortfolioLive | undefined): PowerChartPoint[] {
  const [buffer, setBuffer] = useState<PowerChartPoint[]>([]);
  const lastTimestamp = useRef<string | null>(null);

  useEffect(() => {
    if (!live?.timestamp || live.current_power_kw === null) return;
    if (live.timestamp === lastTimestamp.current) return;
    lastTimestamp.current = live.timestamp;

    const point: PowerChartPoint = {
      timestamp: live.timestamp,
      currentPowerKw: live.current_power_kw,
      expectedPowerKw: live.expected_power_kw,
    };

    setBuffer((previous) => {
      const next = [...previous, point];
      return next.length > MAX_POINTS ? next.slice(next.length - MAX_POINTS) : next;
    });
  }, [live]);

  return buffer;
}
