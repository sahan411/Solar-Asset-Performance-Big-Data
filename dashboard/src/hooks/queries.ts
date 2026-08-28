// Typed React Query hooks — one per API call. Live data polls every 5s per
// the playbook's "moderate polling interval" rule; daily/report data does not
// poll (it only changes once per simulated day, after the Airflow DAG runs).

import { useQuery } from "@tanstack/react-query";
import { api, type AlertFilters } from "../api/client";

const LIVE_POLL_MS = 5000;

export function usePortfolioLive() {
  return useQuery({
    queryKey: ["portfolio", "live"],
    queryFn: api.portfolioLive,
    refetchInterval: LIVE_POLL_MS,
  });
}

export function usePortfolioDaily(date: string | undefined) {
  return useQuery({
    queryKey: ["portfolio", "daily", date],
    queryFn: () => api.portfolioDaily(date as string),
    enabled: Boolean(date),
    retry: (failureCount, error) => {
      // A 404 means "no report yet" — a real, expected state, not a transient
      // failure worth retrying.
      if (typeof error === "object" && error !== null && "status" in error && (error as { status: number }).status === 404) {
        return false;
      }
      return failureCount < 2;
    },
  });
}

export function usePlants() {
  return useQuery({
    queryKey: ["plants"],
    queryFn: api.plants,
    refetchInterval: LIVE_POLL_MS,
  });
}

export function usePlantLive(plantId: string | undefined) {
  return useQuery({
    queryKey: ["plants", plantId, "live"],
    queryFn: () => api.plantLive(plantId as string),
    enabled: Boolean(plantId),
    refetchInterval: LIVE_POLL_MS,
  });
}

// `rangeMinutes` (not from/to timestamps) is the query key: the window is
// computed fresh inside queryFn on every poll, so a "last 15 minutes" chart
// actually slides forward instead of freezing at the range captured on mount.
export function usePlantHistory(plantId: string | undefined, rangeMinutes: number) {
  return useQuery({
    queryKey: ["plants", plantId, "history", rangeMinutes],
    queryFn: () => {
      const to = new Date();
      const from = new Date(to.getTime() - rangeMinutes * 60_000);
      return api.plantHistory(plantId as string, from.toISOString(), to.toISOString());
    },
    enabled: Boolean(plantId),
    refetchInterval: LIVE_POLL_MS,
  });
}

export function usePlantInverters(plantId: string | undefined) {
  return useQuery({
    queryKey: ["plants", plantId, "inverters"],
    queryFn: () => api.plantInverters(plantId as string),
    enabled: Boolean(plantId),
  });
}

export function useAlerts(filters?: AlertFilters) {
  return useQuery({
    queryKey: ["alerts", filters],
    queryFn: () => api.alerts(filters),
    refetchInterval: LIVE_POLL_MS,
  });
}

export function useDailyReport(date: string | undefined) {
  return useQuery({
    queryKey: ["reports", "daily", date],
    queryFn: () => api.dailyReport(date as string),
    enabled: Boolean(date),
    retry: (failureCount, error) => {
      if (typeof error === "object" && error !== null && "status" in error && (error as { status: number }).status === 404) {
        return false;
      }
      return failureCount < 2;
    },
  });
}
