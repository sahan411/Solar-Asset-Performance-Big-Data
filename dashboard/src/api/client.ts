// Thin fetch wrapper. No fallback data lives here: every failure raises an
// ApiError the caller renders as an explicit error state — the dashboard must
// never substitute a fabricated number for one the API could not return.

import type {
  Alert,
  AlertStatus,
  DailyReport,
  HealthResponse,
  InverterInfo,
  PlantHistoryResponse,
  PlantLive,
  PlantSummary,
  PortfolioDailyResponse,
  PortfolioLive,
  ReadyResponse,
  Severity,
} from "./types";

const API_BASE_URL = (import.meta.env.VITE_API_BASE_URL as string | undefined) ?? "http://localhost:8000";

export class ApiError extends Error {
  status: number;

  constructor(status: number, message: string) {
    super(message);
    this.name = "ApiError";
    this.status = status;
  }
}

type QueryParams = Record<string, string | number | undefined>;

async function request<T>(path: string, params?: QueryParams): Promise<T> {
  const url = new URL(path, API_BASE_URL);
  if (params) {
    for (const [key, value] of Object.entries(params)) {
      if (value !== undefined) url.searchParams.set(key, String(value));
    }
  }

  let response: Response;
  try {
    response = await fetch(url.toString());
  } catch {
    throw new ApiError(0, "Could not reach the SolarIQ API. Is it running?");
  }

  if (!response.ok) {
    let detail = response.statusText || `Request failed with status ${response.status}`;
    try {
      const body = (await response.json()) as { detail?: unknown };
      if (typeof body.detail === "string") detail = body.detail;
    } catch {
      // Response body was not JSON; keep the statusText-derived message.
    }
    throw new ApiError(response.status, detail);
  }

  return (await response.json()) as T;
}

export interface AlertFilters {
  status?: AlertStatus;
  plant_id?: string;
  severity?: Severity;
  limit?: number;
}

export const api = {
  health: () => request<HealthResponse>("/health"),
  ready: () => request<ReadyResponse>("/ready"),

  portfolioLive: () => request<PortfolioLive>("/api/v1/portfolio/live"),
  portfolioDaily: (date: string) => request<PortfolioDailyResponse>("/api/v1/portfolio/daily", { date }),

  plants: () => request<PlantSummary[]>("/api/v1/plants"),
  plantLive: (plantId: string) => request<PlantLive>(`/api/v1/plants/${encodeURIComponent(plantId)}/live`),
  plantHistory: (plantId: string, from: string, to: string) =>
    request<PlantHistoryResponse>(`/api/v1/plants/${encodeURIComponent(plantId)}/history`, { from, to }),
  plantInverters: (plantId: string) =>
    request<InverterInfo[]>(`/api/v1/plants/${encodeURIComponent(plantId)}/inverters`),

  alerts: (filters?: AlertFilters) =>
    request<Alert[]>("/api/v1/alerts", {
      status: filters?.status?.toLowerCase(),
      plant_id: filters?.plant_id,
      severity: filters?.severity,
      limit: filters?.limit,
    }),

  dailyReport: (date: string) => request<DailyReport>("/api/v1/reports/daily", { date }),
};
