import { describe, expect, it, vi, beforeEach } from "vitest";
import { screen, waitFor } from "@testing-library/react";
import { renderWithProviders } from "./testUtils";
import { DailyReportPage } from "../pages/DailyReportPage";
import { api, ApiError } from "../api/client";
import type { DailyReport } from "../api/types";

vi.mock("../api/client", async () => {
  const actual = await vi.importActual<typeof import("../api/client")>("../api/client");
  return { ...actual, api: { ...actual.api, dailyReport: vi.fn() } };
});

const dailyReportMock = api.dailyReport as unknown as ReturnType<typeof vi.fn>;

const REPORT: DailyReport = {
  simulation_date: "2026-08-21",
  portfolio: {
    simulation_date: "2026-08-21",
    actual_generation_kwh: 101200,
    expected_generation_kwh: 110000,
    performance_pct: 92.0,
    lost_energy_kwh: 8800,
    actual_revenue: 15180,
    lost_revenue: 1320,
  },
  plants: [
    {
      plant_id: "PLANT_01",
      plant_name: "North Ridge Solar",
      actual_generation_kwh: 60000,
      expected_generation_kwh: 62000,
      performance_pct: 96.8,
      availability_pct: 100,
      lost_energy_kwh: 2000,
      lost_revenue: 300,
      alert_count: 0,
      maintenance_flag: false,
    },
  ],
  best_performer: { plant_id: "PLANT_01", plant_name: "North Ridge Solar", performance_pct: 96.8 },
  worst_performer: { plant_id: "PLANT_02", plant_name: "East Field Solar", performance_pct: 80.1 },
  generated_at: "2026-08-21T05:00:10Z",
};

beforeEach(() => {
  dailyReportMock.mockReset();
});

describe("DailyReportPage", () => {
  it("renders the portfolio summary and plant table from the API response", async () => {
    dailyReportMock.mockResolvedValue(REPORT);

    renderWithProviders(<DailyReportPage />, { route: "/reports/daily" });

    // "North Ridge Solar" appears twice (best-performer KPI sublabel and the
    // plant table row) — assert on the unique portfolio totals instead.
    await waitFor(() => expect(screen.getByText("101.20 MWh")).toBeInTheDocument());
    expect(screen.getByText("92.0%")).toBeInTheDocument(); // portfolio performance
    expect(screen.getAllByText("North Ridge Solar").length).toBeGreaterThan(0);
  });

  it("shows the no-report-yet empty state on a 404, not a generic error", async () => {
    dailyReportMock.mockRejectedValue(new ApiError(404, "No daily reconciliation report exists yet for 2026-08-22."));

    renderWithProviders(<DailyReportPage />, { route: "/reports/daily" });

    await waitFor(() =>
      expect(screen.getByText("No daily report is available for this simulated date yet.")).toBeInTheDocument(),
    );
    expect(screen.queryByRole("alert")).not.toBeInTheDocument();
  });
});
