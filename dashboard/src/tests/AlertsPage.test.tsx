import { describe, expect, it, vi, beforeEach } from "vitest";
import { screen, waitFor } from "@testing-library/react";
import { renderWithProviders } from "./testUtils";
import { AlertsPage } from "../pages/AlertsPage";
import { api } from "../api/client";
import type { Alert } from "../api/types";

vi.mock("../api/client", async () => {
  const actual = await vi.importActual<typeof import("../api/client")>("../api/client");
  return { ...actual, api: { ...actual.api, alerts: vi.fn() } };
});

const alertsMock = api.alerts as unknown as ReturnType<typeof vi.fn>;

function alert(overrides: Partial<Alert>): Alert {
  return {
    id: "alert-1",
    plant_id: "PLANT_03",
    inverter_id: "INV_02",
    alert_type: "UNDERPERFORMANCE",
    severity: "WARNING",
    message: "Inverter underperforming",
    started_at: "2026-08-21T05:10:00Z",
    ended_at: null,
    status: "ACTIVE",
    estimated_loss_kwh: 12.5,
    estimated_revenue_loss: null,
    ...overrides,
  };
}

beforeEach(() => {
  alertsMock.mockReset();
});

describe("AlertsPage", () => {
  it("renders severity and status for each alert row", async () => {
    alertsMock.mockResolvedValue([
      alert({ id: "a1", severity: "CRITICAL", status: "ACTIVE" }),
      alert({ id: "a2", severity: "WARNING", status: "RESOLVED" }),
    ]);

    renderWithProviders(<AlertsPage />, { route: "/alerts" });

    await waitFor(() => expect(screen.getByText("Critical")).toBeInTheDocument());
    expect(screen.getByText("Warning")).toBeInTheDocument();
    expect(screen.getByText("Active")).toBeInTheDocument();
    expect(screen.getByText("Resolved")).toBeInTheDocument();
  });

  it("shows the empty state when no alerts match the filters", async () => {
    alertsMock.mockResolvedValue([]);

    renderWithProviders(<AlertsPage />, { route: "/alerts" });

    await waitFor(() => expect(screen.getByText("No alerts match the current filters.")).toBeInTheDocument());
  });
});
