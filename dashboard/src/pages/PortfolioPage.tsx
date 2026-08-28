import { Link } from "react-router-dom";
import { KpiCard } from "../components/KpiCard";
import { DataState } from "../components/DataState";
import { DataStatusBadge, SeverityBadge } from "../components/StatusBadge";
import { PowerChart } from "../components/charts/PowerChart";
import { usePortfolioLive, usePortfolioDaily, usePlants, useAlerts } from "../hooks/queries";
import { useLivePortfolioPowerBuffer } from "../hooks/useLivePortfolioPowerBuffer";
import { formatEnergy, formatPercent, formatPower, formatRelativeAge, formatRevenue, formatTimestamp } from "../lib/format";
import { ApiError } from "../api/client";

// The simulated "today" is derived from the latest live timestamp the API
// actually returned, never from the browser's clock — the demo clock runs at
// 288x, so wall-clock "today" and simulated "today" agree only by accident.
function deriveSimulationDate(timestamp: string | null | undefined): string | undefined {
  return timestamp ? timestamp.slice(0, 10) : undefined;
}

export function PortfolioPage() {
  const live = usePortfolioLive();
  const simulationDate = deriveSimulationDate(live.data?.timestamp);
  const daily = usePortfolioDaily(simulationDate);
  const plants = usePlants();
  const activeAlerts = useAlerts({ status: "ACTIVE", limit: 5 });
  const chartData = useLivePortfolioPowerBuffer(live.data);

  const dailyIsNoReportYet = daily.isError && daily.error instanceof ApiError && daily.error.status === 404;

  return (
    <div>
      <section className="section">
        <div className="section__header">
          <h1>Portfolio Overview</h1>
          {live.data && (
            <span className="text-muted">
              Updated {formatTimestamp(live.data.timestamp)} <DataStatusBadge status={live.data.data_status} />
            </span>
          )}
        </div>

        <DataState isLoading={live.isLoading} isError={live.isError} error={live.error} onRetry={() => live.refetch()}>
          {live.data && (
            <div className="kpi-grid">
              <KpiCard label="Installed Capacity" value={formatPower(live.data.installed_capacity_kw)} />
              <KpiCard
                label="Current Generation"
                value={formatPower(live.data.current_power_kw)}
                sublabel="Live"
                tone={live.data.data_status === "STALE" ? "warning" : "default"}
              />
              <KpiCard label="Live Performance" value={formatPercent(live.data.performance_pct)} sublabel="vs. measured irradiance" />
              <KpiCard label="Availability" value={formatPercent(live.data.availability_pct)} />
            </div>
          )}
        </DataState>
      </section>

      <section className="section">
        <div className="section__header">
          <h2>Today (Reconciled)</h2>
          {daily.data && <span className="text-muted">Simulated date {daily.data.portfolio.simulation_date}</span>}
        </div>
        <DataState
          isLoading={daily.isLoading || live.isLoading}
          isError={daily.isError && !dailyIsNoReportYet}
          error={daily.error}
          isEmpty={dailyIsNoReportYet || !simulationDate}
          emptyMessage="No daily report is available for this simulated date yet."
          onRetry={() => daily.refetch()}
        >
          {daily.data && (
            <div className="kpi-grid">
              <KpiCard label="Actual Generation" value={formatEnergy(daily.data.portfolio.actual_generation_kwh)} />
              <KpiCard label="Expected Generation" value={formatEnergy(daily.data.portfolio.expected_generation_kwh)} />
              <KpiCard label="Lost Energy" value={formatEnergy(daily.data.portfolio.lost_energy_kwh)} tone="warning" />
              <KpiCard label="Lost Revenue" value={formatRevenue(daily.data.portfolio.lost_revenue)} tone="warning" />
            </div>
          )}
        </DataState>
      </section>

      <section className="section">
        <h2>Live Power</h2>
        <div className="panel">
          {chartData.length > 1 ? (
            <PowerChart data={chartData} />
          ) : (
            <div className="data-state">Collecting live samples…</div>
          )}
        </div>
      </section>

      <div style={{ display: "grid", gridTemplateColumns: "2fr 1fr", gap: "24px" }}>
        <section className="section">
          <h2>Plant Ranking (live)</h2>
          <DataState isLoading={plants.isLoading} isError={plants.isError} error={plants.error} onRetry={() => plants.refetch()}>
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Plant</th>
                    <th>Current Power</th>
                    <th>Performance</th>
                    <th>Availability</th>
                    <th>Status</th>
                    <th>Last Update</th>
                  </tr>
                </thead>
                <tbody>
                  {[...(plants.data ?? [])]
                    .sort((a, b) => (b.performance_pct ?? -1) - (a.performance_pct ?? -1))
                    .map((plant) => (
                      <tr key={plant.id}>
                        <td>
                          <Link to={`/plants/${plant.id}`}>{plant.name}</Link>
                        </td>
                        <td>{formatPower(plant.current_power_kw)}</td>
                        <td>{formatPercent(plant.performance_pct)}</td>
                        <td>{formatPercent(plant.availability_pct)}</td>
                        <td>
                          <DataStatusBadge status={plant.data_status} />
                        </td>
                        <td>{formatRelativeAge(plant.last_update)}</td>
                      </tr>
                    ))}
                </tbody>
              </table>
            </div>
          </DataState>
        </section>

        <section className="section">
          <h2>Active Alerts</h2>
          <DataState
            isLoading={activeAlerts.isLoading}
            isError={activeAlerts.isError}
            error={activeAlerts.error}
            isEmpty={(activeAlerts.data ?? []).length === 0}
            emptyMessage="No active alerts."
            onRetry={() => activeAlerts.refetch()}
          >
            <ul style={{ listStyle: "none", margin: 0, padding: 0, display: "grid", gap: "8px" }}>
              {(activeAlerts.data ?? []).map((alert) => (
                <li key={alert.id} className="panel">
                  <div style={{ display: "flex", justifyContent: "space-between", gap: "8px" }}>
                    <strong>{alert.plant_id}</strong>
                    <SeverityBadge severity={alert.severity} />
                  </div>
                  <div className="text-muted">{alert.message}</div>
                </li>
              ))}
            </ul>
          </DataState>
        </section>
      </div>
    </div>
  );
}
