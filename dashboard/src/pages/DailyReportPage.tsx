import { useState } from "react";
import { Link } from "react-router-dom";
import { DataState } from "../components/DataState";
import { KpiCard } from "../components/KpiCard";
import { useDailyReport } from "../hooks/queries";
import { formatEnergy, formatPercent, formatRevenue, formatTimestamp } from "../lib/format";
import { downloadCsv, toCsv } from "../lib/csv";
import { ApiError } from "../api/client";
import type { DailyReport } from "../api/types";

function todayIso(): string {
  return new Date().toISOString().slice(0, 10);
}

function exportReport(report: DailyReport): void {
  const csv = toCsv(
    ["plant_id", "plant_name", "actual_generation_kwh", "expected_generation_kwh", "performance_pct", "availability_pct", "lost_energy_kwh", "lost_revenue", "alert_count", "maintenance_flag"],
    report.plants.map((plant) => [
      plant.plant_id,
      plant.plant_name,
      plant.actual_generation_kwh,
      plant.expected_generation_kwh,
      plant.performance_pct,
      plant.availability_pct,
      plant.lost_energy_kwh,
      plant.lost_revenue,
      plant.alert_count,
      plant.maintenance_flag,
    ]),
  );
  downloadCsv(`solariq-daily-report-${report.simulation_date}.csv`, csv);
}

export function DailyReportPage() {
  const [date, setDate] = useState(todayIso());
  const report = useDailyReport(date);
  const isNoReportYet = report.isError && report.error instanceof ApiError && report.error.status === 404;

  return (
    <div>
      <div className="section__header">
        <h1>Daily Reconciliation Report</h1>
        <label>
          Simulated date:{" "}
          <input type="date" value={date} onChange={(event) => setDate(event.target.value)} />
        </label>
      </div>

      <DataState
        isLoading={report.isLoading}
        isError={report.isError && !isNoReportYet}
        error={report.error}
        isEmpty={isNoReportYet}
        emptyMessage="No daily report is available for this simulated date yet."
        onRetry={() => report.refetch()}
      >
        {report.data && (
          <>
            <section className="section">
              <div className="section__header">
                <h2>Portfolio Summary</h2>
                <button type="button" className="button" onClick={() => exportReport(report.data as DailyReport)}>
                  Export CSV
                </button>
              </div>
              <div className="kpi-grid">
                <KpiCard label="Actual Generation" value={formatEnergy(report.data.portfolio.actual_generation_kwh)} />
                <KpiCard label="Expected Generation" value={formatEnergy(report.data.portfolio.expected_generation_kwh)} />
                <KpiCard label="Performance" value={formatPercent(report.data.portfolio.performance_pct)} />
                <KpiCard label="Lost Energy" value={formatEnergy(report.data.portfolio.lost_energy_kwh)} tone="warning" />
                <KpiCard label="Actual Revenue" value={formatRevenue(report.data.portfolio.actual_revenue)} />
                <KpiCard label="Lost Revenue" value={formatRevenue(report.data.portfolio.lost_revenue)} tone="warning" />
              </div>
              <p className="text-muted">Report generated at {formatTimestamp(report.data.generated_at)}. Revenue figures use a simulated PPA rate.</p>
            </section>

            <section className="section">
              <h2>Best / Worst Performer</h2>
              <div className="kpi-grid">
                <KpiCard
                  label="Best Performer"
                  value={report.data.best_performer ? formatPercent(report.data.best_performer.performance_pct) : "—"}
                  sublabel={report.data.best_performer?.plant_name}
                />
                <KpiCard
                  label="Worst Performer"
                  value={report.data.worst_performer ? formatPercent(report.data.worst_performer.performance_pct) : "—"}
                  sublabel={report.data.worst_performer?.plant_name}
                  tone={report.data.worst_performer ? "warning" : "default"}
                />
              </div>
            </section>

            <section className="section">
              <h2>Plants</h2>
              <div className="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>Plant</th>
                      <th>Actual</th>
                      <th>Expected</th>
                      <th>Performance</th>
                      <th>Availability</th>
                      <th>Lost Energy</th>
                      <th>Lost Revenue</th>
                      <th>Alerts</th>
                    </tr>
                  </thead>
                  <tbody>
                    {report.data.plants.map((plant) => (
                      <tr key={plant.plant_id}>
                        <td>
                          <Link to={`/plants/${plant.plant_id}`}>{plant.plant_name}</Link>
                          {plant.maintenance_flag && <span className="badge badge--stale" style={{ marginLeft: 6 }}>Maintenance</span>}
                        </td>
                        <td>{formatEnergy(plant.actual_generation_kwh)}</td>
                        <td>{formatEnergy(plant.expected_generation_kwh)}</td>
                        <td>{formatPercent(plant.performance_pct)}</td>
                        <td>{formatPercent(plant.availability_pct)}</td>
                        <td>{formatEnergy(plant.lost_energy_kwh)}</td>
                        <td>{formatRevenue(plant.lost_revenue)}</td>
                        <td>{plant.alert_count}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </section>
          </>
        )}
      </DataState>
    </div>
  );
}
