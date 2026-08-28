import { useMemo } from "react";
import { Link, useParams } from "react-router-dom";
import { KpiCard } from "../components/KpiCard";
import { DataState } from "../components/DataState";
import { DataStatusBadge, SeverityBadge } from "../components/StatusBadge";
import { PowerChart, type PowerChartPoint } from "../components/charts/PowerChart";
import { usePlantLive, usePlantHistory, usePlantInverters, useAlerts } from "../hooks/queries";
import { formatPercent, formatPower, formatTimestamp } from "../lib/format";

const HISTORY_RANGE_MINUTES = 15;

export function PlantPage() {
  const { plantId } = useParams<{ plantId: string }>();
  const live = usePlantLive(plantId);
  const history = usePlantHistory(plantId, HISTORY_RANGE_MINUTES);
  const inverters = usePlantInverters(plantId);
  const alerts = useAlerts({ plant_id: plantId });

  const chartData: PowerChartPoint[] = useMemo(
    () =>
      (history.data?.points ?? []).map((point) => ({
        timestamp: point.window_end,
        currentPowerKw: point.current_power_kw,
        expectedPowerKw: point.expected_power_kw,
      })),
    [history.data],
  );

  return (
    <div>
      <p>
        <Link to="/">&larr; Back to portfolio</Link>
      </p>

      <DataState isLoading={live.isLoading} isError={live.isError} error={live.error} onRetry={() => live.refetch()}>
        {live.data && (
          <>
            <section className="section">
              <div className="section__header">
                <h1>
                  {live.data.plant_name} <span className="text-muted">({live.data.plant_id})</span>
                </h1>
                <span className="text-muted">
                  Updated {formatTimestamp(live.data.timestamp)} <DataStatusBadge status={live.data.data_status} />
                </span>
              </div>

              <div className="kpi-grid">
                <KpiCard label="Capacity" value={formatPower(live.data.capacity_kw)} />
                <KpiCard
                  label="Current Power"
                  value={formatPower(live.data.current_power_kw)}
                  tone={live.data.data_status === "STALE" ? "warning" : "default"}
                />
                <KpiCard label="Performance" value={formatPercent(live.data.performance_pct)} />
                <KpiCard label="Availability" value={formatPercent(live.data.availability_pct)} />
                <KpiCard label="Estimated Loss" value={formatPower(live.data.estimated_loss_kw)} tone="warning" />
                <KpiCard
                  label="Inverters Online"
                  value={
                    live.data.online_inverters === null
                      ? "—"
                      : `${live.data.online_inverters}/${live.data.online_inverters + (live.data.offline_inverters ?? 0)}`
                  }
                />
              </div>
            </section>

            <section className="section">
              <h2>Power — last {HISTORY_RANGE_MINUTES} minutes</h2>
              <div className="panel">
                <DataState
                  isLoading={history.isLoading}
                  isError={history.isError}
                  error={history.error}
                  isEmpty={chartData.length === 0}
                  emptyMessage="No live data in this window yet."
                  onRetry={() => history.refetch()}
                >
                  <PowerChart data={chartData} />
                </DataState>
              </div>
            </section>
          </>
        )}
      </DataState>

      <section className="section">
        <h2>Inverters</h2>
        <DataState
          isLoading={inverters.isLoading}
          isError={inverters.isError}
          error={inverters.error}
          isEmpty={(inverters.data ?? []).length === 0}
          emptyMessage="No inverters configured for this plant."
          onRetry={() => inverters.refetch()}
        >
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Inverter</th>
                  <th>Name</th>
                  <th>Rated Power</th>
                  <th>Active</th>
                </tr>
              </thead>
              <tbody>
                {(inverters.data ?? []).map((inverter) => (
                  <tr key={inverter.id}>
                    <td>{inverter.id}</td>
                    <td>{inverter.name}</td>
                    <td>{formatPower(inverter.rated_power_kw)}</td>
                    <td>{inverter.active ? "Yes" : "No"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </DataState>
      </section>

      <section className="section">
        <h2>Recent Alerts</h2>
        <DataState
          isLoading={alerts.isLoading}
          isError={alerts.isError}
          error={alerts.error}
          isEmpty={(alerts.data ?? []).length === 0}
          emptyMessage="No alerts recorded for this plant."
          onRetry={() => alerts.refetch()}
        >
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Severity</th>
                  <th>Type</th>
                  <th>Started</th>
                  <th>Message</th>
                  <th>Status</th>
                </tr>
              </thead>
              <tbody>
                {(alerts.data ?? []).map((alert) => (
                  <tr key={alert.id}>
                    <td>
                      <SeverityBadge severity={alert.severity} />
                    </td>
                    <td>{alert.alert_type}</td>
                    <td>{formatTimestamp(alert.started_at)}</td>
                    <td>{alert.message}</td>
                    <td>{alert.status}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </DataState>
      </section>
    </div>
  );
}
