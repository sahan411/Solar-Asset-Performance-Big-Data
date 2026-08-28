import { useState } from "react";
import { Link } from "react-router-dom";
import { DataState } from "../components/DataState";
import { AlertStatusBadge, SeverityBadge } from "../components/StatusBadge";
import { useAlerts } from "../hooks/queries";
import { formatEnergy, formatRevenue, formatTimestamp } from "../lib/format";
import type { AlertStatus, Severity } from "../api/types";

type StatusFilter = "ALL" | AlertStatus;

export function AlertsPage() {
  const [statusFilter, setStatusFilter] = useState<StatusFilter>("ACTIVE");
  const [severityFilter, setSeverityFilter] = useState<Severity | "ALL">("ALL");

  const alerts = useAlerts({
    status: statusFilter === "ALL" ? undefined : statusFilter,
    severity: severityFilter === "ALL" ? undefined : severityFilter,
    limit: 200,
  });

  return (
    <div>
      <div className="section__header">
        <h1>Operational Alerts</h1>
        <div className="filter-bar">
          <label>
            Status:{" "}
            <select value={statusFilter} onChange={(event) => setStatusFilter(event.target.value as StatusFilter)}>
              <option value="ALL">All</option>
              <option value="ACTIVE">Active</option>
              <option value="RESOLVED">Resolved</option>
            </select>
          </label>
          <label>
            Severity:{" "}
            <select value={severityFilter} onChange={(event) => setSeverityFilter(event.target.value as Severity | "ALL")}>
              <option value="ALL">All</option>
              <option value="WARNING">Warning</option>
              <option value="CRITICAL">Critical</option>
            </select>
          </label>
        </div>
      </div>

      <DataState
        isLoading={alerts.isLoading}
        isError={alerts.isError}
        error={alerts.error}
        isEmpty={(alerts.data ?? []).length === 0}
        emptyMessage="No alerts match the current filters."
        onRetry={() => alerts.refetch()}
      >
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Severity</th>
                <th>Started</th>
                <th>Plant</th>
                <th>Inverter</th>
                <th>Type</th>
                <th>Message</th>
                <th>Estimated Impact</th>
                <th>Status</th>
              </tr>
            </thead>
            <tbody>
              {(alerts.data ?? []).map((alert) => (
                <tr key={alert.id}>
                  <td>
                    <SeverityBadge severity={alert.severity} />
                  </td>
                  <td>{formatTimestamp(alert.started_at)}</td>
                  <td>
                    <Link to={`/plants/${alert.plant_id}`}>{alert.plant_id}</Link>
                  </td>
                  <td>{alert.inverter_id ?? "—"}</td>
                  <td>{alert.alert_type}</td>
                  <td>{alert.message}</td>
                  <td>
                    {formatEnergy(alert.estimated_loss_kwh)}
                    {alert.estimated_revenue_loss !== null && ` / ${formatRevenue(alert.estimated_revenue_loss)}`}
                  </td>
                  <td>
                    <AlertStatusBadge status={alert.status} />
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </DataState>
    </div>
  );
}
