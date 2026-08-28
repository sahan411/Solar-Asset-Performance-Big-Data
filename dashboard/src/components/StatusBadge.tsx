import type { AlertStatus, DataStatus, Severity } from "../api/types";

// Every badge carries its own text — color is never the only signal, per the
// project's accessibility requirement for the alerts screen.

export function DataStatusBadge({ status }: { status: DataStatus | null | undefined }) {
  const resolved = status ?? "NO_DATA";
  const label = resolved === "LIVE" ? "Live" : resolved === "STALE" ? "Stale" : "No data";
  return <span className={`badge badge--${resolved.toLowerCase()}`}>{label}</span>;
}

export function SeverityBadge({ severity }: { severity: Severity }) {
  const label = severity === "CRITICAL" ? "Critical" : "Warning";
  return <span className={`badge badge--${severity.toLowerCase()}`}>{label}</span>;
}

export function AlertStatusBadge({ status }: { status: AlertStatus }) {
  const label = status === "ACTIVE" ? "Active" : "Resolved";
  return <span className={`badge badge--${status.toLowerCase()}`}>{label}</span>;
}
