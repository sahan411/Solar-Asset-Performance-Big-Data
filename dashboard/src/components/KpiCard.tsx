import type { ReactNode } from "react";

interface KpiCardProps {
  label: string;
  value: string;
  sublabel?: string;
  tone?: "default" | "warning" | "critical";
  badge?: ReactNode;
}

// One number, clearly labelled, with an explicit unit baked into `value` by
// the caller (lib/format.ts) — this component never formats numbers itself.
export function KpiCard({ label, value, sublabel, tone = "default", badge }: KpiCardProps) {
  return (
    <div className={`kpi-card kpi-card--${tone}`}>
      <div className="kpi-card__header">
        <span className="kpi-card__label">{label}</span>
        {badge}
      </div>
      <div className="kpi-card__value">{value}</div>
      {sublabel && <div className="kpi-card__sublabel">{sublabel}</div>}
    </div>
  );
}
