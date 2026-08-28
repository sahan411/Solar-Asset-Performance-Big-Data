// Mirrors api/app/models/*.py exactly — field names, nullability and enum
// values. If a field is missing here, it does not exist in the API response;
// add it to the backend model first rather than inventing it on this side.

export type DataStatus = "LIVE" | "STALE" | "NO_DATA";

export interface PortfolioLive {
  timestamp: string | null;
  installed_capacity_kw: number;
  current_power_kw: number | null;
  avg_power_kw: number | null;
  expected_power_kw: number | null;
  availability_pct: number | null;
  performance_pct: number | null;
  online_inverters: number | null;
  offline_inverters: number | null;
  estimated_loss_kw: number | null;
  data_status: DataStatus;
}

export interface DailyPortfolioTotals {
  simulation_date: string;
  actual_generation_kwh: number;
  expected_generation_kwh: number;
  performance_pct: number | null;
  lost_energy_kwh: number | null;
  actual_revenue: number | null;
  lost_revenue: number | null;
}

export interface DailyPlantRow {
  plant_id: string;
  plant_name: string;
  actual_generation_kwh: number;
  expected_generation_kwh: number;
  performance_pct: number | null;
  availability_pct: number | null;
  lost_energy_kwh: number | null;
  lost_revenue: number | null;
  alert_count: number;
  maintenance_flag: boolean;
}

export interface PortfolioDailyResponse {
  portfolio: DailyPortfolioTotals;
  plants: DailyPlantRow[];
}

export interface PlantSummary {
  id: string;
  name: string;
  capacity_kw: number;
  active: boolean;
  current_power_kw: number | null;
  performance_pct: number | null;
  availability_pct: number | null;
  data_status: DataStatus | null;
  last_update: string | null;
}

export interface PlantLive {
  plant_id: string;
  plant_name: string;
  capacity_kw: number;
  timestamp: string | null;
  current_power_kw: number | null;
  avg_power_kw: number | null;
  expected_power_kw: number | null;
  availability_pct: number | null;
  performance_pct: number | null;
  estimated_loss_kw: number | null;
  online_inverters: number | null;
  offline_inverters: number | null;
  data_status: DataStatus;
}

export interface PlantHistoryPoint {
  window_start: string;
  window_end: string;
  current_power_kw: number;
  avg_power_kw: number;
  expected_power_kw: number | null;
  performance_pct: number | null;
  availability_pct: number | null;
}

export interface PlantHistoryResponse {
  plant_id: string;
  points: PlantHistoryPoint[];
}

export interface InverterInfo {
  id: string;
  plant_id: string;
  name: string;
  rated_power_kw: number;
  active: boolean;
}

export type AlertType = "UNDERPERFORMANCE" | "INVERTER_OFFLINE" | "TELEMETRY_GAP";
export type Severity = "WARNING" | "CRITICAL";
export type AlertStatus = "ACTIVE" | "RESOLVED";

export interface Alert {
  id: string;
  plant_id: string;
  inverter_id: string | null;
  alert_type: AlertType;
  severity: Severity;
  message: string;
  started_at: string;
  ended_at: string | null;
  status: AlertStatus;
  estimated_loss_kwh: number | null;
  estimated_revenue_loss: number | null;
}

export interface PerformerRef {
  plant_id: string;
  plant_name: string;
  performance_pct: number;
}

export interface DailyReport {
  simulation_date: string;
  portfolio: DailyPortfolioTotals;
  plants: DailyPlantRow[];
  best_performer: PerformerRef | null;
  worst_performer: PerformerRef | null;
  generated_at: string;
}

export interface HealthResponse {
  status: string;
  service: string;
}

export interface ReadyResponse {
  status: string;
  database: string;
}
