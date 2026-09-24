"""Billing rules used by the Airflow batch layer.

Kept separate from the DAG so they can be unit-tested without Airflow.

Bill for one household and one day:
    energy_charge  = grid_import_kwh x tariff_rate
    solar_credit   = solar_export_kwh x tariff_rate x 50%
    service_charge = fixed daily charge by billing tier (A 0.50, B 0.75, C 1.00)
    subsidy        = 20% of energy_charge if subsidy_flag
    total_bill     = energy_charge - solar_credit + service_charge - subsidy
    solar_share %  = part of consumption covered by the home's own panels
"""

SERVICE_CHARGE = {"A": 0.50, "B": 0.75, "C": 1.00}
SOLAR_EXPORT_CREDIT = 0.5
SUBSIDY_DISCOUNT = 0.2
MAX_TARIFF_RATE = 5.0  # anything above this per kWh is treated as a data error


def validate_tariff_row(row: dict):
    """Return (household_id, rate, tier, subsidy) for a valid CSV row, else None."""
    try:
        household_id = row["household_id"].strip()
        rate = float(row["tariff_rate"])
        tier = row["billing_tier"].strip()
        subsidy = row["subsidy_flag"].strip().lower()
    except (KeyError, ValueError, AttributeError):
        return None
    if not household_id or not 0 < rate < MAX_TARIFF_RATE or tier not in SERVICE_CHARGE:
        return None
    if subsidy not in ("true", "false"):
        return None
    return household_id, rate, tier, subsidy == "true"


def compute_bill(consumption_kwh: float, grid_import_kwh: float, solar_export_kwh: float,
                 tariff_rate: float, billing_tier: str, subsidy: bool) -> dict:
    """Price one household-day. Money is rounded to 2 decimals."""
    energy_charge = round(grid_import_kwh * tariff_rate, 2)
    solar_credit = round(solar_export_kwh * tariff_rate * SOLAR_EXPORT_CREDIT, 2)
    service_charge = SERVICE_CHARGE[billing_tier]
    subsidy_discount = round(energy_charge * SUBSIDY_DISCOUNT, 2) if subsidy else 0.0
    total_bill = round(energy_charge - solar_credit + service_charge - subsidy_discount, 2)

    self_consumed = consumption_kwh - grid_import_kwh
    solar_share = self_consumed / consumption_kwh * 100 if consumption_kwh > 0 else 0.0
    return {
        "energy_charge": energy_charge,
        "solar_credit": solar_credit,
        "service_charge": service_charge,
        "subsidy_discount": subsidy_discount,
        "total_bill": total_bill,
        "solar_contribution_pct": round(solar_share, 2),
    }
