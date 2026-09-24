"""Batch layer: tariff validation and the bill formula (common/billing.py)."""

import pytest

from common.billing import compute_bill, validate_tariff_row


def row(**overrides):
    base = {"household_id": "H001", "tariff_rate": "0.2000", "billing_tier": "B",
            "subsidy_flag": "false"}
    base.update(overrides)
    return base


def test_valid_tariff_row_is_parsed():
    assert validate_tariff_row(row(subsidy_flag="TRUE")) == ("H001", 0.2, "B", True)


@pytest.mark.parametrize("bad", [
    row(tariff_rate=""),            # blank rate (the simulator injects this)
    row(tariff_rate="-0.1900"),     # negative rate (the simulator injects this)
    row(tariff_rate="0"),
    row(tariff_rate="abc"),
    row(tariff_rate="9.99"),        # unrealistically high
    row(household_id="  "),
    row(billing_tier="Z"),
    row(subsidy_flag="maybe"),
    {"household_id": "H001"},       # missing columns
])
def test_invalid_tariff_rows_are_rejected(bad):
    assert validate_tariff_row(bad) is None


def test_bill_without_solar_or_subsidy():
    bill = compute_bill(consumption_kwh=10, grid_import_kwh=10, solar_export_kwh=0,
                        tariff_rate=0.20, billing_tier="A", subsidy=False)
    assert bill["energy_charge"] == 2.00       # 10 kWh x 0.20
    assert bill["solar_credit"] == 0.00
    assert bill["service_charge"] == 0.50      # tier A
    assert bill["subsidy_discount"] == 0.00
    assert bill["total_bill"] == 2.50
    assert bill["solar_contribution_pct"] == 0.0


def test_bill_with_solar_export_and_subsidy():
    # Used 12 kWh: 4 from own panels, 8 from the grid; exported 6 kWh.
    bill = compute_bill(consumption_kwh=12, grid_import_kwh=8, solar_export_kwh=6,
                        tariff_rate=0.25, billing_tier="C", subsidy=True)
    assert bill["energy_charge"] == 2.00       # 8 x 0.25
    assert bill["solar_credit"] == 0.75        # 6 x 0.25 x 50%
    assert bill["service_charge"] == 1.00      # tier C
    assert bill["subsidy_discount"] == 0.40    # 20% of 2.00
    assert bill["total_bill"] == 1.85          # 2.00 - 0.75 + 1.00 - 0.40
    assert bill["solar_contribution_pct"] == pytest.approx(33.33, abs=0.01)


def test_bill_can_be_a_credit_when_export_is_large():
    bill = compute_bill(consumption_kwh=5, grid_import_kwh=0, solar_export_kwh=20,
                        tariff_rate=0.20, billing_tier="A", subsidy=False)
    assert bill["total_bill"] == -1.50         # 0 - 2.00 + 0.50
    assert bill["solar_contribution_pct"] == 100.0


def test_zero_consumption_does_not_divide_by_zero():
    bill = compute_bill(0, 0, 0, 0.2, "B", False)
    assert bill["solar_contribution_pct"] == 0.0
    assert bill["total_bill"] == 0.75
