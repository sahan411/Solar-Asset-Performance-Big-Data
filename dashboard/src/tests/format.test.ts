import { describe, expect, it } from "vitest";
import { formatEnergy, formatPercent, formatPower, formatRevenue } from "../lib/format";

describe("formatPower", () => {
  it("renders sub-1000 kW without conversion", () => {
    expect(formatPower(422.7)).toBe("422.7 kW");
  });

  it("converts to MW at 1000 kW and above", () => {
    expect(formatPower(18700)).toBe("18.70 MW");
  });

  it("renders null as an em dash, never as 0", () => {
    expect(formatPower(null)).toBe("—");
  });

  it("renders a real zero as 0 kW (night-time output is a fact)", () => {
    expect(formatPower(0)).toBe("0.0 kW");
  });
});

describe("formatEnergy", () => {
  it("converts to MWh at 1000 kWh and above", () => {
    expect(formatEnergy(101200)).toBe("101.20 MWh");
  });

  it("renders null as an em dash", () => {
    expect(formatEnergy(null)).toBe("—");
  });
});

describe("formatPercent", () => {
  it("renders one decimal place", () => {
    expect(formatPercent(92.649)).toBe("92.6%");
  });

  it("renders values above 100 as-is (the expected-power model is conservative)", () => {
    expect(formatPercent(104.2)).toBe("104.2%");
  });

  it("renders null as an em dash rather than 0%", () => {
    expect(formatPercent(null)).toBe("—");
  });
});

describe("formatRevenue", () => {
  it("renders null as an em dash", () => {
    expect(formatRevenue(null)).toBe("—");
  });

  it("renders a real figure with two decimal places", () => {
    expect(formatRevenue(1320)).toBe("1,320.00");
  });
});
