import { describe, expect, it } from "vitest";
import { render, screen } from "@testing-library/react";
import { AlertStatusBadge, DataStatusBadge, SeverityBadge } from "../components/StatusBadge";

describe("DataStatusBadge", () => {
  it("renders a visible Stale label, not just a color change", () => {
    render(<DataStatusBadge status="STALE" />);
    expect(screen.getByText("Stale")).toBeInTheDocument();
  });

  it("renders Live for fresh data", () => {
    render(<DataStatusBadge status="LIVE" />);
    expect(screen.getByText("Live")).toBeInTheDocument();
  });

  it("treats a missing status as No data", () => {
    render(<DataStatusBadge status={null} />);
    expect(screen.getByText("No data")).toBeInTheDocument();
  });
});

describe("SeverityBadge", () => {
  it("renders Critical with text, not color alone", () => {
    render(<SeverityBadge severity="CRITICAL" />);
    expect(screen.getByText("Critical")).toBeInTheDocument();
  });

  it("renders Warning", () => {
    render(<SeverityBadge severity="WARNING" />);
    expect(screen.getByText("Warning")).toBeInTheDocument();
  });
});

describe("AlertStatusBadge", () => {
  it("renders Active and Resolved distinctly", () => {
    const { rerender } = render(<AlertStatusBadge status="ACTIVE" />);
    expect(screen.getByText("Active")).toBeInTheDocument();
    rerender(<AlertStatusBadge status="RESOLVED" />);
    expect(screen.getByText("Resolved")).toBeInTheDocument();
  });
});
