import { describe, expect, it, vi } from "vitest";
import { render, screen, fireEvent } from "@testing-library/react";
import { DataState } from "../components/DataState";
import { ApiError } from "../api/client";

describe("DataState", () => {
  it("shows a loading indicator and withholds children", () => {
    render(
      <DataState isLoading isError={false}>
        <p>content</p>
      </DataState>,
    );
    expect(screen.getByRole("status")).toBeInTheDocument();
    expect(screen.queryByText("content")).not.toBeInTheDocument();
  });

  it("surfaces the API's own message on an offline/unreachable API", () => {
    render(
      <DataState isLoading={false} isError error={new ApiError(0, "Could not reach the SolarIQ API. Is it running?")}>
        <p>content</p>
      </DataState>,
    );
    expect(screen.getByRole("alert")).toHaveTextContent("Could not reach the SolarIQ API. Is it running?");
  });

  it("calls onRetry when the retry button is clicked", () => {
    const onRetry = vi.fn();
    render(
      <DataState isLoading={false} isError error={new ApiError(500, "Internal server error")} onRetry={onRetry}>
        <p>content</p>
      </DataState>,
    );
    fireEvent.click(screen.getByRole("button", { name: /retry/i }));
    expect(onRetry).toHaveBeenCalledOnce();
  });

  it("shows the empty message when there is no error but no data either", () => {
    render(
      <DataState isLoading={false} isError={false} isEmpty emptyMessage="No daily report is available for this simulated date yet.">
        <p>content</p>
      </DataState>,
    );
    expect(screen.getByText("No daily report is available for this simulated date yet.")).toBeInTheDocument();
  });

  it("renders children once loaded, present and non-empty", () => {
    render(
      <DataState isLoading={false} isError={false} isEmpty={false}>
        <p>content</p>
      </DataState>,
    );
    expect(screen.getByText("content")).toBeInTheDocument();
  });
});
