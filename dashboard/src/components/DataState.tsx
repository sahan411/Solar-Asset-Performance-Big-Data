import type { ReactNode } from "react";
import { ApiError } from "../api/client";

interface DataStateProps {
  isLoading: boolean;
  isError: boolean;
  error?: unknown;
  isEmpty?: boolean;
  emptyMessage?: string;
  loadingLabel?: string;
  onRetry?: () => void;
  children: ReactNode;
}

// The one place loading/error/empty rendering happens, so every panel on the
// dashboard handles all three states the same way instead of each inventing
// its own (or, worse, silently rendering nothing).
export function DataState({
  isLoading,
  isError,
  error,
  isEmpty = false,
  emptyMessage = "No data available.",
  loadingLabel = "Loading…",
  onRetry,
  children,
}: DataStateProps) {
  if (isLoading) {
    return (
      <div className="data-state data-state--loading" role="status" aria-live="polite">
        {loadingLabel}
      </div>
    );
  }

  if (isError) {
    const message = error instanceof ApiError ? error.message : "Unable to load data.";
    return (
      <div className="data-state data-state--error" role="alert">
        <p>{message}</p>
        {onRetry && (
          <button type="button" className="button" onClick={onRetry}>
            Retry
          </button>
        )}
      </div>
    );
  }

  if (isEmpty) {
    return <div className="data-state data-state--empty">{emptyMessage}</div>;
  }

  return <>{children}</>;
}
