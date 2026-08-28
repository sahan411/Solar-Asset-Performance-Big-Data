import { Component, type ErrorInfo, type ReactNode } from "react";

interface Props {
  children: ReactNode;
}

interface State {
  error: Error | null;
}

// Last line of defence: an offline API should only ever surface as a
// DataState error panel inside one section (handled by React Query), but a
// genuinely unexpected render error must still not take down the whole app.
export class ErrorBoundary extends Component<Props, State> {
  state: State = { error: null };

  static getDerivedStateFromError(error: Error): State {
    return { error };
  }

  componentDidCatch(error: Error, info: ErrorInfo): void {
    console.error("SolarIQ dashboard crashed", error, info.componentStack);
  }

  render() {
    if (this.state.error) {
      return (
        <div className="error-boundary">
          <h2>Something went wrong</h2>
          <p>The dashboard hit an unexpected error. Reloading the page usually fixes this.</p>
        </div>
      );
    }
    return this.props.children;
  }
}
