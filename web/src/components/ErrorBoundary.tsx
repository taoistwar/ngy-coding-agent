import { Component, type ErrorInfo, type ReactNode } from "react";

export interface ErrorBoundaryProps {
  children: ReactNode;
  fallback?: ReactNode;
  resetKey?: string | number;
}

interface ErrorBoundaryState {
  failed: boolean;
}

export class ErrorBoundary extends Component<
  ErrorBoundaryProps,
  ErrorBoundaryState
> {
  state: ErrorBoundaryState = { failed: false };

  static getDerivedStateFromError(): ErrorBoundaryState {
    return { failed: true };
  }

  componentDidCatch(_error: Error, _info: ErrorInfo): void {
    // The boundary deliberately keeps failures local. A future diagnostics
    // transport can report the captured error without coupling view panels to it.
  }

  componentDidUpdate(previousProps: ErrorBoundaryProps): void {
    if (
      this.state.failed &&
      previousProps.resetKey !== this.props.resetKey
    ) {
      this.setState({ failed: false });
    }
  }

  render(): ReactNode {
    if (this.state.failed) {
      return (
        this.props.fallback ?? (
          <p role="alert">This panel could not be displayed.</p>
        )
      );
    }
    return this.props.children;
  }
}
