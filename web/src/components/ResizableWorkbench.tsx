import {
  useEffect,
  useLayoutEffect,
  useRef,
  useState,
  type CSSProperties,
  type KeyboardEvent,
  type PointerEvent,
  type ReactNode,
} from "react";

const DEFAULT_SIDEBAR_WIDTH = 304;
const DEFAULT_RESULTS_WIDTH = 400;
const MIN_SIDEBAR_WIDTH = 256;
const MIN_WORKSPACE_WIDTH = 400;
const MIN_RESULTS_WIDTH = 336;
const DESKTOP_LAYOUT_MIN_WIDTH = 1185;
const KEYBOARD_RESIZE_STEP = 16;

export const WORKBENCH_WIDTHS_STORAGE_KEY = "ngy.workbench.column-widths.v1";

interface ColumnWidths {
  sidebar: number;
  results: number;
}

interface DragState extends ColumnWidths {
  side: "sidebar" | "results";
  pointerId: number;
  startX: number;
  containerWidth: number;
}

export interface ResizableWorkbenchProps {
  children: ReactNode;
  inert?: boolean;
}

function clamp(value: number, minimum: number, maximum: number): number {
  return Math.min(Math.max(value, minimum), Math.max(minimum, maximum));
}

function validStoredWidth(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value) && value > 0;
}

function initialWidths(): ColumnWidths {
  try {
    const stored = window.localStorage.getItem(WORKBENCH_WIDTHS_STORAGE_KEY);
    if (stored === null) {
      return { sidebar: DEFAULT_SIDEBAR_WIDTH, results: DEFAULT_RESULTS_WIDTH };
    }
    const candidate: unknown = JSON.parse(stored);
    if (
      typeof candidate === "object" &&
      candidate !== null &&
      "sidebar" in candidate &&
      "results" in candidate &&
      validStoredWidth(candidate.sidebar) &&
      validStoredWidth(candidate.results)
    ) {
      return { sidebar: candidate.sidebar, results: candidate.results };
    }
  } catch {
    // Storage can be unavailable or contain data from an incompatible version.
  }
  return { sidebar: DEFAULT_SIDEBAR_WIDTH, results: DEFAULT_RESULTS_WIDTH };
}

function fitWidths(widths: ColumnWidths, containerWidth: number): ColumnWidths {
  const width = Math.max(containerWidth, DESKTOP_LAYOUT_MIN_WIDTH);
  const sidebar = clamp(
    widths.sidebar,
    MIN_SIDEBAR_WIDTH,
    width - MIN_WORKSPACE_WIDTH - MIN_RESULTS_WIDTH,
  );
  const results = clamp(
    widths.results,
    MIN_RESULTS_WIDTH,
    width - MIN_WORKSPACE_WIDTH - sidebar,
  );
  return { sidebar, results };
}

function sameWidths(left: ColumnWidths, right: ColumnWidths): boolean {
  return left.sidebar === right.sidebar && left.results === right.results;
}

export function ResizableWorkbench({
  children,
  inert = false,
}: ResizableWorkbenchProps) {
  const gridRef = useRef<HTMLDivElement>(null);
  const dragRef = useRef<DragState | null>(null);
  const [widths, setWidths] = useState<ColumnWidths>(initialWidths);
  const [layoutWidth, setLayoutWidth] = useState(() =>
    Math.max(window.innerWidth, DESKTOP_LAYOUT_MIN_WIDTH),
  );

  const measuredWidth = () => {
    const width = gridRef.current?.getBoundingClientRect().width ?? 0;
    return width > 0 ? width : Math.max(window.innerWidth, DESKTOP_LAYOUT_MIN_WIDTH);
  };

  useLayoutEffect(() => {
    const grid = gridRef.current;
    if (grid === null) return;

    const measure = () => {
      const width = grid.getBoundingClientRect().width;
      if (width <= 0) return;
      setLayoutWidth(width);
      if (width >= DESKTOP_LAYOUT_MIN_WIDTH) {
        setWidths((current) => {
          const fitted = fitWidths(current, width);
          return sameWidths(current, fitted) ? current : fitted;
        });
      }
    };
    measure();
    if (typeof ResizeObserver !== "undefined") {
      const observer = new ResizeObserver(measure);
      observer.observe(grid);
      return () => observer.disconnect();
    }
    window.addEventListener("resize", measure);
    return () => window.removeEventListener("resize", measure);
  }, []);

  useEffect(() => {
    try {
      window.localStorage.setItem(WORKBENCH_WIDTHS_STORAGE_KEY, JSON.stringify(widths));
    } catch {
      // Resizing remains available when persistent browser storage is disabled.
    }
  }, [widths]);

  const resizeSidebar = (desiredWidth: number, containerWidth = measuredWidth()) => {
    setWidths((current) => ({
      ...current,
      sidebar: clamp(
        desiredWidth,
        MIN_SIDEBAR_WIDTH,
        containerWidth - MIN_WORKSPACE_WIDTH - current.results,
      ),
    }));
  };

  const resizeResults = (desiredWidth: number, containerWidth = measuredWidth()) => {
    setWidths((current) => ({
      ...current,
      results: clamp(
        desiredWidth,
        MIN_RESULTS_WIDTH,
        containerWidth - MIN_WORKSPACE_WIDTH - current.sidebar,
      ),
    }));
  };

  const startDrag = (
    side: DragState["side"],
    event: PointerEvent<HTMLDivElement>,
  ) => {
    if (event.button !== 0) return;
    event.preventDefault();
    dragRef.current = {
      side,
      pointerId: event.pointerId,
      startX: event.clientX,
      containerWidth: measuredWidth(),
      ...widths,
    };
    try {
      event.currentTarget.setPointerCapture(event.pointerId);
    } catch {
      // Pointer capture is absent in some embedded browsers and test DOMs.
    }
  };

  const continueDrag = (event: PointerEvent<HTMLDivElement>) => {
    const drag = dragRef.current;
    if (drag === null || drag.pointerId !== event.pointerId) return;
    event.preventDefault();
    const delta = event.clientX - drag.startX;
    if (drag.side === "sidebar") {
      resizeSidebar(drag.sidebar + delta, drag.containerWidth);
    } else {
      resizeResults(drag.results - delta, drag.containerWidth);
    }
  };

  const finishDrag = (event: PointerEvent<HTMLDivElement>) => {
    if (dragRef.current?.pointerId !== event.pointerId) return;
    dragRef.current = null;
    try {
      event.currentTarget.releasePointerCapture(event.pointerId);
    } catch {
      // The pointer may already have been released by the browser.
    }
  };

  const resizeWithKeyboard = (
    side: DragState["side"],
    event: KeyboardEvent<HTMLDivElement>,
  ) => {
    const width = measuredWidth();
    let dividerDelta: number | null = null;
    if (event.key === "ArrowLeft") dividerDelta = -KEYBOARD_RESIZE_STEP;
    if (event.key === "ArrowRight") dividerDelta = KEYBOARD_RESIZE_STEP;
    if (event.key === "Home") dividerDelta = Number.NEGATIVE_INFINITY;
    if (event.key === "End") dividerDelta = Number.POSITIVE_INFINITY;
    if (dividerDelta === null) return;
    event.preventDefault();

    if (side === "sidebar") {
      const desired =
        dividerDelta === Number.NEGATIVE_INFINITY
          ? MIN_SIDEBAR_WIDTH
          : dividerDelta === Number.POSITIVE_INFINITY
            ? width
            : widths.sidebar + dividerDelta;
      resizeSidebar(desired, width);
    } else {
      const desired =
        dividerDelta === Number.NEGATIVE_INFINITY
          ? width
          : dividerDelta === Number.POSITIVE_INFINITY
            ? MIN_RESULTS_WIDTH
            : widths.results - dividerDelta;
      resizeResults(desired, width);
    }
  };

  const sidebarMaximum = Math.max(
    MIN_SIDEBAR_WIDTH,
    layoutWidth - MIN_WORKSPACE_WIDTH - widths.results,
  );
  const resultsMaximum = Math.max(
    MIN_RESULTS_WIDTH,
    layoutWidth - MIN_WORKSPACE_WIDTH - widths.sidebar,
  );
  const style = {
    "--sidebar-width": `${widths.sidebar}px`,
    "--results-width": `${widths.results}px`,
  } as CSSProperties;

  return (
    <div
      ref={gridRef}
      className="workbench-grid"
      style={style}
      inert={inert ? true : undefined}
    >
      {children}
      <div
        className="column-resize-handle sidebar-resize-handle"
        role="separator"
        aria-label="Resize repositories column"
        aria-orientation="vertical"
        aria-valuemin={MIN_SIDEBAR_WIDTH}
        aria-valuemax={Math.round(sidebarMaximum)}
        aria-valuenow={Math.round(widths.sidebar)}
        tabIndex={0}
        title="Drag to resize. Double-click to reset."
        onPointerDown={(event) => startDrag("sidebar", event)}
        onPointerMove={continueDrag}
        onPointerUp={finishDrag}
        onPointerCancel={finishDrag}
        onLostPointerCapture={() => {
          dragRef.current = null;
        }}
        onKeyDown={(event) => resizeWithKeyboard("sidebar", event)}
        onDoubleClick={() => resizeSidebar(DEFAULT_SIDEBAR_WIDTH)}
      />
      <div
        className="column-resize-handle results-resize-handle"
        role="separator"
        aria-label="Resize results column"
        aria-orientation="vertical"
        aria-valuemin={MIN_RESULTS_WIDTH}
        aria-valuemax={Math.round(resultsMaximum)}
        aria-valuenow={Math.round(widths.results)}
        tabIndex={0}
        title="Drag to resize. Double-click to reset."
        onPointerDown={(event) => startDrag("results", event)}
        onPointerMove={continueDrag}
        onPointerUp={finishDrag}
        onPointerCancel={finishDrag}
        onLostPointerCapture={() => {
          dragRef.current = null;
        }}
        onKeyDown={(event) => resizeWithKeyboard("results", event)}
        onDoubleClick={() => resizeResults(DEFAULT_RESULTS_WIDTH)}
      />
    </div>
  );
}
