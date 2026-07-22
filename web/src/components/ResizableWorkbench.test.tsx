import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest";

import {
  ResizableWorkbench,
  WORKBENCH_WIDTHS_STORAGE_KEY,
} from "./ResizableWorkbench";

const storageValues = new Map<string, string>();
const memoryStorage: Storage = {
  get length() {
    return storageValues.size;
  },
  clear: () => storageValues.clear(),
  getItem: (key) => storageValues.get(key) ?? null,
  key: (index) => [...storageValues.keys()][index] ?? null,
  removeItem: (key) => storageValues.delete(key),
  setItem: (key, value) => storageValues.set(key, value),
};

beforeAll(() => {
  Object.defineProperty(window, "localStorage", {
    configurable: true,
    value: memoryStorage,
  });
});
beforeEach(() => window.localStorage.clear());
afterEach(() => {
  cleanup();
  window.localStorage.clear();
});
afterAll(() => Reflect.deleteProperty(window, "localStorage"));

function renderWorkbench() {
  render(
    <ResizableWorkbench>
      <nav>Repositories</nav>
      <main>Workspace</main>
      <aside>Results</aside>
    </ResizableWorkbench>,
  );
  const grid = screen.getByText("Workspace").parentElement;
  if (!(grid instanceof HTMLDivElement)) throw new Error("workbench grid missing");
  Object.defineProperty(grid, "getBoundingClientRect", {
    value: () => ({
      x: 0,
      y: 0,
      top: 0,
      right: 1600,
      bottom: 900,
      left: 0,
      width: 1600,
      height: 900,
      toJSON: () => ({}),
    }),
  });
  return grid;
}

describe("ResizableWorkbench", () => {
  it("resizes both outer columns by pointer and keyboard", () => {
    const grid = renderWorkbench();
    const sidebar = screen.getByRole("separator", {
      name: "Resize repositories column",
    });
    const results = screen.getByRole("separator", {
      name: "Resize results column",
    });

    fireEvent.pointerDown(sidebar, { button: 0, clientX: 304, pointerId: 1 });
    fireEvent.pointerMove(sidebar, { clientX: 384, pointerId: 1 });
    fireEvent.pointerUp(sidebar, { clientX: 384, pointerId: 1 });
    expect(grid.style.getPropertyValue("--sidebar-width")).toBe("384px");

    fireEvent.keyDown(results, { key: "ArrowLeft" });
    expect(grid.style.getPropertyValue("--results-width")).toBe("416px");
    expect(sidebar).toHaveAttribute("aria-valuenow", "384");
    expect(results).toHaveAttribute("aria-valuenow", "416");
  });

  it("restores saved widths and persists later changes", async () => {
    window.localStorage.setItem(
      WORKBENCH_WIDTHS_STORAGE_KEY,
      JSON.stringify({ sidebar: 360, results: 440 }),
    );
    const grid = renderWorkbench();

    expect(grid.style.getPropertyValue("--sidebar-width")).toBe("360px");
    expect(grid.style.getPropertyValue("--results-width")).toBe("440px");
    fireEvent.keyDown(
      screen.getByRole("separator", { name: "Resize repositories column" }),
      { key: "ArrowRight" },
    );

    await waitFor(() =>
      expect(JSON.parse(window.localStorage.getItem(WORKBENCH_WIDTHS_STORAGE_KEY) ?? "")).toEqual({
        sidebar: 376,
        results: 440,
      }),
    );
  });

  it("supports keyboard limits and double-click reset", () => {
    const grid = renderWorkbench();
    const sidebar = screen.getByRole("separator", {
      name: "Resize repositories column",
    });
    const results = screen.getByRole("separator", {
      name: "Resize results column",
    });

    fireEvent.keyDown(sidebar, { key: "Home" });
    fireEvent.keyDown(results, { key: "End" });
    expect(grid.style.getPropertyValue("--sidebar-width")).toBe("256px");
    expect(grid.style.getPropertyValue("--results-width")).toBe("336px");

    fireEvent.doubleClick(sidebar);
    fireEvent.doubleClick(results);
    expect(grid.style.getPropertyValue("--sidebar-width")).toBe("304px");
    expect(grid.style.getPropertyValue("--results-width")).toBe("400px");
  });
});
