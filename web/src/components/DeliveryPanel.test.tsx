import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
  within,
} from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import type {
  DeliveryCommand,
  DeliveryAction,
} from "../api/deliveryClient";
import type {
  DeliveryCleanupOperationEnvelope,
  DeliveryCommandResponse,
  DeliveryMergeOperationEnvelope,
  DeliveryTask,
} from "../api/types";
import {
  initialDeliveryState,
  shouldPollDeliveryOperation,
} from "../state/deliveryModel";
import type { DeliveryPollingController } from "../state/useDeliveryPolling";
import {
  DeliveryPanel,
  type DeliveryPanelApi,
} from "./DeliveryPanel";

const TASK_ID = "11111111-1111-4111-8111-111111111111";
const TASK_B = "22222222-2222-4222-8222-222222222222";
const PREFLIGHT_ID = "33333333-3333-4333-8333-333333333333";
const CLEANUP_ID = "44444444-4444-4444-8444-444444444444";
const REQUEST_ID = "55555555-5555-4555-8555-555555555555";
const MERGE_ID = "66666666-6666-4666-8666-666666666666";
const OID_A = "1".repeat(40);
const OID_B = "2".repeat(40);
const OID_64 = "3".repeat(64);
const FINGERPRINT = "a".repeat(64);
const LONG_REF = `refs/heads/${"r".repeat(4_085)}`;
const LONG_PATH = `nested/${"p".repeat(4_089)}`;

afterEach(cleanup);

function mergeOperation(
  state: DeliveryMergeOperationEnvelope["state"] = "preflight_ready",
  version = 3,
): DeliveryMergeOperationEnvelope {
  const conflict = state === "conflict" || state === "abort_pending";
  return {
    kind: "merge",
    operation_id: PREFLIGHT_ID,
    version,
    state,
    review_generation: 7,
    workspace_fingerprint: FINGERPRINT,
    candidate_source_tree: OID_B,
    preflight_source_commit: OID_B,
    source_commit:
      state === "accepted" ||
      state === "preflight_ready" ||
      state === "preflight_pending" ||
      state === "conflict"
        ? null
        : OID_B,
    target_branch: "refs/heads/main",
    target_head: OID_A,
    conflicts: conflict
      ? {
          path_count: 3,
          paths: [
            { encoding: "utf8", path: "src/relative.rs" },
            { encoding: "base64url", path: "_w" },
          ],
          payload_bytes: 16,
          truncated: true,
        }
      : null,
    failure: state === "conflict" ? { code: "MERGE_CONFLICT" } : null,
  };
}

function cleanupOperation(
  kind: DeliveryCleanupOperationEnvelope["cleanup_kind"],
  state: DeliveryCleanupOperationEnvelope["state"],
): DeliveryCleanupOperationEnvelope {
  return {
    kind: "cleanup",
    operation_id: CLEANUP_ID,
    cleanup_kind: kind,
    version: 2,
    state,
    expected_disposition_version: 1,
    expected_merge_operation_id: MERGE_ID,
    expected_source_ref: "refs/heads/coding-agent/task",
    expected_source_oid: OID_B,
    target_branch: kind === "delete_branch" ? "refs/heads/main" : null,
    target_head: kind === "delete_branch" ? OID_A : null,
    failure: null,
  };
}

function eligibleTask(
  overrides: Partial<DeliveryTask> = {},
): DeliveryTask {
  return {
    task_id: TASK_ID,
    eligibility: "eligible",
    reasons: [],
    evidence: { review_generation: 7, workspace_fingerprint: FINGERPRINT },
    target: { available: true, branch: "refs/heads/main", head: OID_A },
    source: null,
    latest_merge: null,
    latest_cleanup: null,
    disposition: null,
    allowed_actions: ["run_preflight"],
    ...overrides,
  };
}

function mergedTask(
  allowedActions: DeliveryTask["allowed_actions"],
  worktree: NonNullable<DeliveryTask["disposition"]>["worktree"]["state"] =
    "retained_locked",
  branch: NonNullable<DeliveryTask["disposition"]>["branch"]["state"] =
    "retained",
): DeliveryTask {
  const { kind: _, ...merged } = mergeOperation("merged", 8);
  return eligibleTask({
    eligibility: "ineligible",
    reasons: ["already_merged"],
    source: {
      state: "committed",
      version: 3,
      source_ref: "refs/heads/coding-agent/task",
      source_oid: OID_B,
    },
    latest_merge: { ...merged, operation_id: MERGE_ID },
    disposition: {
      merged_operation_id: MERGE_ID,
      source_ref: "refs/heads/coding-agent/task",
      source_oid: OID_B,
      worktree: { state: worktree, version: worktree === "removed" ? 3 : 1, failure: null },
      branch: { state: branch, version: 1, failure: null },
    },
    allowed_actions: allowedActions,
  });
}

function controllerFixture(
  projection: DeliveryTask | null,
  operation: DeliveryPollingController["state"]["operation"] = null,
  phase: DeliveryPollingController["state"]["phase"] = "ready",
): DeliveryPollingController {
  const controller: DeliveryPollingController = {
    state: {
      ...initialDeliveryState,
      taskId: projection?.task_id ?? TASK_ID,
      generation: 1,
      phase,
      projection,
      operation,
      trackedOperationId:
        operation !== null && shouldPollDeliveryOperation(operation)
          ? operation.operation_id
          : null,
    },
    refresh: vi.fn(),
    trackOperation: vi.fn((next) => {
      controller.state = {
        ...controller.state,
        phase: shouldPollDeliveryOperation(next) ? "polling" : "refreshing",
        operation: next,
        trackedOperationId: shouldPollDeliveryOperation(next)
          ? next.operation_id
          : null,
      };
    }),
    openModal: vi.fn((modal) => {
      controller.state = {
        ...controller.state,
        modal: {
          ...modal,
          taskId: controller.state.taskId ?? TASK_ID,
        },
      };
    }),
    clearModal: vi.fn(() => {
      controller.state = { ...controller.state, modal: null };
    }),
  };
  return controller;
}

function command(
  action: DeliveryAction,
  execute: DeliveryCommand["execute"],
  clientRequestId = REQUEST_ID,
): DeliveryCommand {
  return { action, clientRequestId, execute };
}

function response(
  operation: DeliveryCommandResponse["operation"],
  receipt: DeliveryCommandResponse["receipt"] = "created",
): DeliveryCommandResponse {
  return { receipt, operation };
}

function apiFixture(overrides: Partial<DeliveryPanelApi> = {}) {
  const defaultMerge = command("merge", async () =>
    response(mergeOperation("accepted", 4)),
  );
  const defaultPreflight = command("preflight", async () =>
    response(mergeOperation("preflight_pending", 2)),
  );
  const defaultRemove = command("remove_worktree", async () =>
    response(cleanupOperation("remove_worktree", "unlock_pending")),
  );
  const defaultDelete = command("delete_branch", async () =>
    response(cleanupOperation("delete_branch", "delete_pending")),
  );
  const api: DeliveryPanelApi = {
    newPreflight: vi.fn(() => defaultPreflight),
    newMerge: vi.fn(() => defaultMerge),
    newRemoveWorktree: vi.fn(() => defaultRemove),
    newDeleteBranch: vi.fn(() => defaultDelete),
    ...overrides,
  };
  return api;
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

describe("DeliveryPanel", () => {
  it("shows named loading/error/retry states without inventing a projection", async () => {
    const user = userEvent.setup();
    const controller = controllerFixture(null, null, "loading");
    const api = apiFixture();
    const { rerender } = render(
      <DeliveryPanel taskId={TASK_ID} api={api} controller={controller} />,
    );

    expect(
      screen.getByRole("status", { name: "Delivery projection status" }),
    ).toHaveTextContent("Loading delivery eligibility");
    expect(screen.queryByRole("button", { name: /preflight|merge/i })).toBeNull();

    controller.state = {
      ...controller.state,
      phase: "error",
      error: {
        code: "STORE_DEGRADED",
        message: "Delivery state is temporarily unavailable.",
        retryable: true,
        requestId: "request-7",
      },
    };
    rerender(<DeliveryPanel taskId={TASK_ID} api={api} controller={controller} />);
    expect(screen.getByRole("alert")).toHaveTextContent("STORE_DEGRADED");
    await user.click(screen.getByRole("button", { name: "Retry delivery status" }));
    expect(controller.refresh).toHaveBeenCalledTimes(1);
  });

  it("renders only stable ineligibility reasons and no clickable merge", () => {
    const delivery = eligibleTask({
      eligibility: "ineligible",
      reasons: ["review_not_approved", "target_worktree_dirty"],
      evidence: null,
      target: { available: false, reason: "observation_unavailable" },
      allowed_actions: [],
    });
    render(
      <DeliveryPanel
        taskId={TASK_ID}
        api={apiFixture()}
        controller={controllerFixture(delivery)}
      />,
    );

    expect(screen.getByText("The final review is not approved.")).toBeVisible();
    expect(screen.getByText("The target worktree has local changes.")).toBeVisible();
    expect(screen.queryByRole("button", { name: /merge|preflight/i })).toBeNull();
    expect(screen.getByRole("region", { name: "Delivery" })).not.toHaveTextContent(
      /C:\\|\/tmp\/|diff --git/i,
    );
  });

  it("shows bounded evidence summary, then traps focus and returns it on Escape", async () => {
    const user = userEvent.setup();
    const delivery = eligibleTask();
    const controller = controllerFixture(delivery);
    render(
      <div className="app-shell">
        <DeliveryPanel taskId={TASK_ID} api={apiFixture()} controller={controller} />
      </div>,
    );

    const panel = screen.getByRole("region", { name: "Delivery" });
    expect(within(panel).getByText("refs/heads/main")).toBeVisible();
    expect(within(panel).getByText(OID_A)).toBeVisible();
    expect(within(panel).getByText("7")).toBeVisible();
    expect(within(panel).getByText("aaaaaaaaaaaa…")).toBeVisible();
    expect(panel).not.toHaveTextContent(FINGERPRINT);

    const trigger = screen.getByRole("button", { name: "Run delivery preflight" });
    trigger.focus();
    await user.keyboard("{Enter}");
    const dialog = screen.getByRole("dialog", {
      name: "Confirm local merge preflight",
    });
    expect(dialog).toHaveAttribute("aria-modal", "true");
    expect(within(dialog).getByText(FINGERPRINT)).toBeVisible();
    expect(within(dialog).getByText(OID_A)).toBeVisible();
    expect(document.querySelector(".app-shell")).toHaveAttribute("inert");
    const close = within(dialog).getByRole("button", { name: "Close" });
    const submit = within(dialog).getByRole("button", { name: "Run preflight" });
    expect(close).toHaveFocus();
    await user.keyboard("{Shift>}{Tab}{/Shift}");
    expect(submit).toHaveFocus();
    await user.keyboard("{Tab}");
    expect(close).toHaveFocus();
    await user.keyboard("{Escape}");
    expect(screen.queryByRole("dialog")).toBeNull();
    expect(trigger).toHaveFocus();
  });

  it("reuses one command after reply loss and disables duplicate preflight submit", async () => {
    const user = userEvent.setup();
    const first = deferred<DeliveryCommandResponse>();
    const execute = vi
      .fn<DeliveryCommand["execute"]>()
      .mockImplementationOnce(() => first.promise)
      .mockResolvedValueOnce(response(mergeOperation("preflight_pending", 2), "existing"));
    const preflight = command("preflight", execute);
    const newPreflight = vi.fn(() => preflight);
    const api = apiFixture({ newPreflight });
    const controller = controllerFixture(eligibleTask());
    render(<DeliveryPanel taskId={TASK_ID} api={api} controller={controller} />);

    await user.click(screen.getByRole("button", { name: "Run delivery preflight" }));
    const dialog = screen.getByRole("dialog");
    await user.click(within(dialog).getByRole("button", { name: "Run preflight" }));
    expect(within(dialog).getByRole("button", { name: "Running preflight…" })).toBeDisabled();
    expect(newPreflight).toHaveBeenCalledTimes(1);
    expect(execute).toHaveBeenCalledTimes(1);

    await act(async () => {
      first.reject(new TypeError("reply lost after send"));
      await Promise.resolve();
    });
    const retry = await within(dialog).findByRole("button", {
      name: "Retry preflight",
    });
    expect(within(dialog).getByText(REQUEST_ID)).toBeVisible();
    expect(controller.refresh).not.toHaveBeenCalled();
    await user.click(retry);

    await waitFor(() => expect(execute).toHaveBeenCalledTimes(2));
    expect(newPreflight).toHaveBeenCalledTimes(1);
    expect(await within(dialog).findByText("Durable receipt: existing")).toBeVisible();
    expect(controller.trackOperation).toHaveBeenCalledWith(
      expect.objectContaining({ operation_id: PREFLIGHT_ID, version: 2 }),
    );
    expect(controller.refresh).not.toHaveBeenCalled();
  });

  it("mints a fresh request after a known non-retryable preflight failure", async () => {
    const user = userEvent.setup();
    const freshRequestId = "77777777-7777-4777-8777-777777777777";
    const failed = command("preflight", async () => {
      throw Object.assign(new Error("The command was not applied."), {
        code: "DELIVERY_NOT_APPLIED",
        retryable: false,
      });
    });
    const fresh = command(
      "preflight",
      async () => response(mergeOperation("preflight_pending", 2)),
      freshRequestId,
    );
    const newPreflight = vi
      .fn<DeliveryPanelApi["newPreflight"]>()
      .mockReturnValueOnce(failed)
      .mockReturnValueOnce(fresh);
    const api = apiFixture({ newPreflight });
    const controller = controllerFixture(eligibleTask());
    render(<DeliveryPanel taskId={TASK_ID} api={api} controller={controller} />);

    await user.click(screen.getByRole("button", { name: "Run delivery preflight" }));
    let dialog = screen.getByRole("dialog");
    await user.click(within(dialog).getByRole("button", { name: "Run preflight" }));
    expect(
      await within(dialog).findByRole("button", { name: "Retry preflight" }),
    ).toBeDisabled();
    await user.click(within(dialog).getByRole("button", { name: "Close" }));

    await user.click(screen.getByRole("button", { name: "Run delivery preflight" }));
    dialog = screen.getByRole("dialog");
    await user.click(within(dialog).getByRole("button", { name: "Run preflight" }));

    expect(newPreflight).toHaveBeenCalledTimes(2);
    expect(await within(dialog).findByText(freshRequestId)).toBeVisible();
  });

  it("submits only the exact ready operation and disables a stale confirmation", async () => {
    const user = userEvent.setup();
    const ready = mergeOperation();
    const { kind: _, ...latestMerge } = ready;
    const delivery = eligibleTask({
      latest_merge: latestMerge,
      allowed_actions: ["accept_merge"],
    });
    const mergeCommand = command("merge", async () =>
      response(mergeOperation("accepted", 4)),
    );
    const newMerge = vi.fn(() => mergeCommand);
    const api = apiFixture({ newMerge });
    const controller = controllerFixture(delivery, ready);
    const { rerender } = render(
      <DeliveryPanel taskId={TASK_ID} api={api} controller={controller} />,
    );

    await user.click(
      screen.getByRole("button", { name: "Review and confirm local merge" }),
    );
    const dialog = screen.getByRole("dialog", {
      name: "Confirm exact local merge",
    });
    expect(within(dialog).getByText(PREFLIGHT_ID)).toBeVisible();
    expect(within(dialog).getByText(FINGERPRINT)).toBeVisible();
    await user.click(within(dialog).getByRole("button", { name: "Merge locally" }));

    expect(newMerge).toHaveBeenCalledWith(TASK_ID, {
      preflight_operation_id: PREFLIGHT_ID,
      expected_operation_version: 3,
      expected_review_generation: 7,
      expected_workspace_fingerprint: FINGERPRINT,
      target_branch: "refs/heads/main",
      expected_target_head: OID_A,
    });
    expect(
      await within(dialog).findByRole("button", { name: "Merge accepted" }),
    ).toBeDisabled();

    controller.state = {
      ...controller.state,
      phase: "ready",
      operation: mergeOperation("preflight_ready", 5),
      modal: null,
    };
    rerender(<DeliveryPanel taskId={TASK_ID} api={api} controller={controller} />);
    expect(
      within(dialog).getByText(/This confirmation is stale/),
    ).toBeVisible();
    expect(within(dialog).getByRole("button", { name: "Merge accepted" })).toBeDisabled();
    expect(newMerge).toHaveBeenCalledTimes(1);
  });

  it("refreshes a durable stale rejection once and replaces the old accept without reloading", async () => {
    const user = userEvent.setup();
    const ready = mergeOperation();
    const { kind: _, ...latestMerge } = ready;
    const delivery = eligibleTask({
      latest_merge: latestMerge,
      allowed_actions: ["accept_merge"],
    });
    const rejected = command("merge", async () => {
      throw Object.assign(new Error("The target HEAD changed."), {
        code: "TARGET_HEAD_CHANGED",
        retryable: false,
        requestId: "stale-request",
      });
    });
    const controller = controllerFixture(delivery, ready);
    const { rerender } = render(
      <DeliveryPanel
        taskId={TASK_ID}
        api={apiFixture({ newMerge: vi.fn(() => rejected) })}
        controller={controller}
      />,
    );

    await user.click(
      screen.getByRole("button", { name: "Review and confirm local merge" }),
    );
    const dialog = screen.getByRole("dialog", {
      name: "Confirm exact local merge",
    });
    await user.click(within(dialog).getByRole("button", { name: "Merge locally" }));

    expect(await within(dialog).findByText("TARGET_HEAD_CHANGED")).toBeVisible();
    await waitFor(() => expect(controller.refresh).toHaveBeenCalledTimes(1));

    const stale = mergeOperation("stale", 4);
    stale.failure = { code: "TARGET_HEAD_CHANGED" };
    const { kind: _staleKind, ...latestStale } = stale;
    controller.state = {
      ...controller.state,
      phase: "ready",
      projection: eligibleTask({
        latest_merge: latestStale,
        allowed_actions: ["run_preflight"],
      }),
      operation: stale,
      trackedOperationId: null,
      modal: null,
    };
    rerender(
      <DeliveryPanel
        taskId={TASK_ID}
        api={apiFixture()}
        controller={controller}
      />,
    );

    expect(
      within(dialog).getByText(/This confirmation is stale/),
    ).toBeVisible();
    await user.click(within(dialog).getByRole("button", { name: "Cancel" }));
    expect(
      screen.queryByRole("button", { name: "Review and confirm local merge" }),
    ).toBeNull();
    await user.click(
      screen.getByRole("button", { name: "Run delivery preflight" }),
    );
    expect(
      screen.getByRole("dialog", { name: "Confirm local merge preflight" }),
    ).toBeVisible();
    expect(controller.refresh).toHaveBeenCalledTimes(1);
  });

  it("uses operation state to block duplicates and shows only bounded conflict paths", async () => {
    const user = userEvent.setup();
    const accepted = mergeOperation("accepted", 4);
    const { kind: _acceptedKind, ...acceptedPayload } = accepted;
    const acceptedDelivery = eligibleTask({
      latest_merge: acceptedPayload,
      allowed_actions: [],
    });
    const controller = controllerFixture(acceptedDelivery, accepted, "polling");
    const api = apiFixture();
    const { rerender } = render(
      <DeliveryPanel taskId={TASK_ID} api={api} controller={controller} />,
    );
    expect(screen.getByRole("button", { name: "Local merge accepted" })).toBeDisabled();
    expect(screen.queryByRole("button", { name: /confirm local merge/i })).toBeNull();

    const conflict = mergeOperation("conflict", 6);
    const { kind: _conflictKind, ...conflictPayload } = conflict;
    const conflictDelivery = eligibleTask({
      latest_merge: conflictPayload,
      allowed_actions: ["run_preflight"],
    });
    controller.state = {
      ...controller.state,
      phase: "ready",
      projection: conflictDelivery,
      operation: conflict,
      trackedOperationId: null,
    };
    rerender(<DeliveryPanel taskId={TASK_ID} api={api} controller={controller} />);

    expect(screen.getByText("src/relative.rs")).toBeVisible();
    expect(screen.getByText("_w")).toBeVisible();
    expect(screen.getByText("base64url encoded")).toBeVisible();
    expect(screen.getByText(/Showing 2 of 3 conflict paths/)).toBeVisible();
    expect(screen.getByText(/truncated this bounded path summary/)).toBeVisible();
    expect(screen.queryByRole("button", { name: /edit|resolve/i })).toBeNull();
    const retry = screen.getByRole("button", {
      name: "Run delivery preflight again",
    });
    await user.click(retry);
    expect(screen.getByRole("dialog", { name: "Confirm local merge preflight" })).toBeVisible();
  });

  it("marks maximum-size refs, object IDs, and conflict paths for resilient wrapping", () => {
    const operation = mergeOperation("conflict", 6);
    operation.target_branch = LONG_REF;
    operation.target_head = OID_64;
    operation.conflicts = {
      path_count: 1,
      paths: [{ encoding: "utf8", path: LONG_PATH }],
      payload_bytes: 4_096,
      truncated: false,
    };
    const { kind: _kind, ...latestMerge } = operation;
    const projection = eligibleTask({
      target: { available: true, branch: LONG_REF, head: OID_64 },
      latest_merge: latestMerge,
      allowed_actions: ["run_preflight"],
    });

    render(
      <DeliveryPanel
        taskId={TASK_ID}
        api={apiFixture()}
        controller={controllerFixture(projection, operation)}
      />,
    );

    for (const element of screen.getAllByText(LONG_REF)) {
      expect(element).toHaveClass("delivery-long-value");
    }
    for (const element of screen.getAllByText(OID_64)) {
      expect(element).toHaveClass("delivery-long-value");
    }
    expect(screen.getByText(LONG_PATH)).toHaveClass(
      "delivery-long-value",
      "delivery-conflict-path",
    );
  });

  it("rebuilds a pending cleanup from latest_cleanup without offering a duplicate action", () => {
    const restoredOperation = cleanupOperation(
      "remove_worktree",
      "unlock_pending",
    );
    const { kind: _kind, ...latestCleanup } = restoredOperation;
    const restored = mergedTask([]);
    restored.latest_cleanup = latestCleanup;
    const api = apiFixture();

    render(
      <DeliveryPanel
        taskId={TASK_ID}
        api={api}
        controller={controllerFixture(restored, restoredOperation, "polling")}
      />,
    );

    expect(
      screen.getByRole("status", { name: "Cleanup operation status" }),
    ).toHaveTextContent("Remove worktree: unlock pending");
    expect(screen.queryByRole("button", { name: "Remove worktree" })).toBeNull();
    expect(screen.queryByRole("button", { name: "Delete source branch" })).toBeNull();
    expect(api.newRemoveWorktree).not.toHaveBeenCalled();
    expect(api.newDeleteBranch).not.toHaveBeenCalled();
  });

  it("keeps worktree and branch cleanup as separate dialogs and receipts", async () => {
    const user = userEvent.setup();
    const removeCommand = command("remove_worktree", async () =>
      response(cleanupOperation("remove_worktree", "unlock_pending"), "created"),
    );
    const deleteCommand = command("delete_branch", async () =>
      response(cleanupOperation("delete_branch", "delete_pending"), "existing"),
    );
    const newRemoveWorktree = vi.fn(() => removeCommand);
    const newDeleteBranch = vi.fn(() => deleteCommand);
    const api = apiFixture({ newRemoveWorktree, newDeleteBranch });
    const initial = mergedTask(["remove_worktree"]);
    const controller = controllerFixture(initial);
    const { rerender } = render(
      <DeliveryPanel taskId={TASK_ID} api={api} controller={controller} />,
    );

    expect(screen.getByText("Retained and locked (default)")).toBeVisible();
    expect(screen.getByText("Retained (default)")).toBeVisible();
    expect(screen.queryByRole("button", { name: "Delete source branch" })).toBeNull();
    const removeTrigger = screen.getByRole("button", { name: "Remove worktree" });
    await user.click(removeTrigger);
    let dialog = screen.getByRole("dialog", { name: "Remove local worktree?" });
    expect(dialog).toHaveTextContent("The source branch is retained");
    await user.click(
      within(dialog).getByRole("button", { name: "Remove exact local worktree" }),
    );
    expect(newRemoveWorktree).toHaveBeenCalledWith(TASK_ID, {
      expected_disposition_version: 1,
      expected_merge_operation_id: MERGE_ID,
      expected_source_ref: "refs/heads/coding-agent/task",
      expected_source_oid: OID_B,
    });
    expect(await within(dialog).findByText("Durable receipt: created")).toBeVisible();
    await user.click(within(dialog).getByRole("button", { name: "Cancel" }));
    expect(screen.getByRole("region", { name: "Delivery" })).toHaveFocus();

    const afterRemove = mergedTask(["delete_branch"], "removed");
    const completedCleanup = cleanupOperation("remove_worktree", "completed");
    const { kind: _cleanupKind, ...latestCleanup } = completedCleanup;
    afterRemove.latest_cleanup = latestCleanup;
    controller.state = {
      ...controller.state,
      phase: "ready",
      projection: afterRemove,
      operation: completedCleanup,
      trackedOperationId: null,
      modal: null,
    };
    rerender(<DeliveryPanel taskId={TASK_ID} api={api} controller={controller} />);
    expect(screen.queryByRole("button", { name: "Remove worktree" })).toBeNull();
    const deleteTrigger = screen.getByRole("button", { name: "Delete source branch" });
    await user.click(deleteTrigger);
    dialog = screen.getByRole("dialog", { name: "Delete local source branch?" });
    expect(dialog).toHaveTextContent("never deletes a remote branch");
    await user.click(
      within(dialog).getByRole("button", { name: "Delete exact local branch" }),
    );
    expect(newDeleteBranch).toHaveBeenCalledWith(TASK_ID, {
      expected_disposition_version: 1,
      expected_merge_operation_id: MERGE_ID,
      expected_source_ref: "refs/heads/coding-agent/task",
      expected_source_oid: OID_B,
      target_branch: "refs/heads/main",
      target_head: OID_A,
    });
    expect(await within(dialog).findByText("Durable receipt: existing")).toBeVisible();
    expect(screen.getByText(/Delete source branch receipt: existing/)).toBeVisible();
  });

  it("replays cleanup with one receipt ID and exposes the server request ID", async () => {
    const user = userEvent.setup();
    const execute = vi
      .fn<DeliveryCommand["execute"]>()
      .mockRejectedValueOnce(
        Object.assign(new Error("reply was lost"), {
          code: "NETWORK_ERROR",
          retryable: true,
          requestId: "cleanup-http-request",
        }),
      )
      .mockResolvedValueOnce(
        response(cleanupOperation("remove_worktree", "unlock_pending"), "existing"),
      );
    const removeCommand = command("remove_worktree", execute);
    const newRemoveWorktree = vi.fn(() => removeCommand);
    const controller = controllerFixture(mergedTask(["remove_worktree"]));
    render(
      <DeliveryPanel
        taskId={TASK_ID}
        api={apiFixture({ newRemoveWorktree })}
        controller={controller}
      />,
    );

    await user.click(screen.getByRole("button", { name: "Remove worktree" }));
    const dialog = screen.getByRole("dialog", { name: "Remove local worktree?" });
    await user.click(
      within(dialog).getByRole("button", { name: "Remove exact local worktree" }),
    );

    expect(await within(dialog).findByText("cleanup-http-request")).toBeVisible();
    expect(within(dialog).getByText(REQUEST_ID)).toBeVisible();
    await user.click(
      within(dialog).getByRole("button", { name: "Retry worktree cleanup" }),
    );

    await waitFor(() => expect(execute).toHaveBeenCalledTimes(2));
    expect(newRemoveWorktree).toHaveBeenCalledTimes(1);
    expect(await within(dialog).findByText("Durable receipt: existing")).toBeVisible();
  });

  it("aborts a stale command on task switch and ignores its late result", async () => {
    const user = userEvent.setup();
    const pending = deferred<DeliveryCommandResponse>();
    let signal: AbortSignal | undefined;
    const preflight = command("preflight", (nextSignal) => {
      signal = nextSignal;
      return pending.promise;
    });
    const api = apiFixture({ newPreflight: vi.fn(() => preflight) });
    const controllerA = controllerFixture(eligibleTask());
    const { rerender } = render(
      <DeliveryPanel taskId={TASK_ID} api={api} controller={controllerA} />,
    );
    await user.click(screen.getByRole("button", { name: "Run delivery preflight" }));
    await user.click(screen.getByRole("button", { name: "Run preflight" }));
    expect(signal?.aborted).toBe(false);

    const taskB = eligibleTask({ task_id: TASK_B });
    const controllerB = controllerFixture(taskB);
    rerender(<DeliveryPanel taskId={TASK_B} api={api} controller={controllerB} />);
    await waitFor(() => expect(signal?.aborted).toBe(true));
    await act(async () => {
      pending.resolve(response(mergeOperation("preflight_pending", 2)));
      await Promise.resolve();
    });
    expect(controllerA.trackOperation).not.toHaveBeenCalled();
    expect(controllerB.trackOperation).not.toHaveBeenCalled();
    expect(screen.queryByRole("dialog")).toBeNull();
  });

  it("does not refresh either task when an old command rejects after task switch", async () => {
    const user = userEvent.setup();
    const pending = deferred<DeliveryCommandResponse>();
    let signal: AbortSignal | undefined;
    const preflight = command("preflight", (nextSignal) => {
      signal = nextSignal;
      return pending.promise;
    });
    const api = apiFixture({ newPreflight: vi.fn(() => preflight) });
    const controllerA = controllerFixture(eligibleTask());
    const { rerender } = render(
      <DeliveryPanel taskId={TASK_ID} api={api} controller={controllerA} />,
    );
    await user.click(screen.getByRole("button", { name: "Run delivery preflight" }));
    await user.click(screen.getByRole("button", { name: "Run preflight" }));

    const controllerB = controllerFixture(eligibleTask({ task_id: TASK_B }));
    rerender(<DeliveryPanel taskId={TASK_B} api={api} controller={controllerB} />);
    await waitFor(() => expect(signal?.aborted).toBe(true));
    await act(async () => {
      pending.reject(
        Object.assign(new Error("The old target changed."), {
          code: "TARGET_HEAD_CHANGED",
          retryable: false,
        }),
      );
      await Promise.resolve();
    });

    expect(controllerA.refresh).not.toHaveBeenCalled();
    expect(controllerB.refresh).not.toHaveBeenCalled();
    expect(controllerA.trackOperation).not.toHaveBeenCalled();
    expect(controllerB.trackOperation).not.toHaveBeenCalled();
    expect(screen.queryByRole("dialog")).toBeNull();
  });

  it("keeps a visible cleanup confirmation fail-closed after disposition drift", async () => {
    const user = userEvent.setup();
    const projection = mergedTask(["remove_worktree"]);
    const controller = controllerFixture(projection);
    const api = apiFixture();
    const { rerender } = render(
      <DeliveryPanel taskId={TASK_ID} api={api} controller={controller} />,
    );
    await user.click(screen.getByRole("button", { name: "Remove worktree" }));
    const dialog = screen.getByRole("dialog", { name: "Remove local worktree?" });

    controller.state = {
      ...controller.state,
      phase: "polling",
      operation: cleanupOperation("remove_worktree", "unlock_pending"),
      trackedOperationId: CLEANUP_ID,
    };
    rerender(<DeliveryPanel taskId={TASK_ID} api={api} controller={controller} />);
    expect(within(dialog).getByRole("alert")).toHaveTextContent(
      "server state no longer allows",
    );
    expect(
      within(dialog).getByRole("button", { name: "Remove exact local worktree" }),
    ).toBeDisabled();

    controller.state = {
      ...controller.state,
      phase: "ready",
      operation: null,
      trackedOperationId: null,
    };

    const drifted = structuredClone(projection);
    if (drifted.disposition === null) throw new Error("fixture");
    drifted.disposition.worktree.version = 2;
    controller.state = { ...controller.state, projection: drifted };
    rerender(<DeliveryPanel taskId={TASK_ID} api={api} controller={controller} />);

    expect(within(dialog).getByRole("alert")).toHaveTextContent("stale");
    expect(
      within(dialog).getByRole("button", { name: "Remove exact local worktree" }),
    ).toBeDisabled();

    const unavailable = structuredClone(drifted);
    unavailable.disposition = null;
    unavailable.allowed_actions = [];
    controller.state = { ...controller.state, projection: unavailable };
    rerender(<DeliveryPanel taskId={TASK_ID} api={api} controller={controller} />);
    expect(screen.getByRole("dialog", { name: "Remove local worktree?" })).toBe(
      dialog,
    );
    expect(within(dialog).getByRole("alert")).toHaveTextContent("stale");

    fireEvent.keyDown(dialog, { key: "Escape" });
    expect(screen.queryByRole("dialog")).toBeNull();
  });

  it("disables branch deletion when the exact target changes after confirmation", async () => {
    const user = userEvent.setup();
    const projection = mergedTask(["delete_branch"], "removed");
    const controller = controllerFixture(projection);
    const api = apiFixture();
    const { rerender } = render(
      <DeliveryPanel taskId={TASK_ID} api={api} controller={controller} />,
    );

    await user.click(screen.getByRole("button", { name: "Delete source branch" }));
    const dialog = screen.getByRole("dialog", {
      name: "Delete local source branch?",
    });
    const drifted = structuredClone(projection);
    drifted.target = {
      available: true,
      branch: "refs/heads/main",
      head: "3".repeat(40),
    };
    controller.state = { ...controller.state, projection: drifted };
    rerender(<DeliveryPanel taskId={TASK_ID} api={api} controller={controller} />);

    expect(within(dialog).getByRole("alert")).toHaveTextContent("stale");
    expect(
      within(dialog).getByRole("button", { name: "Delete exact local branch" }),
    ).toBeDisabled();
    expect(api.newDeleteBranch).not.toHaveBeenCalled();
  });
});
