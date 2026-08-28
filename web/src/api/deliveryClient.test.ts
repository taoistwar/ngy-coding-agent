import { describe, expect, it, vi } from "vitest";

import { AuthenticatedTransport } from "./authenticatedTransport";
import { DeliveryClient } from "./deliveryClient";
import type {
  DeliveryCommandResponse,
  DeliveryCleanupOperationEnvelope,
  DeliveryMergeOperationEnvelope,
  DeliveryTask,
} from "./types";

const TASK_ID = "11111111-1111-4111-8111-111111111111";
const OPERATION_ID = "22222222-2222-4222-8222-222222222222";
const MERGED_OPERATION_ID = "33333333-3333-4333-8333-333333333333";
const OID_A = "1".repeat(40);
const OID_B = "2".repeat(40);
const FINGERPRINT = "a".repeat(64);

const OPERATION: DeliveryMergeOperationEnvelope = {
  kind: "merge",
  operation_id: OPERATION_ID,
  version: 3,
  state: "preflight_ready",
  review_generation: 7,
  workspace_fingerprint: FINGERPRINT,
  candidate_source_tree: OID_B,
  preflight_source_commit: OID_B,
  source_commit: null,
  target_branch: "refs/heads/main",
  target_head: OID_A,
  conflicts: null,
  failure: null,
};
const COMMAND_RESPONSE: DeliveryCommandResponse = {
  receipt: "created",
  operation: OPERATION,
};
const ACCEPTED_COMMAND_RESPONSE: DeliveryCommandResponse = {
  receipt: "created",
  operation: { ...OPERATION, version: 4, state: "accepted" },
};

function cleanupOperation(
  cleanupKind: DeliveryCleanupOperationEnvelope["cleanup_kind"],
): DeliveryCleanupOperationEnvelope {
  return {
    kind: "cleanup",
    operation_id: OPERATION_ID,
    cleanup_kind: cleanupKind,
    version: 1,
    state: cleanupKind === "remove_worktree" ? "unlock_pending" : "delete_pending",
    expected_disposition_version: 1,
    expected_merge_operation_id: MERGED_OPERATION_ID,
    expected_source_ref: "refs/heads/coding-agent/task",
    expected_source_oid: OID_B,
    target_branch: cleanupKind === "delete_branch" ? "refs/heads/main" : null,
    target_head: cleanupKind === "delete_branch" ? OID_A : null,
    failure: null,
  };
}
const TASK: DeliveryTask = {
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
};

function jsonResponse(value: unknown): Response {
  return new Response(JSON.stringify(value), {
    status: 200,
    headers: { "content-type": "application/json" },
  });
}

describe("DeliveryClient", () => {
  it("validates both GET projections and forwards AbortSignal", async () => {
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValueOnce(jsonResponse(TASK))
      .mockResolvedValueOnce(jsonResponse(OPERATION));
    const transport = new AuthenticatedTransport({ fetch });
    const client = new DeliveryClient({ transport });
    const controller = new AbortController();

    await expect(client.taskDelivery(TASK_ID, controller.signal)).resolves.toEqual(TASK);
    await expect(client.deliveryOperation(OPERATION_ID)).resolves.toEqual(OPERATION);

    expect(fetch.mock.calls[0]?.[0]).toBe(`/api/tasks/${TASK_ID}/delivery`);
    expect(fetch.mock.calls[0]?.[1]).toEqual(
      expect.objectContaining({ signal: controller.signal }),
    );
    expect(fetch.mock.calls[1]?.[0]).toBe(
      `/api/delivery-operations/${OPERATION_ID}`,
    );
  });

  it("fails closed on route-ID or action-discriminant mismatches", async () => {
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValueOnce(
        jsonResponse({
          ...TASK,
          task_id: "44444444-4444-4444-8444-444444444444",
        }),
      )
      .mockResolvedValueOnce(
        jsonResponse({
          ...OPERATION,
          operation_id: "55555555-5555-4555-8555-555555555555",
        }),
      )
      .mockResolvedValueOnce(jsonResponse(COMMAND_RESPONSE));
    const transport = new AuthenticatedTransport({ fetch });
    transport.setCsrfToken("shared-csrf");
    const client = new DeliveryClient({
      transport,
      randomUUID: () => "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
    });

    await expect(client.taskDelivery(TASK_ID)).rejects.toMatchObject({
      code: "INVALID_RESPONSE",
      details: { path: "$.task_id" },
    });
    await expect(client.deliveryOperation(OPERATION_ID)).rejects.toMatchObject({
      code: "INVALID_RESPONSE",
      details: { path: "$.operation_id" },
    });
    await expect(
      client
        .newRemoveWorktree(TASK_ID, {
          expected_disposition_version: 1,
          expected_merge_operation_id: MERGED_OPERATION_ID,
          expected_source_ref: "refs/heads/coding-agent/task",
          expected_source_oid: OID_B,
        })
        .execute(),
    ).rejects.toMatchObject({
      code: "INVALID_RESPONSE",
      details: { path: "$.operation.kind" },
    });
  });

  it("holds a distinct durable request ID per action and reuses it after reply loss", async () => {
    const calls: Array<{ path: string; body: string; headers: HeadersInit }> = [];
    const replyLost = new TypeError("connection closed after send");
    const fetch = vi.fn<typeof globalThis.fetch>(async (input, init) => {
      calls.push({
        path: String(input),
        body: String(init?.body),
        headers: init?.headers ?? {},
      });
      if (calls.length === 1) throw replyLost;
      const path = String(input);
      if (path.endsWith("/cleanup/worktree")) {
        return jsonResponse({
          receipt: "created",
          operation: cleanupOperation("remove_worktree"),
        });
      }
      if (path.endsWith("/cleanup/branch")) {
        return jsonResponse({
          receipt: "created",
          operation: cleanupOperation("delete_branch"),
        });
      }
      if (path.endsWith("/merge")) {
        return jsonResponse(ACCEPTED_COMMAND_RESPONSE);
      }
      return jsonResponse(COMMAND_RESPONSE);
    });
    const ids = [
      "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa1",
      "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa2",
      "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa3",
      "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa4",
      "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa5",
    ];
    const randomUUID = vi.fn(() => ids.shift() ?? "unexpected");
    const transport = new AuthenticatedTransport({ fetch });
    transport.setCsrfToken("shared-csrf");
    const client = new DeliveryClient({ transport, randomUUID });

    const preflight = client.newPreflight(TASK_ID, {
      target_branch: "refs/heads/main",
      expected_target_head: OID_A,
    });
    const merge = client.newMerge(TASK_ID, {
      preflight_operation_id: OPERATION_ID,
      expected_operation_version: 3,
      expected_review_generation: 7,
      expected_workspace_fingerprint: FINGERPRINT,
      target_branch: "refs/heads/main",
      expected_target_head: OID_A,
    });
    const remove = client.newRemoveWorktree(TASK_ID, {
      expected_disposition_version: 1,
      expected_merge_operation_id: MERGED_OPERATION_ID,
      expected_source_ref: "refs/heads/coding-agent/task",
      expected_source_oid: OID_B,
    });
    const deleteBranch = client.newDeleteBranch(TASK_ID, {
      expected_disposition_version: 1,
      expected_merge_operation_id: MERGED_OPERATION_ID,
      expected_source_ref: "refs/heads/coding-agent/task",
      expected_source_oid: OID_B,
      target_branch: "refs/heads/main",
      target_head: OID_A,
    });

    await expect(preflight.execute()).rejects.toMatchObject({
      code: "NETWORK_ERROR",
      cause: replyLost,
    });
    await expect(preflight.execute()).resolves.toEqual(COMMAND_RESPONSE);
    await merge.execute();
    await remove.execute();
    await deleteBranch.execute();
    const freshPreflight = client.newPreflight(TASK_ID, {
      target_branch: "refs/heads/main",
      expected_target_head: OID_A,
    });

    expect(randomUUID).toHaveBeenCalledTimes(5);
    expect([
      preflight.clientRequestId,
      merge.clientRequestId,
      remove.clientRequestId,
      deleteBranch.clientRequestId,
      freshPreflight.clientRequestId,
    ]).toEqual([
      "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa1",
      "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa2",
      "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa3",
      "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa4",
      "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa5",
    ]);
    expect(calls.map(({ path }) => path)).toEqual([
      `/api/tasks/${TASK_ID}/merge/preflight`,
      `/api/tasks/${TASK_ID}/merge/preflight`,
      `/api/tasks/${TASK_ID}/merge`,
      `/api/tasks/${TASK_ID}/cleanup/worktree`,
      `/api/tasks/${TASK_ID}/cleanup/branch`,
    ]);
    expect(calls[1]?.body).toBe(calls[0]?.body);
    expect(JSON.parse(calls[0]?.body ?? "null").client_request_id).toBe(
      preflight.clientRequestId,
    );
    expect(JSON.parse(calls[2]?.body ?? "null").client_request_id).toBe(
      merge.clientRequestId,
    );
    for (const call of calls) {
      expect(call.headers).toEqual(
        expect.objectContaining({ "x-csrf-token": "shared-csrf" }),
      );
    }
  });
});
