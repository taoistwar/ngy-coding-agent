import { AuthenticatedTransport } from "./authenticatedTransport";
import type {
  DeliveryCommandResponse,
  DeliveryDeleteBranchRequest,
  DeliveryMergeRequest,
  DeliveryOperation,
  DeliveryPreflightRequest,
  DeliveryRemoveWorktreeRequest,
  DeliveryTask,
} from "./types";
import {
  validateDeliveryCommandResponse,
  validateDeliveryOperation,
  validateDeliveryTask,
} from "./deliveryValidation";
import { ValidationError } from "./validation";

export type DeliveryAction =
  | "preflight"
  | "merge"
  | "remove_worktree"
  | "delete_branch";

export interface DeliveryCommand {
  readonly action: DeliveryAction;
  readonly clientRequestId: string;
  execute(signal?: AbortSignal): Promise<DeliveryCommandResponse>;
}

export interface DeliveryClientOptions {
  transport: AuthenticatedTransport;
  randomUUID?: () => string;
}

export type NewDeliveryPreflight = Omit<
  DeliveryPreflightRequest,
  "client_request_id"
>;
export type NewDeliveryMerge = Omit<DeliveryMergeRequest, "client_request_id">;
export type NewDeliveryRemoveWorktree = Omit<
  DeliveryRemoveWorktreeRequest,
  "client_request_id"
>;
export type NewDeliveryDeleteBranch = Omit<
  DeliveryDeleteBranchRequest,
  "client_request_id"
>;

export class DeliveryClient {
  readonly #transport: AuthenticatedTransport;
  readonly #randomUUID: () => string;

  constructor(options: DeliveryClientOptions) {
    this.#transport = options.transport;
    this.#randomUUID = options.randomUUID ?? (() => globalThis.crypto.randomUUID());
  }

  taskDelivery(taskId: string, signal?: AbortSignal): Promise<DeliveryTask> {
    return this.#transport.request(
      `/api/tasks/${encodeURIComponent(taskId)}/delivery`,
      signal === undefined ? {} : { signal },
      (value) => {
        const projection = validateDeliveryTask(value);
        if (projection.task_id !== taskId) {
          throw new ValidationError(
            "$.task_id",
            "must match the requested delivery task",
          );
        }
        return projection;
      },
    );
  }

  deliveryOperation(
    operationId: string,
    signal?: AbortSignal,
  ): Promise<DeliveryOperation> {
    return this.#transport.request(
      `/api/delivery-operations/${encodeURIComponent(operationId)}`,
      signal === undefined ? {} : { signal },
      (value) => {
        const operation = validateDeliveryOperation(value);
        if (operation.operation_id !== operationId) {
          throw new ValidationError(
            "$.operation_id",
            "must match the requested delivery operation",
          );
        }
        return operation;
      },
    );
  }

  newPreflight(taskId: string, input: NewDeliveryPreflight): DeliveryCommand {
    return this.#command(
      "preflight",
      `/api/tasks/${encodeURIComponent(taskId)}/merge/preflight`,
      input,
    );
  }

  newMerge(taskId: string, input: NewDeliveryMerge): DeliveryCommand {
    return this.#command(
      "merge",
      `/api/tasks/${encodeURIComponent(taskId)}/merge`,
      input,
    );
  }

  newRemoveWorktree(
    taskId: string,
    input: NewDeliveryRemoveWorktree,
  ): DeliveryCommand {
    return this.#command(
      "remove_worktree",
      `/api/tasks/${encodeURIComponent(taskId)}/cleanup/worktree`,
      input,
    );
  }

  newDeleteBranch(
    taskId: string,
    input: NewDeliveryDeleteBranch,
  ): DeliveryCommand {
    return this.#command(
      "delete_branch",
      `/api/tasks/${encodeURIComponent(taskId)}/cleanup/branch`,
      input,
    );
  }

  #command(
    action: DeliveryAction,
    path: string,
    input: object,
  ): DeliveryCommand {
    const clientRequestId = this.#randomUUID();
    const body = { ...input, client_request_id: clientRequestId };
    return {
      action,
      clientRequestId,
      execute: (signal) =>
        this.#transport.request(
          path,
          {
            method: "POST",
            mutation: true,
            body,
            ...(signal === undefined ? {} : { signal }),
          },
          (value) => validateActionResponse(value, action, input),
        ),
    };
  }
}

function validateActionResponse(
  value: unknown,
  action: DeliveryAction,
  input: object,
): DeliveryCommandResponse {
  const response = validateDeliveryCommandResponse(value);
  const expected = input as Record<string, unknown>;
  if (action === "preflight" || action === "merge") {
    if (response.operation.kind !== "merge") {
      throw new ValidationError(
        "$.operation.kind",
        `must be merge for the ${action} action`,
      );
    }
    requireEqual(
      response.operation.target_branch,
      expected.target_branch,
      "$.operation.target_branch",
    );
    requireEqual(
      response.operation.target_head,
      expected.expected_target_head,
      "$.operation.target_head",
    );
    if (action === "merge") {
      if (
        response.operation.state === "preflight_pending" ||
        response.operation.state === "preflight_ready" ||
        response.operation.state === "rejected" ||
        response.operation.state === "stale" ||
        response.operation.state === "superseded"
      ) {
        throw new ValidationError(
          "$.operation.state",
          "must be accepted or later for the merge action",
        );
      }
      requireEqual(
        response.operation.operation_id,
        expected.preflight_operation_id,
        "$.operation.operation_id",
      );
      requireEqual(
        response.operation.review_generation,
        expected.expected_review_generation,
        "$.operation.review_generation",
      );
      requireEqual(
        response.operation.workspace_fingerprint,
        expected.expected_workspace_fingerprint,
        "$.operation.workspace_fingerprint",
      );
      if (
        typeof expected.expected_operation_version !== "number" ||
        response.operation.version <= expected.expected_operation_version
      ) {
        throw new ValidationError(
          "$.operation.version",
          "must advance beyond the accepted preflight version",
        );
      }
    }
    return response;
  }
  if (response.operation.kind !== "cleanup") {
    throw new ValidationError(
      "$.operation.kind",
      `must be cleanup for the ${action} action`,
    );
  }
  const expectedKind =
    action === "remove_worktree" ? "remove_worktree" : "delete_branch";
  if (response.operation.cleanup_kind !== expectedKind) {
    throw new ValidationError(
      "$.operation.cleanup_kind",
      `must be ${expectedKind} for the ${action} action`,
    );
  }
  requireEqual(
    response.operation.expected_disposition_version,
    expected.expected_disposition_version,
    "$.operation.expected_disposition_version",
  );
  requireEqual(
    response.operation.expected_merge_operation_id,
    expected.expected_merge_operation_id,
    "$.operation.expected_merge_operation_id",
  );
  requireEqual(
    response.operation.expected_source_ref,
    expected.expected_source_ref,
    "$.operation.expected_source_ref",
  );
  requireEqual(
    response.operation.expected_source_oid,
    expected.expected_source_oid,
    "$.operation.expected_source_oid",
  );
  if (action === "delete_branch") {
    requireEqual(
      response.operation.target_branch,
      expected.target_branch,
      "$.operation.target_branch",
    );
    requireEqual(
      response.operation.target_head,
      expected.target_head,
      "$.operation.target_head",
    );
  }
  return response;
}

function requireEqual(actual: unknown, expected: unknown, path: string): void {
  if (!Object.is(actual, expected)) {
    throw new ValidationError(path, "must match the accepted command input");
  }
}
