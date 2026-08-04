export class SchedulerSnapshotError extends Error {
  readonly path: string;

  constructor(path: string, message: string, options?: ErrorOptions) {
    super(`${path}: ${message}`, options);
    this.name = "SchedulerSnapshotError";
    this.path = path;
  }
}

export function fail(path: string, message: string): never {
  throw new SchedulerSnapshotError(path, message);
}
