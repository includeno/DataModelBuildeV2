export type ProjectState = Record<string, any>;

export type ProjectPatch =
  | { op: 'set_top_level'; key: string; value: any }
  | { op: 'replace_state'; state: ProjectState };

export type CommitPayload = {
  baseVersion: number;
  clientOpId: string;
  patches?: ProjectPatch[];
  state?: ProjectState;
};

export type CommitResult = {
  version: number;
  state: ProjectState;
  conflict?: boolean;
  latestVersion?: number;
};

type ConflictResolver = (
  conflict: CommitResult,
  request: CommitPayload
) => Promise<CommitPayload | null> | CommitPayload | null;

const jsonEqual = (a: any, b: any): boolean => {
  try {
    return JSON.stringify(a) === JSON.stringify(b);
  } catch {
    return a === b;
  }
};

const cloneState = (value: ProjectState): ProjectState => JSON.parse(JSON.stringify(value || {}));

export const buildStatePatches = (prevState: ProjectState, nextState: ProjectState): ProjectPatch[] => {
  const prev = prevState || {};
  const next = nextState || {};

  const prevKeys = Object.keys(prev);
  const nextKeys = Object.keys(next);
  const deletedKey = prevKeys.find((k) => !nextKeys.includes(k));
  if (deletedKey) {
    return [{ op: 'replace_state', state: cloneState(next) }];
  }

  const patches: ProjectPatch[] = [];
  for (const key of nextKeys) {
    if (!jsonEqual(prev[key], next[key])) {
      patches.push({ op: 'set_top_level', key, value: next[key] });
    }
  }
  return patches;
};

export const applyPatch = (state: ProjectState, patch: ProjectPatch): ProjectState => {
  const base = cloneState(state || {});
  if (patch.op === 'replace_state') {
    return cloneState(patch.state || {});
  }
  base[patch.key] = patch.value;
  return base;
};

export const applyPatches = (state: ProjectState, patches: ProjectPatch[]): ProjectState => {
  return (patches || []).reduce((acc, patch) => applyPatch(acc, patch), cloneState(state || {}));
};

export const replayLocalPatchesOnRemote = (
  remoteState: ProjectState,
  localPatches: ProjectPatch[]
): ProjectState => {
  return applyPatches(remoteState || {}, localPatches || []);
};

export const buildConflictNotice = (latestVersion: number): string => {
  return `远端已更新到 v${latestVersion}，已基于最新版本重放本地修改。`;
};

export class DebouncedCommitQueue {
  private readonly commitFn: (request: CommitPayload) => Promise<CommitResult>;
  private readonly conflictResolver?: ConflictResolver;
  private readonly debounceMs: number;
  private timer: ReturnType<typeof setTimeout> | null = null;
  private pendingRequest: CommitPayload | null = null;
  private pendingResolvers: Array<(value: CommitResult) => void> = [];
  private pendingRejectors: Array<(reason?: unknown) => void> = [];
  private flushChain: Promise<void> = Promise.resolve();
  private disposed = false;

  constructor(
    commitFn: (request: CommitPayload) => Promise<CommitResult>,
    options?: { debounceMs?: number; conflictResolver?: ConflictResolver }
  ) {
    this.commitFn = commitFn;
    this.conflictResolver = options?.conflictResolver;
    this.debounceMs = options?.debounceMs ?? 500;
  }

  enqueue(request: CommitPayload): Promise<CommitResult> {
    if (this.disposed) {
      return Promise.reject(new Error('commit queue disposed'));
    }
    this.pendingRequest = request;
    if (this.timer) {
      clearTimeout(this.timer);
    }
    this.timer = setTimeout(() => {
      this.triggerFlush();
    }, this.debounceMs);

    return new Promise<CommitResult>((resolve, reject) => {
      this.pendingResolvers.push(resolve);
      this.pendingRejectors.push(reject);
    });
  }

  async flushNow(): Promise<void> {
    this.triggerFlush();
    await this.flushChain;
  }

  dispose() {
    this.disposed = true;
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
  }

  private triggerFlush() {
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    this.flushChain = this.flushChain.then(() => this.flushOnce());
  }

  private async flushOnce() {
    if (!this.pendingRequest) return;
    const request = this.pendingRequest;
    const resolvers = this.pendingResolvers.splice(0);
    const rejectors = this.pendingRejectors.splice(0);
    this.pendingRequest = null;

    try {
      let result = await this.commitFn(request);
      if (result.conflict && this.conflictResolver) {
        const replayRequest = await this.conflictResolver(result, request);
        if (replayRequest) {
          result = await this.commitFn(replayRequest);
        }
      }
      resolvers.forEach((resolve) => resolve(result));
    } catch (err) {
      rejectors.forEach((reject) => reject(err));
    }
  }
}
