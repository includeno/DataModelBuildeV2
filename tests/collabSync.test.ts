import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import {
  DebouncedCommitQueue,
  applyPatches,
  buildConflictNotice,
  buildStatePatches,
  replayLocalPatchesOnRemote
} from '../utils/collabSync';

describe('collab sync utilities', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it('builds top-level patches for changed keys', () => {
    const patches = buildStatePatches(
      { tree: { id: 'root', name: 'A' }, note: 'old' },
      { tree: { id: 'root', name: 'B' }, note: 'new', extra: 1 }
    );
    expect(patches).toEqual([
      { op: 'set_top_level', key: 'tree', value: { id: 'root', name: 'B' } },
      { op: 'set_top_level', key: 'note', value: 'new' },
      { op: 'set_top_level', key: 'extra', value: 1 }
    ]);
  });

  it('falls back to replace_state when keys are deleted', () => {
    const patches = buildStatePatches({ tree: { id: 'root' }, note: 'x' }, { tree: { id: 'root' } });
    expect(patches).toEqual([{ op: 'replace_state', state: { tree: { id: 'root' } } }]);
  });

  it('replays local patches on top of remote state', () => {
    const merged = replayLocalPatchesOnRemote(
      { tree: { id: 'root', name: 'Remote' }, serverOnly: true },
      [{ op: 'set_top_level', key: 'draft', value: 'local-edit' }]
    );
    expect(merged).toEqual({
      tree: { id: 'root', name: 'Remote' },
      serverOnly: true,
      draft: 'local-edit'
    });
    expect(applyPatches({ a: 1 }, [{ op: 'set_top_level', key: 'b', value: 2 }])).toEqual({ a: 1, b: 2 });
  });

  it('debounces commit requests and only sends latest payload', async () => {
    const commitFn = vi.fn().mockResolvedValue({ version: 2, state: { note: 'latest' }, conflict: false });
    const queue = new DebouncedCommitQueue(commitFn, { debounceMs: 500 });

    const p1 = queue.enqueue({ baseVersion: 1, clientOpId: 'op1', patches: [{ op: 'set_top_level', key: 'note', value: 'first' }] });
    const p2 = queue.enqueue({ baseVersion: 1, clientOpId: 'op2', patches: [{ op: 'set_top_level', key: 'note', value: 'latest' }] });

    vi.advanceTimersByTime(500);
    await queue.flushNow();
    const [r1, r2] = await Promise.all([p1, p2]);

    expect(commitFn).toHaveBeenCalledTimes(1);
    expect(commitFn.mock.calls[0][0].clientOpId).toBe('op2');
    expect(r1.version).toBe(2);
    expect(r2.version).toBe(2);
  });

  it('resolves conflict by replaying local patches on latest state', async () => {
    const commitFn = vi
      .fn()
      .mockResolvedValueOnce({
        version: 10,
        state: { tree: { id: 'root', name: 'Remote' } },
        conflict: true,
        latestVersion: 10
      })
      .mockResolvedValueOnce({
        version: 11,
        state: { tree: { id: 'root', name: 'Remote' }, note: 'local' },
        conflict: false
      });

    const queue = new DebouncedCommitQueue(commitFn, {
      debounceMs: 500,
      conflictResolver: (conflict, req) => {
        const merged = replayLocalPatchesOnRemote(conflict.state, req.patches || []);
        return {
          baseVersion: conflict.latestVersion || conflict.version,
          clientOpId: `${req.clientOpId}_replay`,
          state: merged
        };
      }
    });

    const commitPromise = queue.enqueue({
      baseVersion: 9,
      clientOpId: 'local_op_1',
      patches: [{ op: 'set_top_level', key: 'note', value: 'local' }]
    });
    vi.advanceTimersByTime(500);
    await queue.flushNow();
    const result = await commitPromise;

    expect(commitFn).toHaveBeenCalledTimes(2);
    expect(commitFn.mock.calls[1][0].baseVersion).toBe(10);
    expect(result.version).toBe(11);
    expect(buildConflictNotice(10)).toContain('v10');
  });
});
