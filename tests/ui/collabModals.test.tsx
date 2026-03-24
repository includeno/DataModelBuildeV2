import React from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { act } from 'react-dom/test-utils';
import { createRoot, Root } from 'react-dom/client';

import { ConflictNoticeModal } from '../../components/ConflictNoticeModal';
import { DraftRecoveryModal } from '../../components/DraftRecoveryModal';
import { SqlPreviewModal } from '../../components/command-editor/SqlPreviewModal';

const flush = () => new Promise((resolve) => setTimeout(resolve, 0));

describe('collaboration modals', () => {
  let container: HTMLDivElement;
  let root: Root;

  beforeEach(() => {
    container = document.createElement('div');
    document.body.appendChild(container);
    root = createRoot(container);
  });

  afterEach(() => {
    root.unmount();
    container.remove();
    vi.restoreAllMocks();
  });

  it('renders conflict details and fires actions', async () => {
    const onClose = vi.fn();
    const onSyncNow = vi.fn();

    await act(async () => {
      root.render(
        <ConflictNoticeModal
          isOpen={true}
          conflict={{
            latestVersion: 9,
            remoteState: { tree: { id: 'root' }, datasets: [], sqlHistory: [], metadata: { displayName: '', settings: { cascadeDisable: false, panelPosition: 'right' } } },
            pendingPatchesCount: 3,
            message: 'Remote project has newer changes',
          }}
          onClose={onClose}
          onSyncNow={onSyncNow}
        />
      );
      await flush();
    });

    expect(container.textContent).toContain('Sync conflict notice');
    expect(container.textContent).toContain('Remote project has newer changes');
    expect(container.textContent).toContain('v9');
    expect(container.textContent).toContain('3');

    const buttons = Array.from(container.querySelectorAll('button'));
    await act(async () => {
      buttons[0].dispatchEvent(new MouseEvent('click', { bubbles: true }));
      buttons[2].dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });

    expect(onClose).toHaveBeenCalledTimes(1);
    expect(onSyncNow).toHaveBeenCalledTimes(1);
  });

  it('hides conflict modal when closed or missing conflict payload', async () => {
    await act(async () => {
      root.render(<ConflictNoticeModal isOpen={false} conflict={null} onClose={() => {}} onSyncNow={() => {}} />);
      await flush();
    });

    expect(container.textContent).toBe('');
  });

  it('renders draft recovery details and fires restore/discard actions', async () => {
    const onRestore = vi.fn();
    const onDiscard = vi.fn();
    const dateSpy = vi.spyOn(Date.prototype, 'toLocaleString').mockReturnValue('2026/03/19 10:00:00');
    const timeSpy = vi.spyOn(Date.prototype, 'toLocaleTimeString').mockReturnValue('10:00:00');

    await act(async () => {
      root.render(
        <DraftRecoveryModal
          isOpen={true}
          draft={{
            projectId: 'prj_1',
            version: 4,
            snapshot: { tree: { id: 'root' }, datasets: [], sqlHistory: [], metadata: { displayName: '', settings: { cascadeDisable: false, panelPosition: 'right' } } },
            savedAt: 1700000000000,
          }}
          onRestore={onRestore}
          onDiscard={onDiscard}
        />
      );
      await flush();
    });

    expect(container.textContent).toContain('Recover local draft');
    expect(container.textContent).toContain('Draft version base: v4');
    expect(container.textContent).toContain('2026/03/19 10:00:00');
    expect(container.textContent).toContain('10:00:00');

    const buttons = Array.from(container.querySelectorAll('button'));
    await act(async () => {
      buttons[0].dispatchEvent(new MouseEvent('click', { bubbles: true }));
      buttons[1].dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });

    expect(onDiscard).toHaveBeenCalledTimes(1);
    expect(onRestore).toHaveBeenCalledTimes(1);
    dateSpy.mockRestore();
    timeSpy.mockRestore();
  });

  it('renders SQL preview states and copies generated SQL', async () => {
    const onClose = vi.fn();
    const clipboardSpy = vi.fn().mockResolvedValue(undefined);
    Object.assign(navigator, {
      clipboard: {
        writeText: clipboardSpy,
      },
    });

    await act(async () => {
      root.render(
        <SqlPreviewModal
          isOpen={true}
          onClose={onClose}
          sql="select * from customers"
          loading={false}
        />
      );
      await flush();
    });

    expect(container.textContent).toContain('Generated SQL');
    expect(container.textContent).toContain('select * from customers');

    let buttons = Array.from(container.querySelectorAll('button'));
    await act(async () => {
      buttons[1].dispatchEvent(new MouseEvent('click', { bubbles: true }));
      buttons[2].dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });
    expect(onClose).toHaveBeenCalledTimes(1);
    expect(clipboardSpy).toHaveBeenCalledWith('select * from customers');

    await act(async () => {
      root.render(
        <SqlPreviewModal
          isOpen={true}
          onClose={onClose}
          sql=""
          loading={true}
        />
      );
      await flush();
    });
    expect(container.textContent).toContain('Generating SQL from logic');

    await act(async () => {
      root.render(
        <SqlPreviewModal
          isOpen={true}
          onClose={onClose}
          sql=""
          loading={false}
        />
      );
      await flush();
    });
    expect(container.textContent).toContain('-- No SQL generated');
  });

  it('does not render draft recovery or SQL preview when closed', async () => {
    await act(async () => {
      root.render(
        <>
          <DraftRecoveryModal isOpen={false} draft={null} onRestore={() => {}} onDiscard={() => {}} />
          <SqlPreviewModal isOpen={false} onClose={() => {}} sql="ignored" loading={false} />
        </>
      );
      await flush();
    });

    expect(container.textContent).toBe('');
  });
});
