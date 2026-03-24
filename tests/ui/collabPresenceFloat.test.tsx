import React from 'react';
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { act } from 'react-dom/test-utils';
import { createRoot, Root } from 'react-dom/client';
import { CollabPresenceFloat } from '../../components/CollabPresenceFloat';

const flush = () => new Promise(resolve => setTimeout(resolve, 0));

describe('CollabPresenceFloat', () => {
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

  it('renders remote editing details in a floating panel', async () => {
    const openMembers = vi.fn();

    await act(async () => {
      root.render(
        <CollabPresenceFloat
          visible
          projectName="Shared Project"
          realtimeStatus="connected"
          onlineMembersCount={2}
          remoteEditingLabel="Alice 正在编辑 Operation 1"
          members={[
            {
              connectionId: 'conn-1',
              label: 'Alice',
              email: 'alice@example.com',
              role: 'editor',
              editingNodeName: 'Operation 1',
            },
            {
              connectionId: 'conn-2',
              label: 'Bob',
              email: 'bob@example.com',
              role: 'viewer',
              editingNodeName: null,
            },
          ]}
          onOpenMembers={openMembers}
        />,
      );
      await flush();
    });

    expect(container.querySelector('[data-testid="collab-float"]')).toBeTruthy();
    expect(container.textContent).toContain('Shared Project');
    expect(container.textContent).toContain('Alice 正在编辑 Operation 1');
    expect(container.textContent).toContain('Alice');
    expect(container.textContent).toContain('Bob');
    expect(container.textContent).toContain('正在编辑 Operation 1');

    const openButton = Array.from(container.querySelectorAll('button')).find(button => button.textContent?.includes('打开成员管理'));
    expect(openButton).toBeTruthy();

    await act(async () => {
      openButton!.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });

    expect(openMembers).toHaveBeenCalledTimes(1);
  });

  it('collapses the floating panel body', async () => {
    await act(async () => {
      root.render(
        <CollabPresenceFloat
          visible
          projectName="Shared Project"
          realtimeStatus="connected"
          onlineMembersCount={1}
          remoteEditingLabel={null}
          members={[]}
        />,
      );
      await flush();
    });

    const toggle = container.querySelector('[data-testid="collab-float-toggle"]') as HTMLButtonElement | null;
    expect(toggle).toBeTruthy();
    expect(container.querySelector('[data-testid="collab-float-body"]')).toBeTruthy();

    await act(async () => {
      toggle!.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });

    expect(container.querySelector('[data-testid="collab-float-body"]')).toBeNull();
    expect(container.querySelector('[data-testid="collab-float-summary"]')?.textContent).toContain('在线 1 人');
  });
});
