import React from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { act } from 'react-dom/test-utils';
import { createRoot, Root } from 'react-dom/client';

import { ProjectMembersModal } from '../../components/ProjectMembersModal';
import { SessionSettingsModal } from '../../components/SessionSettingsModal';
import type { ProjectMember } from '../../types';

const flush = () => new Promise((resolve) => setTimeout(resolve, 0));

const setInputValue = (element: HTMLInputElement, value: string) => {
  const setter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')?.set;
  setter?.call(element, value);
  element.dispatchEvent(new Event('input', { bubbles: true }));
};

const setSelectValue = (element: HTMLSelectElement, value: string) => {
  const setter = Object.getOwnPropertyDescriptor(HTMLSelectElement.prototype, 'value')?.set;
  setter?.call(element, value);
  element.dispatchEvent(new Event('change', { bubbles: true }));
};

describe('member and settings modals', () => {
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

  it('manages invites, role updates, removals and loading states in project members modal', async () => {
    const onInvite = vi.fn().mockResolvedValue(undefined);
    const onUpdateRole = vi.fn().mockResolvedValue(undefined);
    const onRemoveMember = vi.fn().mockResolvedValue(undefined);
    vi.spyOn(Date.prototype, 'toLocaleDateString').mockReturnValue('2026/03/19');
    vi.stubGlobal('confirm', vi.fn().mockReturnValue(true));

    const members: ProjectMember[] = [
      {
        userId: 'usr_owner',
        email: 'owner@example.com',
        displayName: 'Owner',
        role: 'owner',
        createdAt: 1,
        updatedAt: 2,
      },
      {
        userId: 'usr_editor',
        email: 'editor@example.com',
        displayName: 'Editor',
        role: 'editor',
        createdAt: 3,
        updatedAt: 4,
      },
    ];

    await act(async () => {
      root.render(
        <ProjectMembersModal
          isOpen={true}
          onClose={() => {}}
          projectName="Coverage Project"
          members={members}
          canManage={true}
          error="temporary error"
          loading={true}
          onInvite={onInvite}
          onUpdateRole={onUpdateRole}
          onRemoveMember={onRemoveMember}
        />
      );
      await flush();
    });

    expect(container.textContent).toContain('Project Members');
    expect(container.textContent).toContain('Coverage Project');
    expect(container.textContent).toContain('temporary error');
    expect(container.textContent).toContain('Loading members');
    expect(container.textContent).toContain('joined 2026/03/19');

    const emailInput = container.querySelector('input[type="email"]') as HTMLInputElement;
    const selects = Array.from(container.querySelectorAll('select')) as HTMLSelectElement[];
    await act(async () => {
      setInputValue(emailInput, 'teammate@example.com');
      setSelectValue(selects[0], 'admin');
      await flush();
    });

    const inviteButton = Array.from(container.querySelectorAll('button')).find((button) => button.textContent?.includes('邀请'));
    await act(async () => {
      inviteButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });
    expect(onInvite).toHaveBeenCalledWith('teammate@example.com', 'admin');

    await act(async () => {
      setSelectValue(selects[2], 'admin');
      await flush();
    });
    expect(onUpdateRole).toHaveBeenCalledWith(expect.objectContaining({ userId: 'usr_editor' }), 'admin');

    const removeButton = Array.from(container.querySelectorAll('button')).find((button) => button.getAttribute('title') === 'Remove member');
    await act(async () => {
      removeButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });
    expect(onRemoveMember).toHaveBeenCalledWith(expect.objectContaining({ userId: 'usr_editor' }));
  });

  it('renders empty member state and ignores disabled invite', async () => {
    const onInvite = vi.fn().mockResolvedValue(undefined);

    await act(async () => {
      root.render(
        <ProjectMembersModal
          isOpen={true}
          onClose={() => {}}
          members={[]}
          canManage={true}
          onInvite={onInvite}
          onUpdateRole={vi.fn().mockResolvedValue(undefined)}
          onRemoveMember={vi.fn().mockResolvedValue(undefined)}
        />
      );
      await flush();
    });

    expect(container.textContent).toContain('No members found');
    const inviteButton = Array.from(container.querySelectorAll('button')).find((button) => button.textContent?.includes('邀请'));
    await act(async () => {
      inviteButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });
    expect(onInvite).not.toHaveBeenCalled();
  });

  it('edits session settings, enforces name length and handles save success/failure', async () => {
    const onClose = vi.fn();
    const onSave = vi.fn().mockResolvedValue(undefined);
    const alertSpy = vi.spyOn(window, 'alert').mockImplementation(() => {});

    await act(async () => {
      root.render(
        <SessionSettingsModal
          isOpen={true}
          projectId="prj_coverage"
          initialDisplayName="Initial Project"
          initialSettings={{ cascadeDisable: false, panelPosition: 'right' }}
          onClose={onClose}
          onSave={onSave}
        />
      );
      await flush();
    });

    expect(container.textContent).toContain('Project Settings');
    expect(container.textContent).toContain('prj_coverage');

    const nameInput = container.querySelector('input[type="text"]') as HTMLInputElement;
    await act(async () => {
      setInputValue(nameInput, 'A'.repeat(31));
      await flush();
    });
    expect(nameInput.value).toBe('Initial Project');

    await act(async () => {
      setInputValue(nameInput, 'Renamed Project');
      await flush();
    });
    expect(nameInput.value).toBe('Renamed Project');

    const cascadeCard = container.querySelector('div.cursor-pointer.transition-colors.mb-4') as HTMLDivElement | null;
    const positionButton = Array.from(container.querySelectorAll('button')).find((button) => button.textContent?.includes('Left'));
    await act(async () => {
      cascadeCard?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      positionButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });

    const saveButton = Array.from(container.querySelectorAll('button')).find((button) => button.textContent?.includes('Save Settings'));
    await act(async () => {
      saveButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });
    expect(onSave).toHaveBeenCalledWith('Renamed Project', expect.objectContaining({ panelPosition: 'left' }));
    expect(onClose).toHaveBeenCalledTimes(1);

    onSave.mockRejectedValueOnce(new Error('save failed'));
    await act(async () => {
      root.render(
        <SessionSettingsModal
          isOpen={true}
          sessionId="sess_legacy"
          initialDisplayName="Legacy"
          initialSettings={{ cascadeDisable: false, panelPosition: 'right' }}
          onClose={onClose}
          onSave={onSave}
        />
      );
      await flush();
    });

    const legacySaveButton = Array.from(container.querySelectorAll('button')).find((button) => button.textContent?.includes('Save Settings'));
    await act(async () => {
      legacySaveButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });
    expect(alertSpy).toHaveBeenCalledWith('Failed to save settings');
  });
});
