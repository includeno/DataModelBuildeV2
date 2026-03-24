import React from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { act } from 'react';
import { createRoot, Root } from 'react-dom/client';

import App from '../../App';
import { api } from '../../utils/api';
import type { ProjectMetadata, ProjectSnapshot } from '../../types';
import { createEmptyProjectSnapshot, getProjectDraftStorageKey } from '../../utils/projectStore';

const flush = () => new Promise((resolve) => setTimeout(resolve, 0));

const waitFor = async (check: () => boolean, attempts = 20) => {
  for (let i = 0; i < attempts; i += 1) {
    if (check()) return;
    await act(async () => {
      await flush();
    });
  }
  throw new Error('Condition not met in time');
};

describe('App project flows', () => {
  let container: HTMLDivElement;
  let root: Root;
  let projects: ProjectMetadata[];

  const makeProject = (id: string, name: string, updatedAt = Date.now()): ProjectMetadata => ({
    id,
    name,
    role: 'owner',
    createdAt: updatedAt - 1000,
    updatedAt,
  });

  const configureAppMocks = () => {
    vi.spyOn(api, 'ping').mockResolvedValue(true);
    vi.spyOn(api, 'get').mockImplementation(async (_cfg: any, endpoint: string) => {
      if (endpoint === '/projects') return projects;
      const projectId = endpoint.split('/')[2];
      if (endpoint.endsWith('/state')) {
        const snapshot = createEmptyProjectSnapshot(projects.find((project) => project.id === projectId)?.name || '');
        return {
          version: 3,
          updatedAt: Date.now(),
          state: {
            tree: snapshot.tree,
            datasets: snapshot.datasets,
            sqlHistory: snapshot.sqlHistory,
          },
        };
      }
      if (endpoint.endsWith('/metadata')) {
        return {
          displayName: projects.find((project) => project.id === projectId)?.name || projectId,
          settings: {
            cascadeDisable: false,
            panelPosition: 'right',
          },
        };
      }
      if (endpoint.endsWith('/datasets')) return [];
      if (endpoint.endsWith('/members')) {
        return [
          {
            userId: 'usr_owner',
            email: 'owner@example.com',
            displayName: 'Owner',
            role: 'owner',
            createdAt: Date.now(),
            updatedAt: Date.now(),
          },
        ];
      }
      if (endpoint.endsWith('/jobs')) return [];
      return {} as any;
    });
    vi.spyOn(api, 'post').mockImplementation(async (_cfg: any, endpoint: string, body?: any) => {
      if (endpoint === '/projects') {
        const created = makeProject('proj_3', 'New Project', Date.now() + 3000);
        projects = [created, ...projects];
        return created as any;
      }
      if (endpoint.endsWith('/metadata')) {
        return { status: 'ok', ...body } as any;
      }
      return { status: 'ok' } as any;
    });
    vi.spyOn(api, 'delete').mockResolvedValue({ status: 'ok' } as any);

    const fetchMock = vi.fn(async (input: RequestInfo) => {
      const url = typeof input === 'string' ? input : input.toString();
      if (url.includes('/config/default_server')) {
        return Promise.resolve({ ok: true, json: async () => ({ server: 'http://localhost:8000', authEnabled: false }) });
      }
      if (url.includes('/config/auth')) {
        return Promise.resolve({ ok: true, json: async () => ({ authEnabled: false, mode: 'disabled' }) });
      }
      return Promise.resolve({ ok: true, json: async () => ({}) });
    });

    // @ts-expect-error test override
    global.fetch = fetchMock;
  };

  beforeEach(() => {
    projects = [
      makeProject('proj_1', 'Alpha Project', Date.now()),
      makeProject('proj_2', 'Beta Project', Date.now() - 5000),
    ];
    container = document.createElement('div');
    document.body.appendChild(container);
    root = createRoot(container);
    localStorage.clear();
    configureAppMocks();
    vi.stubGlobal('confirm', vi.fn().mockReturnValue(true));
  });

  afterEach(() => {
    root.unmount();
    container.remove();
    localStorage.clear();
    vi.restoreAllMocks();
  });

  const openProjectMenu = async () => {
    const switcher = container.querySelector('button[title="Project Switcher"]');
    await act(async () => {
      switcher?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });
  };

  it('creates projects, opens members, saves settings, syncs manually and deletes projects', async () => {
    await act(async () => {
      root.render(<App />);
      await flush();
      await flush();
    });

    await openProjectMenu();
    const createButton = Array.from(document.querySelectorAll('button')).find((button) => button.textContent?.includes('Create New Project'));
    await act(async () => {
      createButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
      await flush();
    });
    expect(api.post).toHaveBeenCalledWith(expect.anything(), '/projects', expect.anything());
    expect(container.textContent).toContain('New Project');

    const manualSyncButton = container.querySelector('button[title="Manual sync"]');
    await act(async () => {
      manualSyncButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
      await flush();
    });
    expect(vi.mocked(api.get).mock.calls.some(([, endpoint]) => endpoint === '/projects/proj_3/state')).toBe(true);

    const membersButton = container.querySelector('button[title="Project members"]');
    await act(async () => {
      membersButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
      await flush();
    });
    expect(document.body.textContent).toContain('Project Members');
    expect(document.body.textContent).toContain('owner@example.com');

    await openProjectMenu();
    const settingsButton = document.querySelector('button[title="Project Settings"]');
    await act(async () => {
      settingsButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });

    const displayNameInput = document.querySelector('input[placeholder="My Analysis Project"]') as HTMLInputElement | null;
    await act(async () => {
      if (displayNameInput) {
        const setter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')?.set;
        setter?.call(displayNameInput, 'Renamed Project');
        displayNameInput.dispatchEvent(new Event('input', { bubbles: true }));
      }
      await flush();
    });

    const saveSettingsButton = Array.from(document.querySelectorAll('button')).find((button) => button.textContent?.includes('Save Settings'));
    await act(async () => {
      saveSettingsButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
      await flush();
    });
    expect(api.post).toHaveBeenCalledWith(
      expect.anything(),
      '/projects/proj_3/metadata',
      expect.objectContaining({ displayName: 'Renamed Project' })
    );

    await openProjectMenu();
    const deleteButtons = Array.from(document.querySelectorAll('button[title="Delete Project"]'));
    await act(async () => {
      deleteButtons[0]?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
      await flush();
    });
    expect(api.delete).toHaveBeenCalled();
  });

  it('offers draft recovery and restores local changes', async () => {
    const draftSnapshot: ProjectSnapshot = createEmptyProjectSnapshot('Alpha Project');
    draftSnapshot.tree.children[0].name = 'Draft Setup Node';
    localStorage.setItem(
      getProjectDraftStorageKey('proj_1'),
      JSON.stringify({
        projectId: 'proj_1',
        version: 9,
        snapshot: draftSnapshot,
        savedAt: Date.now(),
      })
    );

    await act(async () => {
      root.render(<App />);
      await flush();
      await flush();
    });

    await waitFor(() => document.body.textContent?.includes('Recover local draft') ?? false);
    expect(document.body.textContent).toContain('Recover local draft');

    const restoreButton = Array.from(document.querySelectorAll('button')).find((button) => button.textContent?.includes('恢复草稿'));
    await act(async () => {
      restoreButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });
    expect(document.body.textContent).toContain('Draft Setup Node');
  });

  it('offers draft recovery and allows discarding the local draft', async () => {
    const draftSnapshot: ProjectSnapshot = createEmptyProjectSnapshot('Alpha Project');
    draftSnapshot.tree.children[0].name = 'Draft Setup Node';
    localStorage.setItem(
      getProjectDraftStorageKey('proj_1'),
      JSON.stringify({
        projectId: 'proj_1',
        version: 10,
        snapshot: draftSnapshot,
        savedAt: Date.now(),
      })
    );

    await act(async () => {
      root.render(<App />);
      await flush();
      await flush();
    });
    await waitFor(() => document.body.textContent?.includes('Recover local draft') ?? false);

    const discardButton = Array.from(document.querySelectorAll('button')).find((button) => button.textContent?.includes('放弃草稿'));
    await act(async () => {
      discardButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });
    expect(localStorage.getItem(getProjectDraftStorageKey('proj_1'))).toBeNull();
  });
});
