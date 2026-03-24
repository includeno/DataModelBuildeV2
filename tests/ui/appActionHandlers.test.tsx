import React, { act } from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { createRoot, Root } from 'react-dom/client';

vi.mock('../../components/TopBar', () => ({
  TopBar: (props: any) => (
    <div data-testid="mock-topbar">
      <span>{props.projectName || 'NO_PROJECT'}</span>
      <button onClick={props.onSettingsOpen}>open-settings</button>
      <button onClick={props.onProjectSettingsOpen}>open-project-settings</button>
      <button onClick={props.onProjectDiagnostics}>open-diagnostics</button>
      <button onClick={props.onProjectMembersOpen}>open-members</button>
      <button onClick={props.onManualSync}>manual-sync</button>
      <button onClick={props.onProjectCreate}>create-project</button>
      <button onClick={props.onLogout}>logout</button>
    </div>
  ),
}));

vi.mock('../../components/Sidebar', () => ({
  Sidebar: (props: any) => (
    <div data-testid="mock-sidebar">
      <button onClick={() => props.onOpenSchema?.('customers')}>open-schema</button>
      <button onClick={() => props.onDeleteDataset?.('customers')}>delete-dataset</button>
      <button onClick={() => props.onExportOperations?.()}>export-operations</button>
      <button
        onClick={() => props.onImportOperations?.({
          name: 'ops.json',
          text: async () => JSON.stringify({
            version: 1,
            tree: {
              id: 'import_root',
              type: 'operation',
              operationType: 'root',
              name: 'Imported Root',
              enabled: true,
              commands: [],
              children: [],
            },
          }),
        })}
      >
        import-operations
      </button>
      <button onClick={() => props.onOpenTableInSql?.('customers')}>open-sql</button>
      <button onClick={() => props.onOpenTableInData?.('customers')}>open-data</button>
    </div>
  ),
}));

vi.mock('../../components/Workspace', () => ({
  Workspace: (props: any) => (
    <div data-testid="mock-workspace">
      <span>{props.currentView}</span>
      <button onClick={() => props.onRefreshPreview?.(1)}>run-preview</button>
      <button onClick={() => props.onExportFull?.()}>export-full</button>
      <button onClick={() => props.onUpdatePageSize?.(100)}>update-page-size</button>
      <button onClick={() => props.onViewPath?.('cmd_filter_1')}>open-paths</button>
    </div>
  ),
}));

vi.mock('../../components/SettingsModal', () => ({
  SettingsModal: (props: any) => props.isOpen ? (
    <div data-testid="mock-settings-modal">
      <button onClick={() => props.onCreateSessionStorage?.('team-a')}>create-storage</button>
      <button onClick={() => props.onSelectSessionStorage?.('team-a')}>select-storage</button>
      <button onClick={() => props.onAddServer?.('http://127.0.0.1:9000')}>add-server</button>
      <button onClick={() => props.onSelectServer?.('http://127.0.0.1:9000')}>select-server</button>
      <button onClick={() => props.onRemoveServer?.('http://127.0.0.1:9000')}>remove-server</button>
    </div>
  ) : null,
}));

vi.mock('../../components/SessionSettingsModal', () => ({
  SessionSettingsModal: (props: any) => props.isOpen ? (
    <div data-testid="mock-project-settings">
      <button onClick={() => props.onSave?.('Renamed Project', { cascadeDisable: true, panelPosition: 'left' })}>save-project-settings</button>
    </div>
  ) : null,
}));

vi.mock('../../components/ProjectMembersModal', () => ({
  ProjectMembersModal: (props: any) => props.isOpen ? (
    <div data-testid="mock-members-modal">
      <span>{props.projectName}</span>
      <button onClick={() => props.onInvite?.('invitee@example.com', 'editor')}>invite-member</button>
      <button onClick={() => props.onUpdateRole?.({ userId: 'usr_member' }, 'admin')}>promote-member</button>
      <button onClick={() => props.onRemoveMember?.({ userId: 'usr_member' })}>remove-member</button>
    </div>
  ) : null,
}));

vi.mock('../../components/SessionDiagnosticsModal', () => ({
  SessionDiagnosticsModal: (props: any) => props.isOpen ? (
    <div data-testid="mock-diagnostics-modal">
      {props.loading ? 'loading' : props.report?.projectId || props.error || 'no-report'}
    </div>
  ) : null,
}));

vi.mock('../../components/DatasetSchemaModal', () => ({
  DatasetSchemaModal: (props: any) => props.isOpen ? (
    <div data-testid="mock-schema-modal">
      <span>{props.dataset?.name}</span>
      <button onClick={() => props.onSave?.(props.dataset?.id || 'ds_customers', { customer_id: { type: 'string' } })}>save-schema</button>
    </div>
  ) : null,
}));

vi.mock('../../components/DataImport', () => ({
  DataImportModal: () => null,
}));

vi.mock('../../components/PathConditionsModal', () => ({
  PathConditionsModal: (props: any) => props.isOpen ? <div data-testid="mock-path-modal">PATH_MODAL</div> : null,
}));

vi.mock('../../components/ConflictNoticeModal', () => ({
  ConflictNoticeModal: () => null,
}));

vi.mock('../../components/DraftRecoveryModal', () => ({
  DraftRecoveryModal: () => null,
}));

vi.mock('../../components/CollabPresenceFloat', () => ({
  CollabPresenceFloat: () => <div data-testid="mock-collab-float">COLLAB_FLOAT</div>,
}));

vi.mock('../../utils/realtimeCollab', () => ({
  RealtimeProjectClient: class {
    connect() {}
    disconnect() {}
    sendPresenceUpdate() {}
  },
}));

vi.mock('../../utils/collabSync', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../../utils/collabSync')>();
  return {
    ...actual,
    DebouncedCommitQueue: class {
      enqueue() {
        return new Promise(() => {});
      }
      dispose() {}
    },
    buildConflictNotice: () => 'conflict',
  };
});

import App from '../../App';
import { api } from '../../utils/api';
import { createEmptyProjectSnapshot } from '../../utils/projectStore';

const flush = () => new Promise((resolve) => setTimeout(resolve, 0));

const waitFor = async (check: () => boolean, attempts = 25) => {
  for (let i = 0; i < attempts; i += 1) {
    if (check()) return;
    await act(async () => {
      await flush();
      await flush();
    });
  }
  throw new Error('Condition not met in time');
};

describe('App action handlers', () => {
  let container: HTMLDivElement;
  let root: Root;
  let snapshot: ReturnType<typeof createEmptyProjectSnapshot>;
  let projects: any[];
  let datasets: any[];
  let members: any[];
  let metadata: any;
  let stateVersion: number;

  beforeEach(() => {
    snapshot = createEmptyProjectSnapshot('Alpha Project');
    if (snapshot.tree.children[0]) {
      snapshot.tree.children[0].commands = [
        {
          id: 'src_customers',
          type: 'source',
          order: 1,
          config: { mainTable: 'customers' },
        } as any,
      ];
    }
    projects = [
      {
        id: 'proj_1',
        name: 'Alpha Project',
        role: 'owner',
        createdAt: Date.now() - 1000,
        updatedAt: Date.now(),
      },
    ];
    datasets = [
      {
        id: 'ds_customers',
        name: 'customers',
        fields: ['customer_id', 'name'],
        rows: [],
        totalCount: 1,
      },
    ];
    members = [
      {
        userId: 'usr_owner',
        email: 'owner@example.com',
        displayName: 'Owner',
        role: 'owner',
        createdAt: Date.now(),
        updatedAt: Date.now(),
      },
      {
        userId: 'usr_member',
        email: 'member@example.com',
        displayName: 'Member',
        role: 'editor',
        createdAt: Date.now(),
        updatedAt: Date.now(),
      },
    ];
    metadata = {
      displayName: 'Alpha Project',
      settings: {
        cascadeDisable: false,
        panelPosition: 'right',
      },
    };
    stateVersion = 2;

    container = document.createElement('div');
    document.body.appendChild(container);
    root = createRoot(container);

    vi.stubGlobal('confirm', vi.fn(() => true));
    vi.stubGlobal('alert', vi.fn());
    vi.spyOn(api, 'ping').mockResolvedValue(true);
    vi.spyOn(api, 'getAuthTokens').mockReturnValue(null);

    vi.spyOn(api, 'get').mockImplementation(async (_cfg: any, endpoint: string) => {
      if (endpoint === '/projects') return projects;
      if (endpoint === '/projects/proj_1/state') {
        return {
          version: stateVersion,
          updatedAt: Date.now(),
          state: {
            tree: snapshot.tree,
            datasets,
            sqlHistory: snapshot.sqlHistory,
          },
        };
      }
      if (endpoint === '/projects/proj_1/metadata') return metadata;
      if (endpoint === '/projects/proj_1/datasets') return datasets;
      if (endpoint === '/projects/proj_1/members') return members;
      if (endpoint === '/projects/proj_1/jobs') return [];
      if (endpoint.includes('/datasets/customers/preview')) {
        return { rows: [{ customer_id: 'C001', name: 'Alice' }] };
      }
      if (endpoint === '/projects/proj_1/diagnostics') {
        return { projectId: 'proj_1', version: stateVersion };
      }
      return {} as any;
    });

    vi.spyOn(api, 'post').mockImplementation(async (_cfg: any, endpoint: string, body?: any) => {
      if (endpoint === '/projects') {
        const created = {
          id: 'proj_2',
          name: 'New Project',
          role: 'owner',
          createdAt: Date.now(),
          updatedAt: Date.now(),
        };
        projects = [created, ...projects];
        return created as any;
      }
      if (endpoint === '/projects/proj_1/metadata') {
        metadata = {
          displayName: body.displayName,
          settings: body.settings,
        };
        return { status: 'ok' } as any;
      }
      if (endpoint === '/projects/proj_1/datasets/update') {
        datasets = datasets.map((item) => item.id === body.datasetId ? { ...item, fieldTypes: body.fieldTypes } : item);
        return { status: 'ok' } as any;
      }
      if (endpoint === '/projects/proj_1/execute') {
        return {
          rows: [{ customer_id: 'C001', name: 'Alice' }],
          totalCount: 1,
          columns: ['customer_id', 'name'],
          page: body.page || 1,
          pageSize: body.pageSize || 50,
        } as any;
      }
      if (endpoint === '/projects/proj_1/members') {
        members = [...members, { userId: 'usr_invited', email: body.memberEmail, role: body.role }];
        return { status: 'ok' } as any;
      }
      return { status: 'ok' } as any;
    });

    vi.spyOn(api, 'patch').mockResolvedValue({ status: 'ok' } as any);
    vi.spyOn(api, 'delete').mockImplementation(async (_cfg: any, endpoint: string) => {
      if (endpoint === '/projects/proj_1/datasets/customers') {
        datasets = [];
      }
      if (endpoint === '/projects/proj_1/members/usr_member') {
        members = members.filter((item) => item.userId !== 'usr_member');
      }
      return { status: 'ok' } as any;
    });

    const fetchMock = vi.fn(async (input: RequestInfo, init?: RequestInit) => {
      const url = typeof input === 'string' ? input : input.toString();
      if (url.includes('/config/default_server')) {
        return { ok: true, json: async () => ({ server: 'http://localhost:8000', authEnabled: false }) } as Response;
      }
      if (url.includes('/config/auth')) {
        return { ok: true, json: async () => ({ authEnabled: false, mode: 'disabled' }) } as Response;
      }
      if (url.includes('/config/session_storage') && (!init || init.method === undefined || init.method === 'GET')) {
        if (url.includes('/list')) {
          return { ok: true, json: async () => ({ folders: [{ name: 'team-a', path: 'team-a' }] }) } as Response;
        }
        return { ok: true, json: async () => ({ dataRoot: '/tmp/data', sessionsDir: '/tmp/data/sessions', relative: 'sessions' }) } as Response;
      }
      if (url.includes('/config/session_storage/create')) {
        return { ok: true, json: async () => ({ status: 'ok' }) } as Response;
      }
      if (url.includes('/config/session_storage/select')) {
        return { ok: true, json: async () => ({ dataRoot: '/tmp/data/team-a', sessionsDir: '/tmp/data/team-a/sessions', relative: 'team-a/sessions' }) } as Response;
      }
      return { ok: true, json: async () => ({}) } as Response;
    });

    // @ts-expect-error test override
    global.fetch = fetchMock;

    vi.stubGlobal('URL', {
      createObjectURL: vi.fn(() => 'blob:test'),
      revokeObjectURL: vi.fn(),
    });
    vi.spyOn(HTMLAnchorElement.prototype, 'click').mockImplementation(() => {});
  });

  afterEach(() => {
    root.unmount();
    container.remove();
    vi.restoreAllMocks();
  });

  it('handles project-level actions, dataset management and local export flows', async () => {
    await act(async () => {
      root.render(<App />);
      await flush();
      await flush();
    });

    await waitFor(() => container.querySelector('[data-testid="mock-workspace"]') !== null);

    await act(async () => {
      Array.from(container.querySelectorAll('button')).find((node) => node.textContent === 'open-project-settings')?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });
    expect(container.querySelector('[data-testid="mock-project-settings"]')).not.toBeNull();

    await act(async () => {
      Array.from(container.querySelectorAll('button')).find((node) => node.textContent === 'save-project-settings')?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });
    expect(api.post).toHaveBeenCalledWith(expect.anything(), '/projects/proj_1/metadata', expect.objectContaining({ displayName: 'Renamed Project' }));

    await act(async () => {
      Array.from(container.querySelectorAll('button')).find((node) => node.textContent === 'open-members')?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });
    expect(container.querySelector('[data-testid="mock-members-modal"]')).not.toBeNull();

    await act(async () => {
      Array.from(container.querySelectorAll('button')).find((node) => node.textContent === 'invite-member')?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      Array.from(container.querySelectorAll('button')).find((node) => node.textContent === 'promote-member')?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      Array.from(container.querySelectorAll('button')).find((node) => node.textContent === 'remove-member')?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
      await flush();
    });
    expect(api.post).toHaveBeenCalledWith(expect.anything(), '/projects/proj_1/members', { memberEmail: 'invitee@example.com', role: 'editor' });
    expect(api.patch).toHaveBeenCalledWith(expect.anything(), '/projects/proj_1/members/usr_member', { role: 'admin' });
    expect(api.delete).toHaveBeenCalledWith(expect.anything(), '/projects/proj_1/members/usr_member');

    await act(async () => {
      Array.from(container.querySelectorAll('button')).find((node) => node.textContent === 'open-diagnostics')?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
      await flush();
    });
    expect(container.textContent).toContain('proj_1');

    await act(async () => {
      Array.from(container.querySelectorAll('button')).find((node) => node.textContent === 'manual-sync')?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
      await flush();
    });
    expect(vi.mocked(api.get).mock.calls.filter(([, endpoint]) => endpoint === '/projects/proj_1/state').length).toBeGreaterThan(1);

    await act(async () => {
      Array.from(container.querySelectorAll('button')).find((node) => node.textContent === 'open-settings')?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
      await flush();
    });
    expect(container.querySelector('[data-testid="mock-settings-modal"]')).not.toBeNull();

    await act(async () => {
      Array.from(container.querySelectorAll('button')).find((node) => node.textContent === 'create-storage')?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      Array.from(container.querySelectorAll('button')).find((node) => node.textContent === 'select-storage')?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
      await flush();
    });

    await act(async () => {
      Array.from(container.querySelectorAll('button')).find((node) => node.textContent === 'open-schema')?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
      await flush();
    });
    expect(container.querySelector('[data-testid="mock-schema-modal"]')).not.toBeNull();
    expect(container.textContent).toContain('customers');

    await act(async () => {
      Array.from(container.querySelectorAll('button')).find((node) => node.textContent === 'save-schema')?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });
    expect(api.post).toHaveBeenCalledWith(expect.anything(), '/projects/proj_1/datasets/update', expect.objectContaining({ datasetId: 'ds_customers' }));

    await act(async () => {
      Array.from(container.querySelectorAll('button')).find((node) => node.textContent === 'run-preview')?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      Array.from(container.querySelectorAll('button')).find((node) => node.textContent === 'export-full')?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
      await flush();
    });
    expect(api.post).toHaveBeenCalledWith(expect.anything(), '/projects/proj_1/execute', expect.objectContaining({ projectId: 'proj_1' }));
    expect(URL.createObjectURL).toHaveBeenCalled();

    await act(async () => {
      Array.from(container.querySelectorAll('button')).find((node) => node.textContent === 'export-operations')?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      Array.from(container.querySelectorAll('button')).find((node) => node.textContent === 'import-operations')?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      Array.from(container.querySelectorAll('button')).find((node) => node.textContent === 'delete-dataset')?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
      await flush();
    });
    expect(api.delete).toHaveBeenCalledWith(expect.anything(), '/projects/proj_1/datasets/customers');
    expect(global.alert).not.toHaveBeenCalled();
  });
});
