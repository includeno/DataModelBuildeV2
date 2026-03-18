import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { act } from 'react';
import { createRoot, Root } from 'react-dom/client';
import App from '../../App';
import { api } from '../../utils/api';
import { ProjectMetadata, SessionDiagnosticsReport } from '../../types';
import { INITIAL_TREE } from '../../utils/projectStore';

const flush = () => new Promise(resolve => setTimeout(resolve, 0));

const project: ProjectMetadata = {
  id: 'proj_123',
  name: 'Test Project',
  role: 'owner',
  createdAt: Date.now(),
  updatedAt: Date.now(),
};

const mockReport: SessionDiagnosticsReport = {
  projectId: project.id,
  generatedAt: new Date().toISOString(),
  sources: [
    { id: 'src_1', mainTable: 'customers', alias: 'c', linkId: 'link_1' }
  ],
  datasets: [
    { id: 'customers', name: 'customers', fieldCount: 2, totalCount: 2 }
  ],
  operations: [
    {
      id: 'op_1',
      name: 'Op 1',
      operationType: 'process',
      commands: [
        { id: 'cmd_1', type: 'filter', order: 1, dataSource: 'link_1' }
      ]
    }
  ],
  dataSourceResolution: [
    { commandId: 'cmd_1', dataSource: 'customers', resolved: 'customers', status: 'ok' }
  ],
  warnings: ['Test warning']
};

describe('Session diagnostics (UI)', () => {
  let container: HTMLDivElement;
  let root: Root;

  beforeEach(async () => {
    container = document.createElement('div');
    document.body.appendChild(container);
    root = createRoot(container);

    vi.spyOn(api, 'get').mockImplementation(async (_cfg: any, endpoint: string) => {
      if (endpoint === '/projects') return [project];
      if (endpoint === `/projects/${project.id}/state`) {
        return {
          version: 3,
          updatedAt: Date.now(),
          state: {
            tree: INITIAL_TREE,
            sqlHistory: [],
          },
        };
      }
      if (endpoint === `/projects/${project.id}/metadata`) {
        return {
          displayName: project.name,
          settings: {
            cascadeDisable: true,
            panelPosition: 'right',
          },
        };
      }
      if (endpoint === `/projects/${project.id}/datasets`) return [];
      if (endpoint === `/projects/${project.id}/members`) return [];
      if (endpoint === `/projects/${project.id}/jobs`) return [];
      if (endpoint === `/projects/${project.id}/diagnostics`) return mockReport;
      return {} as any;
    });
    vi.spyOn(api, 'ping').mockResolvedValue(true);

    const fetchMock = vi.fn(async (input: RequestInfo) => {
      const url = typeof input === 'string' ? input : input.toString();
      if (url.includes('/config/default_server')) {
        return Promise.resolve({ ok: true, json: async () => ({ server: 'http://localhost:8000' }) });
      }
      return Promise.resolve({ ok: true, json: async () => ({}) });
    });

    // @ts-expect-error test override
    global.fetch = fetchMock;

    await act(async () => {
      root.render(<App />);
      await flush();
      await flush();
    });
  });

  afterEach(() => {
    root.unmount();
    container.remove();
    vi.restoreAllMocks();
  });

  const click = (el: Element | null) => {
    if (!el) throw new Error('Element not found');
    el.dispatchEvent(new MouseEvent('click', { bubbles: true }));
  };

  const openSessionMenu = async () => {
    const btn = document.querySelector('button[title="Project Switcher"]');
    click(btn);
    await act(async () => { await flush(); });
  };

  it('opens diagnostics modal with report content', async () => {
    await openSessionMenu();

    const diagBtn = document.querySelector('button[title="Project Diagnostics"]');
    click(diagBtn);
    await act(async () => { await flush(); await flush(); });

    expect(document.body.textContent).toContain('Project Diagnostics');
    expect(document.body.textContent).toContain('Test warning');
    expect(document.body.textContent).toContain('customers');
  });

  it('shows error message when diagnostics request fails', async () => {
    vi.mocked(api.get).mockImplementation(async (_cfg: any, endpoint: string) => {
      if (endpoint === '/projects') return [project];
      if (endpoint === `/projects/${project.id}/state`) {
        return {
          version: 3,
          updatedAt: Date.now(),
          state: {
            tree: INITIAL_TREE,
            sqlHistory: [],
          },
        };
      }
      if (endpoint === `/projects/${project.id}/metadata`) {
        return {
          displayName: project.name,
          settings: {
            cascadeDisable: true,
            panelPosition: 'right',
          },
        };
      }
      if (endpoint === `/projects/${project.id}/datasets`) return [];
      if (endpoint === `/projects/${project.id}/members`) return [];
      if (endpoint === `/projects/${project.id}/jobs`) return [];
      if (endpoint === `/projects/${project.id}/diagnostics`) throw new Error('Diagnostics unavailable');
      return {} as any;
    });

    await openSessionMenu();
    const diagBtn = document.querySelector('button[title="Project Diagnostics"]');
    click(diagBtn);
    await act(async () => { await flush(); await flush(); });

    expect(document.body.textContent).toContain('Diagnostics unavailable');
  });

  it('renders empty sections when report has no sources or resolutions', async () => {
    const emptyReport: SessionDiagnosticsReport = {
      projectId: project.id,
      generatedAt: new Date().toISOString(),
      sources: [],
      sourceMap: [],
      datasets: [],
      operations: [],
      dataSourceResolution: [],
      warnings: []
    };
    vi.mocked(api.get).mockImplementation(async (_cfg: any, endpoint: string) => {
      if (endpoint === '/projects') return [project];
      if (endpoint === `/projects/${project.id}/state`) {
        return {
          version: 3,
          updatedAt: Date.now(),
          state: {
            tree: INITIAL_TREE,
            sqlHistory: [],
          },
        };
      }
      if (endpoint === `/projects/${project.id}/metadata`) {
        return {
          displayName: project.name,
          settings: {
            cascadeDisable: true,
            panelPosition: 'right',
          },
        };
      }
      if (endpoint === `/projects/${project.id}/datasets`) return [];
      if (endpoint === `/projects/${project.id}/members`) return [];
      if (endpoint === `/projects/${project.id}/jobs`) return [];
      if (endpoint === `/projects/${project.id}/diagnostics`) return emptyReport;
      return {} as any;
    });

    await openSessionMenu();
    const diagBtn = document.querySelector('button[title="Project Diagnostics"]');
    click(diagBtn);
    await act(async () => { await flush(); await flush(); });

    expect(document.body.textContent).toContain('No sources found.');
    expect(document.body.textContent).toContain('No dataSource overrides found.');
  });
});
