import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { api } from '../utils/api';
import type { OperationNode } from '../types';

const REAL_FETCH = global.fetch;
const MOCK_CONFIG = { baseUrl: 'mockServer', isMock: true };
const REAL_CONFIG = { baseUrl: 'http://localhost:8000', isMock: false };

const runMock = async <T,>(promise: Promise<T>, advanceMs = 2500): Promise<T> => {
  await vi.advanceTimersByTimeAsync(advanceMs);
  return promise;
};

const jsonResponse = (status: number, payload: any) => ({
  ok: status >= 200 && status < 300,
  status,
  statusText: String(status),
  json: async () => payload,
} as Response);

const blobResponse = (status: number, body: string) => ({
  ok: status >= 200 && status < 300,
  status,
  statusText: String(status),
  blob: async () => new Blob([body], { type: 'text/csv' }),
  json: async () => ({ detail: body }),
} as unknown as Response);

const buildTree = (): OperationNode => ({
  id: 'root',
  type: 'operation',
  operationType: 'root',
  name: 'Project',
  enabled: true,
  commands: [],
  children: [
    {
      id: 'setup_1',
      type: 'operation',
      operationType: 'setup',
      name: 'Data Setup',
      enabled: true,
      commands: [
        {
          id: 'src_1',
          type: 'source',
          order: 0,
          config: { mainTable: 'employees.csv', alias: 'main', linkId: 'employees_link' },
        },
      ],
      children: [
        {
          id: 'proc_1',
          type: 'operation',
          operationType: 'process',
          name: 'Process 1',
          enabled: true,
          commands: [
            {
              id: 'multi_1',
              type: 'multi_table',
              order: 1,
              config: {
                subTables: [
                  {
                    id: 'sub_sales',
                    table: 'sales_data.csv',
                    label: 'Sales',
                    on: 'main.id = sub.uid',
                  },
                ],
              },
            },
          ],
          children: [],
        },
      ],
    },
  ],
});

describe('api mock and transport client', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    api.clearAuthTokens();
    api.setAuthStorageMode('local_storage');
    api.setAuthApiEnabled(true);
  });

  afterEach(() => {
    vi.useRealTimers();
    api.clearAuthTokens();
    api.setAuthApiEnabled(true);
    vi.restoreAllMocks();
    global.fetch = REAL_FETCH;
  });

  it('supports mock project lifecycle and collaboration endpoints', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000);
    const project = await runMock(api.post(MOCK_CONFIG, '/projects', { name: 'Coverage Project', description: 'demo' }));
    const projectId = project.id;

    expect(project.name).toBe('Coverage Project');
    expect((await runMock(api.get(MOCK_CONFIG, '/projects'))).some((item: any) => item.id === projectId)).toBe(true);

    const queryListing = await runMock(api.get(MOCK_CONFIG, '/projects/query?search='));
    expect(queryListing.items.some((item: any) => item.id === projectId)).toBe(true);

    await runMock(api.post(MOCK_CONFIG, `/projects/${projectId}/archive`, { archived: true }));
    const archivedProject = await runMock(api.get(MOCK_CONFIG, `/projects/${projectId}`));
    expect(archivedProject.archived).toBe(true);

    await runMock(api.post(MOCK_CONFIG, `/projects/${projectId}/metadata`, { displayName: 'Coverage Renamed' }));
    const metadata = await runMock(api.get(MOCK_CONFIG, `/projects/${projectId}/metadata`));
    expect(metadata.displayName).toBe('Coverage Renamed');

    const uploadForm = new FormData();
    uploadForm.set('file', new File(['id,name\n1,Alice'], 'coverage.csv', { type: 'text/csv' }));
    uploadForm.set('name', 'coverage.csv');
    const uploaded = await runMock(api.upload(MOCK_CONFIG, `/projects/${projectId}/upload`, uploadForm));
    expect(uploaded.name).toBe('coverage.csv');

    const datasets = await runMock(api.get(MOCK_CONFIG, `/projects/${projectId}/datasets`));
    expect(datasets.some((item: any) => item.name === 'coverage.csv')).toBe(true);

    const preview = await runMock(api.get(MOCK_CONFIG, `/projects/${projectId}/datasets/coverage.csv/preview?limit=20`));
    expect(preview.rows[0].col1).toBe('A');

    const imports = await runMock(api.get(MOCK_CONFIG, `/projects/${projectId}/imports`));
    expect(imports[imports.length - 1].datasetName).toBe('coverage.csv');

    const commitOk = await runMock(api.post(MOCK_CONFIG, `/projects/${projectId}/state/commit`, {
      baseVersion: 0,
      state: { tree: buildTree(), metadata: { displayName: 'Saved' } },
    }));
    expect(commitOk.conflict).toBe(false);
    const commitConflict = await runMock(api.post(MOCK_CONFIG, `/projects/${projectId}/state/commit`, {
      baseVersion: 0,
      state: { tree: { id: 'stale' } },
    }));
    expect(commitConflict.conflict).toBe(true);

    const state = await runMock(api.get(MOCK_CONFIG, `/projects/${projectId}/state`));
    expect(state.version).toBe(1);

    const diagnostics = await runMock(api.get(MOCK_CONFIG, `/projects/${projectId}/diagnostics`));
    expect(diagnostics.projectId).toBe(projectId);

    const executeMain = await runMock(api.post(MOCK_CONFIG, `/projects/${projectId}/execute`, {
      tree: buildTree(),
      targetNodeId: 'proc_1',
      page: 1,
      pageSize: 5,
    }));
    expect(executeMain.rows.length).toBeGreaterThan(0);

    const executeSubView = await runMock(api.post(MOCK_CONFIG, `/projects/${projectId}/execute`, {
      tree: buildTree(),
      targetNodeId: 'proc_1',
      page: 1,
      pageSize: 5,
      viewId: 'sub_sales',
    }));
    expect(executeSubView.activeViewId).toBe('sub_sales');

    const job = await runMock(api.post(MOCK_CONFIG, `/projects/${projectId}/jobs/execute`, {
      tree: buildTree(),
      targetNodeId: 'proc_1',
    }));
    expect(job.status).toBe('completed');

    const jobs = await runMock(api.get(MOCK_CONFIG, `/projects/${projectId}/jobs`));
    expect(jobs[0].id).toBe(job.id);

    const jobDetail = await runMock(api.get(MOCK_CONFIG, `/jobs/${job.id}`));
    expect(jobDetail.id).toBe(job.id);

    const exportJob = await runMock(api.post(MOCK_CONFIG, `/projects/${projectId}/export`, { format: 'csv' }));
    expect(exportJob.result.fileName).toContain(projectId);

    const sql = await runMock(api.post(MOCK_CONFIG, `/projects/${projectId}/generate_sql`, { targetCommandId: 'cmd_1' }));
    expect(sql.sql).toContain('cmd_1');

    const analysis = await runMock(api.post(MOCK_CONFIG, `/projects/${projectId}/analyze`, {}));
    expect(analysis.report[0]).toContain('Mock Analysis');

    const query = await runMock(api.post(MOCK_CONFIG, `/projects/${projectId}/query`, { query: 'select \\* from coverage.csv' }));
    expect(query.totalCount).toBe(0);

    const newMember = await runMock(api.post(MOCK_CONFIG, `/projects/${projectId}/members`, { memberEmail: 'viewer@example.com', role: 'viewer' }));
    expect(newMember.role).toBe('viewer');

    const patchedMember = await runMock(api.patch(MOCK_CONFIG, `/projects/${projectId}/members/${newMember.userId}`, { role: 'editor' }));
    expect(patchedMember.role).toBe('editor');

    const members = await runMock(api.get(MOCK_CONFIG, `/projects/${projectId}/members`));
    expect(members.some((member: any) => member.userId === newMember.userId)).toBe(true);

    await runMock(api.post(MOCK_CONFIG, `/jobs/${job.id}:cancel`, {}));
    const canceledJob = await runMock(api.get(MOCK_CONFIG, `/jobs/${job.id}`));
    expect(canceledJob.status).toBe('canceled');

    await runMock(api.delete(MOCK_CONFIG, `/projects/${projectId}/members/${newMember.userId}`), 10);
    await runMock(api.delete(MOCK_CONFIG, `/projects/${projectId}/datasets/coverage.csv`), 10);
    const datasetsAfterDelete = await runMock(api.get(MOCK_CONFIG, `/projects/${projectId}/datasets`));
    expect(datasetsAfterDelete.some((item: any) => item.name === 'coverage.csv')).toBe(false);

    await runMock(api.delete(MOCK_CONFIG, `/projects/${projectId}`), 10);
    const projectsAfterDelete = await runMock(api.get(MOCK_CONFIG, '/projects'));
    expect(projectsAfterDelete.some((item: any) => item.id === projectId)).toBe(false);
  });

  it('supports mock session lifecycle and legacy endpoints', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000001000);
    const created = await runMock(api.post(MOCK_CONFIG, '/sessions', {}));
    const sessionId = created.sessionId;
    expect(sessionId).toContain('mock-sess');

    const uploadForm = new FormData();
    uploadForm.set('file', new File(['a,b\n1,2'], 'session.csv', { type: 'text/csv' }));
    uploadForm.set('name', 'session.csv');
    await runMock(api.upload(MOCK_CONFIG, `/sessions/${sessionId}/upload`, uploadForm));

    const sessions = await runMock(api.get(MOCK_CONFIG, '/sessions'));
    expect(Array.isArray(sessions)).toBe(true);

    const sessionDatasets = await runMock(api.get(MOCK_CONFIG, `/sessions/${sessionId}/datasets`));
    expect(sessionDatasets.some((item: any) => item.name === 'session.csv')).toBe(true);

    const sessionPreview = await runMock(api.get(MOCK_CONFIG, `/sessions/${sessionId}/datasets/session.csv/preview?limit=20`));
    expect(sessionPreview.rows[0].col1).toBe('A');

    const tree = buildTree();
    await runMock(api.post(MOCK_CONFIG, `/sessions/${sessionId}/state`, { tree }));
    await runMock(api.post(MOCK_CONFIG, `/sessions/${sessionId}/metadata`, { displayName: 'Legacy Session' }));
    await runMock(api.post(MOCK_CONFIG, `/sessions/${sessionId}/datasets/update`, { datasetId: 'session.csv', fieldTypes: { col1: { type: 'string' } } }));

    const legacyState = await runMock(api.get(MOCK_CONFIG, `/sessions/${sessionId}/state`));
    expect(legacyState.tree.id).toBe('root');

    const legacyMetadata = await runMock(api.get(MOCK_CONFIG, `/sessions/${sessionId}/metadata`));
    expect(legacyMetadata.displayName).toBe('Legacy Session');

    const legacyDiagnostics = await runMock(api.get(MOCK_CONFIG, `/sessions/${sessionId}/diagnostics`));
    expect(legacyDiagnostics.sessionId).toBe(sessionId);

    const legacyImports = await runMock(api.get(MOCK_CONFIG, `/sessions/${sessionId}/imports`));
    expect(Array.isArray(legacyImports)).toBe(true);

    const execute = await runMock(api.post(MOCK_CONFIG, '/execute', {
      tree,
      targetNodeId: 'proc_1',
      page: 1,
      pageSize: 5,
      viewId: 'sub_sales',
    }));
    expect(execute.activeViewId).toBe('sub_sales');

    const analysis = await runMock(api.post(MOCK_CONFIG, '/analyze', {}));
    expect(analysis.report[0]).toContain('Mock Analysis');

    const query = await runMock(api.post(MOCK_CONFIG, '/query', { query: 'select \\* from session.csv' }));
    expect(query.totalCount).toBe(0);

    await runMock(api.delete(MOCK_CONFIG, `/sessions/${sessionId}/datasets/session.csv`), 10);
    const datasetsAfterDelete = await runMock(api.get(MOCK_CONFIG, `/sessions/${sessionId}/datasets`));
    expect(datasetsAfterDelete.some((item: any) => item.name === 'session.csv')).toBe(false);
  });

  it('covers auth-disabled, ping and real fetch transport helpers', async () => {
    api.setAuthApiEnabled(false);
    await expect(api.authRegister(REAL_CONFIG, { email: 'u@example.com', password: 'secret123' })).rejects.toThrow('Authentication is disabled');
    await expect(api.authLogin(REAL_CONFIG, { email: 'u@example.com', password: 'secret123' })).rejects.toThrow('Authentication is disabled');
    await expect(api.authMe(REAL_CONFIG)).rejects.toThrow('Authentication is disabled');
    await expect(api.authLogout(REAL_CONFIG)).resolves.toEqual({ status: 'skipped', reason: 'auth_disabled' });

    api.setAuthApiEnabled(true);
    global.fetch = vi.fn().mockRejectedValueOnce(new Error('offline')) as unknown as typeof fetch;
    await expect(api.ping(REAL_CONFIG, 5)).resolves.toBe(false);

    global.fetch = vi.fn().mockResolvedValueOnce(jsonResponse(200, {})) as unknown as typeof fetch;
    await expect(api.ping(REAL_CONFIG, 5)).resolves.toBe(true);

    const fetchMock = vi.fn()
      .mockResolvedValueOnce(jsonResponse(200, { data: { status: 'registered' }, error: null, meta: {} }))
      .mockResolvedValueOnce(jsonResponse(400, { data: null, error: { message: 'Bad login' }, meta: {} }))
      .mockResolvedValueOnce(jsonResponse(200, { data: { status: 'logged_out' }, error: null, meta: {} }))
      .mockResolvedValueOnce(jsonResponse(200, { data: { ok: true }, error: null, meta: {} }))
      .mockResolvedValueOnce(jsonResponse(200, { data: { ok: true }, error: null, meta: {} }))
      .mockResolvedValueOnce(jsonResponse(200, { data: { ok: true }, error: null, meta: {} }))
      .mockResolvedValueOnce(blobResponse(200, 'a,b\n1,2'));
    global.fetch = fetchMock as unknown as typeof fetch;

    expect(await api.authRegister(REAL_CONFIG, { email: 'u@example.com', password: 'secret123' })).toEqual({ status: 'registered' });
    await expect(api.authLogin(REAL_CONFIG, { email: 'u@example.com', password: 'secret123' })).rejects.toThrow('Bad login');
    expect(await api.authLogout(REAL_CONFIG)).toEqual({ data: { status: 'logged_out' }, error: null, meta: {} });
    expect(await api.patch(REAL_CONFIG, '/projects/prj_1/members/usr_1', { role: 'editor' })).toEqual({ ok: true });
    expect(await api.delete(REAL_CONFIG, '/projects/prj_1')).toEqual({ ok: true });

    const uploadForm = new FormData();
    uploadForm.set('file', new File(['a,b\n1,2'], 'real.csv', { type: 'text/csv' }));
    expect(await api.upload(REAL_CONFIG, '/projects/prj_1/upload', uploadForm)).toEqual({ ok: true });

    const anchorClick = vi.spyOn(HTMLAnchorElement.prototype, 'click').mockImplementation(() => {});
    Object.defineProperty(window.URL, 'createObjectURL', {
      value: vi.fn().mockReturnValue('blob:real-export'),
      configurable: true,
      writable: true,
    });
    await api.export(REAL_CONFIG, '/projects/prj_1/export', { format: 'csv' });
    expect(anchorClick).toHaveBeenCalled();
  });
});
