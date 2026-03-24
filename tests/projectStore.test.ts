import { beforeEach, afterEach, describe, expect, it, vi } from 'vitest';

import type { Dataset, OperationNode, ProjectJob, ProjectMember, ProjectSnapshot } from '../types';
import {
  INITIAL_TREE,
  buildNodeNameLookup,
  clearProjectDraft,
  coerceProjectSnapshot,
  createDefaultProjectMetadata,
  createEmptyProjectSnapshot,
  getProjectDraftStorageKey,
  initialProjectStoreState,
  loadProjectDraft,
  projectStoreReducer,
  rebasePendingPatches,
  saveProjectDraft,
  shouldOfferDraftRecovery,
  snapshotToState,
} from '../utils/projectStore';

const makeTree = (): OperationNode => ({
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
          id: 'cmd_source',
          type: 'source',
          order: 0,
          config: { mainTable: 'customers.csv' },
        },
      ],
      children: [
        {
          id: 'op_1',
          type: 'operation',
          operationType: 'process',
          name: 'Operation 1',
          enabled: true,
          commands: [
            {
              id: 'cmd_filter',
              type: 'filter',
              order: 1,
              config: {
                filterRoot: {
                  id: 'group_root',
                  type: 'group',
                  logicalOperator: 'AND',
                  conditions: [],
                },
              },
            },
          ],
          children: [],
        },
      ],
    },
  ],
});

const makeDatasets = (): Dataset[] => [
  {
    id: 'ds_1',
    name: 'customers.csv',
    fields: ['id', 'name'],
    fieldTypes: {
      id: { type: 'number' },
      name: { type: 'string' },
    },
    rows: [{ id: 1, name: 'Alice' }],
    totalCount: 1,
  },
];

const makeSnapshot = (): ProjectSnapshot => ({
  tree: makeTree(),
  datasets: makeDatasets(),
  sqlHistory: [
    {
      query: 'select * from customers.csv',
      timestamp: 1,
      rowCount: 1,
    },
  ],
  metadata: createDefaultProjectMetadata('Demo Project'),
});

describe('projectStore utilities', () => {
  beforeEach(() => {
    window.localStorage.clear();
    vi.restoreAllMocks();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('creates default metadata and snapshot clones', () => {
    const metadata = createDefaultProjectMetadata('Alpha');
    expect(metadata).toEqual({
      displayName: 'Alpha',
      settings: { cascadeDisable: false, panelPosition: 'right' },
    });

    const snapshot = createEmptyProjectSnapshot('Beta');
    expect(snapshot.tree).toEqual(INITIAL_TREE);
    expect(snapshot.metadata.displayName).toBe('Beta');

    snapshot.metadata.settings.panelPosition = 'left';
    const next = createEmptyProjectSnapshot('Gamma');
    expect(next.metadata.settings.panelPosition).toBe('right');
  });

  it('coerces partial snapshots and builds plain state', () => {
    const snapshot = coerceProjectSnapshot({
      tree: makeTree(),
      metadata: { displayName: 'Remote', settings: { panelPosition: 'left' } },
    });

    expect(snapshot.metadata).toEqual({
      displayName: 'Remote',
      settings: { cascadeDisable: false, panelPosition: 'left' },
    });
    expect(snapshot.datasets).toEqual([]);
    expect(snapshot.sqlHistory).toEqual([]);

    const plain = snapshotToState(snapshot);
    plain.metadata.displayName = 'Mutated';
    expect(snapshot.metadata.displayName).toBe('Remote');
  });

  it('saves, loads and clears draft snapshots from localStorage', () => {
    const nowSpy = vi.spyOn(Date, 'now').mockReturnValue(1700000000000);
    const snapshot = makeSnapshot();

    saveProjectDraft('prj_1', 3, snapshot);
    const key = getProjectDraftStorageKey('prj_1');
    const raw = JSON.parse(window.localStorage.getItem(key) || '{}');
    expect(raw.savedAt).toBe(1700000000000);

    const loaded = loadProjectDraft('prj_1');
    expect(loaded).toMatchObject({ projectId: 'prj_1', version: 3 });
    expect(loaded?.snapshot.tree.children[0].name).toBe('Data Setup');

    clearProjectDraft('prj_1');
    expect(window.localStorage.getItem(key)).toBeNull();
    nowSpy.mockRestore();
  });

  it('returns null for invalid draft payloads and ignores storage errors', () => {
    window.localStorage.setItem(getProjectDraftStorageKey('bad'), '{not-json');
    expect(loadProjectDraft('bad')).toBeNull();

    window.localStorage.setItem(getProjectDraftStorageKey('empty'), JSON.stringify({ projectId: 'empty' }));
    expect(loadProjectDraft('empty')).toBeNull();

    const setItemSpy = vi.spyOn(Storage.prototype, 'setItem').mockImplementation(() => {
      throw new Error('blocked');
    });
    expect(() => saveProjectDraft('prj_2', 1, makeSnapshot())).not.toThrow();
    setItemSpy.mockRestore();

    const removeItemSpy = vi.spyOn(Storage.prototype, 'removeItem').mockImplementation(() => {
      throw new Error('blocked');
    });
    expect(() => clearProjectDraft('prj_2')).not.toThrow();
    removeItemSpy.mockRestore();
  });

  it('reduces local editing actions and keeps pending patches in sync', () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000100);
    let state = projectStoreReducer(initialProjectStoreState(), {
      type: 'RESET',
      projectId: 'prj_local',
      version: 2,
      snapshot: makeSnapshot(),
    });

    state = projectStoreReducer(state, {
      type: 'APPLY_LOCAL_ACTION',
      action: { type: 'UPDATE_NODE_NAME', nodeId: 'op_1', name: 'Renamed Op' },
    });
    expect(state.snapshot.tree.children[0].children[0].name).toBe('Renamed Op');
    expect(state.saveStatus).toBe('dirty');
    expect(state.pendingPatches.length).toBeGreaterThan(0);

    state = projectStoreReducer(state, {
      type: 'APPLY_LOCAL_ACTION',
      action: { type: 'UPDATE_COMMANDS', operationId: 'op_1', commands: [] },
    });
    expect(state.snapshot.tree.children[0].children[0].commands).toEqual([]);

    state = projectStoreReducer(state, {
      type: 'APPLY_LOCAL_ACTION',
      action: { type: 'ADD_CHILD', parentId: 'root' },
    });
    expect(state.snapshot.tree.children).toHaveLength(2);
    expect(state.snapshot.tree.children[1].operationType).toBe('setup');
    expect(state.snapshot.tree.children[1].name).toMatch(/Data Setup/);

    state = projectStoreReducer(state, {
      type: 'APPLY_LOCAL_ACTION',
      action: { type: 'ADD_CHILD', parentId: 'setup_1' },
    });
    const processChildren = state.snapshot.tree.children[0].children;
    expect(processChildren[processChildren.length - 1].operationType).toBe('process');

    state = projectStoreReducer(state, {
      type: 'APPLY_LOCAL_ACTION',
      action: { type: 'MOVE_NODE', nodeId: 'setup_1', direction: 'down' },
    });
    expect(state.snapshot.tree.children.map((child) => child.id)).toEqual([
      expect.not.stringMatching(/^setup_1$/),
      'setup_1',
    ]);

    state = projectStoreReducer(state, {
      type: 'APPLY_LOCAL_ACTION',
      action: { type: 'DELETE_NODE', nodeId: 'root' },
    });
    expect(state.snapshot.tree.id).toBe('root');

    state = projectStoreReducer(state, {
      type: 'APPLY_LOCAL_ACTION',
      action: { type: 'DELETE_NODE', nodeId: 'op_1' },
    });
    expect(buildNodeNameLookup(state.snapshot.tree).op_1).toBeUndefined();

    state = projectStoreReducer(state, {
      type: 'APPLY_LOCAL_ACTION',
      action: { type: 'UPDATE_DATASET_SCHEMA', datasetId: 'ds_1', fieldTypes: { id: { type: 'number' }, extra: { type: 'string' } } },
    });
    expect(state.snapshot.datasets[0].fieldTypes.extra.type).toBe('string');

    state = projectStoreReducer(state, {
      type: 'APPLY_LOCAL_ACTION',
      action: { type: 'DELETE_DATASET', datasetName: 'customers.csv' },
    });
    expect(state.snapshot.datasets).toEqual([]);

    const importedTree = makeTree();
    importedTree.name = 'Imported';
    state = projectStoreReducer(state, {
      type: 'APPLY_LOCAL_ACTION',
      action: { type: 'IMPORT_OPERATIONS', tree: importedTree },
    });
    expect(state.snapshot.tree.name).toBe('Imported');

    state = projectStoreReducer(state, {
      type: 'APPLY_LOCAL_ACTION',
      action: { type: 'UPDATE_PROJECT_SETTINGS', displayName: 'Renamed Project', settings: { cascadeDisable: true, panelPosition: 'left' } },
    });
    expect(state.snapshot.metadata).toEqual({
      displayName: 'Renamed Project',
      settings: { cascadeDisable: true, panelPosition: 'left' },
    });
  });

  it('handles tree, dataset, sql history and metadata replacement actions', () => {
    let state = projectStoreReducer(initialProjectStoreState(), {
      type: 'RESET',
      projectId: 'prj_replace',
      snapshot: makeSnapshot(),
      version: 1,
    });

    const newTree = makeTree();
    newTree.name = 'Alternate';
    state = projectStoreReducer(state, {
      type: 'APPLY_LOCAL_ACTION',
      action: { type: 'SET_TREE', tree: newTree },
    });
    expect(state.snapshot.tree.name).toBe('Alternate');

    state = projectStoreReducer(state, {
      type: 'APPLY_LOCAL_ACTION',
      action: { type: 'SET_DATASETS', datasets: [] },
    });
    expect(state.snapshot.datasets).toEqual([]);

    state = projectStoreReducer(state, {
      type: 'APPLY_LOCAL_ACTION',
      action: { type: 'SET_SQL_HISTORY', sqlHistory: [] },
    });
    expect(state.snapshot.sqlHistory).toEqual([]);

    state = projectStoreReducer(state, {
      type: 'APPLY_LOCAL_ACTION',
      action: { type: 'SET_METADATA', metadata: { displayName: 'Merged', settings: { panelPosition: 'left' } } },
    });
    expect(state.snapshot.metadata).toEqual({
      displayName: 'Merged',
      settings: { cascadeDisable: false, panelPosition: 'left' },
    });
  });

  it('toggles nodes with and without cascade disable', () => {
    let state = projectStoreReducer(initialProjectStoreState(), {
      type: 'RESET',
      projectId: 'prj_toggle',
      snapshot: makeSnapshot(),
      version: 1,
    });

    state = projectStoreReducer(state, {
      type: 'APPLY_LOCAL_ACTION',
      action: { type: 'TOGGLE_NODE_ENABLED', nodeId: 'setup_1', cascadeDisable: true },
    });
    expect(state.snapshot.tree.children[0].enabled).toBe(false);
    expect(state.snapshot.tree.children[0].children[0].enabled).toBe(false);

    state = projectStoreReducer(state, {
      type: 'RESET',
      projectId: 'prj_toggle',
      snapshot: makeSnapshot(),
      version: 1,
    });
    state = projectStoreReducer(state, {
      type: 'APPLY_LOCAL_ACTION',
      action: { type: 'TOGGLE_NODE_ENABLED', nodeId: 'setup_1', cascadeDisable: false },
    });
    expect(state.snapshot.tree.children[0].enabled).toBe(false);
    expect(state.snapshot.tree.children[0].children[0].enabled).toBe(true);
  });

  it('hydrates remote state, rebases local changes and manages save lifecycle', () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000200);
    let state = projectStoreReducer(initialProjectStoreState(), {
      type: 'RESET',
      projectId: 'prj_sync',
      snapshot: makeSnapshot(),
      version: 1,
    });

    state = projectStoreReducer(state, {
      type: 'APPLY_LOCAL_ACTION',
      action: { type: 'UPDATE_NODE_NAME', nodeId: 'op_1', name: 'Local Change' },
    });
    expect(state.pendingPatches.length).toBeGreaterThan(0);

    const remoteSnapshot = makeSnapshot();
    remoteSnapshot.metadata.displayName = 'Remote Snapshot';
    state = projectStoreReducer(state, {
      type: 'HYDRATE_REMOTE',
      projectId: 'prj_sync',
      snapshot: remoteSnapshot,
      version: 3,
      updatedAt: 456,
    });
    expect(state.version).toBe(3);
    expect(state.snapshot.tree.children[0].children[0].name).toBe('Local Change');
    expect(state.syncedSnapshot.metadata.displayName).toBe('Remote Snapshot');
    expect(state.lastSyncAt).toBe(456);

    state = projectStoreReducer(state, { type: 'SAVE_STARTED' });
    expect(state.saveStatus).toBe('saving');

    state = projectStoreReducer(state, {
      type: 'SAVE_SUCCESS',
      version: 4,
      snapshot: state.snapshot,
      updatedAt: 789,
    });
    expect(state.version).toBe(4);
    expect(state.lastSavedAt).toBe(1700000000200);
    expect(state.lastSyncAt).toBe(789);
    expect(state.saveStatus).toBe('saved');

    state = projectStoreReducer(state, {
      type: 'SAVE_ERROR',
      message: 'network failed',
    });
    expect(state.lastError).toBe('network failed');
    expect(state.saveStatus).toBe('saved');

    state = projectStoreReducer(state, {
      type: 'APPLY_LOCAL_ACTION',
      action: { type: 'UPDATE_NODE_NAME', nodeId: 'setup_1', name: 'Unsynced Setup' },
    });
    state = projectStoreReducer(state, {
      type: 'SAVE_ERROR',
      message: 'still offline',
    });
    expect(state.saveStatus).toBe('error');

    state = projectStoreReducer(state, {
      type: 'SAVE_CONFLICT',
      latestVersion: 7,
      remoteState: makeSnapshot(),
      message: 'conflict happened',
    });
    expect(state.saveStatus).toBe('conflict');
    expect(state.conflict?.pendingPatchesCount).toBeGreaterThan(0);

    state = projectStoreReducer(state, { type: 'DISMISS_CONFLICT' });
    expect(state.conflict).toBeNull();
    expect(state.saveStatus).toBe('dirty');
  });

  it('tracks connection state, presence, members, jobs and draft recovery', () => {
    let state = projectStoreReducer(initialProjectStoreState(), {
      type: 'RESET',
      projectId: 'prj_presence',
      snapshot: makeSnapshot(),
      version: 1,
    });

    const members: ProjectMember[] = [
      {
        userId: 'usr_1',
        email: 'owner@example.com',
        displayName: 'Owner',
        role: 'owner',
        createdAt: 1,
        updatedAt: 2,
      },
    ];
    const jobs: ProjectJob[] = [
      {
        id: 'job_1',
        projectId: 'prj_presence',
        type: 'execute',
        status: 'running',
        progress: 50,
        payload: {},
        createdAt: 1,
        updatedAt: 2,
      },
    ];
    const draft = {
      projectId: 'prj_presence',
      version: 5,
      snapshot: { ...makeSnapshot(), metadata: createDefaultProjectMetadata('Recovered Draft') },
      savedAt: 123,
    };

    state = projectStoreReducer(state, { type: 'SET_CONNECTION_STATE', connectionState: 'connected' });
    state = projectStoreReducer(state, {
      type: 'SET_PRESENCE',
      presence: [
        { connectionId: 'conn_1', projectId: 'prj_presence', userId: 'usr_1', editingNodeId: 'setup_1' },
        { connectionId: 'conn_2', projectId: 'prj_presence', userId: 'usr_2' },
      ],
    });
    state = projectStoreReducer(state, { type: 'SET_MEMBERS', members });
    state = projectStoreReducer(state, { type: 'SET_JOBS', jobs });
    state = projectStoreReducer(state, { type: 'SET_DRAFT_RECOVERY', draft });

    expect(state.connectionState).toBe('connected');
    expect(state.remoteEditing.setup_1).toHaveLength(1);
    expect(state.members[0].role).toBe('owner');
    expect(state.jobs[0].status).toBe('running');
    expect(state.draftRecovery?.version).toBe(5);

    state = projectStoreReducer(state, { type: 'RESTORE_DRAFT', draft });
    expect(state.draftRecovery).toBeNull();
    expect(state.saveStatus).toBe('dirty');
  });

  it('evaluates draft recovery and patch rebasing helpers', () => {
    const remote = makeSnapshot();
    const localDraft = {
      projectId: 'prj_1',
      version: 2,
      snapshot: makeSnapshot(),
      savedAt: 1,
    };

    expect(shouldOfferDraftRecovery(null, remote, 2)).toBe(false);
    expect(shouldOfferDraftRecovery({ ...localDraft, version: 3 }, remote, 2)).toBe(true);

    localDraft.snapshot.metadata.displayName = 'Changed';
    expect(shouldOfferDraftRecovery(localDraft, remote, 2)).toBe(true);

    const rebased = rebasePendingPatches(remote, [
      { op: 'set_top_level', key: 'metadata', value: { displayName: 'Local', settings: { cascadeDisable: true, panelPosition: 'left' } } },
    ]);
    expect(rebased.metadata.displayName).toBe('Local');
    expect(buildNodeNameLookup(rebased.tree)).toMatchObject({
      root: 'Project',
      setup_1: 'Data Setup',
      op_1: 'Operation 1',
    });
  });
});
