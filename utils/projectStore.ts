import {
  Command,
  Dataset,
  FieldInfo,
  OperationNode,
  OperationType,
  ProjectConflictInfo,
  ProjectJob,
  ProjectMember,
  ProjectMetadataDetail,
  ProjectSaveStatus,
  ProjectSnapshot,
  SessionConfig,
  SqlHistoryItem,
} from '../types';
import { ProjectPatch, applyPatches, buildStatePatches, replayLocalPatchesOnRemote } from './collabSync';
import { PresenceMember, RealtimeStatus } from './realtimeCollab';

export const PROJECT_DRAFT_STORAGE_PREFIX = 'dmb_project_draft_v1:';

const clone = <T,>(value: T): T => JSON.parse(JSON.stringify(value));

export const DEFAULT_PROJECT_SETTINGS: SessionConfig = {
  cascadeDisable: false,
  panelPosition: 'right',
};

export const INITIAL_TREE: OperationNode = {
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
      commands: [],
      children: [],
    },
  ],
};

export const createDefaultProjectMetadata = (displayName = ''): ProjectMetadataDetail => ({
  displayName,
  settings: clone(DEFAULT_PROJECT_SETTINGS),
});

export const createEmptyProjectSnapshot = (displayName = ''): ProjectSnapshot => ({
  tree: clone(INITIAL_TREE),
  datasets: [],
  sqlHistory: [],
  metadata: createDefaultProjectMetadata(displayName),
});

export type ProjectDraft = {
  projectId: string;
  version: number;
  snapshot: ProjectSnapshot;
  savedAt: number;
};

export type ProjectEditorAction =
  | { type: 'SET_TREE'; tree: OperationNode }
  | { type: 'SET_DATASETS'; datasets: Dataset[] }
  | { type: 'SET_SQL_HISTORY'; sqlHistory: SqlHistoryItem[] }
  | { type: 'SET_METADATA'; metadata: ProjectMetadataDetail }
  | { type: 'UPDATE_COMMANDS'; operationId: string; commands: Command[] }
  | { type: 'UPDATE_NODE_NAME'; nodeId: string; name: string }
  | { type: 'ADD_CHILD'; parentId: string }
  | { type: 'MOVE_NODE'; nodeId: string; direction: 'up' | 'down' }
  | { type: 'DELETE_NODE'; nodeId: string }
  | { type: 'TOGGLE_NODE_ENABLED'; nodeId: string; cascadeDisable: boolean }
  | { type: 'UPDATE_DATASET_SCHEMA'; datasetId: string; fieldTypes: Record<string, FieldInfo> }
  | { type: 'DELETE_DATASET'; datasetName: string }
  | { type: 'IMPORT_OPERATIONS'; tree: OperationNode }
  | { type: 'UPDATE_PROJECT_SETTINGS'; displayName: string; settings: SessionConfig };

export interface ProjectStoreState {
  projectId: string;
  version: number;
  snapshot: ProjectSnapshot;
  syncedSnapshot: ProjectSnapshot;
  pendingPatches: ProjectPatch[];
  saveStatus: ProjectSaveStatus;
  connectionState: RealtimeStatus;
  lastSavedAt: number | null;
  lastSyncAt: number | null;
  lastError: string | null;
  presence: PresenceMember[];
  members: ProjectMember[];
  jobs: ProjectJob[];
  conflict: ProjectConflictInfo | null;
  draftRecovery: ProjectDraft | null;
  remoteEditing: Record<string, PresenceMember[]>;
}

type ProjectStoreEvent =
  | { type: 'RESET'; projectId?: string; snapshot?: ProjectSnapshot; version?: number }
  | { type: 'HYDRATE_REMOTE'; projectId: string; snapshot: ProjectSnapshot; version: number; updatedAt?: number | null }
  | { type: 'APPLY_LOCAL_ACTION'; action: ProjectEditorAction }
  | { type: 'SAVE_STARTED' }
  | { type: 'SAVE_SUCCESS'; version: number; snapshot: ProjectSnapshot; updatedAt?: number | null }
  | { type: 'SAVE_ERROR'; message: string }
  | { type: 'SAVE_CONFLICT'; latestVersion: number; remoteState: ProjectSnapshot; message: string }
  | { type: 'SET_CONNECTION_STATE'; connectionState: RealtimeStatus }
  | { type: 'SET_PRESENCE'; presence: PresenceMember[] }
  | { type: 'SET_MEMBERS'; members: ProjectMember[] }
  | { type: 'SET_JOBS'; jobs: ProjectJob[] }
  | { type: 'SET_DRAFT_RECOVERY'; draft: ProjectDraft | null }
  | { type: 'RESTORE_DRAFT'; draft: ProjectDraft }
  | { type: 'DISMISS_CONFLICT' };

const findNode = (node: OperationNode, targetId: string): OperationNode | null => {
  if (node.id === targetId) return node;
  for (const child of node.children || []) {
    const found = findNode(child, targetId);
    if (found) return found;
  }
  return null;
};

const collectNames = (node: OperationNode, names: Set<string>) => {
  if (node.name) names.add(node.name);
  for (const child of node.children || []) collectNames(child, names);
};

const getUniqueOperationName = (tree: OperationNode, base: string): string => {
  const names = new Set<string>();
  collectNames(tree, names);
  let index = 1;
  let candidate = `${base} ${index}`;
  while (names.has(candidate)) {
    index += 1;
    candidate = `${base} ${index}`;
  }
  return candidate;
};

const mapTree = (node: OperationNode, mapper: (candidate: OperationNode) => OperationNode): OperationNode => {
  const mapped = mapper(node);
  if (!mapped.children || mapped.children.length === 0) return mapped;
  return {
    ...mapped,
    children: mapped.children.map((child) => mapTree(child, mapper)),
  };
};

const moveNodeInTree = (node: OperationNode, nodeId: string, direction: 'up' | 'down'): { node: OperationNode; moved: boolean } => {
  if (!node.children || node.children.length === 0) return { node, moved: false };

  const index = node.children.findIndex((child) => child.id === nodeId);
  if (index >= 0) {
    const nextIndex = direction === 'up' ? index - 1 : index + 1;
    if (nextIndex < 0 || nextIndex >= node.children.length) return { node, moved: false };
    const nextChildren = [...node.children];
    [nextChildren[index], nextChildren[nextIndex]] = [nextChildren[nextIndex], nextChildren[index]];
    return { node: { ...node, children: nextChildren }, moved: true };
  }

  let moved = false;
  const nextChildren = node.children.map((child) => {
    if (moved) return child;
    const result = moveNodeInTree(child, nodeId, direction);
    moved = moved || result.moved;
    return result.node;
  });

  return moved ? { node: { ...node, children: nextChildren }, moved: true } : { node, moved: false };
};

const deleteNodeInTree = (node: OperationNode, nodeId: string): OperationNode => {
  if (!node.children || node.children.length === 0) return node;
  return {
    ...node,
    children: node.children.filter((child) => child.id !== nodeId).map((child) => deleteNodeInTree(child, nodeId)),
  };
};

const toggleNodeInTree = (node: OperationNode, nodeId: string, cascadeDisable: boolean, parentDisabled = false): OperationNode => {
  let nextNode = node;
  if (node.id === nodeId) {
    nextNode = { ...node, enabled: !node.enabled };
  } else if (parentDisabled && cascadeDisable && node.enabled) {
    nextNode = { ...node, enabled: false };
  }

  const nextParentDisabled = !nextNode.enabled;
  if (!nextNode.children || nextNode.children.length === 0) return nextNode;
  return {
    ...nextNode,
    children: nextNode.children.map((child) => toggleNodeInTree(child, nodeId, cascadeDisable, nextParentDisabled)),
  };
};

const mergeMetadata = (metadata?: Partial<ProjectMetadataDetail> | null): ProjectMetadataDetail => ({
  displayName: metadata?.displayName || '',
  settings: {
    ...clone(DEFAULT_PROJECT_SETTINGS),
    ...(metadata?.settings || {}),
  },
});

export const snapshotToState = (snapshot: ProjectSnapshot) => ({
  tree: clone(snapshot.tree),
  datasets: clone(snapshot.datasets),
  sqlHistory: clone(snapshot.sqlHistory),
  metadata: clone(snapshot.metadata),
});

export const coerceProjectSnapshot = (rawState?: Partial<ProjectSnapshot> | null, fallbackDisplayName = ''): ProjectSnapshot => {
  const fallback = createEmptyProjectSnapshot(fallbackDisplayName);
  return {
    tree: rawState?.tree ? clone(rawState.tree) : fallback.tree,
    datasets: Array.isArray(rawState?.datasets) ? clone(rawState.datasets) : fallback.datasets,
    sqlHistory: Array.isArray(rawState?.sqlHistory) ? clone(rawState.sqlHistory) : fallback.sqlHistory,
    metadata: mergeMetadata(rawState?.metadata),
  };
};

const buildRemoteEditingMap = (presence: PresenceMember[]): Record<string, PresenceMember[]> => {
  return presence.reduce<Record<string, PresenceMember[]>>((acc, member) => {
    const key = String(member.editingNodeId || '').trim();
    if (!key) return acc;
    if (!acc[key]) acc[key] = [];
    acc[key].push(member);
    return acc;
  }, {});
};

export const getProjectDraftStorageKey = (projectId: string): string => `${PROJECT_DRAFT_STORAGE_PREFIX}${projectId}`;

export const loadProjectDraft = (projectId: string): ProjectDraft | null => {
  try {
    if (typeof window === 'undefined' || !window.localStorage || !projectId) return null;
    const raw = window.localStorage.getItem(getProjectDraftStorageKey(projectId));
    if (!raw) return null;
    const parsed = JSON.parse(raw) as ProjectDraft;
    if (!parsed?.projectId || !parsed?.snapshot) return null;
    return {
      ...parsed,
      snapshot: coerceProjectSnapshot(parsed.snapshot),
    };
  } catch {
    return null;
  }
};

export const saveProjectDraft = (projectId: string, version: number, snapshot: ProjectSnapshot) => {
  try {
    if (typeof window === 'undefined' || !window.localStorage || !projectId) return;
    const draft: ProjectDraft = {
      projectId,
      version,
      snapshot: clone(snapshot),
      savedAt: Date.now(),
    };
    window.localStorage.setItem(getProjectDraftStorageKey(projectId), JSON.stringify(draft));
  } catch {
    // Ignore storage failures.
  }
};

export const clearProjectDraft = (projectId: string) => {
  try {
    if (typeof window === 'undefined' || !window.localStorage || !projectId) return;
    window.localStorage.removeItem(getProjectDraftStorageKey(projectId));
  } catch {
    // Ignore storage failures.
  }
};

const reduceEditorAction = (snapshot: ProjectSnapshot, action: ProjectEditorAction): ProjectSnapshot => {
  switch (action.type) {
    case 'SET_TREE':
      return { ...snapshot, tree: clone(action.tree) };
    case 'SET_DATASETS':
      return { ...snapshot, datasets: clone(action.datasets) };
    case 'SET_SQL_HISTORY':
      return { ...snapshot, sqlHistory: clone(action.sqlHistory) };
    case 'SET_METADATA':
      return { ...snapshot, metadata: mergeMetadata(action.metadata) };
    case 'UPDATE_COMMANDS':
      return {
        ...snapshot,
        tree: mapTree(snapshot.tree, (candidate) => (
          candidate.id === action.operationId ? { ...candidate, commands: clone(action.commands) } : candidate
        )),
      };
    case 'UPDATE_NODE_NAME':
      return {
        ...snapshot,
        tree: mapTree(snapshot.tree, (candidate) => (
          candidate.id === action.nodeId ? { ...candidate, name: action.name } : candidate
        )),
      };
    case 'ADD_CHILD': {
      const parent = findNode(snapshot.tree, action.parentId);
      if (!parent) return snapshot;
      let newOperationType: OperationType = 'process';
      let newName = getUniqueOperationName(snapshot.tree, 'Operation');
      if (parent.operationType === 'root') {
        newOperationType = 'setup';
        newName = getUniqueOperationName(snapshot.tree, 'Data Setup');
      }
      const newNode: OperationNode = {
        id: `op_${Date.now()}`,
        type: 'operation',
        operationType: newOperationType,
        name: newName,
        enabled: true,
        commands: [],
        children: [],
      };
      return {
        ...snapshot,
        tree: mapTree(snapshot.tree, (candidate) => (
          candidate.id === action.parentId
            ? { ...candidate, children: [...(candidate.children || []), newNode] }
            : candidate
        )),
      };
    }
    case 'MOVE_NODE':
      return {
        ...snapshot,
        tree: moveNodeInTree(snapshot.tree, action.nodeId, action.direction).node,
      };
    case 'DELETE_NODE':
      if (action.nodeId === 'root') return snapshot;
      return {
        ...snapshot,
        tree: deleteNodeInTree(snapshot.tree, action.nodeId),
      };
    case 'TOGGLE_NODE_ENABLED':
      return {
        ...snapshot,
        tree: toggleNodeInTree(snapshot.tree, action.nodeId, action.cascadeDisable),
      };
    case 'UPDATE_DATASET_SCHEMA':
      return {
        ...snapshot,
        datasets: snapshot.datasets.map((dataset) => (
          dataset.id === action.datasetId ? { ...dataset, fieldTypes: clone(action.fieldTypes) } : dataset
        )),
      };
    case 'DELETE_DATASET':
      return {
        ...snapshot,
        datasets: snapshot.datasets.filter((dataset) => dataset.name !== action.datasetName),
      };
    case 'IMPORT_OPERATIONS':
      return {
        ...snapshot,
        tree: clone(action.tree),
      };
    case 'UPDATE_PROJECT_SETTINGS':
      return {
        ...snapshot,
        metadata: {
          displayName: action.displayName,
          settings: {
            ...clone(DEFAULT_PROJECT_SETTINGS),
            ...clone(action.settings),
          },
        },
      };
    default:
      return snapshot;
  }
};

export const initialProjectStoreState = (): ProjectStoreState => {
  const snapshot = createEmptyProjectSnapshot();
  return {
    projectId: '',
    version: 0,
    snapshot,
    syncedSnapshot: clone(snapshot),
    pendingPatches: [],
    saveStatus: 'idle',
    connectionState: 'idle',
    lastSavedAt: null,
    lastSyncAt: null,
    lastError: null,
    presence: [],
    members: [],
    jobs: [],
    conflict: null,
    draftRecovery: null,
    remoteEditing: {},
  };
};

const refreshPendingPatches = (syncedSnapshot: ProjectSnapshot, snapshot: ProjectSnapshot): ProjectPatch[] => {
  return buildStatePatches(snapshotToState(syncedSnapshot), snapshotToState(snapshot));
};

export const projectStoreReducer = (state: ProjectStoreState, event: ProjectStoreEvent): ProjectStoreState => {
  switch (event.type) {
    case 'RESET': {
      const snapshot = event.snapshot ? clone(event.snapshot) : createEmptyProjectSnapshot();
      return {
        ...initialProjectStoreState(),
        projectId: event.projectId || '',
        version: Math.max(Number(event.version || 0), 0),
        snapshot,
        syncedSnapshot: clone(snapshot),
      };
    }
    case 'HYDRATE_REMOTE': {
      const syncedSnapshot = clone(event.snapshot);
      const hasLocalChanges = state.pendingPatches.length > 0;
      const rebasedState = hasLocalChanges
        ? applyPatches(snapshotToState(syncedSnapshot), state.pendingPatches)
        : snapshotToState(syncedSnapshot);
      const snapshot = hasLocalChanges ? coerceProjectSnapshot(rebasedState) : syncedSnapshot;
      const pendingPatches = hasLocalChanges ? refreshPendingPatches(syncedSnapshot, snapshot) : [];
      return {
        ...state,
        projectId: event.projectId,
        version: Math.max(Number(event.version || 0), 0),
        syncedSnapshot,
        snapshot,
        pendingPatches,
        saveStatus: pendingPatches.length > 0 ? 'dirty' : 'saved',
        lastSavedAt: pendingPatches.length > 0 ? state.lastSavedAt : Date.now(),
        lastSyncAt: event.updatedAt ? Number(event.updatedAt) : Date.now(),
        lastError: null,
      };
    }
    case 'APPLY_LOCAL_ACTION': {
      const snapshot = reduceEditorAction(state.snapshot, event.action);
      const pendingPatches = refreshPendingPatches(state.syncedSnapshot, snapshot);
      return {
        ...state,
        snapshot,
        pendingPatches,
        saveStatus: pendingPatches.length > 0 ? 'dirty' : 'saved',
        lastError: null,
      };
    }
    case 'SAVE_STARTED':
      return {
        ...state,
        saveStatus: state.pendingPatches.length > 0 ? 'saving' : state.saveStatus,
        lastError: null,
      };
    case 'SAVE_SUCCESS': {
      const syncedSnapshot = clone(event.snapshot);
      const snapshot = state.pendingPatches.length > 0
        ? state.snapshot
        : syncedSnapshot;
      const pendingPatches = refreshPendingPatches(syncedSnapshot, snapshot);
      return {
        ...state,
        version: Math.max(Number(event.version || 0), state.version),
        syncedSnapshot,
        snapshot,
        pendingPatches,
        saveStatus: pendingPatches.length > 0 ? 'dirty' : 'saved',
        lastSavedAt: Date.now(),
        lastSyncAt: event.updatedAt ? Number(event.updatedAt) : Date.now(),
        lastError: null,
        conflict: pendingPatches.length > 0 ? state.conflict : null,
      };
    }
    case 'SAVE_ERROR':
      return {
        ...state,
        saveStatus: state.pendingPatches.length > 0 ? 'error' : state.saveStatus,
        lastError: event.message,
      };
    case 'SAVE_CONFLICT':
      return {
        ...state,
        saveStatus: state.pendingPatches.length > 0 ? 'conflict' : state.saveStatus,
        conflict: {
          latestVersion: event.latestVersion,
          remoteState: clone(event.remoteState),
          pendingPatchesCount: state.pendingPatches.length,
          message: event.message,
        },
        lastError: event.message,
      };
    case 'SET_CONNECTION_STATE':
      return {
        ...state,
        connectionState: event.connectionState,
      };
    case 'SET_PRESENCE':
      return {
        ...state,
        presence: clone(event.presence),
        remoteEditing: buildRemoteEditingMap(event.presence),
      };
    case 'SET_MEMBERS':
      return {
        ...state,
        members: clone(event.members),
      };
    case 'SET_JOBS':
      return {
        ...state,
        jobs: clone(event.jobs),
      };
    case 'SET_DRAFT_RECOVERY':
      return {
        ...state,
        draftRecovery: event.draft ? clone(event.draft) : null,
      };
    case 'RESTORE_DRAFT': {
      const snapshot = clone(event.draft.snapshot);
      const pendingPatches = refreshPendingPatches(state.syncedSnapshot, snapshot);
      return {
        ...state,
        snapshot,
        pendingPatches,
        draftRecovery: null,
        saveStatus: pendingPatches.length > 0 ? 'dirty' : 'saved',
      };
    }
    case 'DISMISS_CONFLICT':
      return {
        ...state,
        conflict: null,
        saveStatus: state.pendingPatches.length > 0 ? 'dirty' : state.saveStatus,
      };
    default:
      return state;
  }
};

export const shouldOfferDraftRecovery = (draft: ProjectDraft | null, remoteSnapshot: ProjectSnapshot, remoteVersion: number): boolean => {
  if (!draft) return false;
  if (draft.version > remoteVersion) return true;
  try {
    return JSON.stringify(snapshotToState(draft.snapshot)) !== JSON.stringify(snapshotToState(remoteSnapshot));
  } catch {
    return true;
  }
};

export const rebasePendingPatches = (remoteSnapshot: ProjectSnapshot, pendingPatches: ProjectPatch[]): ProjectSnapshot => {
  const rebased = replayLocalPatchesOnRemote(snapshotToState(remoteSnapshot), pendingPatches);
  return coerceProjectSnapshot(rebased);
};

export const buildNodeNameLookup = (node: OperationNode): Record<string, string> => {
  const entries: Record<string, string> = {};
  const walk = (candidate: OperationNode) => {
    entries[candidate.id] = candidate.name;
    for (const child of candidate.children || []) walk(child);
  };
  walk(node);
  return entries;
};
