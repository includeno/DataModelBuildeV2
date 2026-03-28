import React, { useCallback, useEffect, useMemo, useReducer, useRef, useState } from 'react';
import { Sidebar } from './components/Sidebar';
import { LineagePanel } from './components/LineagePanel';
import { Workspace } from './components/Workspace';
import { TopBar } from './components/TopBar';
import { DataImportModal } from './components/DataImport';
import { SettingsModal } from './components/SettingsModal';
import { SessionSettingsModal } from './components/SessionSettingsModal';
import { SessionDiagnosticsModal } from './components/SessionDiagnosticsModal';
import { PathConditionsModal } from './components/PathConditionsModal';
import { DatasetSchemaModal } from './components/DatasetSchemaModal';
import { LoginPage } from './components/LoginPage';
import { ProjectMembersModal } from './components/ProjectMembersModal';
import { ConflictNoticeModal } from './components/ConflictNoticeModal';
import { DraftRecoveryModal } from './components/DraftRecoveryModal';
import { CollabPresenceFloat, type CollabPresenceItem } from './components/CollabPresenceFloat';
import {
  AppearanceConfig,
  ApiConfig,
  Command,
  DataType,
  Dataset,
  ExecutionResult,
  FieldInfo,
  OperationNode,
  OperationType,
  ProjectMember,
  ProjectMetadata,
  ProjectRole,
  SessionConfig,
  SessionDiagnosticsReport,
  SqlHistoryItem,
  LineageMap,
} from './types';
import { api } from './utils/api';
import { buildConflictNotice, DebouncedCommitQueue } from './utils/collabSync';
import { RealtimeProjectClient, RealtimeServerEvent } from './utils/realtimeCollab';
import {
  buildNodeNameLookup,
  clearProjectDraft,
  coerceProjectSnapshot,
  createDefaultProjectMetadata,
  INITIAL_TREE,
  initialProjectStoreState,
  loadProjectDraft,
  projectStoreReducer,
  rebasePendingPatches,
  saveProjectDraft,
  shouldOfferDraftRecovery,
  snapshotToState,
  type ProjectDraft,
  type ProjectEditorAction,
} from './utils/projectStore';

const DEFAULT_APPEARANCE: AppearanceConfig = {
  textSize: 13,
  textColor: '#374151',
  guideLineColor: '#E5E7EB',
  showGuideLines: true,
  showNodeIds: false,
  showOperationIds: false,
  showCommandIds: false,
  showDatasetIds: false,
};

const CONNECTION_STATE_STORAGE_KEY = 'dmb_connection_state_v1';

type PersistedConnectionState = {
  apiConfig: ApiConfig;
  knownServers: string[];
  savedAt: number;
};

const parseAuthEnabled = (payload: any, fallback = true): boolean => {
  if (typeof payload?.authEnabled === 'boolean') return payload.authEnabled;
  if (typeof payload?.authRequired === 'boolean') return payload.authRequired;
  return fallback;
};

const isConnectionPersistenceEnabled = (): boolean => {
  if (typeof window === 'undefined' || !window.localStorage) return false;
  const userAgent = typeof navigator !== 'undefined' ? String(navigator.userAgent || '') : '';
  return !/jsdom/i.test(userAgent);
};

const loadPersistedConnectionState = (): PersistedConnectionState | null => {
  try {
    if (!isConnectionPersistenceEnabled()) return null;
    const raw = window.localStorage.getItem(CONNECTION_STATE_STORAGE_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw) as PersistedConnectionState;
    if (!parsed?.apiConfig?.baseUrl || typeof parsed.apiConfig.isMock !== 'boolean') return null;
    if (!Array.isArray(parsed.knownServers)) return null;
    return parsed;
  } catch {
    return null;
  }
};

const savePersistedConnectionState = (apiConfig: ApiConfig, knownServers: string[]) => {
  try {
    if (!isConnectionPersistenceEnabled()) return;
    const next: PersistedConnectionState = {
      apiConfig,
      knownServers: Array.from(new Set(knownServers.filter(Boolean))),
      savedAt: Date.now(),
    };
    window.localStorage.setItem(CONNECTION_STATE_STORAGE_KEY, JSON.stringify(next));
  } catch {
    // ignore storage failures
  }
};

const findNodeById = (node: OperationNode, targetId: string): OperationNode | null => {
  if (node.id === targetId) return node;
  for (const child of node.children || []) {
    const found = findNodeById(child, targetId);
    if (found) return found;
  }
  return null;
};

const getPreferredSelectedNodeId = (nextTree: OperationNode): string => {
  const firstSetup = nextTree.children?.find(c => c.operationType === 'setup');
  if (firstSetup) return firstSetup.id;
  const firstChild = nextTree.children?.[0];
  if (firstChild) return firstChild.id;
  return 'root';
};

const getErrorMessage = (error: unknown, fallback: string): string => {
  if (error instanceof Error && error.message) return error.message;
  return fallback;
};

function App() {
  const [projects, setProjects] = useState<ProjectMetadata[]>([]);
  const [currentProject, setCurrentProject] = useState<ProjectMetadata | null>(null);
  const [projectStore, dispatchProjectStore] = useReducer(projectStoreReducer, undefined, initialProjectStoreState);
  const projectsRef = useRef<ProjectMetadata[]>(projects);
  const projectStoreRef = useRef(projectStore);
  const currentProjectRef = useRef<ProjectMetadata | null>(currentProject);
  const commitQueueRef = useRef<DebouncedCommitQueue | null>(null);
  const realtimeClientRef = useRef<RealtimeProjectClient | null>(null);
  const lastQueuedSignatureRef = useRef('');

  const [selectedNodeId, setSelectedNodeId] = useState<string>('setup_1');
  const [currentView, setCurrentView] = useState<'workflow' | 'sql' | 'data'>('workflow');

  const [isImportOpen, setIsImportOpen] = useState(false);
  const [isSettingsOpen, setIsSettingsOpen] = useState(false);
  const [isSessionSettingsOpen, setIsSessionSettingsOpen] = useState(false);
  const [isDiagnosticsOpen, setIsDiagnosticsOpen] = useState(false);
  const [isMembersOpen, setIsMembersOpen] = useState(false);
  const [isPathModalOpen, setIsPathModalOpen] = useState(false);
  const [targetCommandId, setTargetCommandId] = useState<string | undefined>(undefined);
  const [isSchemaModalOpen, setIsSchemaModalOpen] = useState(false);
  const [selectedDatasetForSchema, setSelectedDatasetForSchema] = useState<Dataset | null>(null);
  const [diagnosticsReport, setDiagnosticsReport] = useState<SessionDiagnosticsReport | null>(null);
  const [diagnosticsLoading, setDiagnosticsLoading] = useState(false);
  const [diagnosticsError, setDiagnosticsError] = useState<string | null>(null);

  const [lineageData, setLineageData] = useState<{ nodeId: string; commandId?: string; sourceTable?: string; map: LineageMap } | null>(null);
  const [lineageLoading, setLineageLoading] = useState(false);

  const [membersLoading, setMembersLoading] = useState(false);
  const [membersError, setMembersError] = useState<string | null>(null);
  const [syncingProject, setSyncingProject] = useState(false);

  const [isRightPanelOpen, setIsRightPanelOpen] = useState(true);
  const [rightPanelWidth, setRightPanelWidth] = useState(400);
  const [sidebarWidth, setSidebarWidth] = useState(260);
  const [isSidebarResizing, setIsSidebarResizing] = useState(false);
  const [isMobileSidebarOpen, setIsMobileSidebarOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const [previewData, setPreviewData] = useState<ExecutionResult | null>(null);
  const [isResizing, setIsResizing] = useState(false);
  const [backendStatus, setBackendStatus] = useState<'mock' | 'checking' | 'online' | 'offline'>('checking');
  const [sqlRunRequestId, setSqlRunRequestId] = useState(0);
  const [sqlRunState, setSqlRunState] = useState({ canRun: false, running: false });
  const [authChecking, setAuthChecking] = useState(false);
  const [authChecked, setAuthChecked] = useState(false);
  const [authRequired, setAuthRequired] = useState(false);
  const [authLoading, setAuthLoading] = useState(false);
  const [authError, setAuthError] = useState<string | null>(null);
  const [authEnabled, setAuthEnabled] = useState(true);
  const [authModeReady, setAuthModeReady] = useState(false);
  const [currentUser, setCurrentUser] = useState<{ id?: string; email?: string; displayName?: string } | null>(null);

  const [apiConfig, setApiConfig] = useState<ApiConfig>({ baseUrl: 'mockServer', isMock: true });
  const [knownServers, setKnownServers] = useState<string[]>(['mockServer', 'http://localhost:8000']);
  const [appearance, setAppearance] = useState<AppearanceConfig>(DEFAULT_APPEARANCE);
  const [sessionStorageInfo, setSessionStorageInfo] = useState<{ dataRoot: string; sessionsDir: string; relative: string } | null>(null);
  const [sessionStorageFolders, setSessionStorageFolders] = useState<{ name: string; path: string }[]>([]);
  const [sessionStorageError, setSessionStorageError] = useState<string | null>(null);
  const [configReady, setConfigReady] = useState(false);

  const [targetSqlTable, setTargetSqlTable] = useState<string | null>(null);
  const [targetDataTable, setTargetDataTable] = useState<string | null>(null);

  useEffect(() => {
    projectsRef.current = projects;
  }, [projects]);

  useEffect(() => {
    projectStoreRef.current = projectStore;
  }, [projectStore]);

  useEffect(() => {
    currentProjectRef.current = currentProject;
  }, [currentProject]);

  const selectedNode = useMemo(() => findNodeById(projectStore.snapshot.tree, selectedNodeId), [projectStore.snapshot.tree, selectedNodeId]);

  useEffect(() => {
    if (findNodeById(projectStore.snapshot.tree, selectedNodeId)) return;
    setSelectedNodeId(getPreferredSelectedNodeId(projectStore.snapshot.tree));
  }, [projectStore.snapshot.tree, selectedNodeId]);

  const sourceValidation = useMemo(() => {
    const sources: Command[] = [];
    const collectSources = (node: OperationNode) => {
      node.commands?.forEach(c => {
        if (c.type === 'source') sources.push(c);
      });
      node.children?.forEach(collectSources);
    };
    collectSources(projectStore.snapshot.tree);

    const configured = sources.filter(c => {
      const table = c.config?.mainTable;
      return !!table && String(table).trim().length > 0;
    });
    const names = configured.map(c => String(c.config?.mainTable).trim());
    const uniqueNames = new Set(names);

    return {
      total: sources.length,
      configured: configured.length,
      hasAnyConfigured: configured.length > 0,
      hasIncomplete: sources.some(c => {
        const table = c.config?.mainTable;
        return !table || String(table).trim().length === 0;
      }),
      hasDuplicate: names.length !== uniqueNames.size,
    };
  }, [projectStore.snapshot.tree]);

  const globalInputSchema = useMemo(() => {
    const schema: Record<string, DataType> = {};
    projectStore.snapshot.datasets.forEach(ds => {
      if (ds.fieldTypes) {
        Object.entries(ds.fieldTypes).forEach(([key, value]) => {
          schema[key] = (value as FieldInfo).type;
        });
      } else {
        ds.fields.forEach(field => {
          schema[field] = 'string';
        });
      }
    });
    return schema;
  }, [projectStore.snapshot.datasets]);

  const nodeNameLookup = useMemo(() => buildNodeNameLookup(projectStore.snapshot.tree), [projectStore.snapshot.tree]);

  const remoteEditorsByNode = useMemo(() => {
    const next: Record<string, string[]> = {};
    Object.entries(projectStore.remoteEditing).forEach(([nodeId, members]) => {
      const names = members
        .filter(member => member.userId !== currentUser?.id)
        .map(member => member.displayName || member.email || member.userId)
        .filter(Boolean);
      if (names.length > 0) next[nodeId] = names;
    });
    return next;
  }, [currentUser?.id, projectStore.remoteEditing]);

  const remoteEditingLabel = useMemo(() => {
    const entries = Object.entries(remoteEditorsByNode);
    if (entries.length === 0) return null;
    const [nodeId, names] = entries[0];
    const nodeName = nodeNameLookup[nodeId] || nodeId;
    return `${names.join(', ')} 正在编辑 ${nodeName}`;
  }, [nodeNameLookup, remoteEditorsByNode]);

  const remotePresenceMembers = useMemo<CollabPresenceItem[]>(() => {
    return projectStore.presence
      .filter(member => member.userId !== currentUser?.id)
      .map(member => ({
        connectionId: member.connectionId,
        label: member.displayName || member.email || member.userId,
        email: member.email || undefined,
        role: member.role || undefined,
        editingNodeName: member.editingNodeId ? (nodeNameLookup[member.editingNodeId] || member.editingNodeId) : null,
      }));
  }, [currentUser?.id, nodeNameLookup, projectStore.presence]);

  const isAuthenticated = useMemo(() => {
    if (apiConfig.isMock || !authEnabled) return false;
    return authChecked && !authChecking && !authRequired;
  }, [apiConfig.isMock, authEnabled, authChecked, authChecking, authRequired]);

  const currentProjectName = projectStore.snapshot.metadata.displayName || currentProject?.name || '';
  const currentProjectSettings = projectStore.snapshot.metadata.settings || createDefaultProjectMetadata().settings;
  const currentProjectCanManage = ['owner', 'admin'].includes(String(currentProject?.role || ''));
  const onlineMembersCount = projectStore.presence.length;

  const resetProjectState = useCallback((projectId = '') => {
    dispatchProjectStore({ type: 'RESET', projectId });
    setSelectedNodeId('setup_1');
    setPreviewData(null);
    setCurrentView('workflow');
    setTargetSqlTable(null);
    setTargetDataTable(null);
    setDiagnosticsReport(null);
    setDiagnosticsError(null);
    setMembersError(null);
  }, []);

  const applyLocalAction = useCallback((action: ProjectEditorAction) => {
    dispatchProjectStore({ type: 'APPLY_LOCAL_ACTION', action });
  }, []);

  const fetchProjectMembers = useCallback(async (projectId: string) => {
    setMembersLoading(true);
    setMembersError(null);
    try {
      const members = await api.get(apiConfig, `/projects/${projectId}/members`) as ProjectMember[];
      dispatchProjectStore({ type: 'SET_MEMBERS', members: members || [] });
    } catch (error) {
      setMembersError(getErrorMessage(error, 'Failed to load members'));
    } finally {
      setMembersLoading(false);
    }
  }, [apiConfig]);

  const fetchProjectJobs = useCallback(async (projectId: string) => {
    try {
      const jobs = await api.get(apiConfig, `/projects/${projectId}/jobs`) as any[];
      dispatchProjectStore({ type: 'SET_JOBS', jobs: jobs || [] });
    } catch {
      // ignore jobs failures in the main surface for now
    }
  }, [apiConfig]);

  const loadProject = useCallback(async (projectOrId: string | ProjectMetadata) => {
    const projectMeta = typeof projectOrId === 'string'
      ? projectsRef.current.find(item => item.id === projectOrId) || await api.get(apiConfig, `/projects/${projectOrId}`) as ProjectMetadata
      : projectOrId;
    if (!projectMeta?.id) return;

    const projectId = projectMeta.id;
    setCurrentProject(projectMeta);
    setPreviewData(null);
    setTargetSqlTable(null);
    setTargetDataTable(null);
    setDiagnosticsReport(null);
    setDiagnosticsError(null);

    const [stateEnvelope, metadata, datasets] = await Promise.all([
      api.get(apiConfig, `/projects/${projectId}/state`) as Promise<any>,
      api.get(apiConfig, `/projects/${projectId}/metadata`).catch(() => createDefaultProjectMetadata(projectMeta.name)),
      api.get(apiConfig, `/projects/${projectId}/datasets`) as Promise<Dataset[]>,
    ]);

    const snapshot = coerceProjectSnapshot({
      ...(stateEnvelope?.state || {}),
      datasets: datasets || [],
      metadata: metadata || createDefaultProjectMetadata(projectMeta.name),
    }, projectMeta.name);

    dispatchProjectStore({
      type: 'HYDRATE_REMOTE',
      projectId,
      snapshot,
      version: Number(stateEnvelope?.version || 0),
      updatedAt: stateEnvelope?.updatedAt || Date.now(),
    });
    dispatchProjectStore({ type: 'DISMISS_CONFLICT' });
    setSelectedNodeId(getPreferredSelectedNodeId(snapshot.tree));

    const draft = loadProjectDraft(projectId);
    if (shouldOfferDraftRecovery(draft, snapshot, Number(stateEnvelope?.version || 0))) {
      dispatchProjectStore({ type: 'SET_DRAFT_RECOVERY', draft: draft as ProjectDraft });
    } else {
      dispatchProjectStore({ type: 'SET_DRAFT_RECOVERY', draft: null });
      clearProjectDraft(projectId);
    }

    await Promise.all([
      fetchProjectMembers(projectId),
      fetchProjectJobs(projectId),
    ]);
  }, [apiConfig, fetchProjectJobs, fetchProjectMembers]);

  const fetchProjects = useCallback(async () => {
    try {
      const list = await api.get(apiConfig, '/projects') as ProjectMetadata[];
      const nextProjects = list || [];
      setProjects(nextProjects);

      const activeId = currentProjectRef.current?.id || '';
      if (activeId && nextProjects.some(project => project.id === activeId)) {
        const latest = nextProjects.find(project => project.id === activeId) || null;
        setCurrentProject(latest);
        return;
      }

      if (nextProjects.length > 0) {
        await loadProject(nextProjects[0]);
      } else {
        setCurrentProject(null);
        resetProjectState();
      }
    } catch (error) {
      console.error('Failed to fetch projects', error);
    }
  }, [apiConfig, loadProject, resetProjectState]);

  useEffect(() => {
    let cancelled = false;
    const loadDefaultServer = async () => {
      const persisted = loadPersistedConnectionState();
      if (persisted) {
        if (!cancelled) {
          setApiConfig(persisted.apiConfig);
          const mergedServers = Array.from(new Set([
            'mockServer',
            'http://localhost:8000',
            ...persisted.knownServers,
            persisted.apiConfig.baseUrl,
          ].filter(Boolean)));
          setKnownServers(mergedServers);
          const initialAuthEnabled = !persisted.apiConfig.isMock;
          setAuthEnabled(initialAuthEnabled);
          api.setAuthApiEnabled(initialAuthEnabled);
          setConfigReady(true);
        }
        return;
      }
      try {
        const res = await fetch('http://localhost:8000/config/default_server');
        if (!res.ok) throw new Error('Failed to load default server');
        const data = await res.json();
        const server = typeof data?.server === 'string' ? data.server : 'mockServer';
        const isMock = server === 'mockServer';
        const nextAuthEnabled = isMock ? false : parseAuthEnabled(data, true);
        if (!cancelled) {
          setApiConfig({ baseUrl: server, isMock });
          setAuthEnabled(nextAuthEnabled);
          api.setAuthApiEnabled(nextAuthEnabled);
          if (server) {
            setKnownServers(prev => prev.includes(server) ? prev : [...prev, server]);
          }
        }
      } catch {
        // Default to mock if backend unreachable.
      } finally {
        if (!cancelled) setConfigReady(true);
      }
    };
    loadDefaultServer();
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    if (!configReady) return;
    savePersistedConnectionState(apiConfig, knownServers);
  }, [apiConfig, knownServers, configReady]);

  useEffect(() => {
    let cancelled = false;
    const detectAuthMode = async () => {
      if (!configReady) return;
      if (apiConfig.isMock) {
        if (!cancelled) {
          setAuthEnabled(false);
          api.setAuthApiEnabled(false);
          setAuthModeReady(true);
          setAuthChecking(false);
          setAuthChecked(true);
          setAuthRequired(false);
          setAuthLoading(false);
          setAuthError(null);
          setCurrentUser({ id: 'usr_mock_owner', email: 'mock.owner@example.com', displayName: 'Mock Owner' });
        }
        return;
      }
      if (!cancelled) setAuthModeReady(false);
      try {
        const res = await fetch(`${apiConfig.baseUrl}/config/auth`, { credentials: 'include' });
        const payload = res.ok ? await res.json() : null;
        const enabled = parseAuthEnabled(payload, true);
        if (!cancelled) {
          setAuthEnabled(enabled);
          api.setAuthApiEnabled(enabled);
          setAuthModeReady(true);
          if (!enabled) {
            setAuthChecking(false);
            setAuthChecked(true);
            setAuthRequired(false);
            setAuthLoading(false);
            setAuthError(null);
            setCurrentUser({ id: 'usr_auth_disabled', email: 'auth-disabled@local', displayName: 'Auth Disabled' });
          }
        }
      } catch {
        if (!cancelled) {
          setAuthEnabled(true);
          api.setAuthApiEnabled(true);
          setAuthModeReady(true);
        }
      }
    };
    detectAuthMode();
    return () => {
      cancelled = true;
    };
  }, [configReady, apiConfig.baseUrl, apiConfig.isMock]);

  useEffect(() => {
    if (!configReady || !authModeReady) return;
    if (apiConfig.isMock || !authEnabled) {
      setAuthChecking(false);
      setAuthChecked(true);
      setAuthRequired(false);
      setAuthLoading(false);
      setAuthError(null);
      return;
    }

    let cancelled = false;
    const verifyAuth = async () => {
      if (backendStatus === 'checking' || backendStatus === 'mock') return;
      if (backendStatus === 'offline') {
        if (!cancelled) {
          setAuthChecking(false);
          setAuthChecked(true);
          setAuthRequired(false);
          setAuthError(`服务器连接失败：${apiConfig.baseUrl}`);
        }
        return;
      }

      setAuthChecking(true);
      setAuthChecked(false);
      setAuthError(null);
      try {
        const me = await api.authMe(apiConfig);
        if (!cancelled) {
          setAuthRequired(false);
          setCurrentUser(me);
        }
      } catch (error: any) {
        if (!cancelled) {
          const message = String(error?.message || '');
          setAuthRequired(true);
          if (message && !message.toLowerCase().includes('unauthorized')) {
            setAuthError(`认证状态检查失败：${message}`);
          }
        }
      } finally {
        if (!cancelled) {
          setAuthChecking(false);
          setAuthChecked(true);
        }
      }
    };
    verifyAuth();
    return () => {
      cancelled = true;
    };
  }, [apiConfig, authEnabled, authModeReady, backendStatus, configReady]);

  useEffect(() => {
    if (!configReady || !authModeReady || !authChecked) return;
    if (!apiConfig.isMock && authEnabled && authRequired) return;
    fetchProjects();
  }, [apiConfig, authChecked, authEnabled, authModeReady, authRequired, configReady, fetchProjects]);

  useEffect(() => {
    if (!isSettingsOpen) return;
    if (apiConfig.isMock) {
      setSessionStorageError('Switch to a real backend server to manage session storage.');
      return;
    }
    const run = async () => {
      try {
        const infoRes = await fetch(`${apiConfig.baseUrl}/config/session_storage`);
        if (!infoRes.ok) throw new Error('Failed to load session storage');
        const info = await infoRes.json();
        const listRes = await fetch(`${apiConfig.baseUrl}/config/session_storage/list?path=`);
        if (!listRes.ok) throw new Error('Failed to list session storage');
        const list = await listRes.json();
        setSessionStorageInfo(info);
        setSessionStorageFolders(list.folders || []);
        setSessionStorageError(null);
      } catch (error) {
        setSessionStorageError(getErrorMessage(error, 'Failed to load session storage'));
      }
    };
    run();
  }, [apiConfig.baseUrl, apiConfig.isMock, isSettingsOpen]);

  useEffect(() => {
    if (!isSidebarResizing) return;
    const handleMove = (e: MouseEvent) => {
      const minWidth = 200;
      const maxWidth = 520;
      const nextWidth = Math.min(maxWidth, Math.max(minWidth, e.clientX));
      setSidebarWidth(nextWidth);
    };
    const handleUp = () => setIsSidebarResizing(false);
    window.addEventListener('mousemove', handleMove);
    window.addEventListener('mouseup', handleUp);
    return () => {
      window.removeEventListener('mousemove', handleMove);
      window.removeEventListener('mouseup', handleUp);
    };
  }, [isSidebarResizing]);

  useEffect(() => {
    if (!configReady) return;
    let cancelled = false;

    const checkBackend = async () => {
      if (apiConfig.isMock) {
        if (!cancelled) setBackendStatus('mock');
        return;
      }
      const ok = await api.ping(apiConfig);
      if (!cancelled) setBackendStatus(ok ? 'online' : 'offline');
    };

    setBackendStatus(apiConfig.isMock ? 'mock' : 'checking');
    checkBackend();
    const intervalId = setInterval(checkBackend, 10000);
    return () => {
      cancelled = true;
      clearInterval(intervalId);
    };
  }, [apiConfig, configReady]);

  useEffect(() => {
    const handleMouseMove = (e: MouseEvent) => {
      if (!isResizing) return;
      e.preventDefault();
      const position = currentProjectSettings.panelPosition || 'right';
      if (position === 'right') {
        const newWidth = document.body.clientWidth - e.clientX;
        setRightPanelWidth(Math.max(200, Math.min(newWidth, document.body.clientWidth - 300)));
      } else if (position === 'left') {
        setRightPanelWidth(Math.max(200, Math.min(e.clientX, document.body.clientWidth - 300)));
      } else if (position === 'bottom') {
        const newHeight = document.body.clientHeight - e.clientY;
        setRightPanelWidth(Math.max(100, Math.min(newHeight, document.body.clientHeight - 100)));
      } else if (position === 'top') {
        const headerHeight = 56;
        setRightPanelWidth(Math.max(100, Math.min(e.clientY - headerHeight, document.body.clientHeight - 100)));
      }
    };

    const handleMouseUp = () => {
      setIsResizing(false);
      document.body.style.cursor = 'default';
    };

    if (isResizing) {
      document.addEventListener('mousemove', handleMouseMove);
      document.addEventListener('mouseup', handleMouseUp);
      const position = currentProjectSettings.panelPosition || 'right';
      document.body.style.cursor = position === 'right' || position === 'left' ? 'col-resize' : 'row-resize';
    }

    return () => {
      document.removeEventListener('mousemove', handleMouseMove);
      document.removeEventListener('mouseup', handleMouseUp);
      document.body.style.cursor = 'default';
    };
  }, [currentProjectSettings.panelPosition, isResizing]);

  useEffect(() => {
    if (!currentProject?.id) {
      realtimeClientRef.current?.disconnect();
      realtimeClientRef.current = null;
      dispatchProjectStore({ type: 'SET_CONNECTION_STATE', connectionState: 'idle' });
      return;
    }
    if (apiConfig.isMock || backendStatus !== 'online') {
      realtimeClientRef.current?.disconnect();
      realtimeClientRef.current = null;
      dispatchProjectStore({ type: 'SET_CONNECTION_STATE', connectionState: apiConfig.isMock ? 'idle' : 'closed' });
      return;
    }

    const client = new RealtimeProjectClient({
      baseUrl: apiConfig.baseUrl,
      projectId: currentProject.id,
      tokenProvider: () => api.getAuthTokens()?.accessToken || null,
      initialState: snapshotToState(projectStoreRef.current.syncedSnapshot),
      initialVersion: projectStoreRef.current.version,
      onStatusChange: (status) => {
        dispatchProjectStore({ type: 'SET_CONNECTION_STATE', connectionState: status });
      },
      onPresenceChange: (presence) => {
        dispatchProjectStore({ type: 'SET_PRESENCE', presence });
      },
      onStateChange: (state, event: RealtimeServerEvent) => {
        const hadPending = projectStoreRef.current.pendingPatches.length > 0;
        const snapshot = coerceProjectSnapshot(state as any, currentProjectRef.current?.name || '');
        dispatchProjectStore({
          type: 'HYDRATE_REMOTE',
          projectId: currentProject.id,
          snapshot,
          version: Number(event.version || 0),
          updatedAt: event.serverTime || Date.now(),
        });
        if (hadPending && event.payload?.updatedBy !== currentUser?.id) {
          dispatchProjectStore({
            type: 'SAVE_CONFLICT',
            latestVersion: Number(event.version || 0),
            remoteState: snapshot,
            message: buildConflictNotice(Number(event.version || 0)),
          });
        }
      },
      onConflictNotice: (event) => {
        dispatchProjectStore({
          type: 'SAVE_CONFLICT',
          latestVersion: Number(event.version || projectStoreRef.current.version),
          remoteState: projectStoreRef.current.syncedSnapshot,
          message: String(event.payload?.message || 'Realtime conflict detected.'),
        });
      },
      onGapDetected: async () => {
        await handleManualSync();
      },
    });

    realtimeClientRef.current = client;
    client.connect();

    return () => {
      client.disconnect();
      if (realtimeClientRef.current === client) realtimeClientRef.current = null;
    };
  }, [apiConfig, backendStatus, currentProject?.id, currentUser?.id]);

  useEffect(() => {
    if (!currentProject?.id) return;
    if (projectStore.pendingPatches.length > 0) {
      saveProjectDraft(currentProject.id, projectStore.version, projectStore.snapshot);
      return;
    }
    clearProjectDraft(currentProject.id);
  }, [currentProject?.id, projectStore.pendingPatches, projectStore.snapshot, projectStore.version]);

  useEffect(() => {
    if (!currentProject?.id) {
      commitQueueRef.current?.dispose();
      commitQueueRef.current = null;
      lastQueuedSignatureRef.current = '';
      return;
    }

    const queue = new DebouncedCommitQueue(
      async (request) => api.post(apiConfig, `/projects/${currentProject.id}/state/commit`, request),
      {
        debounceMs: 600,
        conflictResolver: async (conflict, request) => {
          const remoteSnapshot = coerceProjectSnapshot(conflict.state as any, currentProject.name);
          dispatchProjectStore({
            type: 'SAVE_CONFLICT',
            latestVersion: Number(conflict.latestVersion || 0),
            remoteState: remoteSnapshot,
            message: buildConflictNotice(Number(conflict.latestVersion || 0)),
          });
          const rebased = rebasePendingPatches(remoteSnapshot, request.patches || projectStoreRef.current.pendingPatches);
          return {
            baseVersion: Number(conflict.latestVersion || 0),
            state: snapshotToState(rebased),
            clientOpId: `web_rebase_${Date.now()}`,
          };
        },
      }
    );

    commitQueueRef.current = queue;
    lastQueuedSignatureRef.current = '';
    return () => {
      queue.dispose();
      if (commitQueueRef.current === queue) commitQueueRef.current = null;
    };
  }, [apiConfig, currentProject?.id, currentProject?.name]);

  useEffect(() => {
    if (!currentProject?.id || !commitQueueRef.current) return;
    if (projectStore.pendingPatches.length === 0) {
      lastQueuedSignatureRef.current = '';
      return;
    }

    const signature = `${projectStore.version}:${JSON.stringify(projectStore.pendingPatches)}`;
    if (signature === lastQueuedSignatureRef.current) return;
    lastQueuedSignatureRef.current = signature;

    dispatchProjectStore({ type: 'SAVE_STARTED' });
    commitQueueRef.current.enqueue({
      baseVersion: projectStore.version,
      patches: projectStore.pendingPatches,
      clientOpId: `web_${Date.now()}`,
    }).then((result: any) => {
      const snapshot = coerceProjectSnapshot(result.state as any, currentProjectRef.current?.name || '');
      dispatchProjectStore({
        type: 'SAVE_SUCCESS',
        version: Number(result.version || projectStoreRef.current.version),
        snapshot,
        updatedAt: result.updatedAt || Date.now(),
      });
      if (projectStoreRef.current.pendingPatches.length > 0) {
        lastQueuedSignatureRef.current = '';
      }
    }).catch((error: any) => {
      lastQueuedSignatureRef.current = '';
      dispatchProjectStore({ type: 'SAVE_ERROR', message: getErrorMessage(error, 'Failed to save project state') });
    });
  }, [currentProject?.id, projectStore.pendingPatches, projectStore.version]);

  useEffect(() => {
    if (!currentProject?.id || !realtimeClientRef.current) return;
    const timer = window.setTimeout(() => {
      realtimeClientRef.current?.sendPresenceUpdate({ editingNodeId: selectedNodeId || null });
    }, 180);
    return () => window.clearTimeout(timer);
  }, [currentProject?.id, selectedNodeId, projectStore.connectionState]);

  useEffect(() => {
    if (!currentProject?.id) return;
    const hasActiveJobs = projectStore.jobs.some(job => job.status === 'queued' || job.status === 'running');
    if (!hasActiveJobs) return;
    const intervalId = window.setInterval(() => {
      fetchProjectJobs(currentProject.id);
    }, 2000);
    return () => window.clearInterval(intervalId);
  }, [currentProject?.id, projectStore.jobs, fetchProjectJobs]);

  const refreshSessionStorage = async () => {
    if (apiConfig.isMock) {
      setSessionStorageError('Switch to a real backend server to manage session storage.');
      return;
    }
    try {
      const infoRes = await fetch(`${apiConfig.baseUrl}/config/session_storage`);
      if (!infoRes.ok) throw new Error('Failed to load session storage');
      const info = await infoRes.json();
      const listRes = await fetch(`${apiConfig.baseUrl}/config/session_storage/list?path=`);
      if (!listRes.ok) throw new Error('Failed to list session storage');
      const list = await listRes.json();
      setSessionStorageInfo(info);
      setSessionStorageFolders(list.folders || []);
      setSessionStorageError(null);
    } catch (error) {
      setSessionStorageError(getErrorMessage(error, 'Failed to load session storage'));
    }
  };

  const createSessionStorageFolder = async (path: string) => {
    if (apiConfig.isMock) return;
    try {
      const res = await fetch(`${apiConfig.baseUrl}/config/session_storage/create`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ path }),
      });
      if (!res.ok) throw new Error('Failed to create folder');
      await refreshSessionStorage();
    } catch (error) {
      setSessionStorageError(getErrorMessage(error, 'Failed to create folder'));
    }
  };

  const selectSessionStorageFolder = async (path: string) => {
    if (apiConfig.isMock) return;
    try {
      const res = await fetch(`${apiConfig.baseUrl}/config/session_storage/select`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ path }),
      });
      if (!res.ok) throw new Error('Failed to select folder');
      const info = await res.json();
      setSessionStorageInfo(info);
      setSessionStorageError(null);
      await refreshSessionStorage();
      await fetchProjects();
    } catch (error) {
      setSessionStorageError(getErrorMessage(error, 'Failed to select folder'));
    }
  };

  const handleLogin = async (email: string, password: string) => {
    if (!authEnabled) {
      setAuthRequired(false);
      setAuthChecked(true);
      setAuthChecking(false);
      setAuthLoading(false);
      setAuthError(null);
      setCurrentUser({ id: 'usr_auth_disabled', email: 'auth-disabled@local', displayName: 'Auth Disabled' });
      return;
    }
    setAuthLoading(true);
    setAuthError(null);
    try {
      await api.authLogin(apiConfig, { email, password });
      const me = await api.authMe(apiConfig);
      setCurrentUser(me);
      setAuthRequired(false);
      setAuthChecked(true);
      await fetchProjects();
    } catch (error) {
      setAuthRequired(true);
      setAuthError(getErrorMessage(error, '登录失败'));
    } finally {
      setAuthLoading(false);
    }
  };

  const handleBackToIndex = () => {
    setAuthRequired(false);
    setAuthChecked(true);
    setAuthChecking(false);
    setAuthLoading(false);
    setAuthError(null);
    setAuthEnabled(false);
    setAuthModeReady(true);
    api.setAuthApiEnabled(false);
    setCurrentUser({ id: 'usr_mock_owner', email: 'mock.owner@example.com', displayName: 'Mock Owner' });
    setApiConfig({ baseUrl: 'mockServer', isMock: true });
  };

  const handleLogout = async () => {
    if (apiConfig.isMock || !authEnabled) return;
    setAuthLoading(true);
    let logoutMessage: string | null = null;
    try {
      await api.authLogout(apiConfig);
    } catch (error) {
      const raw = getErrorMessage(error, '');
      logoutMessage = raw ? `登出请求失败（已清理本地状态）：${raw}` : '登出请求失败（已清理本地状态）';
    } finally {
      api.clearAuthTokens();
      setAuthRequired(true);
      setAuthChecked(true);
      setAuthChecking(false);
      setAuthLoading(false);
      setAuthError(logoutMessage);
      setCurrentUser(null);
      setProjects([]);
      setCurrentProject(null);
      resetProjectState();
    }
  };

  const handleCreateProject = async () => {
    try {
      const created = await api.post(apiConfig, '/projects', { name: 'New Project', description: 'Collaborative workflow' }) as ProjectMetadata;
      setProjects(prev => [created, ...prev.filter(project => project.id !== created.id)]);
      await loadProject(created);
    } catch (error) {
      alert(getErrorMessage(error, 'Failed to create project'));
    }
  };

  const handleDeleteProject = async (e: React.MouseEvent, projectId: string) => {
    e.stopPropagation();
    if (!confirm('Delete this project?')) return;
    try {
      await api.delete(apiConfig, `/projects/${projectId}`);
      const nextProjects = projects.filter(project => project.id !== projectId);
      setProjects(nextProjects);
      if (currentProject?.id === projectId) {
        if (nextProjects[0]) {
          await loadProject(nextProjects[0]);
        } else {
          setCurrentProject(null);
          resetProjectState();
        }
      }
    } catch (error) {
      alert(getErrorMessage(error, 'Failed to delete project'));
    }
  };

  const handleSaveProjectSettings = async (name: string, settings: SessionConfig) => {
    if (!currentProject?.id) return;
    await api.post(apiConfig, `/projects/${currentProject.id}/metadata`, { displayName: name, settings });
    applyLocalAction({ type: 'UPDATE_PROJECT_SETTINGS', displayName: name, settings });
    setProjects(prev => prev.map(project => project.id === currentProject.id ? { ...project, name, updatedAt: Date.now() } : project));
    setCurrentProject(prev => prev ? { ...prev, name, updatedAt: Date.now() } : prev);
  };

  const fetchDatasetPreview = async (dataset: Dataset) => {
    if (!currentProject?.id) return dataset;
    try {
      const res = await api.get(apiConfig, `/projects/${currentProject.id}/datasets/${encodeURIComponent(dataset.name)}/preview?limit=50`) as any;
      const updated = { ...dataset, rows: res.rows || [] };
      applyLocalAction({
        type: 'SET_DATASETS',
        datasets: projectStoreRef.current.snapshot.datasets.map(item => item.name === dataset.name ? updated : item),
      });
      return updated;
    } catch (error) {
      console.error('Failed to load dataset preview', error);
      return dataset;
    }
  };

  const handleOpenSchema = async (name: string, closeMobile: boolean) => {
    const dataset = projectStore.snapshot.datasets.find(ds => ds.name === name);
    if (!dataset) return;
    let target = dataset;
    if (!dataset.rows || dataset.rows.length === 0) {
      target = await fetchDatasetPreview(dataset);
    }
    setSelectedDatasetForSchema(target);
    setIsSchemaModalOpen(true);
    if (closeMobile) setIsMobileSidebarOpen(false);
  };

  const handleUpdateSqlHistory = (newHistory: SqlHistoryItem[]) => {
    applyLocalAction({ type: 'SET_SQL_HISTORY', sqlHistory: newHistory });
  };

  const handleViewLineage = async (nodeId: string, commandId?: string, sourceTable?: string) => {
    if (!currentProject) return;
    setLineageLoading(true);
    try {
      const result = await api.getLineage(
        apiConfig,
        currentProject.id,
        nodeId,
        projectStore.snapshot.tree,
        commandId,
      );
      setLineageData({ nodeId, commandId, sourceTable, map: result.lineage ?? {} });
    } catch (e) {
      console.error('Lineage fetch failed', e);
    } finally {
      setLineageLoading(false);
    }
  };

  const handleUpdateCommands = (operationId: string, newCommands: Command[]) => {
    applyLocalAction({ type: 'UPDATE_COMMANDS', operationId, commands: newCommands });
  };

  const handleUpdateName = (name: string) => {
    applyLocalAction({ type: 'UPDATE_NODE_NAME', nodeId: selectedNodeId, name });
  };

  const handleUpdateType = (_opId: string, _type: OperationType) => {
    // operation type edits are not part of the current UI surface.
  };

  const handleAddChild = (parentId: string) => {
    applyLocalAction({ type: 'ADD_CHILD', parentId });
  };

  const handleMoveNode = (nodeId: string, direction: 'up' | 'down') => {
    applyLocalAction({ type: 'MOVE_NODE', nodeId, direction });
  };

  const handleDeleteNode = (nodeId: string) => {
    applyLocalAction({ type: 'DELETE_NODE', nodeId });
    if (selectedNodeId === nodeId) setSelectedNodeId('root');
  };

  const handleToggleEnabled = (nodeId: string) => {
    applyLocalAction({ type: 'TOGGLE_NODE_ENABLED', nodeId, cascadeDisable: Boolean(currentProjectSettings.cascadeDisable) });
  };

  const handleDeleteDataset = async (name: string) => {
    if (!currentProject?.id) return;
    const confirmMsg = `Delete dataset "${name}"? This will not update existing operations that reference it.`;
    if (!confirm(confirmMsg)) return;
    try {
      await api.delete(apiConfig, `/projects/${currentProject.id}/datasets/${encodeURIComponent(name)}`);
      applyLocalAction({ type: 'DELETE_DATASET', datasetName: name });
      if (selectedDatasetForSchema?.name === name) {
        setIsSchemaModalOpen(false);
        setSelectedDatasetForSchema(null);
      }
    } catch (error) {
      alert(getErrorMessage(error, 'Failed to delete dataset'));
    }
  };

  const handleExecute = async (page = 1, commandId?: string, viewId = 'main') => {
    if (!currentProject?.id || !selectedNode || selectedNode.id === 'root') return;
    if (!sourceValidation.hasAnyConfigured) {
      alert('Please add at least one data source before running.');
      return;
    }
    if (sourceValidation.hasIncomplete) {
      alert('Please select a dataset for all data sources before running.');
      return;
    }
    if (sourceValidation.hasDuplicate) {
      alert('Duplicate data sources detected. Please select unique datasets.');
      return;
    }
    setLoading(true);
    setIsRightPanelOpen(true);
    try {
      const res = await api.post(apiConfig, `/projects/${currentProject.id}/execute`, {
        projectId: currentProject.id,
        tree: projectStore.snapshot.tree,
        targetNodeId: selectedNodeId,
        targetCommandId: commandId,
        page,
        pageSize: selectedNode.pageSize || 50,
        viewId,
      });
      setPreviewData(res);
    } catch (error) {
      alert(`Execution Error: ${getErrorMessage(error, 'unknown error')}`);
    } finally {
      setLoading(false);
    }
  };

  const handleExportFull = async () => {
    if (!currentProject?.id || !selectedNode) return;
    try {
      const res = await api.post(apiConfig, `/projects/${currentProject.id}/execute`, {
        projectId: currentProject.id,
        tree: projectStore.snapshot.tree,
        targetNodeId: selectedNodeId,
        page: 1,
        pageSize: 100000,
        viewId: 'main',
      });

      if (!res || !res.rows || res.rows.length === 0) {
        alert('No data to export');
        return;
      }

      const headers = res.columns || Object.keys(res.rows[0]);
      const csvContent = [
        headers.join(','),
        ...res.rows.map((row: any) => headers.map((fieldName: string) => {
          let value = row[fieldName];
          if (value === null || value === undefined) return '';
          value = String(value).replace(/"/g, '""');
          if (String(value).search(/("|,|\n)/g) >= 0) {
            value = `"${value}"`;
          }
          return value;
        }).join(',')),
      ].join('\n');

      const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
      const url = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.setAttribute('download', `export_full_${selectedNode.name}_${Date.now()}.csv`);
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      URL.revokeObjectURL(url);
    } catch (error) {
      alert(`Export Failed: ${getErrorMessage(error, 'unknown error')}`);
    }
  };

  const normalizeOperationNode = (raw: any): OperationNode | null => {
    if (!raw || typeof raw !== 'object') return null;
    const rawChildren = Array.isArray(raw.children) ? raw.children : [];
    const normalizedChildren = rawChildren
      .map((child: any) => normalizeOperationNode(child))
      .filter((child: OperationNode | null): child is OperationNode => child !== null);
    const rawCommands = Array.isArray(raw.commands) ? raw.commands : [];
    const normalizedCommands: Command[] = rawCommands.map((cmd: any, idx: number) => ({
      id: typeof cmd?.id === 'string' && cmd.id.trim() ? cmd.id : `cmd_import_${Date.now()}_${idx}`,
      type: typeof cmd?.type === 'string' && cmd.type.trim() ? cmd.type : 'filter',
      config: cmd?.config && typeof cmd.config === 'object' ? cmd.config : {},
      order: Number.isFinite(Number(cmd?.order)) ? Number(cmd.order) : idx + 1,
    })) as Command[];
    const operationType = typeof raw.operationType === 'string' ? raw.operationType : 'process';
    const normalizedType: OperationType = (
      operationType === 'setup' || operationType === 'dataset' || operationType === 'process' || operationType === 'root'
    ) ? operationType : 'process';
    if (typeof raw.id !== 'string' || !raw.id.trim()) return null;
    if (typeof raw.name !== 'string' || !raw.name.trim()) return null;
    if (raw.type !== 'operation') return null;

    return {
      id: raw.id,
      type: 'operation',
      operationType: normalizedType,
      name: raw.name,
      enabled: raw.enabled !== false,
      commands: normalizedCommands,
      children: normalizedChildren,
    };
  };

  const deriveImportedTree = (payload: any): OperationNode | null => {
    const candidate = payload?.tree ?? payload;
    if (Array.isArray(candidate)) {
      const children = candidate
        .map((item: any) => normalizeOperationNode(item))
        .filter((item: OperationNode | null): item is OperationNode => item !== null);
      return {
        ...INITIAL_TREE,
        children,
      };
    }
    const maybeRoot = normalizeOperationNode(candidate);
    if (!maybeRoot) {
      const ops = payload?.operations;
      if (Array.isArray(ops)) {
        const children = ops
          .map((item: any) => normalizeOperationNode(item))
          .filter((item: OperationNode | null): item is OperationNode => item !== null);
        return {
          ...INITIAL_TREE,
          children,
        };
      }
      return null;
    }
    if (maybeRoot.operationType === 'root') return maybeRoot;
    return {
      ...INITIAL_TREE,
      children: [maybeRoot],
    };
  };

  const handleExportOperations = () => {
    try {
      const payload = {
        version: 1,
        type: 'dmb_operations',
        exportedAt: new Date().toISOString(),
        projectId: currentProject?.id,
        tree: projectStore.snapshot.tree,
      };
      const json = JSON.stringify(payload, null, 2);
      const blob = new Blob([json], { type: 'application/json;charset=utf-8;' });
      const url = URL.createObjectURL(blob);
      const link = document.createElement('a');
      const safeName = (currentProjectName || currentProject?.id || 'workflow').replace(/[^a-zA-Z0-9_-]/g, '_');
      link.href = url;
      link.setAttribute('download', `operations_${safeName}_${Date.now()}.json`);
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      URL.revokeObjectURL(url);
    } catch (error) {
      alert(`Export operations failed: ${getErrorMessage(error, 'unknown error')}`);
    }
  };

  const handleImportOperations = async (file: File) => {
    if (!file) return;
    const proceed = confirm('Import operations will replace the current workflow tree. Continue?');
    if (!proceed) return;
    try {
      const rawText = await file.text();
      const parsed = JSON.parse(rawText);
      const importedTree = deriveImportedTree(parsed);
      if (!importedTree) throw new Error('Invalid operations file structure');
      applyLocalAction({ type: 'IMPORT_OPERATIONS', tree: importedTree });
      setSelectedNodeId(getPreferredSelectedNodeId(importedTree));
      setPreviewData(null);
      setCurrentView('workflow');
    } catch (error) {
      alert(`Import operations failed: ${getErrorMessage(error, 'unknown error')}`);
    }
  };

  const handleSchemaSave = async (datasetId: string, fieldTypes: any) => {
    if (!currentProject?.id) return;
    await api.post(apiConfig, `/projects/${currentProject.id}/datasets/update`, { datasetId, fieldTypes });
    applyLocalAction({ type: 'UPDATE_DATASET_SCHEMA', datasetId, fieldTypes });
  };

  const handleOpenDiagnostics = async () => {
    if (!currentProject?.id) return;
    setIsDiagnosticsOpen(true);
    setDiagnosticsLoading(true);
    setDiagnosticsError(null);
    setDiagnosticsReport(null);
    try {
      const report = await api.get(apiConfig, `/projects/${currentProject.id}/diagnostics`) as SessionDiagnosticsReport;
      setDiagnosticsReport({ ...report, projectId: currentProject.id });
    } catch (error) {
      setDiagnosticsError(getErrorMessage(error, 'Failed to load diagnostics'));
    } finally {
      setDiagnosticsLoading(false);
    }
  };

  const handleManualSync = useCallback(async () => {
    if (!currentProjectRef.current?.id) return;
    setSyncingProject(true);
    try {
      const [stateEnvelope, metadata, datasets] = await Promise.all([
        api.get(apiConfig, `/projects/${currentProjectRef.current.id}/state`) as Promise<any>,
        api.get(apiConfig, `/projects/${currentProjectRef.current.id}/metadata`).catch(() => createDefaultProjectMetadata(currentProjectRef.current?.name || '')),
        api.get(apiConfig, `/projects/${currentProjectRef.current.id}/datasets`) as Promise<Dataset[]>,
      ]);
      const snapshot = coerceProjectSnapshot({
        ...(stateEnvelope?.state || {}),
        datasets: datasets || [],
        metadata: metadata || createDefaultProjectMetadata(currentProjectRef.current?.name || ''),
      }, currentProjectRef.current?.name || '');
      dispatchProjectStore({
        type: 'HYDRATE_REMOTE',
        projectId: currentProjectRef.current.id,
        snapshot,
        version: Number(stateEnvelope?.version || 0),
        updatedAt: stateEnvelope?.updatedAt || Date.now(),
      });
      dispatchProjectStore({ type: 'DISMISS_CONFLICT' });
      await Promise.all([
        fetchProjectMembers(currentProjectRef.current.id),
        fetchProjectJobs(currentProjectRef.current.id),
      ]);
    } catch (error) {
      dispatchProjectStore({ type: 'SAVE_ERROR', message: getErrorMessage(error, 'Manual sync failed') });
    } finally {
      setSyncingProject(false);
    }
  }, [apiConfig, fetchProjectJobs, fetchProjectMembers]);

  const handleInviteMember = async (email: string, role: ProjectRole) => {
    if (!currentProject?.id) return;
    await api.post(apiConfig, `/projects/${currentProject.id}/members`, { memberEmail: email, role });
    await fetchProjectMembers(currentProject.id);
  };

  const handleUpdateMemberRole = async (member: ProjectMember, role: ProjectRole) => {
    if (!currentProject?.id) return;
    await api.patch(apiConfig, `/projects/${currentProject.id}/members/${member.userId}`, { role });
    await fetchProjectMembers(currentProject.id);
  };

  const handleRemoveMember = async (member: ProjectMember) => {
    if (!currentProject?.id) return;
    await api.delete(apiConfig, `/projects/${currentProject.id}/members/${member.userId}`);
    await fetchProjectMembers(currentProject.id);
  };

  const handleRestoreDraft = () => {
    if (!projectStore.draftRecovery) return;
    dispatchProjectStore({ type: 'RESTORE_DRAFT', draft: projectStore.draftRecovery });
  };

  const handleDiscardDraft = () => {
    if (projectStore.draftRecovery?.projectId) {
      clearProjectDraft(projectStore.draftRecovery.projectId);
    } else if (currentProject?.id) {
      clearProjectDraft(currentProject.id);
    }
    dispatchProjectStore({ type: 'SET_DRAFT_RECOVERY', draft: null });
  };

  if (!apiConfig.isMock && (authChecking || !authModeReady)) {
    return (
      <div className="h-screen w-screen flex items-center justify-center bg-slate-100 text-slate-700">
        验证登录状态中...
      </div>
    );
  }

  if (!apiConfig.isMock && authEnabled && authRequired) {
    return (
      <LoginPage
        backendLabel={apiConfig.baseUrl}
        loading={authLoading}
        error={authError}
        onLogin={handleLogin}
        onBack={handleBackToIndex}
      />
    );
  }

  return (
    <div className="flex flex-col h-screen w-screen overflow-hidden bg-white text-slate-900">
      <TopBar
        projectId={currentProject?.id || ''}
        projectName={currentProjectName}
        projects={projects}
        currentView={currentView}
        apiConfig={apiConfig}
        isRightPanelOpen={isRightPanelOpen}
        backendStatus={backendStatus}
        realtimeStatus={projectStore.connectionState}
        saveStatus={projectStore.saveStatus}
        lastSavedAt={projectStore.lastSavedAt}
        onlineMembersCount={onlineMembersCount}
        remoteEditingLabel={remoteEditingLabel}
        syncing={syncingProject}
        onProjectSelect={(id) => { void loadProject(id); }}
        onProjectCreate={() => { void handleCreateProject(); }}
        onProjectDelete={(e, id) => { void handleDeleteProject(e, id); }}
        onViewChange={setCurrentView}
        onSettingsOpen={() => {
          setIsSettingsOpen(true);
          void refreshSessionStorage();
        }}
        onProjectSettingsOpen={() => setIsSessionSettingsOpen(true)}
        onProjectDiagnostics={() => { void handleOpenDiagnostics(); }}
        onProjectMembersOpen={() => {
          if (currentProject?.id) void fetchProjectMembers(currentProject.id);
          setIsMembersOpen(true);
        }}
        onManualSync={() => { void handleManualSync(); }}
        onRunSql={() => setSqlRunRequestId(value => value + 1)}
        onToggleRightPanel={() => setIsRightPanelOpen(!isRightPanelOpen)}
        onToggleMobileSidebar={() => setIsMobileSidebarOpen(!isMobileSidebarOpen)}
        isAuthenticated={isAuthenticated}
        authEnabled={authEnabled}
        authChecking={authChecking}
        authError={authError}
        onLogout={handleLogout}
        canExecute={sqlRunState.canRun && !sqlRunState.running}
      />

      <CollabPresenceFloat
        visible={Boolean(currentProject?.id)}
        projectName={currentProjectName}
        realtimeStatus={projectStore.connectionState}
        onlineMembersCount={onlineMembersCount}
        remoteEditingLabel={remoteEditingLabel}
        members={remotePresenceMembers}
        onOpenMembers={() => {
          if (currentProject?.id) void fetchProjectMembers(currentProject.id);
          setIsMembersOpen(true);
        }}
      />

      <div className="flex flex-1 overflow-hidden relative">
        {!currentProject?.id ? (
          <div className="flex-1 flex items-center justify-center bg-gray-50">
            <div className="text-center">
              <div className="text-lg font-semibold text-gray-800">请选择 Project</div>
              <div className="mt-2 text-sm text-gray-500">请在顶部创建或选择一个 Project 以继续。</div>
            </div>
          </div>
        ) : (
          <>
            {isMobileSidebarOpen && (
              <div className="absolute inset-0 z-40 bg-black/50 md:hidden" onClick={() => setIsMobileSidebarOpen(false)}>
                <div className="h-full w-fit bg-white shadow-xl" onClick={e => e.stopPropagation()}>
                  <Sidebar
                    width={260}
                    currentView={currentView}
                    projectId={currentProject.id}
                    tree={projectStore.snapshot.tree}
                    datasets={projectStore.snapshot.datasets}
                    selectedNodeId={selectedNodeId}
                    onSelectNode={(id) => { setSelectedNodeId(id); setIsMobileSidebarOpen(false); }}
                    onToggleEnabled={handleToggleEnabled}
                    onAddChild={handleAddChild}
                    onDeleteNode={handleDeleteNode}
                    onMoveNode={handleMoveNode}
                    onImportClick={() => setIsImportOpen(true)}
                    onExportOperations={handleExportOperations}
                    onImportOperations={(file) => { void handleImportOperations(file); }}
                    onOpenTableInSql={(table) => { setTargetSqlTable(table); setCurrentView('sql'); setIsMobileSidebarOpen(false); }}
                    onOpenTableInData={(table) => { setTargetDataTable(table); setCurrentView('data'); setIsMobileSidebarOpen(false); }}
                    onOpenSchema={(name) => { void handleOpenSchema(name, true); }}
                    onDeleteDataset={(name) => { void handleDeleteDataset(name); }}
                    onViewLineage={(id) => { void handleViewLineage(id); }}
                    remoteEditorsByNode={remoteEditorsByNode}
                    appearance={appearance}
                  />
                </div>
              </div>
            )}

            <div className="hidden md:block h-full shrink-0" style={{ width: sidebarWidth }}>
              <Sidebar
                width={sidebarWidth}
                currentView={currentView}
                projectId={currentProject.id}
                tree={projectStore.snapshot.tree}
                datasets={projectStore.snapshot.datasets}
                selectedNodeId={selectedNodeId}
                onSelectNode={setSelectedNodeId}
                onToggleEnabled={handleToggleEnabled}
                onAddChild={handleAddChild}
                onDeleteNode={handleDeleteNode}
                onMoveNode={handleMoveNode}
                onImportClick={() => setIsImportOpen(true)}
                onExportOperations={handleExportOperations}
                onImportOperations={(file) => { void handleImportOperations(file); }}
                onOpenTableInSql={(table) => { setTargetSqlTable(table); setCurrentView('sql'); }}
                onOpenTableInData={(table) => { setTargetDataTable(table); setCurrentView('data'); }}
                onOpenSchema={(name) => { void handleOpenSchema(name, false); }}
                onDeleteDataset={(name) => { void handleDeleteDataset(name); }}
                onViewLineage={(id) => { void handleViewLineage(id); }}
                remoteEditorsByNode={remoteEditorsByNode}
                appearance={appearance}
              />
            </div>
            <div
              className="hidden md:block w-1.5 cursor-col-resize bg-transparent hover:bg-blue-100 transition-colors"
              onMouseDown={() => setIsSidebarResizing(true)}
              title="Drag to resize"
            />

            <Workspace
              currentView={currentView}
              projectId={currentProject.id}
              apiConfig={apiConfig}
              targetSqlTable={targetSqlTable}
              targetDataTable={targetDataTable}
              onSelectDataTable={(table) => setTargetDataTable(table)}
              onClearTargetSqlTable={() => setTargetSqlTable(null)}
              sqlRunRequestId={sqlRunRequestId}
              onSqlRunStateChange={setSqlRunState}
              selectedNode={selectedNode}
              datasets={projectStore.snapshot.datasets}
              appearance={appearance}
              inputFields={[]}
              inputSchema={globalInputSchema}
              onUpdateCommands={handleUpdateCommands}
              onUpdateName={handleUpdateName}
              onUpdateType={handleUpdateType}
              onViewPath={(commandId) => {
                setTargetCommandId(commandId);
                setIsPathModalOpen(true);
              }}
              onViewCommandLineage={(commandId, srcTable) => {
                if (selectedNodeId) void handleViewLineage(selectedNodeId, commandId, srcTable);
              }}
              isRightPanelOpen={isRightPanelOpen}
              onCloseRightPanel={() => setIsRightPanelOpen(false)}
              rightPanelWidth={rightPanelWidth}
              onRightPanelResizeStart={() => setIsResizing(true)}
              previewData={previewData}
              onClearPreview={() => setPreviewData(null)}
              loading={loading}
              onRefreshPreview={(page, commandId) => { void handleExecute(page, commandId); }}
              canRunOperation={sourceValidation.hasAnyConfigured && !sourceValidation.hasIncomplete && !sourceValidation.hasDuplicate}
              onUpdatePageSize={(size) => {
                if (!selectedNode) return;
                const updatedNode = { ...selectedNode, pageSize: size };
                const updateTree = (node: OperationNode): OperationNode => {
                  if (node.id === selectedNode.id) return updatedNode;
                  if (node.children) return { ...node, children: node.children.map(updateTree) };
                  return node;
                };
                const nextTree = updateTree(projectStore.snapshot.tree);
                applyLocalAction({ type: 'SET_TREE', tree: nextTree });
                void handleExecute(1);
              }}
              onExportFull={() => { void handleExportFull(); }}
              isMobile={false}
              tree={projectStore.snapshot.tree}
              panelPosition={currentProjectSettings.panelPosition}
              sqlHistory={projectStore.snapshot.sqlHistory}
              onUpdateSqlHistory={handleUpdateSqlHistory}
            />
          </>
        )}
      </div>

      <DataImportModal
        isOpen={isImportOpen}
        onClose={() => setIsImportOpen(false)}
        onImport={(dataset) => {
          applyLocalAction({ type: 'SET_DATASETS', datasets: [...projectStoreRef.current.snapshot.datasets, dataset] });
        }}
        projectId={currentProject?.id || ''}
        apiConfig={apiConfig}
      />

      <SettingsModal
        isOpen={isSettingsOpen}
        onClose={() => setIsSettingsOpen(false)}
        servers={knownServers}
        currentServer={apiConfig.isMock ? 'mockServer' : apiConfig.baseUrl}
        onSelectServer={(url) => {
          setApiConfig({ baseUrl: url, isMock: url === 'mockServer' });
          if (!knownServers.includes(url)) setKnownServers([...knownServers, url]);
        }}
        onAddServer={(url) => setKnownServers([...knownServers, url])}
        onRemoveServer={(url) => setKnownServers(knownServers.filter(server => server !== url))}
        appearance={appearance}
        onUpdateAppearance={setAppearance}
        sessionStorageInfo={sessionStorageInfo}
        sessionStorageFolders={sessionStorageFolders}
        sessionStorageDisabled={apiConfig.isMock}
        sessionStorageError={sessionStorageError}
        onRefreshSessionStorage={refreshSessionStorage}
        onSelectSessionStorage={selectSessionStorageFolder}
        onCreateSessionStorage={createSessionStorageFolder}
      />

      <SessionSettingsModal
        isOpen={isSessionSettingsOpen}
        onClose={() => setIsSessionSettingsOpen(false)}
        projectId={currentProject?.id || ''}
        initialDisplayName={currentProjectName}
        initialSettings={currentProjectSettings}
        onSave={handleSaveProjectSettings}
      />

      <ProjectMembersModal
        isOpen={isMembersOpen}
        onClose={() => setIsMembersOpen(false)}
        projectName={currentProjectName}
        members={projectStore.members}
        loading={membersLoading}
        error={membersError}
        canManage={currentProjectCanManage}
        onInvite={handleInviteMember}
        onUpdateRole={handleUpdateMemberRole}
        onRemoveMember={handleRemoveMember}
      />

      <ConflictNoticeModal
        isOpen={Boolean(projectStore.conflict)}
        conflict={projectStore.conflict}
        onClose={() => dispatchProjectStore({ type: 'DISMISS_CONFLICT' })}
        onSyncNow={() => {
          dispatchProjectStore({ type: 'DISMISS_CONFLICT' });
          void handleManualSync();
        }}
      />

      <DraftRecoveryModal
        isOpen={Boolean(projectStore.draftRecovery)}
        draft={projectStore.draftRecovery}
        onRestore={handleRestoreDraft}
        onDiscard={handleDiscardDraft}
      />

      <SessionDiagnosticsModal
        isOpen={isDiagnosticsOpen}
        onClose={() => setIsDiagnosticsOpen(false)}
        report={diagnosticsReport}
        loading={diagnosticsLoading}
        error={diagnosticsError}
      />

      <PathConditionsModal
        isOpen={isPathModalOpen}
        onClose={() => {
          setIsPathModalOpen(false);
          setTargetCommandId(undefined);
        }}
        tree={projectStore.snapshot.tree}
        targetNodeId={selectedNodeId}
        targetCommandId={targetCommandId}
        projectId={currentProject?.id || ''}
        apiConfig={apiConfig}
      />

      <DatasetSchemaModal
        isOpen={isSchemaModalOpen}
        onClose={() => setIsSchemaModalOpen(false)}
        dataset={selectedDatasetForSchema}
        onSave={handleSchemaSave}
      />

      {lineageData && (
        <LineagePanel
          lineage={lineageData.map}
          nodeId={lineageData.nodeId}
          commandId={lineageData.commandId}
          lockedTable={lineageData.sourceTable}
          onClose={() => setLineageData(null)}
        />
      )}
    </div>
  );
}

export default App;
