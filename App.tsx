import React, { useState, useEffect, useMemo } from 'react';
import { Sidebar } from './components/Sidebar';
import { Workspace } from './components/Workspace';
import { TopBar } from './components/TopBar';
import { DataImportModal } from './components/DataImport';
import { SettingsModal } from './components/SettingsModal';
import { SessionSettingsModal } from './components/SessionSettingsModal';
import { SessionDiagnosticsModal } from './components/SessionDiagnosticsModal';
import { PathConditionsModal } from './components/PathConditionsModal';
import { DatasetSchemaModal } from './components/DatasetSchemaModal';
import { LoginPage } from './components/LoginPage';
import { 
  OperationNode, Dataset, Command, ExecutionResult, ApiConfig, 
  SessionMetadata, AppearanceConfig, SessionConfig, DataType, FieldInfo, OperationType,
  SqlHistoryItem, SessionState, SessionDiagnosticsReport
} from './types';
import { api } from './utils/api';

// Default Tree: Root container with one initial Data Setup
const INITIAL_TREE: OperationNode = {
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
      children: []
    }
  ]
};

// Default Settings
const DEFAULT_APPEARANCE: AppearanceConfig = {
  textSize: 13,
  textColor: '#374151',
  guideLineColor: '#E5E7EB',
  showGuideLines: true,
  showNodeIds: false,
  showOperationIds: false,
  showCommandIds: false,
  showDatasetIds: false
};

const CONNECTION_STATE_STORAGE_KEY = 'dmb_connection_state_v1';

type PersistedConnectionState = {
  apiConfig: ApiConfig;
  knownServers: string[];
  savedAt: number;
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
      savedAt: Date.now()
    };
    window.localStorage.setItem(CONNECTION_STATE_STORAGE_KEY, JSON.stringify(next));
  } catch {
    // ignore storage failures
  }
};

function App() {
  // --- STATE ---
  const [sessionId, setSessionId] = useState<string>('');
  const [sessionName, setSessionName] = useState<string>('');
  const [sessions, setSessions] = useState<SessionMetadata[]>([]);
  const [tree, setTree] = useState<OperationNode>(INITIAL_TREE);
  const [datasets, setDatasets] = useState<Dataset[]>([]);
  const [sqlHistory, setSqlHistory] = useState<SqlHistoryItem[]>([]);
  const [selectedNodeId, setSelectedNodeId] = useState<string>('setup_1');
  const [currentView, setCurrentView] = useState<'workflow' | 'sql' | 'data'>('workflow');
  
  // UI State
  const [isImportOpen, setIsImportOpen] = useState(false);
  const [isSettingsOpen, setIsSettingsOpen] = useState(false);
  const [isSessionSettingsOpen, setIsSessionSettingsOpen] = useState(false);
  const [isDiagnosticsOpen, setIsDiagnosticsOpen] = useState(false);
  const [isPathModalOpen, setIsPathModalOpen] = useState(false);
  const [targetCommandId, setTargetCommandId] = useState<string | undefined>(undefined);
  const [isSchemaModalOpen, setIsSchemaModalOpen] = useState(false);
  const [selectedDatasetForSchema, setSelectedDatasetForSchema] = useState<Dataset | null>(null);
  const [diagnosticsReport, setDiagnosticsReport] = useState<SessionDiagnosticsReport | null>(null);
  const [diagnosticsLoading, setDiagnosticsLoading] = useState(false);
  const [diagnosticsError, setDiagnosticsError] = useState<string | null>(null);

  const [isRightPanelOpen, setIsRightPanelOpen] = useState(true);
  const [rightPanelWidth, setRightPanelWidth] = useState(400); // Or Height if vertical
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
  
  // Configuration State
  const [apiConfig, setApiConfig] = useState<ApiConfig>({ baseUrl: 'mockServer', isMock: true });
  const [knownServers, setKnownServers] = useState<string[]>(['mockServer', 'http://localhost:8000']);
  const [appearance, setAppearance] = useState<AppearanceConfig>(DEFAULT_APPEARANCE);
  const [sessionSettings, setSessionSettings] = useState<SessionConfig>({ cascadeDisable: false, panelPosition: 'right' });
  const [configReady, setConfigReady] = useState(false);
  const [sessionStorageInfo, setSessionStorageInfo] = useState<{ dataRoot: string; sessionsDir: string; relative: string } | null>(null);
  const [sessionStorageFolders, setSessionStorageFolders] = useState<{ name: string; path: string }[]>([]);
  const [sessionStorageError, setSessionStorageError] = useState<string | null>(null);

  // SQL State
  const [targetSqlTable, setTargetSqlTable] = useState<string | null>(null);
  const [targetDataTable, setTargetDataTable] = useState<string | null>(null);

  // --- DERIVED STATE ---
  const selectedNode = useMemo(() => {
    const findNode = (node: OperationNode): OperationNode | null => {
      if (node.id === selectedNodeId) return node;
      if (node.children) {
        for (const child of node.children) {
          const found = findNode(child);
          if (found) return found;
        }
      }
      return null;
    };
    return findNode(tree);
  }, [tree, selectedNodeId]);

  const sourceValidation = useMemo(() => {
      const sources: Command[] = [];
      const collectSources = (node: OperationNode) => {
          node.commands?.forEach(c => {
              if (c.type === 'source') sources.push(c);
          });
          node.children?.forEach(collectSources);
      };
      collectSources(tree);

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
          hasDuplicate: names.length !== uniqueNames.size
      };
  }, [tree]);

  // Flattened schema from all datasets
  const globalInputSchema = useMemo(() => {
     const schema: Record<string, DataType> = {};
     datasets.forEach(ds => {
         if (ds.fieldTypes) {
             Object.entries(ds.fieldTypes).forEach(([k, v]) => schema[k] = (v as FieldInfo).type);
         } else {
             ds.fields.forEach(f => schema[f] = 'string');
         }
     });
     return schema;
  }, [datasets]);
  const isAuthenticated = useMemo(() => {
    if (apiConfig.isMock) return false;
    return authChecked && !authChecking && !authRequired;
  }, [apiConfig.isMock, authChecked, authChecking, authRequired]);


  // --- INITIALIZATION ---
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
              if (!cancelled) {
                  setApiConfig({ baseUrl: server, isMock });
                  if (server) {
                      setKnownServers(prev => prev.includes(server) ? prev : [...prev, server]);
                  }
              }
          } catch (e) {
              // Default to mock if file missing or backend unreachable
          } finally {
              if (!cancelled) setConfigReady(true);
          }
      };
      loadDefaultServer();
      return () => { cancelled = true; };
  }, []);

  useEffect(() => {
    if (!configReady || !authChecked) return;
    if (!apiConfig.isMock && authRequired) return;
    fetchSessions();
  }, [apiConfig, configReady, authChecked, authRequired]);

  useEffect(() => {
    if (!configReady) return;
    savePersistedConnectionState(apiConfig, knownServers);
  }, [apiConfig, knownServers, configReady]);

  useEffect(() => {
    if (!configReady) return;
    if (apiConfig.isMock) {
      setAuthChecking(false);
      setAuthChecked(true);
      setAuthRequired(false);
      setAuthLoading(false);
      setAuthError(null);
      return;
    }

    let cancelled = false;
    const verifyAuth = async () => {
      if (backendStatus === 'checking' || backendStatus === 'mock') {
        return;
      }
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
        await api.authMe(apiConfig);
        if (!cancelled) setAuthRequired(false);
      } catch (e: any) {
        if (!cancelled) {
          const message = String(e?.message || '');
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
  }, [apiConfig.baseUrl, apiConfig.isMock, configReady, backendStatus]);

  useEffect(() => {
      if (!isSettingsOpen) return;
      if (apiConfig.isMock) {
          setSessionStorageError("Switch to a real backend server to manage session storage.");
          return;
      }
      refreshSessionStorage();
  }, [isSettingsOpen, apiConfig.baseUrl, apiConfig.isMock]);

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

  const refreshSessionStorage = async () => {
      if (apiConfig.isMock) {
          setSessionStorageError("Switch to a real backend server to manage session storage.");
          return;
      }
      const baseUrl = apiConfig.baseUrl;
      try {
          const infoRes = await fetch(`${baseUrl}/config/session_storage`);
          if (!infoRes.ok) throw new Error("Failed to load session storage");
          const info = await infoRes.json();
          const listRes = await fetch(`${baseUrl}/config/session_storage/list?path=`);
          if (!listRes.ok) throw new Error("Failed to list session storage");
          const list = await listRes.json();
          setSessionStorageInfo(info);
          setSessionStorageFolders(list.folders || []);
          setSessionStorageError(null);
      } catch (e: any) {
          setSessionStorageError(e.message || "Failed to load session storage");
      }
  };

  const createSessionStorageFolder = async (path: string) => {
      if (apiConfig.isMock) return;
      const baseUrl = apiConfig.baseUrl;
      try {
          const res = await fetch(`${baseUrl}/config/session_storage/create`, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ path })
          });
          if (!res.ok) throw new Error("Failed to create folder");
          await refreshSessionStorage();
      } catch (e: any) {
          setSessionStorageError(e.message || "Failed to create folder");
      }
  };

  const selectSessionStorageFolder = async (path: string) => {
      if (apiConfig.isMock) return;
      const baseUrl = apiConfig.baseUrl;
      try {
          const res = await fetch(`${baseUrl}/config/session_storage/select`, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ path })
          });
          if (!res.ok) throw new Error("Failed to select folder");
          const info = await res.json();
          setSessionStorageInfo(info);
          setSessionStorageError(null);
          setSessionId('');
          setSessionName('');
          setTree(INITIAL_TREE);
          setSqlHistory([]);
          setPreviewData(null);
          setDatasets([]);
          await fetchSessions();
      } catch (e: any) {
          setSessionStorageError(e.message || "Failed to select folder");
      }
  };

  const handleOpenSettings = () => {
      setIsSettingsOpen(true);
      refreshSessionStorage();
  };

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
  }, [apiConfig.baseUrl, apiConfig.isMock, configReady]);

  const fetchSessions = async () => {
      try {
          const list = await api.get(apiConfig, '/sessions') as SessionMetadata[];
          setSessions(list);

          if (sessionId && !list.some(s => s.sessionId === sessionId)) {
              setSessionId('');
              setSessionName('');
              setTree(INITIAL_TREE);
              setSqlHistory([]);
              setPreviewData(null);
              setDatasets([]);
          }
      } catch (e) {
          console.error("Failed to fetch sessions", e);
      }
  };

  const handleLogin = async (email: string, password: string) => {
      setAuthLoading(true);
      setAuthError(null);
      try {
          await api.authLogin(apiConfig, { email, password });
          await api.authMe(apiConfig);
          setAuthRequired(false);
          setAuthChecked(true);
          await fetchSessions();
      } catch (e: any) {
          setAuthRequired(true);
          setAuthError(e?.message || '登录失败');
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
      setApiConfig({ baseUrl: 'mockServer', isMock: true });
  };

  const handleLogout = async () => {
      if (apiConfig.isMock) return;
      setAuthLoading(true);
      let logoutMessage: string | null = null;
      try {
          await api.authLogout(apiConfig);
      } catch (e: any) {
          const raw = String(e?.message || '');
          logoutMessage = raw ? `登出请求失败（已清理本地状态）：${raw}` : '登出请求失败（已清理本地状态）';
      } finally {
          api.clearAuthTokens();
          setAuthRequired(true);
          setAuthChecked(true);
          setAuthChecking(false);
          setAuthLoading(false);
          setAuthError(logoutMessage);
          setSessionId('');
          setSessionName('');
          setSessions([]);
          setTree(INITIAL_TREE);
          setSqlHistory([]);
          setPreviewData(null);
          setDatasets([]);
      }
  };

  // --- RESIZING LOGIC ---
  useEffect(() => {
    const handleMouseMove = (e: MouseEvent) => {
      if (!isResizing) return;
      e.preventDefault();

      const position = sessionSettings.panelPosition || 'right';
      
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
      
      const position = sessionSettings.panelPosition || 'right';
      if (position === 'right' || position === 'left') document.body.style.cursor = 'col-resize';
      else document.body.style.cursor = 'row-resize';
    }

    return () => {
      document.removeEventListener('mousemove', handleMouseMove);
      document.removeEventListener('mouseup', handleMouseUp);
      document.body.style.cursor = 'default';
    };
  }, [isResizing, sessionSettings.panelPosition]);

  // --- HANDLERS ---

  const handleCreateSession = async () => {
      try {
          const res = await api.post(apiConfig, '/sessions', {});
          setSessionId(res.sessionId);
          setSessionName('New Session');
          setTree(INITIAL_TREE);
          setSqlHistory([]);
          setPreviewData(null); // Clear previous results
          
          // Update name in backend immediately so list reflects it
          try {
              await api.post(apiConfig, `/sessions/${res.sessionId}/metadata`, { displayName: 'New Session' });
          } catch (e) { console.error("Failed to set initial session name", e); }

          if (apiConfig.isMock) {
              // Ensure mock datasets are loaded for new mock sessions
              const dss = await api.get(apiConfig, '/datasets');
              setDatasets(dss);
          } else {
              setDatasets([]);
          }
          
          fetchSessions();
      } catch (e) { alert("Failed to create session"); }
  };

  const handleSelectSession = async (id: string) => {
      setSessionId(id);
      setPreviewData(null); // Clear previous results
      try {
          // Load metadata
          const meta = await api.get(apiConfig, `/sessions/${id}/metadata`);
          if (meta) {
              setSessionName(meta.displayName || id);
              if (meta.settings) setSessionSettings(meta.settings);
          }
          
          // Load state (datasets, tree) if persisted (Mock doesn't persist properly in this simplified version but structure allows it)
          // For now, we just reset tree to initial or specific mock state
          const state = await api.get(apiConfig, `/sessions/${id}/state`) as SessionState;
          let appliedDatasets = false;
          if (state && state.tree) {
              setTree(state.tree);
              if (state.datasets && state.datasets.length > 0) {
                  setDatasets(state.datasets);
                  appliedDatasets = true;
              }
              setSqlHistory(state.sqlHistory || []);
          } else {
              setTree(INITIAL_TREE);
              setSqlHistory([]);
          }
          if (!appliedDatasets) {
              if (apiConfig.isMock) {
                  const dss = await api.get(apiConfig, `/datasets`);
                  setDatasets(dss);
              } else {
                  setDatasets(state?.datasets || []);
              }
          }
      } catch (e) { console.error(e); }
  };

  const handleSaveSessionSettings = async (name: string, config: SessionConfig) => {
      setSessionName(name);
      setSessionSettings(config);
      await api.post(apiConfig, `/sessions/${sessionId}/metadata`, { displayName: name, settings: config });
      fetchSessions();
  };

  const handleDeleteSession = async (e: React.MouseEvent, id: string) => {
      e.stopPropagation();
      if (!confirm("Delete this session?")) return;
      await api.delete(apiConfig, `/sessions/${id}`);
      if (sessionId === id) {
          setSessionId('');
          setSessionName('');
          setTree(INITIAL_TREE);
          setSqlHistory([]);
          setPreviewData(null); // Clear previous results
      }
      fetchSessions();
  };

  const handleDeleteDataset = async (name: string) => {
      if (!sessionId) return;
      const confirmMsg = `Delete dataset "${name}"? This will not update existing operations that reference it.`;
      if (!confirm(confirmMsg)) return;
      try {
          await api.delete(apiConfig, `/sessions/${sessionId}/datasets/${encodeURIComponent(name)}`);
          setDatasets(prev => prev.filter(d => d.name !== name));
          if (selectedDatasetForSchema?.name === name) {
              setIsSchemaModalOpen(false);
              setSelectedDatasetForSchema(null);
          }
      } catch (e: any) {
          alert(e.message || "Failed to delete dataset");
      }
  };

  const fetchDatasetPreview = async (dataset: Dataset) => {
      if (!sessionId) return dataset;
      try {
          const res = await api.get(apiConfig, `/sessions/${sessionId}/datasets/${encodeURIComponent(dataset.name)}/preview?limit=50`);
          const updated = { ...dataset, rows: res.rows || [] };
          setDatasets(prev => prev.map(d => d.name === dataset.name ? updated : d));
          return updated;
      } catch (e) {
          console.error("Failed to load dataset preview", e);
          return dataset;
      }
  };

  const handleOpenSchema = async (name: string, closeMobile: boolean) => {
      const ds = datasets.find(d => d.name === name);
      if (!ds) return;
      let target = ds;
      if (!ds.rows || ds.rows.length === 0) {
          target = await fetchDatasetPreview(ds);
      }
      setSelectedDatasetForSchema(target);
      setIsSchemaModalOpen(true);
      if (closeMobile) setIsMobileSidebarOpen(false);
  };

  const handleUpdateSqlHistory = (newHistory: SqlHistoryItem[]) => {
      setSqlHistory(newHistory);
      // Persist state
      api.post(apiConfig, `/sessions/${sessionId}/state`, { tree, datasets, sqlHistory: newHistory });
  };

  const handleUpdateCommands = (opId: string, newCommands: Command[]) => {
      const updateNode = (node: OperationNode): OperationNode => {
          if (node.id === opId) return { ...node, commands: newCommands };
          if (node.children) return { ...node, children: node.children.map(updateNode) };
          return node;
      };
      setTree(updateNode(tree));
  };

  const handleUpdateName = (name: string) => {
      const updateNode = (node: OperationNode): OperationNode => {
          if (node.id === selectedNodeId) return { ...node, name };
          if (node.children) return { ...node, children: node.children.map(updateNode) };
          return node;
      };
      setTree(updateNode(tree));
  };

  const handleUpdateType = (_opId: string, _type: any) => {
        // Not used frequently as type is fixed usually
  };

  const handleAddChild = (parentId: string) => {
      // Find parent to determine what type of child to add
      const findNode = (node: OperationNode): OperationNode | null => {
          if (node.id === parentId) return node;
          if (node.children) {
              for (const child of node.children) {
                  const found = findNode(child);
                  if (found) return found;
              }
          }
          return null;
      };

      const parent = findNode(tree);
      if (!parent) return;

      const collectNames = (node: OperationNode, names: Set<string>) => {
          if (node.name) names.add(node.name);
          if (node.children) node.children.forEach(child => collectNames(child, names));
      };

      const getUniqueName = (base: string) => {
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

      let newOpType: OperationType = 'process';
      let newName = getUniqueName('Operation');

      // If adding to root, we create a new Setup node
      if (parent.operationType === 'root') {
          newOpType = 'setup';
          newName = getUniqueName('Data Setup');
      }

      const newNode: OperationNode = {
          id: `op_${Date.now()}`,
          type: 'operation',
          operationType: newOpType,
          name: newName,
          enabled: true,
          commands: [],
          children: []
      };
      
      const addNode = (node: OperationNode): OperationNode => {
          if (node.id === parentId) {
              return { ...node, children: [...(node.children || []), newNode] };
          }
          if (node.children) {
              return { ...node, children: node.children.map(addNode) };
          }
          return node;
      };
      setTree(addNode(tree));
      setSelectedNodeId(newNode.id);
  };

  const handleMoveNode = (id: string, direction: 'up' | 'down') => {
      const moveInTree = (node: OperationNode): { node: OperationNode; moved: boolean } => {
          if (!node.children || node.children.length === 0) return { node, moved: false };

          const idx = node.children.findIndex(c => c.id === id);
          if (idx !== -1) {
              const newIndex = direction === 'up' ? idx - 1 : idx + 1;
              if (newIndex < 0 || newIndex >= node.children.length) return { node, moved: false };
              const newChildren = [...node.children];
              [newChildren[idx], newChildren[newIndex]] = [newChildren[newIndex], newChildren[idx]];
              return { node: { ...node, children: newChildren }, moved: true };
          }

          let moved = false;
          const newChildren = node.children.map(child => {
              if (moved) return child;
              const result = moveInTree(child);
              if (result.moved) moved = true;
              return result.node;
          });

          if (moved) return { node: { ...node, children: newChildren }, moved: true };
          return { node, moved: false };
      };

      setTree(prev => moveInTree(prev).node);
  };

  const handleDeleteNode = (id: string) => {
      if (id === 'root') return;
      const deleteNode = (node: OperationNode): OperationNode => {
          if (node.children) {
              return { ...node, children: node.children.filter(c => c.id !== id).map(deleteNode) };
          }
          return node;
      };
      setTree(deleteNode(tree));
      if (selectedNodeId === id) setSelectedNodeId('root');
  };

  const handleToggleEnabled = (id: string) => {
    console.log(`[App] Toggle Enabled Request for ID: ${id}`);
    console.log(`[App] Cascade Disable Setting: ${sessionSettings.cascadeDisable}`);

    const toggleNode = (node: OperationNode, parentDisabled: boolean): OperationNode => {
        let newNode = node;
        
        // 1. Check if this is the target node
        if (node.id === id) {
            const newValue = !node.enabled;
            console.log(`[App] Toggling target node '${node.name}' (${node.id}) to ${newValue}`);
            newNode = { ...node, enabled: newValue };
        } 
        // 2. Check cascade disable logic
        else if (parentDisabled && sessionSettings.cascadeDisable) {
             // Cascade disable: If parent is disabled, this node becomes disabled
             if (node.enabled) {
                 console.log(`[App] Cascade disabling child node '${node.name}' (${node.id}) because parent is disabled`);
                 newNode = { ...node, enabled: false };
             }
        }
        
        // Calculate the 'effective' disable state for children
        // Even if this node is enabled, if it was force-disabled by cascade, children should know
        const isSelfDisabled = !newNode.enabled;
        
        if (newNode.children) {
            newNode.children = newNode.children.map(child => toggleNode(child, isSelfDisabled));
        }
        return newNode;
    };
    setTree(toggleNode(tree, false));
  };

  const handleExecute = async (page = 1, _commandId?: string, viewId = "main") => {
      if (!selectedNode || selectedNode.id === 'root') return;
      if (!sourceValidation.hasAnyConfigured) {
          alert("Please add at least one data source before running.");
          return;
      }
      if (sourceValidation.hasIncomplete) {
          alert("Please select a dataset for all data sources before running.");
          return;
      }
      if (sourceValidation.hasDuplicate) {
          alert("Duplicate data sources detected. Please select unique datasets.");
          return;
      }
      setLoading(true);
      setIsRightPanelOpen(true); // Ensure panel is open to show results
      try {
          // If specific commandId is provided, we might be doing a partial run.
          // For now, the API mostly executes the node. 
          // commandId could be used to truncate commands list in a future improvement.
          const res = await api.post(apiConfig, '/execute', {
              sessionId,
              tree,
              targetNodeId: selectedNodeId,
              targetCommandId: _commandId,
              page,
              pageSize: selectedNode.pageSize || 50,
              viewId
          });
          setPreviewData(res);
          // Persist state
          api.post(apiConfig, `/sessions/${sessionId}/state`, { tree, datasets });
      } catch (e: any) {
          alert(`Execution Error: ${e.message}`);
      } finally {
          setLoading(false);
      }
  };

  const handleExportFull = async () => {
      if (!selectedNode) return;
      
      try {
          // Fetch all data (using a large page size for now)
          const res = await api.post(apiConfig, '/execute', {
              sessionId,
              tree,
              targetNodeId: selectedNodeId,
              page: 1,
              pageSize: 100000, // Large limit for full export
              viewId: 'main'
          });

          if (!res || !res.rows || res.rows.length === 0) {
              alert("No data to export");
              return;
          }

          // Convert to CSV
          const headers = res.columns || Object.keys(res.rows[0]);
          const csvContent = [
              headers.join(','),
              ...res.rows.map((row: any) => {
                  return headers.map((fieldName: string) => {
                      let val = row[fieldName];
                      if (val === null || val === undefined) return '';
                      val = String(val).replace(/"/g, '""');
                      if (val.search(/("|,|\n)/g) >= 0) {
                          val = `"${val}"`;
                      }
                      return val;
                  }).join(',');
              })
          ].join('\n');

          // Download
          const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
          const url = URL.createObjectURL(blob);
          const link = document.createElement('a');
          link.href = url;
          link.setAttribute('download', `export_full_${selectedNode.name}_${Date.now()}.csv`);
          document.body.appendChild(link);
          link.click();
          document.body.removeChild(link);

      } catch (e: any) {
          console.error("Export failed", e);
          alert(`Export Failed: ${e.message}`);
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
          order: Number.isFinite(Number(cmd?.order)) ? Number(cmd.order) : idx + 1
      })) as Command[];

      const opType = typeof raw.operationType === 'string' ? raw.operationType : 'process';
      const normalizedType: OperationType = (
          opType === 'setup' || opType === 'dataset' || opType === 'process' || opType === 'root'
      ) ? opType : 'process';

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
          children: normalizedChildren
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
              children
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
                  children
              };
          }
          return null;
      }

      if (maybeRoot.operationType === 'root') {
          return maybeRoot;
      }

      return {
          ...INITIAL_TREE,
          children: [maybeRoot]
      };
  };

  const getPreferredSelectedNodeId = (nextTree: OperationNode): string => {
      const firstSetup = nextTree.children?.find(c => c.operationType === 'setup');
      if (firstSetup) return firstSetup.id;
      const firstChild = nextTree.children?.[0];
      if (firstChild) return firstChild.id;
      return 'root';
  };

  const handleExportOperations = () => {
      try {
          const payload = {
              version: 1,
              type: 'dmb_operations',
              exportedAt: new Date().toISOString(),
              sessionId,
              tree
          };
          const json = JSON.stringify(payload, null, 2);
          const blob = new Blob([json], { type: 'application/json;charset=utf-8;' });
          const url = URL.createObjectURL(blob);
          const link = document.createElement('a');
          const safeName = (sessionName || sessionId || 'workflow').replace(/[^a-zA-Z0-9_-]/g, '_');
          link.href = url;
          link.setAttribute('download', `operations_${safeName}_${Date.now()}.json`);
          document.body.appendChild(link);
          link.click();
          document.body.removeChild(link);
          URL.revokeObjectURL(url);
      } catch (e: any) {
          alert(`Export operations failed: ${e.message || e}`);
      }
  };

  const handleImportOperations = async (file: File) => {
      if (!file) return;
      const proceed = confirm("Import operations will replace the current workflow tree. Continue?");
      if (!proceed) return;

      try {
          const rawText = await file.text();
          const parsed = JSON.parse(rawText);
          const importedTree = deriveImportedTree(parsed);
          if (!importedTree) {
              throw new Error("Invalid operations file structure");
          }
          setTree(importedTree);
          setSelectedNodeId(getPreferredSelectedNodeId(importedTree));
          setPreviewData(null);
          setCurrentView('workflow');

          if (sessionId) {
              api.post(apiConfig, `/sessions/${sessionId}/state`, {
                  tree: importedTree,
                  datasets,
                  sqlHistory
              });
          }
      } catch (e: any) {
          alert(`Import operations failed: ${e.message || e}`);
      }
  };

  const handleSchemaSave = async (datasetId: string, fieldTypes: any) => {
      // Update local state
      const updated = datasets.map(d => d.id === datasetId ? { ...d, fieldTypes } : d);
      setDatasets(updated);
      // Persist to backend if needed
      await api.post(apiConfig, `/sessions/${sessionId}/datasets/update`, { datasetId, fieldTypes });
  };

  const handleOpenDiagnostics = async () => {
      if (!sessionId) return;
      setIsDiagnosticsOpen(true);
      setDiagnosticsLoading(true);
      setDiagnosticsError(null);
      setDiagnosticsReport(null);
      try {
          const report = await api.get(apiConfig, `/sessions/${sessionId}/diagnostics`) as SessionDiagnosticsReport;
          setDiagnosticsReport(report);
      } catch (e: any) {
          setDiagnosticsError(e.message || "Failed to load diagnostics");
      } finally {
          setDiagnosticsLoading(false);
      }
  };

  if (!apiConfig.isMock && authChecking) {
    return (
      <div className="h-screen w-screen flex items-center justify-center bg-slate-100 text-slate-700">
        验证登录状态中...
      </div>
    );
  }

  if (!apiConfig.isMock && authRequired) {
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
        sessionId={sessionId}
        sessionName={sessionName}
        sessions={sessions}
        currentView={currentView}
        apiConfig={apiConfig}
        isRightPanelOpen={isRightPanelOpen}
        backendStatus={backendStatus}
        onSessionSelect={handleSelectSession}
        onSessionCreate={handleCreateSession}
        onSessionDelete={handleDeleteSession}
        onViewChange={setCurrentView}
        onSettingsOpen={handleOpenSettings}
        onSessionSettingsOpen={() => setIsSessionSettingsOpen(true)}
        onSessionDiagnostics={handleOpenDiagnostics}
        onRunSql={() => setSqlRunRequestId(v => v + 1)}
        onToggleRightPanel={() => setIsRightPanelOpen(!isRightPanelOpen)}
        onToggleMobileSidebar={() => setIsMobileSidebarOpen(!isMobileSidebarOpen)}
        isAuthenticated={isAuthenticated}
        authChecking={authChecking}
        authError={authError}
        onLogout={handleLogout}
        canExecute={sqlRunState.canRun && !sqlRunState.running}
      />

      <div className="flex flex-1 overflow-hidden relative">
         {!sessionId ? (
             <div className="flex-1 flex items-center justify-center bg-gray-50">
                 <div className="text-center">
                     <div className="text-lg font-semibold text-gray-800">请选择 Session</div>
                     <div className="mt-2 text-sm text-gray-500">请在顶部创建或选择一个 Session 以继续。</div>
                 </div>
             </div>
         ) : (
             <>
                 {/* Mobile Sidebar Overlay */}
                 {isMobileSidebarOpen && (
                     <div className="absolute inset-0 z-40 bg-black/50 md:hidden" onClick={() => setIsMobileSidebarOpen(false)}>
                         <div className="h-full w-fit bg-white shadow-xl" onClick={e => e.stopPropagation()}>
                            <Sidebar 
                                width={260}
                                currentView={currentView}
                                sessionId={sessionId}
                                tree={tree}
                                datasets={datasets}
                                selectedNodeId={selectedNodeId}
                                onSelectNode={(id) => { setSelectedNodeId(id); setIsMobileSidebarOpen(false); }}
                                onToggleEnabled={handleToggleEnabled}
                                onAddChild={handleAddChild}
                                onDeleteNode={handleDeleteNode}
                                onMoveNode={handleMoveNode}
                                onImportClick={() => { if (sessionId) setIsImportOpen(true); }}
                                onExportOperations={handleExportOperations}
                                onImportOperations={handleImportOperations}
                                onOpenTableInSql={(t) => { setTargetSqlTable(t); setCurrentView('sql'); setIsMobileSidebarOpen(false); }}
                                onOpenTableInData={(t) => { setTargetDataTable(t); setCurrentView('data'); setIsMobileSidebarOpen(false); }}
                                onOpenSchema={(name) => { handleOpenSchema(name, true); }}
                                onDeleteDataset={handleDeleteDataset}
                                appearance={appearance}
                            />
                         </div>
                     </div>
                 )}

                 {/* Desktop Sidebar */}
                 <div className="hidden md:block h-full shrink-0" style={{ width: sidebarWidth }}>
                     <Sidebar 
                        width={sidebarWidth}
                        currentView={currentView}
                        sessionId={sessionId}
                        tree={tree}
                        datasets={datasets}
                        selectedNodeId={selectedNodeId}
                        onSelectNode={setSelectedNodeId}
                        onToggleEnabled={handleToggleEnabled}
                        onAddChild={handleAddChild}
                        onDeleteNode={handleDeleteNode}
                        onMoveNode={handleMoveNode}
                        onImportClick={() => { if (sessionId) setIsImportOpen(true); }}
                        onExportOperations={handleExportOperations}
                        onImportOperations={handleImportOperations}
                        onOpenTableInSql={(t) => { setTargetSqlTable(t); setCurrentView('sql'); }}
                        onOpenTableInData={(t) => { setTargetDataTable(t); setCurrentView('data'); }}
                        onOpenSchema={(name) => { handleOpenSchema(name, false); }}
                        onDeleteDataset={handleDeleteDataset}
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
                    sessionId={sessionId}
                    apiConfig={apiConfig}
                    targetSqlTable={targetSqlTable}
                    targetDataTable={targetDataTable}
                    onSelectDataTable={(t) => setTargetDataTable(t)}
                    onClearTargetSqlTable={() => setTargetSqlTable(null)}
                    sqlRunRequestId={sqlRunRequestId}
                    onSqlRunStateChange={setSqlRunState}
                    selectedNode={selectedNode}
                    datasets={datasets}
                    appearance={appearance}
                    inputFields={[]} 
                    inputSchema={globalInputSchema}
                    onUpdateCommands={handleUpdateCommands}
                    onUpdateName={handleUpdateName}
                    onUpdateType={handleUpdateType}
                    onViewPath={(cmdId) => { 
                        setTargetCommandId(cmdId);
                        setIsPathModalOpen(true); 
                    }}
                    isRightPanelOpen={isRightPanelOpen}
                    onCloseRightPanel={() => setIsRightPanelOpen(false)}
                    rightPanelWidth={rightPanelWidth}
                    onRightPanelResizeStart={() => setIsResizing(true)}
                    previewData={previewData}
                    onClearPreview={() => setPreviewData(null)}
                    loading={loading}
                    onRefreshPreview={(page, cmdId) => handleExecute(page, cmdId)}
                    canRunOperation={sourceValidation.hasAnyConfigured && !sourceValidation.hasIncomplete && !sourceValidation.hasDuplicate}
                    onUpdatePageSize={(size) => {
                         if(selectedNode) {
                            const updatedNode = { ...selectedNode, pageSize: size };
                            const updateTree = (n: OperationNode): OperationNode => {
                                if (n.id === selectedNode.id) return updatedNode;
                                if (n.children) return { ...n, children: n.children.map(updateTree) };
                                return n;
                            };
                            setTree(updateTree(tree));
                            handleExecute(1);
                         }
                    }}
                    onExportFull={handleExportFull}
                    isMobile={false}
                    tree={tree}
                    panelPosition={sessionSettings.panelPosition}
                    sqlHistory={sqlHistory}
                    onUpdateSqlHistory={handleUpdateSqlHistory}
                 />
             </>
         )}
      </div>

      <DataImportModal 
          isOpen={isImportOpen} 
          onClose={() => setIsImportOpen(false)} 
          onImport={(ds) => { setDatasets([...datasets, ds]); }}
          sessionId={sessionId}
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
          onRemoveServer={(url) => setKnownServers(knownServers.filter(s => s !== url))}
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
          sessionId={sessionId}
          initialDisplayName={sessionName}
          initialSettings={sessionSettings}
          onSave={handleSaveSessionSettings}
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
          tree={tree}
          targetNodeId={selectedNodeId}
          targetCommandId={targetCommandId}
          sessionId={sessionId}
          apiConfig={apiConfig}
      />

      <DatasetSchemaModal 
          isOpen={isSchemaModalOpen}
          onClose={() => setIsSchemaModalOpen(false)}
          dataset={selectedDatasetForSchema}
          onSave={handleSchemaSave}
      />
    </div>
  );
}

export default App;
