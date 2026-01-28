import React, { useState, useEffect, useMemo, useRef } from 'react';
import { DataImportModal } from './components/DataImport';
import { PathConditionsModal } from './components/PathConditionsModal';
import { SettingsModal } from './components/SettingsModal';
import { TopBar } from './components/TopBar';
import { Sidebar } from './components/Sidebar';
import { Workspace } from './components/Workspace';
import { OperationNode, Dataset, ExecutionResult, Command, SessionMetadata, ApiConfig } from './types';
import { api } from './utils/api';

// --- INITIAL DATA ---
const initialTree: OperationNode = {
  id: 'root',
  type: 'operation',
  name: 'Root',
  enabled: true,
  commands: [],
  children: [
    {
      id: 'op_1',
      type: 'operation',
      name: 'Initial Analysis',
      enabled: true,
      commands: [],
      children: []
    }
  ]
};

const DEFAULT_SERVERS = ['mockServer', 'http://localhost:8000'];

const App: React.FC = () => {
  // --- STATE ---
  
  // Server Config
  const [availableServers, setAvailableServers] = useState<string[]>(() => {
    const saved = localStorage.getItem('availableServers');
    return saved ? JSON.parse(saved) : DEFAULT_SERVERS;
  });
  const [currentServer, setCurrentServer] = useState<string>(() => {
    return localStorage.getItem('currentServer') || 'mockServer';
  });
  
  // Session Management
  const [sessions, setSessions] = useState<SessionMetadata[]>([]);
  const [sessionId, setSessionId] = useState<string>('');
  const [isSessionLoading, setIsSessionLoading] = useState(false); // To prevent auto-save during load

  // View State (Workflow vs SQL)
  const [currentView, setCurrentView] = useState<'workflow' | 'sql'>('workflow');
  // Target table for SQL View Deep Linking
  const [targetSqlTable, setTargetSqlTable] = useState<string | null>(null);

  const [datasets, setDatasets] = useState<Dataset[]>([]);
  const [tree, setTree] = useState<OperationNode>(initialTree);
  const [selectedNodeId, setSelectedNodeId] = useState<string>('op_1');
  const [previewData, setPreviewData] = useState<ExecutionResult | null>(null);
  const [loading, setLoading] = useState(false);
  
  // Layout State
  const [sidebarWidth, setSidebarWidth] = useState(300);
  const [rightPanelWidth, setRightPanelWidth] = useState(500);
  const [isResizingSidebar, setIsResizingSidebar] = useState(false);
  const [isResizingRight, setIsResizingRight] = useState(false);
  const [isRightPanelOpen, setIsRightPanelOpen] = useState(true);
  
  // Modals
  const [showImportModal, setShowImportModal] = useState(false);
  const [showPathModal, setShowPathModal] = useState(false);
  const [showSettingsModal, setShowSettingsModal] = useState(false);

  // --- DERIVED STATE ---
  const apiConfig: ApiConfig = useMemo(() => ({
    baseUrl: currentServer,
    isMock: currentServer === 'mockServer'
  }), [currentServer]);

  // --- EFFECTS ---
  
  // Save server config to local storage
  useEffect(() => {
    localStorage.setItem('availableServers', JSON.stringify(availableServers));
    localStorage.setItem('currentServer', currentServer);
  }, [availableServers, currentServer]);

  // Fetch sessions when server changes
  useEffect(() => {
    fetchSessions();
    setDatasets([]);
    setSessionId('');
    setPreviewData(null);
    setTree(initialTree);
  }, [apiConfig]); // Re-run when config changes

  // Auto-Save Tree Effect
  useEffect(() => {
    if (!sessionId || isSessionLoading) return;

    const timer = setTimeout(() => {
        api.post(apiConfig, `/sessions/${sessionId}/state`, tree)
           .catch(e => console.error("Auto-save failed", e));
    }, 1000); // 1s debounce

    return () => clearTimeout(timer);
  }, [tree, sessionId, apiConfig, isSessionLoading]);

  const fetchSessions = async () => {
    try {
        const data = await api.get(apiConfig, '/sessions');
        setSessions(data);
        if (data.length > 0) {
            handleSelectSession(data[0].sessionId);
        } else {
             setSessionId('');
        }
    } catch (e) {
        console.error("Failed to fetch sessions", e);
        setSessions([]);
        setSessionId('');
    }
  };

  const fetchDatasets = async (sessId: string) => {
      try {
          const data = await api.get(apiConfig, `/sessions/${sessId}/datasets`);
          setDatasets(data);
      } catch (e) {
          console.error("Failed to fetch datasets", e);
      }
  };

  const fetchSessionState = async (sessId: string) => {
      try {
          const state = await api.get(apiConfig, `/sessions/${sessId}/state`);
          if (state && state.id) {
              setTree(state);
          } else {
              setTree(initialTree);
          }
      } catch (e) {
          console.error("Failed to fetch session state", e);
          setTree(initialTree);
      }
  };

  // --- RESIZING LOGIC ---
  useEffect(() => {
    const handleMouseMove = (e: MouseEvent) => {
        if (isResizingSidebar) {
            const newWidth = Math.max(240, Math.min(600, e.clientX));
            setSidebarWidth(newWidth);
        }
        if (isResizingRight) {
             const newWidth = Math.max(300, Math.min(800, window.innerWidth - e.clientX));
             setRightPanelWidth(newWidth);
        }
    };
    
    const handleMouseUp = () => {
        setIsResizingSidebar(false);
        setIsResizingRight(false);
        document.body.style.cursor = 'default';
        document.body.style.userSelect = 'auto';
    };

    if (isResizingSidebar || isResizingRight) {
        window.addEventListener('mousemove', handleMouseMove);
        window.addEventListener('mouseup', handleMouseUp);
        document.body.style.userSelect = 'none';
        document.body.style.cursor = 'col-resize';
    }

    return () => {
        window.removeEventListener('mousemove', handleMouseMove);
        window.removeEventListener('mouseup', handleMouseUp);
    };
  }, [isResizingSidebar, isResizingRight]);

  // --- ACTIONS ---

  // Server Management
  const handleAddServer = (url: string) => {
      if (!availableServers.includes(url)) {
          setAvailableServers([...availableServers, url]);
          setCurrentServer(url);
      }
  };

  const handleRemoveServer = (url: string) => {
      setAvailableServers(availableServers.filter(s => s !== url));
      if (currentServer === url) {
          setCurrentServer('mockServer');
      }
  };

  // Session Actions
  const handleCreateSession = async () => {
      try {
        const data = await api.post(apiConfig, '/sessions', {});
        if (data && data.sessionId) {
             const newSession = { sessionId: data.sessionId, createdAt: Date.now() }; 
             setSessions(prev => [newSession, ...prev]);
             handleSelectSession(data.sessionId);
        }
      } catch (e) {
          console.error("Create session failed", e);
          alert("Failed to create session. Ensure backend is running.");
      }
  };

  const handleDeleteSession = async (e: React.MouseEvent, idToDelete: string) => {
      e.stopPropagation();
      try {
          await api.delete(apiConfig, `/sessions/${idToDelete}`);
          const newSessions = sessions.filter(s => s.sessionId !== idToDelete);
          setSessions(newSessions);
          
          if (idToDelete === sessionId) {
              if (newSessions.length > 0) {
                  handleSelectSession(newSessions[0].sessionId);
              } else {
                  setSessionId('');
                  setDatasets([]);
                  setTree(initialTree);
              }
          }
      } catch (e) {
          console.error("Delete failed", e);
      }
  };

  const handleSelectSession = async (id: string) => {
      setIsSessionLoading(true); // Lock auto-save
      setSessionId(id);
      
      // Load Data
      await Promise.all([
        fetchDatasets(id),
        fetchSessionState(id)
      ]);
      
      setPreviewData(null);
      setIsSessionLoading(false); // Unlock auto-save
  };

  // Dataset Management
  const handleImport = (dataset: Dataset) => {
    setDatasets(prev => [dataset, ...prev]);
    setShowImportModal(false);
  };

  const handleOpenTableInSql = (tableName: string) => {
      setTargetSqlTable(tableName);
      setCurrentView('sql');
  };

  // Tree Management
  const updateNode = (nodes: OperationNode[], id: string, updater: (n: OperationNode) => OperationNode): OperationNode[] => {
    return nodes.map(node => {
      if (node.id === id) return updater(node);
      if (node.children) return { ...node, children: updateNode(node.children, id, updater) };
      return node;
    });
  };

  const deleteNode = (nodes: OperationNode[], id: string): OperationNode[] => {
    return nodes
        .filter(node => node.id !== id)
        .map(node => ({
            ...node,
            children: node.children ? deleteNode(node.children, id) : undefined
        }));
  };

  const addChildNode = (nodes: OperationNode[], parentId: string): OperationNode[] => {
      return nodes.map(node => {
          if (node.id === parentId) {
              const newChild: OperationNode = {
                  id: `op_${Date.now()}`,
                  type: 'operation',
                  name: `Op_${Date.now().toString().slice(-4)}`, 
                  enabled: true,
                  commands: [],
                  children: []
              };
              return { ...node, children: [...(node.children || []), newChild] };
          }
          if (node.children) return { ...node, children: addChildNode(node.children, parentId) };
          return node;
      });
  };

  const handleUpdateCommands = (opId: string, newCommands: Command[]) => {
     setTree(prev => ({ ...prev, children: updateNode(prev.children || [], opId, n => ({ ...n, commands: newCommands })) }));
  };

  const handleUpdateName = (name: string) => {
    setTree(prev => ({ ...prev, children: updateNode(prev.children || [], selectedNodeId, n => ({ ...n, name })) }));
  };
  
  const handleToggleEnabled = (id: string) => {
    setTree(prev => ({ ...prev, children: updateNode(prev.children || [], id, n => ({ ...n, enabled: !n.enabled })) }));
  };

  const handleAddChild = (parentId: string) => {
      if(parentId === 'root') {
        const newChild: OperationNode = {
            id: `op_${Date.now()}`,
            type: 'operation',
            name: 'New Analysis Path',
            enabled: true,
            commands: [],
            children: []
        };
        setTree(prev => ({ ...prev, children: [...(prev.children || []), newChild] }));
      } else {
        setTree(prev => ({ ...prev, children: addChildNode(prev.children || [], parentId) }));
      }
  };

  const handleDeleteNode = (id: string) => {
      setTree(prev => ({ ...prev, children: deleteNode(prev.children || [], id) }));
      if (selectedNodeId === id) setSelectedNodeId('root');
  };

  // Execution (Connect to Backend)
  const executeOperation = async () => {
    setLoading(true);
    try {
        const result: ExecutionResult = await api.post(apiConfig, '/execute', {
            sessionId: sessionId,
            tree: tree,
            targetNodeId: selectedNodeId
        });

        setPreviewData(result);
        
        if (!isRightPanelOpen) setIsRightPanelOpen(true);
    } catch (err: any) {
        console.error("Execution error:", err);
        alert(`Failed to execute operation: ${err.message}`);
    } finally {
        setLoading(false);
    }
  };

  // Helpers
  const findNode = (nodes: OperationNode[], id: string): OperationNode | null => {
      for (const node of nodes) {
          if (node.id === id) return node;
          if (node.children) {
              const found = findNode(node.children, id);
              if (found) return found;
          }
      }
      return null;
  };
  
  // Find parent to inherit context
  const findParentNode = (root: OperationNode, targetId: string): OperationNode | null => {
      if (root.children) {
          for (const child of root.children) {
              if (child.id === targetId) return root;
              const found = findParentNode(child, targetId);
              if (found) return found;
          }
      }
      return null;
  };

  // Extract last known data source from a node's commands
  const getLastDataSource = (node: OperationNode): string | undefined => {
      if (!node.commands || node.commands.length === 0) return undefined;
      // Search backwards
      for (let i = node.commands.length - 1; i >= 0; i--) {
          const cmd = node.commands[i];
          if (cmd.config.mainTable) return cmd.config.mainTable;
      }
      return undefined;
  };

  const selectedNode = selectedNodeId === 'root' ? tree : (tree.children ? findNode(tree.children, selectedNodeId) : null);
  
  // Calculate inherited datasource
  const parentNode = selectedNode ? findParentNode(tree, selectedNode.id) : null;
  const inheritedDataSource = parentNode ? getLastDataSource(parentNode) : undefined;

  return (
    <div className="flex flex-col h-screen bg-gray-100 overflow-hidden text-gray-800">
      <DataImportModal 
          isOpen={showImportModal} 
          onClose={() => setShowImportModal(false)} 
          onImport={handleImport}
          sessionId={sessionId}
          apiConfig={apiConfig}
      />
      
      <PathConditionsModal 
          isOpen={showPathModal}
          onClose={() => setShowPathModal(false)}
          tree={tree}
          targetNodeId={selectedNodeId}
      />

      <SettingsModal 
          isOpen={showSettingsModal}
          onClose={() => setShowSettingsModal(false)}
          servers={availableServers}
          currentServer={currentServer}
          onSelectServer={setCurrentServer}
          onAddServer={handleAddServer}
          onRemoveServer={handleRemoveServer}
      />

      {/* 1. TOP NAVBAR */}
      <TopBar 
        sessionId={sessionId}
        sessions={sessions}
        currentView={currentView}
        apiConfig={apiConfig}
        isRightPanelOpen={isRightPanelOpen}
        onSessionSelect={handleSelectSession}
        onSessionCreate={handleCreateSession}
        onSessionDelete={handleDeleteSession}
        onViewChange={setCurrentView}
        onSettingsOpen={() => setShowSettingsModal(true)}
        onExecute={executeOperation}
        onToggleRightPanel={() => setIsRightPanelOpen(!isRightPanelOpen)}
      />

      {/* 2. MAIN LAYOUT */}
      <div className="flex flex-1 overflow-hidden">
        
        {/* LEFT SIDEBAR (RESIZABLE) */}
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
          onImportClick={() => setShowImportModal(true)}
          onOpenTableInSql={handleOpenTableInSql}
        />

        {/* RESIZER HANDLER (LEFT) */}
        <div 
            className={`w-1 hover:w-1.5 bg-gray-200 hover:bg-blue-400 cursor-col-resize z-20 flex flex-col justify-center items-center transition-all ${isResizingSidebar ? 'bg-blue-500 w-1.5' : ''}`}
            onMouseDown={() => setIsResizingSidebar(true)}
        >
             <div className="h-8 w-0.5 bg-gray-400 rounded-full" />
        </div>

        {/* WORKSPACE */}
        <Workspace 
            currentView={currentView}
            sessionId={sessionId}
            apiConfig={apiConfig}
            targetSqlTable={targetSqlTable}
            onClearTargetSqlTable={() => setTargetSqlTable(null)}
            selectedNode={selectedNode}
            datasets={datasets}
            inheritedDataSource={inheritedDataSource}
            onUpdateCommands={handleUpdateCommands}
            onUpdateName={handleUpdateName}
            onViewPath={() => setShowPathModal(true)}
            isRightPanelOpen={isRightPanelOpen}
            rightPanelWidth={rightPanelWidth}
            onRightPanelResizeStart={() => setIsResizingRight(true)}
            previewData={previewData}
            loading={loading}
            onRefreshPreview={executeOperation}
        />

      </div>
    </div>
  );
};

export default App;