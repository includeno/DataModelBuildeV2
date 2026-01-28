import React, { useState, useEffect, useMemo } from 'react';
import { Layers, Database, Play, Settings, GitBranch, Save, ChevronDown, ChevronRight, Plus, Trash2, Clock, Check, PanelRight, Terminal, Search, Server } from 'lucide-react';
import { OperationTree } from './components/OperationTree';
import { CommandEditor } from './components/CommandEditor';
import { DataPreview } from './components/DataPreview';
import { DataImportModal } from './components/DataImport';
import { PathConditionsModal } from './components/PathConditionsModal';
import { SqlEditor } from './components/SqlEditor';
import { SettingsModal } from './components/SettingsModal';
import { Button } from './components/Button';
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
  const [isSessionMenuOpen, setIsSessionMenuOpen] = useState(false);

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
  
  // Sidebar State
  const [isOpsExpanded, setIsOpsExpanded] = useState(true);
  const [isDataExpanded, setIsDataExpanded] = useState(true);
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

  const fetchSessions = async () => {
    try {
        const data = await api.get(apiConfig, '/sessions');
        setSessions(data);
        if (data.length > 0) {
            handleSelectSession(data[0].sessionId);
        }
    } catch (e) {
        console.error("Failed to fetch sessions", e);
        // If real fetch fails, don't crash, just show empty
        setSessions([]);
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
        const newSession = { sessionId: data.sessionId, createdAt: Date.now() }; // approximate
        setSessions(prev => [newSession, ...prev]);
        setSessionId(data.sessionId);
        setDatasets([]); // Clear datasets for new session
        setIsSessionMenuOpen(false);
      } catch (e) {
          console.error("Create session failed", e);
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
              }
          }
      } catch (e) {
          console.error("Delete failed", e);
      }
  };

  const handleSelectSession = (id: string) => {
      setSessionId(id);
      fetchDatasets(id);
      setPreviewData(null);
      setTree(initialTree); // Reset tree on session switch
      setIsSessionMenuOpen(false);
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

  const switchToSql = () => setCurrentView('sql');
  const switchToWorkflow = () => setCurrentView('workflow');

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
      
      {/* Click outside handler for session menu */}
      {isSessionMenuOpen && (
          <div className="fixed inset-0 z-30" onClick={() => setIsSessionMenuOpen(false)} />
      )}

      {/* 1. TOP NAVBAR */}
      <header className="h-14 bg-white border-b border-gray-200 flex items-center justify-between px-4 shrink-0 shadow-sm z-40 relative">
        <div className="flex items-center space-x-3">
          <div className="bg-blue-600 p-1.5 rounded-lg">
            <GitBranch className="w-5 h-5 text-white" />
          </div>
          <h1 className="font-bold text-gray-800 tracking-tight hidden md:block">DataFlow Engine</h1>
          <span className="text-gray-300 text-xl font-light hidden md:block">|</span>
          
          {/* SESSION MANAGER */}
          <div className="relative">
              <button 
                  onClick={() => setIsSessionMenuOpen(!isSessionMenuOpen)}
                  disabled={!sessionId && sessions.length === 0 && !apiConfig.isMock}
                  className="flex items-center justify-between space-x-2 bg-white border border-gray-300 hover:border-blue-400 hover:bg-gray-50 text-gray-900 px-3 py-1.5 rounded-md shadow-sm transition-all text-sm min-w-[180px]"
              >
                  <div className="flex items-center overflow-hidden">
                      <span className="text-gray-400 mr-2 text-xs uppercase font-semibold">Session</span>
                      <span className="font-medium truncate">{sessionId || 'No Session'}</span>
                  </div>
                  <ChevronDown className={`w-4 h-4 text-gray-500 transition-transform ${isSessionMenuOpen ? 'rotate-180' : ''}`} />
              </button>

              {isSessionMenuOpen && (
                  <div className="absolute top-full left-0 mt-1 w-72 bg-white border border-gray-200 rounded-lg shadow-xl z-50 flex flex-col animate-in fade-in zoom-in-95 duration-100 origin-top-left">
                      <div className="px-4 py-2 border-b border-gray-100 bg-gray-50 rounded-t-lg">
                          <span className="text-xs font-semibold text-gray-500 uppercase tracking-wider">Switch or Manage</span>
                      </div>
                      
                      <div className="max-h-[300px] overflow-y-auto p-1">
                          {sessions.map(s => (
                              <div 
                                  key={s.sessionId}
                                  onClick={() => handleSelectSession(s.sessionId)}
                                  className={`group flex items-center justify-between px-3 py-2.5 rounded-md cursor-pointer transition-colors ${s.sessionId === sessionId ? 'bg-blue-50' : 'hover:bg-gray-100'}`}
                              >
                                  <div className="flex items-center min-w-0">
                                      <div className={`w-1.5 h-1.5 rounded-full mr-3 ${s.sessionId === sessionId ? 'bg-blue-500' : 'bg-gray-300'}`} />
                                      <div className="flex flex-col min-w-0">
                                          <span className={`text-sm font-medium truncate ${s.sessionId === sessionId ? 'text-blue-900' : 'text-gray-700'}`}>
                                              {s.sessionId}
                                          </span>
                                          <div className="flex items-center text-[10px] text-gray-400 mt-0.5">
                                              <Clock className="w-3 h-3 mr-1" />
                                              {new Date(s.createdAt).toLocaleTimeString()}
                                          </div>
                                      </div>
                                  </div>
                                  
                                  <div className="flex items-center">
                                      {s.sessionId === sessionId && <Check className="w-4 h-4 text-blue-500 mr-2" />}
                                      <button 
                                          onClick={(e) => handleDeleteSession(e, s.sessionId)}
                                          className={`p-1.5 rounded-md text-gray-400 hover:text-red-600 hover:bg-red-50 transition-all ${sessions.length === 1 ? 'hidden' : 'opacity-0 group-hover:opacity-100'}`}
                                          title="Delete Session"
                                      >
                                          <Trash2 className="w-4 h-4" />
                                      </button>
                                  </div>
                              </div>
                          ))}
                          {sessions.length === 0 && (
                             <div className="p-3 text-center text-sm text-gray-400 italic">No active sessions</div>
                          )}
                      </div>
                      
                      <div className="p-2 border-t border-gray-100 bg-gray-50/50 rounded-b-lg">
                          <button 
                              onClick={handleCreateSession}
                              className="w-full flex items-center justify-center space-x-2 bg-white border border-gray-300 text-gray-700 hover:bg-gray-50 hover:border-gray-400 text-sm font-medium py-2 rounded-md transition-colors shadow-sm"
                          >
                              <Plus className="w-4 h-4 text-green-600" />
                              <span>Create New Session</span>
                          </button>
                      </div>
                  </div>
              )}
          </div>
        </div>

        {/* VIEW SWITCHER TABS */}
        <div className="flex items-center bg-gray-100 p-1 rounded-lg">
             <button 
                onClick={switchToWorkflow}
                className={`flex items-center px-3 py-1.5 text-sm font-medium rounded-md transition-all ${currentView === 'workflow' ? 'bg-white text-blue-700 shadow-sm' : 'text-gray-500 hover:text-gray-700'}`}
             >
                <Layers className="w-4 h-4 mr-2" /> Workflow
             </button>
             <button 
                onClick={switchToSql}
                className={`flex items-center px-3 py-1.5 text-sm font-medium rounded-md transition-all ${currentView === 'sql' ? 'bg-white text-blue-700 shadow-sm' : 'text-gray-500 hover:text-gray-700'}`}
             >
                <Terminal className="w-4 h-4 mr-2" /> SQL Studio
             </button>
        </div>

        <div className="flex items-center space-x-3">
             {/* Settings Button (Server Config) */}
             <button 
                onClick={() => setShowSettingsModal(true)}
                className={`flex items-center px-3 py-1.5 text-xs font-medium rounded-full border transition-colors ${
                    apiConfig.isMock 
                    ? 'bg-yellow-50 text-yellow-700 border-yellow-200 hover:bg-yellow-100' 
                    : 'bg-green-50 text-green-700 border-green-200 hover:bg-green-100'
                }`}
                title="Configure Server"
             >
                <Server className="w-3 h-3 mr-1.5" />
                {apiConfig.isMock ? 'Mock Server' : 'Localhost'}
             </button>

            {currentView === 'workflow' && (
                <>
                    <div className="h-6 w-px bg-gray-300 mx-2 hidden sm:block" />
                    <Button variant="primary" size="sm" icon={<Play className="w-4 h-4" />} onClick={executeOperation} disabled={!sessionId}>
                        Run Analysis
                    </Button>
                    <button
                        onClick={() => setIsRightPanelOpen(!isRightPanelOpen)}
                        className={`p-2 rounded-md transition-colors ${isRightPanelOpen ? 'bg-blue-100 text-blue-700' : 'text-gray-500 hover:bg-gray-100'}`}
                        title={isRightPanelOpen ? "Hide Preview" : "Show Preview"}
                    >
                        <PanelRight className="w-5 h-5" />
                    </button>
                </>
            )}
        </div>
      </header>

      {/* 2. MAIN LAYOUT */}
      <div className="flex flex-1 overflow-hidden">
        
        {/* LEFT SIDEBAR (RESIZABLE) - Shared between views or mainly for nav/data */}
        <aside 
            className="bg-white border-r border-gray-200 flex flex-col transition-none z-10 shrink-0"
            style={{ width: sidebarWidth }}
        >
          
          {/* Operations Section (Only in Workflow) */}
          {currentView === 'workflow' && (
              <div className={`flex flex-col transition-all duration-300 ${isOpsExpanded ? 'flex-1 min-h-0' : 'flex-none'}`}>
                 <div 
                    className="p-3 bg-gray-50 border-b border-gray-200 flex justify-between items-center cursor-pointer hover:bg-gray-100 select-none"
                    onClick={() => setIsOpsExpanded(!isOpsExpanded)}
                 >
                    <div className="flex items-center space-x-2">
                        {isOpsExpanded ? <ChevronDown className="w-4 h-4 text-gray-500"/> : <ChevronRight className="w-4 h-4 text-gray-500"/>}
                        <span className="text-xs font-semibold text-gray-600 uppercase tracking-wider">Operations</span>
                    </div>
                    <button onClick={(e) => { e.stopPropagation(); handleAddChild('root'); }} className="p-1 hover:bg-gray-200 rounded text-blue-600">
                        <Layers className="w-4 h-4" />
                    </button>
                 </div>
                 
                 {isOpsExpanded && (
                    <div className="flex-1 overflow-y-auto p-2">
                        {tree.children?.map(node => (
                            <OperationTree 
                                key={node.id} 
                                node={node} 
                                selectedId={selectedNodeId} 
                                onSelect={setSelectedNodeId}
                                onToggleEnabled={handleToggleEnabled}
                                onAddChild={handleAddChild}
                                onDelete={handleDeleteNode}
                            />
                        ))}
                        {(!tree.children || tree.children.length === 0) && (
                            <div className="text-center mt-10 text-gray-400 text-sm p-4">
                                No operations yet.
                            </div>
                        )}
                    </div>
                 )}
              </div>
          )}

          {/* Data Sources Section (Always Visible or Conditional) */}
          <div className={`flex flex-col border-t border-gray-200 transition-all duration-300 ${currentView === 'sql' ? 'flex-1' : (!isOpsExpanded ? 'flex-1' : (isDataExpanded ? 'h-1/3' : 'flex-none'))}`}>
             <div 
                 className="p-3 bg-gray-50 border-b border-gray-200 flex justify-between items-center cursor-pointer hover:bg-gray-100 select-none"
                 onClick={() => setIsDataExpanded(!isDataExpanded)}
             >
                 <div className="flex items-center space-x-2">
                     {isDataExpanded ? <ChevronDown className="w-4 h-4 text-gray-500"/> : <ChevronRight className="w-4 h-4 text-gray-500"/>}
                     <span className="text-xs font-semibold text-gray-600 uppercase tracking-wider">Data Sources</span>
                 </div>
                 <button 
                    onClick={(e) => { e.stopPropagation(); setShowImportModal(true); }}
                    className="p-1 hover:bg-blue-100 rounded text-blue-600 transition-colors"
                    title="Upload Dataset"
                    disabled={!sessionId}
                 >
                    <Plus className="w-4 h-4" />
                 </button>
             </div>
             
             {isDataExpanded && (
                 <div className="flex-1 overflow-y-auto p-2 bg-white">
                    {!sessionId ? (
                        <div className="flex flex-col items-center justify-center h-full text-center p-4 text-gray-400 italic text-xs">
                           Create a session to manage data.
                        </div>
                    ) : datasets.length === 0 ? (
                        <div className="flex flex-col items-center justify-center h-full text-center p-4 text-gray-400 italic text-xs">
                           No tables found in Session DB. <br/> Import CSV to start.
                        </div>
                    ) : (
                        <div className="space-y-1">
                            {datasets.map(ds => (
                                <div 
                                    key={ds.id} 
                                    onClick={() => handleOpenTableInSql(ds.name)}
                                    className="flex items-center px-2 py-1.5 bg-white hover:bg-blue-50 cursor-pointer rounded-md text-sm transition-colors group justify-between"
                                >
                                    <div className="flex items-center min-w-0">
                                        <Database className="w-3.5 h-3.5 text-gray-400 mr-2 shrink-0 group-hover:text-blue-500" />
                                        <div className="font-medium text-gray-700 truncate group-hover:text-blue-700" title={ds.name}>{ds.name}</div>
                                        <div className="text-[10px] text-gray-400 ml-2 shrink-0">{ds.totalCount} rows</div>
                                    </div>
                                    <button 
                                        onClick={(e) => { e.stopPropagation(); handleOpenTableInSql(ds.name); }}
                                        className="p-1 text-gray-300 hover:text-blue-600 opacity-0 group-hover:opacity-100 transition-opacity"
                                        title="Query Table"
                                    >
                                        <Search className="w-3.5 h-3.5" />
                                    </button>
                                </div>
                            ))}
                        </div>
                    )}
                 </div>
             )}
          </div>
        </aside>

        {/* RESIZER HANDLER (LEFT) */}
        <div 
            className={`w-1 hover:w-1.5 bg-gray-200 hover:bg-blue-400 cursor-col-resize z-20 flex flex-col justify-center items-center transition-all ${isResizingSidebar ? 'bg-blue-500 w-1.5' : ''}`}
            onMouseDown={() => setIsResizingSidebar(true)}
        >
             <div className="h-8 w-0.5 bg-gray-400 rounded-full" />
        </div>

        {/* CENTER & RIGHT CONTENT AREA */}
        <main className="flex-1 flex min-w-0">
            
            {currentView === 'sql' ? (
                // SQL VIEW
                <div className="flex-1 h-full overflow-hidden">
                    <SqlEditor 
                        sessionId={sessionId} 
                        apiConfig={apiConfig} 
                        targetTable={targetSqlTable}
                        onClearTarget={() => setTargetSqlTable(null)}
                    />
                </div>
            ) : (
                // WORKFLOW VIEW
                <>
                    {/* MIDDLE: COMMAND EDITOR (FLEX-1) */}
                    <div className="flex-1 flex flex-col bg-gray-50/50 min-w-[300px]">
                        {selectedNode && selectedNode.id !== 'root' ? (
                            <CommandEditor 
                                operationId={selectedNode.id}
                                operationName={selectedNode.name}
                                commands={selectedNode.commands} 
                                datasets={datasets}
                                inheritedDataSource={inheritedDataSource}
                                onUpdateCommands={handleUpdateCommands}
                                onUpdateName={handleUpdateName}
                                onViewPath={() => setShowPathModal(true)}
                            />
                        ) : (
                            <div className="flex-1 flex flex-col items-center justify-center text-gray-400 bg-gray-50">
                                <Settings className="w-12 h-12 mb-4 opacity-20" />
                                <p className="font-medium">Configuration Panel</p>
                                <p className="text-sm mt-2 opacity-70">Select an operation to edit commands.</p>
                            </div>
                        )}
                    </div>

                     {/* RESIZER HANDLER (RIGHT) */}
                     {isRightPanelOpen && (
                         <div 
                            className={`w-1 hover:w-1.5 bg-gray-200 hover:bg-blue-400 cursor-col-resize z-20 flex flex-col justify-center items-center transition-all ${isResizingRight ? 'bg-blue-500 w-1.5' : ''}`}
                            onMouseDown={() => setIsResizingRight(true)}
                        >
                            <div className="h-8 w-0.5 bg-gray-400 rounded-full" />
                        </div>
                    )}

                    {/* RIGHT: DATA PREVIEW (RESIZABLE) */}
                    {isRightPanelOpen && (
                        <div 
                            className="flex flex-col bg-white shrink-0"
                            style={{ width: rightPanelWidth }}
                        >
                            <DataPreview 
                                data={previewData} 
                                loading={loading}
                                onRefresh={executeOperation}
                            />
                        </div>
                    )}
                </>
            )}
        </main>

      </div>
    </div>
  );
};

export default App;
