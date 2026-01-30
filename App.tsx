
import React, { useState, useEffect, useMemo, useRef } from 'react';
import { DataImportModal } from './components/DataImport';
import { PathConditionsModal } from './components/PathConditionsModal';
import { SettingsModal } from './components/SettingsModal';
import { SessionSettingsModal } from './components/SessionSettingsModal';
import { DatasetSchemaModal } from './components/DatasetSchemaModal';
import { TopBar } from './components/TopBar';
import { Sidebar } from './components/Sidebar';
import { Workspace } from './components/Workspace';
import { OperationNode, Dataset, ExecutionResult, Command, SessionMetadata, ApiConfig, OperationType, SessionConfig, DataType, FieldInfo } from './types';
import { api } from './utils/api';

// --- INITIAL DATA ---
const initialTree: OperationNode = {
  id: 'root',
  type: 'operation',
  operationType: 'dataset',
  name: 'Root Source',
  enabled: true,
  commands: [],
  children: [
    {
      id: 'op_1',
      type: 'operation',
      operationType: 'process',
      name: 'Initial Filter',
      enabled: true,
      commands: [],
      children: []
    }
  ]
};

const DEFAULT_SERVERS = ['mockServer', 'http://localhost:8000'];

const App: React.FC = () => {
  // --- STATE ---
  const [isMobile, setIsMobile] = useState(window.innerWidth < 768);
  const [isMobileSidebarOpen, setIsMobileSidebarOpen] = useState(false);

  useEffect(() => {
      const handleResize = () => {
          const mobile = window.innerWidth < 768;
          setIsMobile(mobile);
          if (!mobile) setIsMobileSidebarOpen(false); 
      };
      window.addEventListener('resize', handleResize);
      return () => window.removeEventListener('resize', handleResize);
  }, []);

  const [availableServers, setAvailableServers] = useState<string[]>(() => {
    const saved = localStorage.getItem('availableServers');
    return saved ? JSON.parse(saved) : DEFAULT_SERVERS;
  });
  const [currentServer, setCurrentServer] = useState<string>(() => {
    return localStorage.getItem('currentServer') || 'mockServer';
  });
  
  const [sessions, setSessions] = useState<SessionMetadata[]>([]);
  const [sessionId, setSessionId] = useState<string>('');
  const [sessionDisplayName, setSessionDisplayName] = useState<string>('');
  const [sessionSettings, setSessionSettings] = useState<SessionConfig>({ cascadeDisable: false, panelPosition: 'right' });
  const [isSessionLoading, setIsSessionLoading] = useState(false); 

  const [currentView, setCurrentView] = useState<'workflow' | 'sql'>('workflow');
  const [targetSqlTable, setTargetSqlTable] = useState<string | null>(null);

  const [datasets, setDatasets] = useState<Dataset[]>([]);
  const [tree, setTree] = useState<OperationNode>(initialTree);
  const [selectedNodeId, setSelectedNodeId] = useState<string>('op_1');
  const [previewData, setPreviewData] = useState<ExecutionResult | null>(null);
  const [loading, setLoading] = useState(false);
  
  const [sidebarWidth, setSidebarWidth] = useState(300);
  const [rightPanelWidth, setRightPanelWidth] = useState(500);
  const [isResizingSidebar, setIsResizingSidebar] = useState(false);
  const [isResizingRight, setIsResizingRight] = useState(false);
  const [isRightPanelOpen, setIsRightPanelOpen] = useState(false); 
  
  const [showImportModal, setShowImportModal] = useState(false);
  const [showPathModal, setShowPathModal] = useState(false);
  const [showSettingsModal, setShowSettingsModal] = useState(false);
  const [showSessionSettingsModal, setShowSessionSettingsModal] = useState(false);
  const [schemaModalDataset, setSchemaModalDataset] = useState<Dataset | null>(null);

  // --- DERIVED STATE ---
  const apiConfig: ApiConfig = useMemo(() => ({
    baseUrl: currentServer,
    isMock: currentServer === 'mockServer'
  }), [currentServer]);

  // --- EFFECTS ---
  useEffect(() => {
    localStorage.setItem('availableServers', JSON.stringify(availableServers));
    localStorage.setItem('currentServer', currentServer);
  }, [availableServers, currentServer]);

  useEffect(() => {
    fetchSessions();
    setDatasets([]);
    setSessionId('');
    setSessionDisplayName('');
    setSessionSettings({ cascadeDisable: false, panelPosition: 'right' });
    setPreviewData(null);
    setTree(initialTree);
  }, [apiConfig]);

  useEffect(() => {
    if (!sessionId || isSessionLoading) return;
    const timer = setTimeout(() => {
        api.post(apiConfig, `/sessions/${sessionId}/state`, tree)
           .catch(e => console.error("Auto-save failed", e));
    }, 1000); 
    return () => clearTimeout(timer);
  }, [tree, sessionId, apiConfig, isSessionLoading]);

  const fetchSessions = async () => {
    try {
        const data = await api.get(apiConfig, '/sessions');
        setSessions(data);
        if (data.length > 0) handleSelectSession(data[0].sessionId);
        else setSessionId('');
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
          if (state && state.id) setTree(state);
          else setTree(initialTree);
      } catch (e) {
          console.error("Failed to fetch session state", e);
          setTree(initialTree);
      }
  };

  const fetchSessionMetadata = async (sessId: string) => {
      try {
          const meta = await api.get(apiConfig, `/sessions/${sessId}/metadata`);
          setSessionDisplayName(meta.displayName || '');
          setSessionSettings(meta.settings || { cascadeDisable: false, panelPosition: 'right' });
      } catch (e) {
          console.error("Failed to fetch session metadata", e);
      }
  };

  const findPathToNode = (root: OperationNode, targetId: string): OperationNode[] | null => {
      if (root.id === targetId) return [root];
      if (root.children) {
          for (const child of root.children) {
              const path = findPathToNode(child, targetId);
              if (path) return [root, ...path];
          }
      }
      return null;
  };

  const calculateSchemaForNode = (targetId: string): Record<string, DataType> => {
      const path = findPathToNode(tree, targetId);
      if (!path) return {};

      // Calculate schema based on ALL previous steps in the chain (parent nodes)
      const ancestors = path.slice(0, -1);
      
      let currentSchema: Record<string, DataType> = {};

      for (const node of ancestors) {
          if (!node.enabled) continue;
          
          for (const cmd of node.commands) {
                // 1. Source / Context Switch
                let sourceDsName: string | undefined;
                if (cmd.type === 'source') sourceDsName = cmd.config.mainTable;
                else if (cmd.config.dataSource && cmd.config.dataSource !== 'stream') sourceDsName = cmd.config.dataSource;
                
                if (sourceDsName) {
                    const ds = datasets.find(d => d.name === sourceDsName);
                    if (ds) {
                        if (ds.fieldTypes && Object.keys(ds.fieldTypes).length > 0) {
                            // Fix: Correctly extract DataType from FieldInfo Record.
                            currentSchema = {};
                            Object.keys(ds.fieldTypes).forEach(f => {
                                currentSchema[f] = ds.fieldTypes![f].type;
                            });
                        } else {
                            // Fallback if fieldTypes missing
                            currentSchema = {};
                            ds.fields.forEach(f => currentSchema[f] = 'string');
                        }
                    }
                }

                // 2. Join
                if (cmd.type === 'join' && cmd.config.joinTable) {
                    const ds = datasets.find(d => d.name === cmd.config.joinTable);
                    if (ds) {
                        const suffix = cmd.config.joinSuffix || '_joined';
                        const joinSchema = ds.fieldTypes || {};
                        const joinFields = Object.keys(joinSchema).length > 0 ? Object.keys(joinSchema) : ds.fields;
                        
                        joinFields.forEach(f => {
                             // Fix: Extract .type from FieldInfo if available.
                             const type = joinSchema[f]?.type || 'string';
                             if (currentSchema[f]) {
                                 currentSchema[`${f}${suffix}`] = type as DataType;
                             } else {
                                 currentSchema[f] = type as DataType;
                             }
                        });
                    }
                }

                // 3. Transform
                if (cmd.type === 'transform' && cmd.config.outputField) {
                    currentSchema[cmd.config.outputField] = 'number'; // Assume number mostly
                }

                // 4. Aggregate
                // Fix: Changed 'aggregate' to 'group' to match CommandType definition
                if (cmd.type === 'group') {
                     const nextSchema: Record<string, DataType> = {};
                     const groups = cmd.config.groupByFields || [];
                     const aggs = cmd.config.aggregations || [];

                     groups.forEach(g => {
                         if (currentSchema[g]) nextSchema[g] = currentSchema[g];
                     });

                     aggs.forEach(agg => {
                         const alias = agg.alias || `${agg.func}_${agg.field}`;
                         nextSchema[alias] = 'number';
                     });

                     if (Object.keys(nextSchema).length > 0) currentSchema = nextSchema;
                }
          }
      }
      return currentSchema;
  };
  
  const analyzeOverlap = async (nodeId: string) => {
      const node = findNode([tree], nodeId);
      if(!node || !node.children || node.children.length < 2) {
          alert("Need at least 2 child branches to analyze overlap.");
          return;
      }
      try {
          const response = await api.post(apiConfig, '/analyze', { sessionId, tree, parentNodeId: nodeId });
          if (response.report) alert("Duplicate Record Analysis:\n\n" + response.report.join("\n"));
      } catch (e: any) {
          alert("Analysis failed: " + e.message);
      }
  };

  const handleAddServer = (url: string) => {
      if (!availableServers.includes(url)) {
          setAvailableServers([...availableServers, url]);
          setCurrentServer(url);
      }
  };

  const handleRemoveServer = (url: string) => {
      setAvailableServers(availableServers.filter(s => s !== url));
      if (currentServer === url) setCurrentServer('mockServer');
  };

  const handleCreateSession = async () => {
      try {
        const data = await api.post(apiConfig, '/sessions', {});
        if (data && data.sessionId) {
             const newSession = { sessionId: data.sessionId, createdAt: Date.now() }; 
             setSessions(prev => [newSession, ...prev]);
             handleSelectSession(data.sessionId);
        }
      } catch (e) {
          alert("Failed to create session.");
      }
  };

  const handleDeleteSession = async (e: React.MouseEvent, idToDelete: string) => {
      e.stopPropagation();
      try {
          await api.delete(apiConfig, `/sessions/${idToDelete}`);
          const newSessions = sessions.filter(s => s.sessionId !== idToDelete);
          setSessions(newSessions);
          if (idToDelete === sessionId) {
              if (newSessions.length > 0) handleSelectSession(newSessions[0].sessionId);
              else { setSessionId(''); setSessionDisplayName(''); setDatasets([]); setTree(initialTree); }
          }
      } catch (e) {}
  };

  const handleSelectSession = async (id: string) => {
      setIsSessionLoading(true); 
      setSessionId(id);
      await Promise.all([fetchDatasets(id), fetchSessionState(id), fetchSessionMetadata(id)]);
      setPreviewData(null);
      setIsSessionLoading(false); 
      if(isMobile) setIsMobileSidebarOpen(false);
  };
  
  const handleSaveSessionSettings = async (newName: string, newSettings: SessionConfig) => {
      if (!sessionId) return;
      try {
          await api.post(apiConfig, `/sessions/${sessionId}/metadata`, { displayName: newName, settings: newSettings });
          setSessionDisplayName(newName);
          setSessionSettings(newSettings);
          const sessionList = await api.get(apiConfig, '/sessions');
          setSessions(sessionList);
      } catch (e) { throw e; }
  };

  const handleImport = (dataset: Dataset) => {
    setDatasets(prev => [dataset, ...prev]);
    setShowImportModal(false);
  };

  const handleOpenTableInSql = async (tableName: string) => {
      if (currentView === 'workflow') {
          setLoading(true);
          try {
              const res = await api.post(apiConfig, '/query', { sessionId, query: `SELECT * FROM ${tableName}`, page: 1, pageSize: 50 });
              setPreviewData(res);
              setIsRightPanelOpen(true);
          } catch(e: any) { alert(e.message); } finally { setLoading(false); }
          if (isMobile) setIsMobileSidebarOpen(false);
      } else {
          setTargetSqlTable(tableName);
          setCurrentView('sql');
          if (isMobile) setIsMobileSidebarOpen(false);
      }
  };

  const handleOpenSchemaModal = (datasetName: string) => {
      const ds = datasets.find(d => d.name === datasetName);
      if (ds) setSchemaModalDataset(ds);
  };

  const handleUpdateDatasetSchema = async (datasetId: string, fieldTypes: Record<string, FieldInfo>) => {
      try {
          await api.post(apiConfig, `/sessions/${sessionId}/datasets/update`, { datasetId, fieldTypes });
          // Update local state immediately
          setDatasets(prev => prev.map(d => d.id === datasetId ? { ...d, fieldTypes } : d));
      } catch (e) {
          console.error("Failed to update schema", e);
          throw e;
      }
  };

  // --- TREE MODIFICATION HANDLERS ---
  const updateNode = (nodes: OperationNode[], id: string, updater: (n: OperationNode) => OperationNode): OperationNode[] => {
    return nodes.map(node => {
      if (node.id === id) return updater(node);
      if (node.children) return { ...node, children: updateNode(node.children, id, updater) };
      return node;
    });
  };

  const deleteNode = (nodes: OperationNode[], id: string): OperationNode[] => {
    return nodes.filter(node => node.id !== id).map(node => ({ ...node, children: node.children ? deleteNode(node.children, id) : undefined }));
  };

  const addChildNode = (nodes: OperationNode[], parentId: string): OperationNode[] => {
      return nodes.map(node => {
          if (node.id === parentId) {
              const newChild: OperationNode = { id: `op_${Date.now()}`, type: 'operation', operationType: 'process', name: `New Operation`, enabled: true, commands: [], children: [] };
              return { ...node, children: [...(node.children || []), newChild] };
          }
          if (node.children) return { ...node, children: addChildNode(node.children, parentId) };
          return node;
      });
  };

  const handleUpdateCommands = (opId: string, newCommands: Command[]) => setTree(prev => ({ ...prev, children: updateNode(prev.children || [], opId, n => ({ ...n, commands: newCommands })) }));
  const handleUpdateName = (name: string) => setTree(prev => ({ ...prev, children: updateNode(prev.children || [], selectedNodeId, n => ({ ...n, name })) }));
  const handleUpdateType = (opId: string, type: OperationType) => setTree(prev => ({ ...prev, children: updateNode(prev.children || [], opId, n => ({ ...n, operationType: type })) }));
  const handleToggleEnabled = (id: string) => setTree(prev => ({ ...prev, children: updateNode(prev.children || [], id, n => ({ ...n, enabled: !n.enabled })) }));
  const handleAddChild = (parentId: string) => {
      if(parentId === 'root') setTree(prev => ({ ...prev, children: [...(prev.children || []), { id: `op_${Date.now()}`, type: 'operation', operationType: 'process', name: 'New Analysis Path', enabled: true, commands: [], children: [] }] }));
      else setTree(prev => ({ ...prev, children: addChildNode(prev.children || [], parentId) }));
  };
  const handleDeleteNode = (id: string) => {
      setTree(prev => ({ ...prev, children: deleteNode(prev.children || [], id) }));
      if (selectedNodeId === id) setSelectedNodeId('root');
  };
  const handleUpdatePageSize = (size: number) => {
      const newTree = { ...tree, children: updateNode(tree.children || [], selectedNodeId, n => ({ ...n, pageSize: size })) };
      setTree(newTree);
      executeOperation(1, newTree);
  };
  
  const handleExportFull = async () => {
      try { await api.export(apiConfig, '/export', { sessionId, tree, targetNodeId: selectedNodeId }); } catch (e: any) { alert("Export failed: " + e.message); }
  };

  // Resizing logic...
  useEffect(() => {
    const handleMouseMove = (e: MouseEvent) => {
        if (isResizingSidebar && !isMobile) setSidebarWidth(Math.max(240, Math.min(600, e.clientX)));
        if (isResizingRight && !isMobile) {
             const isVertical = sessionSettings.panelPosition === 'top' || sessionSettings.panelPosition === 'bottom';
             const isLeft = sessionSettings.panelPosition === 'left';
             const isTop = sessionSettings.panelPosition === 'top';
             let newSize = 500;
             if (isVertical) newSize = isTop ? Math.max(200, Math.min(window.innerHeight - 200, e.clientY - 50)) : Math.max(200, Math.min(window.innerHeight - 200, window.innerHeight - e.clientY));
             else newSize = isLeft ? Math.max(300, Math.min(window.innerWidth - 400, e.clientX - sidebarWidth)) : Math.max(300, Math.min(800, window.innerWidth - e.clientX));
             setRightPanelWidth(newSize);
        }
    };
    const handleMouseUp = () => { setIsResizingSidebar(false); setIsResizingRight(false); document.body.style.cursor = 'default'; document.body.style.userSelect = 'auto'; };
    if (isResizingSidebar || isResizingRight) { window.addEventListener('mousemove', handleMouseMove); window.addEventListener('mouseup', handleMouseUp); document.body.style.userSelect = 'none'; document.body.style.cursor = (isResizingRight && (sessionSettings.panelPosition === 'top' || sessionSettings.panelPosition === 'bottom')) ? 'row-resize' : 'col-resize'; }
    return () => { window.removeEventListener('mousemove', handleMouseMove); window.removeEventListener('mouseup', handleMouseUp); };
  }, [isResizingSidebar, isResizingRight, isMobile, sessionSettings.panelPosition, sidebarWidth]);

  const findNode = (nodes: OperationNode[], id: string): OperationNode | null => {
      for (const node of nodes) {
          if (node.id === id) return node;
          if (node.children) { const found = findNode(node.children, id); if (found) return found; }
      }
      return null;
  };
  
  const executeOperation = async (page: number = 1, treeState = tree) => {
    const node = selectedNodeId === 'root' ? treeState : (treeState.children ? findNode(treeState.children, selectedNodeId) : null);
    const pageSize = node?.pageSize || 50;
    setLoading(true);
    try {
        const result = await api.post(apiConfig, '/execute', { sessionId, tree: treeState, targetNodeId: selectedNodeId, page, pageSize });
        setPreviewData(result);
        if (!isRightPanelOpen) setIsRightPanelOpen(true);
    } catch (err: any) { alert(`Failed to execute: ${err.message}`); } finally { setLoading(false); }
  };
  const handleExportOperations = () => { /* ... */ };
  const handleImportOperations = (file: File) => { /* ... */ };

  const selectedNode = selectedNodeId === 'root' ? tree : (tree.children ? findNode(tree.children, selectedNodeId) : null);
  const inheritedSchema = useMemo(() => calculateSchemaForNode(selectedNodeId), [tree, selectedNodeId, datasets]);

  return (
    <div className="flex flex-col h-screen bg-gray-100 overflow-hidden text-gray-800">
      <DataImportModal isOpen={showImportModal} onClose={() => setShowImportModal(false)} onImport={handleImport} sessionId={sessionId} apiConfig={apiConfig} />
      <PathConditionsModal isOpen={showPathModal} onClose={() => setShowPathModal(false)} tree={tree} targetNodeId={selectedNodeId} sessionId={sessionId} apiConfig={apiConfig} />
      <SettingsModal isOpen={showSettingsModal} onClose={() => setShowSettingsModal(false)} servers={availableServers} currentServer={currentServer} onSelectServer={setCurrentServer} onAddServer={handleAddServer} onRemoveServer={handleRemoveServer} />
      <SessionSettingsModal isOpen={showSessionSettingsModal} onClose={() => setShowSessionSettingsModal(false)} sessionId={sessionId} initialDisplayName={sessionDisplayName} initialSettings={sessionSettings} onSave={handleSaveSessionSettings} />
      
      {/* Schema Modal */}
      <DatasetSchemaModal isOpen={!!schemaModalDataset} onClose={() => setSchemaModalDataset(null)} dataset={schemaModalDataset} onSave={handleUpdateDatasetSchema} />

      <TopBar sessionId={sessionId} sessionName={sessionDisplayName} sessions={sessions} currentView={currentView} apiConfig={apiConfig} isRightPanelOpen={isRightPanelOpen} onSessionSelect={handleSelectSession} onSessionCreate={handleCreateSession} onSessionDelete={handleDeleteSession} onViewChange={setCurrentView} onSettingsOpen={() => setShowSettingsModal(true)} onSessionSettingsOpen={() => setShowSessionSettingsModal(true)} onExecute={() => executeOperation(1)} onToggleRightPanel={() => setIsRightPanelOpen(!isRightPanelOpen)} onToggleMobileSidebar={() => setIsMobileSidebarOpen(!isMobileSidebarOpen)} />

      <div className="flex flex-1 overflow-hidden relative">
        {isMobile && isMobileSidebarOpen && <div className="fixed inset-0 bg-black/50 z-30 animate-in fade-in duration-200" onClick={() => setIsMobileSidebarOpen(false)} />}
        <div className={`fixed inset-y-0 left-0 z-40 bg-white shadow-xl transform transition-transform duration-300 md:relative md:translate-x-0 md:shadow-none md:z-auto ${isMobileSidebarOpen ? 'translate-x-0' : '-translate-x-full'}`}>
             <Sidebar width={isMobile ? window.innerWidth * 0.85 : sidebarWidth} currentView={currentView} sessionId={sessionId} tree={tree} datasets={datasets} selectedNodeId={selectedNodeId} onSelectNode={(id) => { setSelectedNodeId(id); if(isMobile) setIsMobileSidebarOpen(false); }} onToggleEnabled={handleToggleEnabled} onAddChild={handleAddChild} onDeleteNode={handleDeleteNode} onImportClick={() => setShowImportModal(true)} onOpenTableInSql={handleOpenTableInSql} onExportOperations={handleExportOperations} onImportOperations={handleImportOperations} onAnalyzeOverlap={analyzeOverlap} onOpenSchema={handleOpenSchemaModal} />
        </div>
        {!isMobile && <div className={`w-1 hover:w-1.5 bg-gray-200 hover:bg-blue-400 cursor-col-resize z-20 flex flex-col justify-center items-center transition-all ${isResizingSidebar ? 'bg-blue-500 w-1.5' : ''}`} onMouseDown={() => setIsResizingSidebar(true)}><div className="h-8 w-0.5 bg-gray-400 rounded-full" /></div>}
        <Workspace currentView={currentView} sessionId={sessionId} apiConfig={apiConfig} targetSqlTable={targetSqlTable} onClearTargetSqlTable={() => setTargetSqlTable(null)} selectedNode={selectedNode} datasets={datasets} inputFields={[]} inputSchema={inheritedSchema} onUpdateCommands={handleUpdateCommands} onUpdateName={handleUpdateName} onUpdateType={handleUpdateType} onViewPath={() => setShowPathModal(true)} isRightPanelOpen={isRightPanelOpen} onCloseRightPanel={() => setIsRightPanelOpen(false)} rightPanelWidth={rightPanelWidth} onRightPanelResizeStart={() => setIsResizingRight(true)} previewData={previewData} onClearPreview={() => setPreviewData(null)} loading={loading} onRefreshPreview={(page) => executeOperation(page || 1)} onUpdatePageSize={handleUpdatePageSize} onExportFull={handleExportFull} isMobile={isMobile} tree={tree} panelPosition={sessionSettings.panelPosition || 'right'} />
      </div>
    </div>
  );
};

export default App;
