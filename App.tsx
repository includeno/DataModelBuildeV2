import React, { useState, useEffect, useMemo } from 'react';
import { Sidebar } from './components/Sidebar';
import { Workspace } from './components/Workspace';
import { TopBar } from './components/TopBar';
import { DataImportModal } from './components/DataImport';
import { SettingsModal } from './components/SettingsModal';
import { SessionSettingsModal } from './components/SessionSettingsModal';
import { PathConditionsModal } from './components/PathConditionsModal';
import { DatasetSchemaModal } from './components/DatasetSchemaModal';
import { 
  OperationNode, Dataset, Command, ExecutionResult, ApiConfig, 
  SessionMetadata, AppearanceConfig, SessionConfig, DataType, FieldInfo, OperationType,
  SqlHistoryItem, SessionState
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
  showGuideLines: true
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
  const [currentView, setCurrentView] = useState<'workflow' | 'sql'>('workflow');
  
  // UI State
  const [isImportOpen, setIsImportOpen] = useState(false);
  const [isSettingsOpen, setIsSettingsOpen] = useState(false);
  const [isSessionSettingsOpen, setIsSessionSettingsOpen] = useState(false);
  const [isPathModalOpen, setIsPathModalOpen] = useState(false);
  const [isSchemaModalOpen, setIsSchemaModalOpen] = useState(false);
  const [selectedDatasetForSchema, setSelectedDatasetForSchema] = useState<Dataset | null>(null);

  const [isRightPanelOpen, setIsRightPanelOpen] = useState(true);
  const [rightPanelWidth, setRightPanelWidth] = useState(400); // Or Height if vertical
  const [sidebarWidth] = useState(260);
  const [isMobileSidebarOpen, setIsMobileSidebarOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const [previewData, setPreviewData] = useState<ExecutionResult | null>(null);
  const [isResizing, setIsResizing] = useState(false);
  
  // Configuration State
  const [apiConfig, setApiConfig] = useState<ApiConfig>({ baseUrl: 'http://localhost:8000', isMock: true });
  const [knownServers, setKnownServers] = useState<string[]>(['mockServer', 'http://localhost:8000']);
  const [appearance, setAppearance] = useState<AppearanceConfig>(DEFAULT_APPEARANCE);
  const [sessionSettings, setSessionSettings] = useState<SessionConfig>({ cascadeDisable: false, panelPosition: 'right' });

  // SQL State
  const [targetSqlTable, setTargetSqlTable] = useState<string | null>(null);

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


  // --- INITIALIZATION ---
  useEffect(() => {
    fetchSessions();
  }, [apiConfig]);

  const fetchSessions = async () => {
      try {
          const list = await api.get(apiConfig, '/sessions');
          setSessions(list);
      } catch (e) { console.error("Failed to fetch sessions", e); }
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
          if (state && state.tree) {
              setTree(state.tree);
              if (state.datasets) setDatasets(state.datasets);
              if (state.sqlHistory) setSqlHistory(state.sqlHistory);
          } else {
              setTree(INITIAL_TREE);
              setSqlHistory([]);
              // If mock, ensure mock datasets are available
              if (apiConfig.isMock) {
                  const dss = await api.get(apiConfig, `/datasets`);
                  setDatasets(dss);
              } else {
                  setDatasets([]);
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
          setTree(INITIAL_TREE);
          setSqlHistory([]);
      }
      fetchSessions();
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

      let newOpType: OperationType = 'process';
      let newName = 'New Operation';

      // If adding to root, we create a new Setup node
      if (parent.operationType === 'root') {
          newOpType = 'setup';
          newName = 'Data Setup';
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
      if (!selectedNode) return;
      setLoading(true);
      try {
          // If specific commandId is provided, we might be doing a partial run.
          // For now, the API mostly executes the node. 
          // commandId could be used to truncate commands list in a future improvement.
          const res = await api.post(apiConfig, '/execute', {
              sessionId,
              tree,
              targetNodeId: selectedNodeId,
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

  const handleSchemaSave = async (datasetId: string, fieldTypes: any) => {
      // Update local state
      const updated = datasets.map(d => d.id === datasetId ? { ...d, fieldTypes } : d);
      setDatasets(updated);
      // Persist to backend if needed
      await api.post(apiConfig, `/sessions/${sessionId}/datasets/update`, { datasetId, fieldTypes });
  };

  return (
    <div className="flex flex-col h-screen w-screen overflow-hidden bg-white text-slate-900">
      <TopBar 
        sessionId={sessionId}
        sessionName={sessionName}
        sessions={sessions}
        currentView={currentView}
        apiConfig={apiConfig}
        isRightPanelOpen={isRightPanelOpen}
        onSessionSelect={handleSelectSession}
        onSessionCreate={handleCreateSession}
        onSessionDelete={handleDeleteSession}
        onViewChange={setCurrentView}
        onSettingsOpen={() => setIsSettingsOpen(true)}
        onSessionSettingsOpen={() => setIsSessionSettingsOpen(true)}
        onExecute={() => handleExecute(1)}
        onToggleRightPanel={() => setIsRightPanelOpen(!isRightPanelOpen)}
        onToggleMobileSidebar={() => setIsMobileSidebarOpen(!isMobileSidebarOpen)}
      />

      <div className="flex flex-1 overflow-hidden relative">
         {/* Mobile Sidebar Overlay */}
         {isMobileSidebarOpen && (
             <div className="absolute inset-0 z-40 bg-black/50 md:hidden" onClick={() => setIsMobileSidebarOpen(false)}>
                 <div className="h-full w-64 bg-white shadow-xl" onClick={e => e.stopPropagation()}>
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
                        onImportClick={() => setIsImportOpen(true)}
                        onOpenTableInSql={(t) => { setTargetSqlTable(t); setCurrentView('sql'); setIsMobileSidebarOpen(false); }}
                        onOpenSchema={(name) => { 
                            const ds = datasets.find(d => d.name === name); 
                            if(ds) { setSelectedDatasetForSchema(ds); setIsSchemaModalOpen(true); setIsMobileSidebarOpen(false); }
                        }}
                        appearance={appearance}
                    />
                 </div>
             </div>
         )}

         {/* Desktop Sidebar */}
         <div className="hidden md:block h-full shrink-0">
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
                onImportClick={() => setIsImportOpen(true)}
                onOpenTableInSql={(t) => { setTargetSqlTable(t); setCurrentView('sql'); }}
                onOpenSchema={(name) => { 
                    const ds = datasets.find(d => d.name === name); 
                    if(ds) { setSelectedDatasetForSchema(ds); setIsSchemaModalOpen(true); }
                }}
                appearance={appearance}
             />
         </div>

         <Workspace 
            currentView={currentView}
            sessionId={sessionId}
            apiConfig={apiConfig}
            targetSqlTable={targetSqlTable}
            onClearTargetSqlTable={() => setTargetSqlTable(null)}
            selectedNode={selectedNode}
            datasets={datasets}
            inputFields={[]} 
            inputSchema={globalInputSchema}
            onUpdateCommands={handleUpdateCommands}
            onUpdateName={handleUpdateName}
            onUpdateType={handleUpdateType}
            onViewPath={() => setIsPathModalOpen(true)}
            isRightPanelOpen={isRightPanelOpen}
            onCloseRightPanel={() => setIsRightPanelOpen(false)}
            rightPanelWidth={rightPanelWidth}
            onRightPanelResizeStart={() => setIsResizing(true)}
            previewData={previewData}
            onClearPreview={() => setPreviewData(null)}
            loading={loading}
            onRefreshPreview={(page, cmdId) => handleExecute(page, cmdId)}
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
          currentServer={apiConfig.baseUrl}
          onSelectServer={(url) => { 
              setApiConfig({ baseUrl: url, isMock: url === 'mockServer' }); 
              // Reset session when switching servers
              setSessionId(''); 
              setTree(INITIAL_TREE);
          }}
          onAddServer={(url) => setKnownServers([...knownServers, url])}
          onRemoveServer={(url) => setKnownServers(knownServers.filter(s => s !== url))}
          appearance={appearance}
          onUpdateAppearance={setAppearance}
      />

      <SessionSettingsModal
          isOpen={isSessionSettingsOpen}
          onClose={() => setIsSessionSettingsOpen(false)}
          sessionId={sessionId}
          initialDisplayName={sessionName}
          initialSettings={sessionSettings}
          onSave={handleSaveSessionSettings}
      />

      <PathConditionsModal
          isOpen={isPathModalOpen}
          onClose={() => setIsPathModalOpen(false)}
          tree={tree}
          targetNodeId={selectedNodeId}
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