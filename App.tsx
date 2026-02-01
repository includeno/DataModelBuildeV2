
import React, { useState, useEffect, useMemo, useRef } from 'react';
import { DataImportModal } from './components/DataImport';
import { PathConditionsModal } from './components/PathConditionsModal';
import { SettingsModal } from './components/SettingsModal';
import { SessionSettingsModal } from './components/SessionSettingsModal';
import { DatasetSchemaModal } from './components/DatasetSchemaModal';
import { TopBar } from './components/TopBar';
import { Sidebar } from './components/Sidebar';
import { Workspace } from './components/Workspace';
import { OperationNode, Dataset, ExecutionResult, Command, SessionMetadata, ApiConfig, OperationType, SessionConfig, DataType, FieldInfo, AppearanceConfig } from './types';
import { api } from './utils/api';

// --- INITIAL DATA ---
const initialTree: OperationNode = {
  id: 'root',
  type: 'operation',
  operationType: 'dataset', // Root container
  name: 'Root',
  enabled: true,
  commands: [],
  children: []
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
  
  // Appearance Settings
  const [appearance, setAppearance] = useState<AppearanceConfig>(() => {
    const saved = localStorage.getItem('appearanceSettings');
    return saved ? JSON.parse(saved) : {
        textSize: 12,
        textColor: '#4b5563', // gray-600
        guideLineColor: '#e5e7eb', // gray-200
        showGuideLines: true
    };
  });

  useEffect(() => {
      localStorage.setItem('appearanceSettings', JSON.stringify(appearance));
  }, [appearance]);
  
  const [sessions, setSessions] = useState<SessionMetadata[]>([]);
  const [sessionId, setSessionId] = useState<string>('');
  const [sessionDisplayName, setSessionDisplayName] = useState<string>('');
  const [sessionSettings, setSessionSettings] = useState<SessionConfig>({ cascadeDisable: false, panelPosition: 'right' });
  const [isSessionLoading, setIsSessionLoading] = useState(false); 

  const [currentView, setCurrentView] = useState<'workflow' | 'sql'>('workflow');
  const [targetSqlTable, setTargetSqlTable] = useState<string | null>(null);

  const [datasets, setDatasets] = useState<Dataset[]>([]);
  const [tree, setTree] = useState<OperationNode>(initialTree);
  const [selectedNodeId, setSelectedNodeId] = useState<string>(''); // Default empty
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

  const canExecute = useMemo(() => {
      if (!selectedNode) return false;
      if (selectedNode.operationType === 'setup') return false;
      if (selectedNode.commands.length === 0) return false;
      return true;
  }, [selectedNode]);

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

  // Clear preview when selecting non-executable node
  useEffect(() => {
      if (!canExecute && previewData) {
          setPreviewData(null);
          setIsRightPanelOpen(false);
      }
  }, [canExecute]);

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
          if (state && state.id) {
              setTree(state);
              // Auto select first child if exists
              if (state.children && state.children.length > 0) {
                  setSelectedNodeId(state.children[0].id);
              } else {
                  setSelectedNodeId('');
              }
          } else {
              setTree(initialTree);
              setSelectedNodeId('');
          }
      } catch (e) {
          console.error("Failed to fetch session state", e);
          setTree(initialTree);
          setSelectedNodeId('');
      }
  };

  const fetchSessionMetadata = async (sessId: string) => {
      try {
          const meta = await api.get(apiConfig, `/sessions/${sessId}/metadata`);
          setSessionDisplayName(meta.displayName || '');
          setSessionSettings(meta.settings || { cascadeDisable: false, panelPosition: 'right' });
      } catch (e) {
          console.error("Failed to fetch metadata", e);
      }
  };

  const handleSelectSession = async (id: string) => {
      setSessionId(id);
      setIsSessionLoading(true);
      await Promise.all([
          fetchDatasets(id),
          fetchSessionState(id),
          fetchSessionMetadata(id)
      ]);
      setIsSessionLoading(false);
  };

  const handleCreateSession = async () => {
      try {
          const res = await api.post(apiConfig, '/sessions', {});
          await fetchSessions();
          handleSelectSession(res.sessionId);
      } catch (e) {
          alert("Failed to create session");
      }
  };

  const handleDeleteSession = async (e: React.MouseEvent, id: string) => {
      e.stopPropagation();
      if (!confirm("Are you sure you want to delete this session?")) return;
      try {
          await api.delete(apiConfig, `/sessions/${id}`);
          fetchSessions();
      } catch (e) {
          alert("Failed to delete session");
      }
  };

  const handleSaveSessionSettings = async (name: string, config: SessionConfig) => {
      if (!sessionId) return;
      try {
          await api.post(apiConfig, `/sessions/${sessionId}/metadata`, {
              displayName: name,
              settings: config
          });
          setSessionDisplayName(name);
          setSessionSettings(config);
          
          // Refresh session list to update name there
          const data = await api.get(apiConfig, '/sessions');
          setSessions(data);
      } catch (e) {
          throw e;
      }
  };

  // --- TREE OPERATIONS ---
  
  const handleAddChild = (parentId: string) => {
    const isRoot = parentId === 'root';
    
    // If adding to root, create a Setup Node
    const operationType: OperationType = isRoot ? 'setup' : 'process';
    const defaultName = isRoot ? 'Import Datasets' : 'New Operation';
    
    const newNode: OperationNode = {
      id: `op_${Date.now()}`,
      type: 'operation',
      operationType: operationType,
      name: defaultName,
      enabled: true,
      commands: isRoot 
          ? [{ id: `cmd_src_${Date.now()}`, type: 'source', config: { mainTable: '' }, order: 1 }]
          : [],
      children: []
    };

    const addToNode = (node: OperationNode): OperationNode => {
      if (node.id === parentId) {
        return { ...node, children: [...(node.children || []), newNode] };
      }
      if (node.children) {
        return { ...node, children: node.children.map(addToNode) };
      }
      return node;
    };

    setTree(addToNode(tree));
    setSelectedNodeId(newNode.id);
  };

  const handleDeleteNode = (id: string) => {
    if (id === 'root') return;
    const deleteFromNode = (node: OperationNode): OperationNode => {
      if (node.children) {
        return {
          ...node,
          children: node.children.filter(child => child.id !== id).map(deleteFromNode)
        };
      }
      return node;
    };
    setTree(deleteFromNode(tree));
    if (selectedNodeId === id) setSelectedNodeId('');
  };

  const handleToggleEnabled = (id: string) => {
    const toggleNode = (node: OperationNode, parentDisabled: boolean): OperationNode => {
        let newNode = node;
        
        if (node.id === id) {
            newNode = { ...node, enabled: !node.enabled };
        } else if (parentDisabled && sessionSettings.cascadeDisable) {
             newNode = node;
        }

        if (newNode.children) {
            newNode.children = newNode.children.map(child => toggleNode(child, !newNode.enabled));
        }
        return newNode;
    };
    setTree(toggleNode(tree, false));
  };

  const handleUpdateCommands = (opId: string, newCommands: Command[]) => {
    const updateNode = (node: OperationNode): OperationNode => {
      if (node.id === opId) {
        return { ...node, commands: newCommands };
      }
      if (node.children) {
        return { ...node, children: node.children.map(updateNode) };
      }
      return node;
    };
    setTree(updateNode(tree));
  };

  const handleUpdateName = (name: string) => {
      if (!selectedNodeId) return;
      const updateNode = (node: OperationNode): OperationNode => {
          if (node.id === selectedNodeId) return { ...node, name };
          if (node.children) return { ...node, children: node.children.map(updateNode) };
          return node;
      };
      setTree(updateNode(tree));
  };

  const handleUpdateType = (opId: string, type: OperationType) => {
      const updateNode = (node: OperationNode): OperationNode => {
          if (node.id === opId) return { ...node, operationType: type };
          if (node.children) return { ...node, children: node.children.map(updateNode) };
          return node;
      };
      setTree(updateNode(tree));
  };

  const handleImport = async (dataset: Dataset) => {
      setDatasets(prev => [...prev, dataset]);
  };

  const handleUpdateSchema = async (datasetId: string, fieldTypes: Record<string, FieldInfo>) => {
      try {
          await api.post(apiConfig, `/sessions/${sessionId}/datasets/update`, { datasetId, fieldTypes });
          // Update local state
          setDatasets(prev => prev.map(d => 
              (d.id === datasetId || d.name === datasetId) 
                  ? { ...d, fieldTypes } 
                  : d
          ));
      } catch (e) {
          console.error("Failed to update schema", e);
          throw e;
      }
  };

  const handleExecute = async (page: number = 1, targetCommandId?: string) => {
    if (!selectedNodeId || !canExecute) return;
    setLoading(true);
    
    // Auto-open panel if closed
    if (!isRightPanelOpen) setIsRightPanelOpen(true);

    try {
        const pageSize = selectedNode?.pageSize || 50;
        const result = await api.post(apiConfig, '/execute', {
            sessionId,
            tree,
            targetNodeId: selectedNodeId,
            targetCommandId, // Pass the specific command ID if provided
            page,
            pageSize
        });
        setPreviewData(result);
    } catch (e: any) {
        console.error("Execution failed", e);
        alert(`Execution failed: ${e.message}`);
    } finally {
        setLoading(false);
    }
  };

  const handleAnalyzeOverlap = async (nodeId: string) => {
      try {
          const res = await api.post(apiConfig, '/analyze', {
              sessionId,
              tree,
              parentNodeId: nodeId
          });
          alert(res.report.join('\n'));
      } catch (e: any) {
          alert(`Analysis failed: ${e.message}`);
      }
  };

  const handleExportFull = async () => {
     if (!selectedNodeId) return;
     try {
         await api.export(apiConfig, '/export', {
             sessionId,
             tree,
             targetNodeId: selectedNodeId
         });
     } catch (e) {
         console.error("Export failed", e);
     }
  };

  const handleExportOperations = () => {
      const dataStr = "data:text/json;charset=utf-8," + encodeURIComponent(JSON.stringify(tree, null, 2));
      const downloadAnchorNode = document.createElement('a');
      downloadAnchorNode.setAttribute("href", dataStr);
      downloadAnchorNode.setAttribute("download", `pipeline_${sessionId}.json`);
      document.body.appendChild(downloadAnchorNode);
      downloadAnchorNode.click();
      downloadAnchorNode.remove();
  };

  const handleImportOperations = (file: File) => {
      const reader = new FileReader();
      reader.onload = async (e) => {
          try {
              const text = e.target?.result;
              if (typeof text === 'string') {
                  const importedTree: any = JSON.parse(text);
                  
                  // Validate basic structure
                  if (importedTree && importedTree.id && importedTree.type === 'operation') {
                      setTree(importedTree as OperationNode);
                  } else {
                      alert("Invalid operation file format");
                  }
              }
          } catch (err) {
              alert("Failed to parse JSON file");
          }
      };
      reader.readAsText(file);
  };

  const handleUpdatePageSize = (size: number) => {
      if (!selectedNodeId) return;
      const updateNode = (node: OperationNode): OperationNode => {
          if (node.id === selectedNodeId) return { ...node, pageSize: size };
          if (node.children) return { ...node, children: node.children.map(updateNode) };
          return node;
      };
      setTree(updateNode(tree));
      // Optionally trigger re-run
      setTimeout(() => handleExecute(1), 50);
  };

  // --- LAYOUT ---
  
  const handleMouseDown = (e: React.MouseEvent, type: 'sidebar' | 'right') => {
      e.preventDefault();
      if (type === 'sidebar') setIsResizingSidebar(true);
      else setIsResizingRight(true);
  };

  useEffect(() => {
      const handleMouseMove = (e: MouseEvent) => {
          if (isResizingSidebar) {
              setSidebarWidth(Math.max(200, Math.min(600, e.clientX)));
          }
          if (isResizingRight) {
              if (sessionSettings.panelPosition === 'bottom') {
                   setRightPanelWidth(Math.max(200, Math.min(window.innerHeight - 100, window.innerHeight - e.clientY)));
              } else if (sessionSettings.panelPosition === 'top') {
                   setRightPanelWidth(Math.max(200, Math.min(window.innerHeight - 100, e.clientY - 56)));
              } else if (sessionSettings.panelPosition === 'left') {
                   setRightPanelWidth(Math.max(300, Math.min(window.innerWidth - 300, e.clientX - sidebarWidth)));
              } else {
                   // Right
                   setRightPanelWidth(Math.max(300, Math.min(window.innerWidth - 300, window.innerWidth - e.clientX)));
              }
          }
      };
      const handleMouseUp = () => {
          setIsResizingSidebar(false);
          setIsResizingRight(false);
      };
      if (isResizingSidebar || isResizingRight) {
          document.addEventListener('mousemove', handleMouseMove);
          document.addEventListener('mouseup', handleMouseUp);
      }
      return () => {
          document.removeEventListener('mousemove', handleMouseMove);
          document.removeEventListener('mouseup', handleMouseUp);
      };
  }, [isResizingSidebar, isResizingRight, sessionSettings.panelPosition, sidebarWidth]);

  // Schema for Command Editor auto-complete
  const inputSchema: Record<string, DataType> = useMemo(() => {
      const s: Record<string, DataType> = {};
      datasets.forEach(d => {
          if (d.fieldTypes) {
              Object.entries(d.fieldTypes).forEach(([k, v]) => s[k] = (v as FieldInfo).type);
          } else {
              d.fields.forEach(f => s[f] = 'string');
          }
      });
      return s;
  }, [datasets]);

  const handleOpenTableInSql = (tableName: string) => {
      setTargetSqlTable(tableName);
      setCurrentView('sql');
      if (isMobile) setIsMobileSidebarOpen(false);
  };

  const handleOpenSchema = (tableName: string) => {
      const ds = datasets.find(d => d.name === tableName);
      if (ds) {
          setSchemaModalDataset(ds);
      }
  };

  return (
    <div className="flex flex-col h-screen overflow-hidden bg-gray-50">
      <TopBar 
          sessionId={sessionId}
          sessionName={sessionDisplayName}
          sessions={sessions}
          currentView={currentView}
          apiConfig={apiConfig}
          isRightPanelOpen={isRightPanelOpen}
          onSessionSelect={handleSelectSession}
          onSessionCreate={handleCreateSession}
          onSessionDelete={handleDeleteSession}
          onViewChange={setCurrentView}
          onSettingsOpen={() => setShowSettingsModal(true)}
          onSessionSettingsOpen={() => setShowSessionSettingsModal(true)}
          onExecute={() => handleExecute(1)}
          onToggleRightPanel={() => setIsRightPanelOpen(!isRightPanelOpen)}
          onToggleMobileSidebar={() => setIsMobileSidebarOpen(!isMobileSidebarOpen)}
          canExecute={canExecute}
      />

      <div className="flex flex-1 overflow-hidden relative">
        {/* Mobile Sidebar Overlay */}
        {isMobile && isMobileSidebarOpen && (
            <div className="absolute inset-0 z-20 bg-black/50" onClick={() => setIsMobileSidebarOpen(false)} />
        )}
        
        {/* Sidebar */}
        <div className={`
             ${isMobile ? 'absolute inset-y-0 left-0 z-30 shadow-xl transition-transform duration-300' : 'relative z-10'}
             ${isMobile && !isMobileSidebarOpen ? '-translate-x-full' : 'translate-x-0'}
        `}>
            <Sidebar
              width={isMobile ? 280 : sidebarWidth}
              currentView={currentView}
              sessionId={sessionId}
              tree={tree}
              datasets={datasets}
              selectedNodeId={selectedNodeId}
              onSelectNode={(id) => { setSelectedNodeId(id); if (isMobile) setIsMobileSidebarOpen(false); }}
              onToggleEnabled={handleToggleEnabled}
              onAddChild={handleAddChild}
              onDeleteNode={handleDeleteNode}
              onImportClick={() => setShowImportModal(true)}
              onOpenTableInSql={handleOpenTableInSql}
              onExportOperations={handleExportOperations}
              onImportOperations={handleImportOperations}
              onAnalyzeOverlap={handleAnalyzeOverlap}
              onOpenSchema={handleOpenSchema}
              appearance={appearance}
            />
            {!isMobile && (
                <div 
                    className="absolute top-0 right-0 w-1 h-full cursor-col-resize hover:bg-blue-400 z-20"
                    onMouseDown={(e) => handleMouseDown(e, 'sidebar')}
                />
            )}
        </div>

        {/* Main Workspace */}
        <Workspace 
          currentView={currentView}
          sessionId={sessionId}
          apiConfig={apiConfig}
          targetSqlTable={targetSqlTable}
          onClearTargetSqlTable={() => setTargetSqlTable(null)}
          selectedNode={selectedNode}
          datasets={datasets}
          inputFields={[]}
          inputSchema={inputSchema}
          onUpdateCommands={handleUpdateCommands}
          onUpdateName={handleUpdateName}
          onUpdateType={handleUpdateType}
          onViewPath={() => setShowPathModal(true)}
          isRightPanelOpen={isRightPanelOpen}
          onCloseRightPanel={() => setIsRightPanelOpen(false)}
          rightPanelWidth={rightPanelWidth}
          onRightPanelResizeStart={() => isResizingRight ? null : setIsResizingRight(true)} // Logic handled in effect, just trigger state
          previewData={previewData}
          onClearPreview={() => setPreviewData(null)}
          loading={loading}
          onRefreshPreview={(page, cmdId) => handleExecute(page, cmdId)}
          onUpdatePageSize={handleUpdatePageSize}
          onExportFull={handleExportFull}
          isMobile={isMobile}
          tree={tree}
          panelPosition={sessionSettings.panelPosition}
        />
      </div>

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
        sessionId={sessionId}
        apiConfig={apiConfig}
      />

      <SettingsModal 
        isOpen={showSettingsModal} 
        onClose={() => setShowSettingsModal(false)}
        servers={availableServers}
        currentServer={currentServer}
        onSelectServer={setCurrentServer}
        onAddServer={(url) => setAvailableServers([...availableServers, url])}
        onRemoveServer={(url) => setAvailableServers(availableServers.filter(s => s !== url))}
        appearance={appearance}
        onUpdateAppearance={setAppearance}
      />

      <SessionSettingsModal 
        isOpen={showSessionSettingsModal}
        onClose={() => setShowSessionSettingsModal(false)}
        sessionId={sessionId}
        initialDisplayName={sessionDisplayName}
        initialSettings={sessionSettings}
        onSave={handleSaveSessionSettings}
      />

      <DatasetSchemaModal 
        isOpen={!!schemaModalDataset}
        onClose={() => setSchemaModalDataset(null)}
        dataset={schemaModalDataset}
        onSave={handleUpdateSchema}
      />
    </div>
  );
};

export default App;
