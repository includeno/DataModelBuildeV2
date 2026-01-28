import React, { useState, useEffect, useRef } from 'react';
import { Layers, Database, Play, Settings, GitBranch, Save, ChevronDown, ChevronRight, Plus, Upload, Trash2, Clock, Check, GripVertical, PanelRight } from 'lucide-react';
import { OperationTree } from './components/OperationTree';
import { CommandEditor } from './components/CommandEditor';
import { DataPreview } from './components/DataPreview';
import { DataImportModal } from './components/DataImport';
import { PathConditionsModal } from './components/PathConditionsModal';
import { Button } from './components/Button';
import { OperationNode, Dataset, ExecutionResult, Command, SessionMetadata } from './types';

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
      name: 'High Value Customers',
      enabled: true,
      commands: [
        { id: 'c1', type: 'filter', config: { field: 'salary', operator: '>', value: '50000', dataType: 'number' }, order: 1 }
      ],
      children: [
         {
             id: 'op_1_1',
             type: 'operation',
             name: 'Engineering Dept',
             enabled: true,
             commands: [
                { id: 'c2', type: 'filter', config: { field: 'department', operator: '=', value: 'Engineering' }, order: 1 }
             ]
         },
         {
            id: 'op_1_2',
            type: 'operation',
            name: 'Advanced Analysis',
            enabled: true,
            commands: [],
         }
      ]
    },
    {
      id: 'op_2',
      type: 'operation',
      name: 'HR Review',
      enabled: false,
      commands: [],
    }
  ]
};

const generateSessionId = () => 'sess_' + Math.floor(Math.random() * 10000).toString().padStart(4, '0');

const App: React.FC = () => {
  // --- STATE ---
  // Session Management
  const [sessions, setSessions] = useState<SessionMetadata[]>([
    { sessionId: generateSessionId(), createdAt: new Date().toISOString() }
  ]);
  const [sessionId, setSessionId] = useState(sessions[0].sessionId);
  const [isSessionMenuOpen, setIsSessionMenuOpen] = useState(false);

  const [datasets, setDatasets] = useState<Dataset[]>([]);
  const [sessionDatasets, setSessionDatasets] = useState<Record<string, Dataset[]>>({});
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

  useEffect(() => {
    const loadSessions = async () => {
      try {
        const response = await fetch('http://localhost:8000/sessions');
        if (!response.ok) {
          throw new Error(`Failed to fetch sessions: ${response.statusText}`);
        }
        const data = await response.json();
        if (data.sessions && data.sessions.length > 0) {
          const loadedSessions = data.sessions.map((s: any) => ({
            sessionId: s.sessionId,
            createdAt: new Date().toISOString()
          }));
          const datasetsMap: Record<string, Dataset[]> = {};
          data.sessions.forEach((s: any) => {
            datasetsMap[s.sessionId] = (s.datasets || []).map((d: any) => ({
              id: d.id,
              name: d.name,
              fields: d.fields || [],
              rows: []
            }));
          });
          setSessions(loadedSessions);
          setSessionDatasets(datasetsMap);
          setSessionId(loadedSessions[0].sessionId);
          setDatasets(datasetsMap[loadedSessions[0].sessionId] || []);
        }
      } catch (err) {
        console.warn("Failed to load sessions from backend:", err);
      }
    };
    loadSessions();
  }, []);

  // --- ACTIONS ---

  // Session Actions
  const handleCreateSession = () => {
      const newId = generateSessionId();
      const newSession: SessionMetadata = { sessionId: newId, createdAt: new Date().toISOString() };
      setSessions(prev => [...prev, newSession]);
      setSessionDatasets(prev => ({ ...prev, [newId]: [] }));
      setDatasets([]);
      setSessionId(newId);
      setIsSessionMenuOpen(false);
  };

  const handleDeleteSession = (e: React.MouseEvent, idToDelete: string) => {
      e.stopPropagation();
      const newSessions = sessions.filter(s => s.sessionId !== idToDelete);
      setSessions(newSessions);
      setSessionDatasets(prev => {
          const updated = { ...prev };
          delete updated[idToDelete];
          return updated;
      });
      
      if (idToDelete === sessionId) {
          if (newSessions.length > 0) {
              const nextSessionId = newSessions[newSessions.length - 1].sessionId;
              setSessionId(nextSessionId);
              setDatasets(sessionDatasets[nextSessionId] || []);
          } else {
              handleCreateSession();
          }
      }
  };

  const handleSelectSession = (id: string) => {
      setSessionId(id);
      setDatasets(sessionDatasets[id] || []);
      setIsSessionMenuOpen(false);
  };

  // Dataset Management
  const handleImport = (dataset: Dataset) => {
    setDatasets(prev => [...prev, dataset]);
    setSessionDatasets(prev => ({
      ...prev,
      [sessionId]: [...(prev[sessionId] || []), dataset]
    }));
    setShowImportModal(false);
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
        const response = await fetch('http://localhost:8000/execute', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                tree: tree,
                targetNodeId: selectedNodeId,
                sessionId: sessionId
            })
        });

        if (!response.ok) {
            throw new Error(`Execution failed: ${response.statusText}`);
        }

        const result: ExecutionResult = await response.json();
        setPreviewData(result);
        
        if (!isRightPanelOpen) setIsRightPanelOpen(true);
    } catch (err) {
        console.error("Execution error:", err);
        alert("Failed to execute operation. Please ensure the backend is running on port 8000.");
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

  const selectedNode = selectedNodeId === 'root' ? tree : (tree.children ? findNode(tree.children, selectedNodeId) : null);

  return (
    <div className="flex flex-col h-screen bg-gray-100 overflow-hidden text-gray-800">
      <DataImportModal
        isOpen={showImportModal}
        onClose={() => setShowImportModal(false)}
        onImport={handleImport}
        sessionId={sessionId}
      />
      
      <PathConditionsModal 
          isOpen={showPathModal}
          onClose={() => setShowPathModal(false)}
          tree={tree}
          targetNodeId={selectedNodeId}
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
                  className="flex items-center justify-between space-x-2 bg-white border border-gray-300 hover:border-blue-400 hover:bg-gray-50 text-gray-900 px-3 py-1.5 rounded-md shadow-sm transition-all text-sm min-w-[180px]"
              >
                  <div className="flex items-center overflow-hidden">
                      <span className="text-gray-400 mr-2 text-xs uppercase font-semibold">Session</span>
                      <span className="font-medium truncate">{sessionId}</span>
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

        <div className="flex items-center space-x-3">
            <div className="text-xs text-gray-500 mr-2 hidden sm:block">
                {datasets.length} Datasets Loaded
            </div>
            <Button variant="secondary" size="sm" icon={<Save className="w-4 h-4" />}>
                Save
            </Button>
            <Button variant="primary" size="sm" icon={<Play className="w-4 h-4" />} onClick={executeOperation}>
                Run Analysis
            </Button>
            
            <div className="h-6 w-px bg-gray-300 mx-2 hidden sm:block" />
            
            <button
                onClick={() => setIsRightPanelOpen(!isRightPanelOpen)}
                className={`p-2 rounded-md transition-colors ${isRightPanelOpen ? 'bg-blue-100 text-blue-700' : 'text-gray-500 hover:bg-gray-100'}`}
                title={isRightPanelOpen ? "Hide Preview" : "Show Preview"}
            >
                <PanelRight className="w-5 h-5" />
            </button>
        </div>
      </header>

      {/* 2. MAIN LAYOUT */}
      <div className="flex flex-1 overflow-hidden">
        
        {/* LEFT SIDEBAR (RESIZABLE) */}
        <aside 
            className="bg-white border-r border-gray-200 flex flex-col transition-none z-10 shrink-0"
            style={{ width: sidebarWidth }}
        >
          
          {/* Operations Section */}
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

          {/* Data Sources Section */}
          <div className={`flex flex-col border-t border-gray-200 transition-all duration-300 ${!isOpsExpanded ? 'flex-1' : (isDataExpanded ? 'h-1/3' : 'flex-none')}`}>
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
                 >
                    <Plus className="w-4 h-4" />
                 </button>
             </div>
             
             {isDataExpanded && (
                 <div className="flex-1 overflow-y-auto p-2 bg-white">
                    {datasets.length === 0 ? (
                        <div className="flex flex-col items-center justify-center h-full text-center p-4 text-gray-400 italic text-xs">
                           Empty
                        </div>
                    ) : (
                        <div className="space-y-1">
                            {datasets.map(ds => (
                                <div key={ds.id} className="flex items-center px-2 py-1.5 bg-white hover:bg-gray-100 rounded-md text-sm transition-colors cursor-pointer group">
                                    <Database className="w-3.5 h-3.5 text-gray-400 mr-2" />
                                    <div className="flex-1 min-w-0">
                                        <div className="font-medium text-gray-700 truncate">{ds.name}</div>
                                    </div>
                                    <div className="text-[10px] text-gray-400 ml-2">{ds.rows.length} rows</div>
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
            
            {/* MIDDLE: COMMAND EDITOR (FLEX-1) */}
            <div className="flex-1 flex flex-col bg-gray-50/50 min-w-[300px]">
                {selectedNode && selectedNode.id !== 'root' ? (
                    <CommandEditor 
                        operationId={selectedNode.id}
                        operationName={selectedNode.name}
                        commands={selectedNode.commands} 
                        datasets={datasets}
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
        </main>

      </div>
    </div>
  );
};

export default App;
