import React, { useRef, useState } from 'react';
import { ChevronDown, ChevronRight, Plus, Database, Search, Download, Upload, Settings, FoldVertical, UnfoldVertical, Trash2 } from 'lucide-react';
import { OperationTree } from './OperationTree';
import { OperationNode, Dataset, AppearanceConfig } from '../types';

interface SidebarProps {
  width: number;
  currentView: 'workflow' | 'sql' | 'data';
  projectId?: string;
  sessionId?: string;
  tree: OperationNode;
  datasets: Dataset[];
  selectedNodeId: string;
  onSelectNode: (id: string) => void;
  onToggleEnabled: (id: string) => void;
  onAddChild: (parentId: string) => void;
  onDeleteNode: (id: string) => void;
  onMoveNode?: (id: string, direction: 'up' | 'down') => void;
  onImportClick: () => void;
  onOpenTableInSql: (tableName: string) => void;
  onOpenTableInData?: (tableName: string) => void;
  onExportOperations?: () => void;
  onImportOperations?: (file: File) => void;
  onAnalyzeOverlap?: (nodeId: string) => void;
  onOpenSchema?: (name: string) => void;
  onDeleteDataset?: (name: string) => void;
  remoteEditorsByNode?: Record<string, string[]>;
  appearance: AppearanceConfig;
}

export const Sidebar: React.FC<SidebarProps> = ({
  width,
  currentView,
  projectId,
  sessionId,
  tree,
  datasets,
  selectedNodeId,
  onSelectNode,
  onToggleEnabled,
  onAddChild,
  onDeleteNode,
  onMoveNode,
  onImportClick,
  onOpenTableInSql,
  onOpenTableInData,
  onExportOperations,
  onImportOperations,
  onAnalyzeOverlap,
  onOpenSchema,
  onDeleteDataset,
  remoteEditorsByNode = {},
  appearance
}) => {
  const [isOpsExpanded, setIsOpsExpanded] = useState(true);
  const [isDataExpanded, setIsDataExpanded] = useState(true);
  const [expandAllCounter, setExpandAllCounter] = useState(0); // For simple trigger
  const [collapseAllCounter, setCollapseAllCounter] = useState(0);
  const [lastGlobalAction, setLastGlobalAction] = useState<'expand' | 'collapse' | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const activeProjectId = projectId || sessionId || '';
  const canImportDataset = Boolean(activeProjectId);

  const handleOpenDataset = (name: string) => {
      if (currentView === 'data' && onOpenTableInData) {
          onOpenTableInData(name);
          return;
      }
      onOpenTableInSql(name);
  };

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
      if (e.target.files && e.target.files[0] && onImportOperations) {
          onImportOperations(e.target.files[0]);
      }
      if (fileInputRef.current) fileInputRef.current.value = '';
  };

  const handleExpandAll = () => {
      setExpandAllCounter(c => c + 1);
      setLastGlobalAction('expand');
  };

  const handleCollapseAll = () => {
      setCollapseAllCounter(c => c + 1);
      setLastGlobalAction('collapse');
  };

  const isCollapsedMode = currentView === 'workflow' 
      ? (!isOpsExpanded && !isDataExpanded)
      : (!isDataExpanded);

  if (isCollapsedMode) {
      return (
        <aside 
            className="bg-white border-r border-gray-200 flex flex-col z-10 shrink-0 h-full max-h-full overflow-hidden transition-all duration-300"
            style={{ width: 48 }}
        >
            <div className="flex flex-col items-center py-4 space-y-6 h-full bg-gray-50/50">
                {currentView === 'workflow' && (
                    <button 
                        className="flex flex-col items-center justify-center space-y-1 p-2 hover:bg-white hover:shadow-sm rounded-md transition-all group w-10"
                        onClick={() => setIsOpsExpanded(true)}
                        title="Expand Operations"
                    >
                        <span className="text-[9px] font-bold text-gray-400 group-hover:text-blue-600 uppercase tracking-widest" style={{ writingMode: 'vertical-rl', textOrientation: 'mixed', transform: 'rotate(180deg)' }}>OPS</span>
                        <ChevronRight className="w-4 h-4 text-gray-400 group-hover:text-blue-600" />
                    </button>
                )}
                
                <div className="flex-1" />

                <button 
                    className="flex flex-col items-center justify-center space-y-1 p-2 hover:bg-white hover:shadow-sm rounded-md transition-all group w-10"
                    onClick={() => setIsDataExpanded(true)}
                    title="Expand Datasets"
                >
                    <span className="text-[9px] font-bold text-gray-400 group-hover:text-blue-600 uppercase tracking-widest" style={{ writingMode: 'vertical-rl', textOrientation: 'mixed', transform: 'rotate(180deg)' }}>DATA</span>
                    <Database className="w-4 h-4 text-gray-400 group-hover:text-blue-600" />
                </button>
            </div>
        </aside>
      );
  }

  return (
    <aside 
        className="bg-white border-r border-gray-200 flex flex-col z-10 shrink-0 h-full max-h-full overflow-hidden"
        style={{ width: width }}
    >
      {/* Operations Section */}
      {currentView === 'workflow' && (
          <div className="flex flex-col flex-1 min-h-0 overflow-hidden">
             <div className="p-3 bg-gray-50 border-b border-gray-200 flex justify-between items-center select-none shrink-0 h-10">
                <div className="flex items-center space-x-2 cursor-pointer" onClick={() => setIsOpsExpanded(!isOpsExpanded)}>
                    {isOpsExpanded ? <ChevronDown className="w-4 h-4 text-gray-500"/> : <ChevronRight className="w-4 h-4 text-gray-500"/>}
                    <span className="text-xs font-bold text-gray-600 uppercase tracking-wider">Operations</span>
                </div>
                
                <div className="flex items-center space-x-1">
                     {isOpsExpanded && (
                         <div className="flex items-center bg-white border border-gray-200 rounded-md p-0.5 shadow-sm mr-1">
                             <button onClick={handleCollapseAll} className="p-1 hover:bg-gray-100 rounded text-gray-400" title="Collapse All"><FoldVertical className="w-3 h-3" /></button>
                             <button onClick={handleExpandAll} className="p-1 hover:bg-gray-100 rounded text-gray-400" title="Expand All"><UnfoldVertical className="w-3 h-3" /></button>
                         </div>
                     )}
                     {onExportOperations && <button onClick={(e) => { e.stopPropagation(); onExportOperations(); }} className="p-1 hover:bg-gray-200 rounded text-gray-500" title="Export"><Download className="w-3.5 h-3.5" /></button>}
                     
                     <div className="relative">
                        <button onClick={(e) => { e.stopPropagation(); fileInputRef.current?.click(); }} className="p-1 hover:bg-gray-200 rounded text-gray-500" title="Import"><Upload className="w-3.5 h-3.5" /></button>
                        <input type="file" ref={fileInputRef} onChange={handleFileChange} className="hidden" accept=".json" />
                     </div>

                     <button onClick={(e) => { e.stopPropagation(); onAddChild('root'); }} className="p-1 hover:bg-blue-100 rounded text-blue-600" title="Add Data Setup"><Plus className="w-4 h-4" /></button>
                </div>
             </div>
             
             {isOpsExpanded && (
                <div className="flex-1 overflow-y-auto overflow-x-hidden py-2 pr-1 custom-scrollbar">
                    {/* Render children of the root (Setup Nodes) as top-level items */}
                    {tree.children?.map((child, childIndex) => (
                        <OperationTree 
                            key={child.id}
                            node={child} 
                            selectedId={selectedNodeId} 
                            onSelect={onSelectNode} 
                            onToggleEnabled={onToggleEnabled}
                            onAddChild={onAddChild}
                            onDelete={onDeleteNode}
                            onMoveNode={onMoveNode}
                            onAnalyzeOverlap={onAnalyzeOverlap}
                            expandTrigger={expandAllCounter}
                            collapseTrigger={collapseAllCounter}
                            globalAction={lastGlobalAction}
                            appearance={appearance}
                            level={0}
                            parentId="root"
                            index={childIndex}
                            siblingCount={tree.children?.length ?? 0}
                            activeEditorsByNode={remoteEditorsByNode}
                        />
                    ))}
                    {(!tree.children || tree.children.length === 0) && (
                        <div className="text-xs text-gray-400 text-center py-4 italic">No Data Setups. Click + to add one.</div>
                    )}
                    {/* Bottom padding to prevent last item being cut off */}
                    <div className="h-4"></div> 
                </div>
             )}
          </div>
      )}

      {/* Data Sources Section */}
      <div 
        className={`flex flex-col border-t border-gray-200 bg-white shrink-0 transition-all duration-300 ease-in-out ${
            currentView !== 'workflow' ? 'flex-1' : 
            (!isOpsExpanded ? 'flex-1' : 
            (isDataExpanded ? 'h-1/3 min-h-[150px]' : 'h-10'))
        }`}
      >
         <div className="p-3 bg-gray-50 border-b border-gray-200 flex justify-between items-center cursor-pointer hover:bg-gray-100 select-none shrink-0 h-10" onClick={() => setIsDataExpanded(!isDataExpanded)}>
             <div className="flex items-center space-x-2">
                 {isDataExpanded ? <ChevronDown className="w-4 h-4 text-gray-500"/> : <ChevronRight className="w-4 h-4 text-gray-500"/>}
                 <span className="text-xs font-bold text-gray-600 uppercase tracking-wider">Datasets</span>
             </div>
             <button
                 onClick={(e) => {
                     e.stopPropagation();
                     if (!canImportDataset) return;
                     onImportClick();
                 }}
                 className={`p-1 rounded transition-colors ${canImportDataset ? 'hover:bg-blue-100 text-blue-600' : 'text-gray-300 cursor-not-allowed'}`}
                 title={canImportDataset ? "Import Dataset" : "Create a project to import datasets"}
                 disabled={!canImportDataset}
             >
                 <Plus className="w-4 h-4" />
             </button>
         </div>
         
         {isDataExpanded && (
             <div className="flex-1 overflow-y-auto p-2 custom-scrollbar">
                {!activeProjectId ? (
                    <div className="flex flex-col items-center justify-center h-full text-center p-4 text-gray-400 italic text-xs">Create a project to manage data.</div>
                ) : datasets.length === 0 ? (
                    <div className="flex flex-col items-center justify-center h-full text-center p-4 text-gray-400 italic text-xs">No tables found.<br/>Import CSV to start.</div>
                ) : (
                    <div className="space-y-0.5">
                        {datasets.map(ds => (
                            <div key={ds.id} className="flex items-center px-2 py-1 bg-white hover:bg-blue-50 rounded-md text-sm transition-colors group justify-between border border-transparent hover:border-blue-100">
                                <div className="flex items-center min-w-0 cursor-pointer flex-1" onClick={() => handleOpenDataset(ds.name)}>
                                    <Database className="w-3 h-3 text-gray-400 mr-2 shrink-0 group-hover:text-blue-500" />
                                    <div className="font-medium text-gray-700 truncate group-hover:text-blue-700 text-xs" title={ds.name}>{ds.name}</div>
                                    {appearance.showDatasetIds && (
                                        <span className="ml-2 text-[9px] font-mono text-gray-400 bg-gray-50 border border-gray-200 rounded px-1.5 py-0.5 shrink-0">
                                            {ds.id}
                                        </span>
                                    )}
                                </div>
                                <div className="flex items-center opacity-0 group-hover:opacity-100 transition-opacity space-x-1">
                                    <button onClick={(e) => { e.stopPropagation(); handleOpenDataset(ds.name); }} className="p-1 text-gray-300 hover:text-blue-600" title="Query"><Search className="w-3 h-3" /></button>
                                    <button onClick={(e) => { e.stopPropagation(); onOpenSchema && onOpenSchema(ds.name); }} className="p-1 text-gray-300 hover:text-gray-600" title="Settings"><Settings className="w-3 h-3" /></button>
                                    {onDeleteDataset && (
                                        <button
                                            onClick={(e) => { e.stopPropagation(); onDeleteDataset(ds.name); }}
                                            className="p-1 text-gray-300 hover:text-red-500"
                                            title="Delete"
                                        >
                                            <Trash2 className="w-3 h-3" />
                                        </button>
                                    )}
                                </div>
                            </div>
                        ))}
                    </div>
                )}
             </div>
         )}
      </div>
    </aside>
  );
};
