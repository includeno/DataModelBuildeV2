
import React, { useRef, useState } from 'react';
import { ChevronDown, ChevronRight, Layers, Plus, Database, Search, Download, Upload, Settings, FoldVertical, UnfoldVertical } from 'lucide-react';
import { OperationTree } from './OperationTree';
import { OperationNode, Dataset, AppearanceConfig } from '../types';

interface SidebarProps {
  width: number;
  currentView: 'workflow' | 'sql';
  sessionId: string;
  tree: OperationNode;
  datasets: Dataset[];
  selectedNodeId: string;
  onSelectNode: (id: string) => void;
  onToggleEnabled: (id: string) => void;
  onAddChild: (parentId: string) => void;
  onDeleteNode: (id: string) => void;
  onImportClick: () => void;
  onOpenTableInSql: (tableName: string) => void;
  onExportOperations?: () => void;
  onImportOperations?: (file: File) => void;
  onAnalyzeOverlap?: (nodeId: string) => void;
  onOpenSchema?: (name: string) => void;
  appearance: AppearanceConfig;
}

export const Sidebar: React.FC<SidebarProps> = ({
  width,
  currentView,
  sessionId,
  tree,
  datasets,
  selectedNodeId,
  onSelectNode,
  onToggleEnabled,
  onAddChild,
  onDeleteNode,
  onImportClick,
  onOpenTableInSql,
  onExportOperations,
  onImportOperations,
  onAnalyzeOverlap,
  onOpenSchema,
  appearance
}) => {
  const [isOpsExpanded, setIsOpsExpanded] = useState(true);
  const [isDataExpanded, setIsDataExpanded] = useState(true);
  const [expandAllCounter, setExpandAllCounter] = useState(0); // For simple trigger
  const [collapseAllCounter, setCollapseAllCounter] = useState(0);
  const [lastGlobalAction, setLastGlobalAction] = useState<'expand' | 'collapse' | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);

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

  return (
    <aside 
        className="bg-white border-r border-gray-200 flex flex-col z-10 shrink-0 h-full"
        style={{ width: width }}
    >
      {/* Operations Section */}
      {currentView === 'workflow' && (
          <div className={`flex flex-col ${isOpsExpanded ? 'flex-1 min-h-0' : 'flex-none'}`}>
             <div className="p-3 bg-gray-50 border-b border-gray-200 flex justify-between items-center select-none shrink-0">
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

                     <button onClick={(e) => { e.stopPropagation(); onAddChild('root'); }} className="p-1 hover:bg-blue-100 rounded text-blue-600" title="Add Setup Node"><Plus className="w-4 h-4" /></button>
                </div>
             </div>
             
             {isOpsExpanded && (
                <div className="flex-1 overflow-auto py-2">
                    {tree.children?.map(node => (
                        <OperationTree 
                            key={node.id} 
                            node={node} 
                            selectedId={selectedNodeId} 
                            onSelect={onSelectNode}
                            onToggleEnabled={onToggleEnabled}
                            onAddChild={onAddChild}
                            onDelete={onDeleteNode}
                            onAnalyzeOverlap={onAnalyzeOverlap}
                            expandTrigger={expandAllCounter}
                            collapseTrigger={collapseAllCounter}
                            globalAction={lastGlobalAction}
                            appearance={appearance}
                        />
                    ))}
                    {(!tree.children || tree.children.length === 0) && (
                        <div className="text-center mt-10 text-gray-400 text-sm p-4 italic">
                            No operations yet.<br/>
                            Click <Plus className="w-3 h-3 inline text-blue-500"/> to add a Setup node.
                        </div>
                    )}
                </div>
             )}
          </div>
      )}

      {/* Data Sources Section */}
      <div className={`flex flex-col border-t border-gray-200 bg-white ${currentView === 'sql' ? 'flex-1' : (!isOpsExpanded ? 'flex-1' : (isDataExpanded ? 'min-h-[160px] h-1/4 max-h-[300px]' : 'flex-none'))}`}>
         <div className="p-3 bg-gray-50 border-b border-gray-200 flex justify-between items-center cursor-pointer hover:bg-gray-100 select-none shrink-0" onClick={() => setIsDataExpanded(!isDataExpanded)}>
             <div className="flex items-center space-x-2">
                 {isDataExpanded ? <ChevronDown className="w-4 h-4 text-gray-500"/> : <ChevronRight className="w-4 h-4 text-gray-500"/>}
                 <span className="text-xs font-bold text-gray-600 uppercase tracking-wider">Datasets</span>
             </div>
             <button onClick={(e) => { e.stopPropagation(); onImportClick(); }} className="p-1 hover:bg-blue-100 rounded text-blue-600 transition-colors" title="Import Dataset"><Plus className="w-4 h-4" /></button>
         </div>
         
         {isDataExpanded && (
             <div className="flex-1 overflow-y-auto p-2">
                {!sessionId ? (
                    <div className="flex flex-col items-center justify-center h-full text-center p-4 text-gray-400 italic text-xs">Create a session to manage data.</div>
                ) : datasets.length === 0 ? (
                    <div className="flex flex-col items-center justify-center h-full text-center p-4 text-gray-400 italic text-xs">No tables found.<br/>Import CSV to start.</div>
                ) : (
                    <div className="space-y-0.5">
                        {datasets.map(ds => (
                            <div key={ds.id} className="flex items-center px-2 py-1 bg-white hover:bg-blue-50 rounded-md text-sm transition-colors group justify-between border border-transparent hover:border-blue-100">
                                <div className="flex items-center min-w-0 cursor-pointer flex-1" onClick={() => onOpenTableInSql(ds.name)}>
                                    <Database className="w-3 h-3 text-gray-400 mr-2 shrink-0 group-hover:text-blue-500" />
                                    <div className="font-medium text-gray-700 truncate group-hover:text-blue-700 text-xs" title={ds.name}>{ds.name}</div>
                                </div>
                                <div className="flex items-center opacity-0 group-hover:opacity-100 transition-opacity space-x-1">
                                    <button onClick={(e) => { e.stopPropagation(); onOpenTableInSql(ds.name); }} className="p-1 text-gray-300 hover:text-blue-600" title="Query"><Search className="w-3 h-3" /></button>
                                    <button onClick={(e) => { e.stopPropagation(); onOpenSchema && onOpenSchema(ds.name); }} className="p-1 text-gray-300 hover:text-gray-600" title="Settings"><Settings className="w-3 h-3" /></button>
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
