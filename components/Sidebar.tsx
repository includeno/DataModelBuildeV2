
import React, { useRef } from 'react';
import { ChevronDown, ChevronRight, Layers, Plus, Database, Search, Download, Upload } from 'lucide-react';
import { OperationTree } from './OperationTree';
import { OperationNode, Dataset } from '../types';

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
  onImportOperations
}) => {
  const [isOpsExpanded, setIsOpsExpanded] = React.useState(true);
  const [isDataExpanded, setIsDataExpanded] = React.useState(true);
  const fileInputRef = useRef<HTMLInputElement>(null);

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
      if (e.target.files && e.target.files[0] && onImportOperations) {
          onImportOperations(e.target.files[0]);
      }
      // Reset input
      if (fileInputRef.current) fileInputRef.current.value = '';
  };

  return (
    <aside 
        className="bg-white border-r border-gray-200 flex flex-col transition-none z-10 shrink-0"
        style={{ width: width }}
    >
      
      {/* Operations Section (Only in Workflow) */}
      {currentView === 'workflow' && (
          <div className={`flex flex-col transition-all duration-300 ${isOpsExpanded ? 'flex-1 min-h-0' : 'flex-none'}`}>
             <div 
                className="p-3 bg-gray-50 border-b border-gray-200 flex justify-between items-center select-none"
             >
                <div 
                    className="flex items-center space-x-2 cursor-pointer"
                    onClick={() => setIsOpsExpanded(!isOpsExpanded)}
                >
                    {isOpsExpanded ? <ChevronDown className="w-4 h-4 text-gray-500"/> : <ChevronRight className="w-4 h-4 text-gray-500"/>}
                    <span className="text-xs font-semibold text-gray-600 uppercase tracking-wider">Operations</span>
                </div>
                
                <div className="flex items-center space-x-1">
                     {/* Import/Export Buttons */}
                     {onExportOperations && (
                         <button 
                            onClick={(e) => { e.stopPropagation(); onExportOperations(); }}
                            className="p-1 hover:bg-gray-200 rounded text-gray-500" 
                            title="Export Operations"
                         >
                            <Download className="w-3.5 h-3.5" />
                         </button>
                     )}
                     {onImportOperations && (
                         <>
                            <button 
                                onClick={(e) => { e.stopPropagation(); fileInputRef.current?.click(); }}
                                className="p-1 hover:bg-gray-200 rounded text-gray-500" 
                                title="Import Operations"
                            >
                                <Upload className="w-3.5 h-3.5" />
                            </button>
                            <input 
                                type="file" 
                                ref={fileInputRef} 
                                className="hidden" 
                                accept=".json" 
                                onChange={handleFileChange} 
                            />
                         </>
                     )}
                    
                    <div className="h-3 w-px bg-gray-300 mx-1"></div>

                    <button 
                        onClick={(e) => { e.stopPropagation(); onAddChild('root'); }} 
                        className="p-1 hover:bg-gray-200 rounded text-blue-600"
                        title="Add Root Operation"
                    >
                        <Layers className="w-4 h-4" />
                    </button>
                </div>
             </div>
             
             {isOpsExpanded && (
                <div className="flex-1 overflow-y-auto p-2">
                    {tree.children?.map(node => (
                        <OperationTree 
                            key={node.id} 
                            node={node} 
                            selectedId={selectedNodeId} 
                            onSelect={onSelectNode}
                            onToggleEnabled={onToggleEnabled}
                            onAddChild={onAddChild}
                            onDelete={onDeleteNode}
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
                onClick={(e) => { e.stopPropagation(); onImportClick(); }}
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
                                onClick={() => onOpenTableInSql(ds.name)}
                                className="flex items-center px-2 py-1.5 bg-white hover:bg-blue-50 cursor-pointer rounded-md text-sm transition-colors group justify-between"
                            >
                                <div className="flex items-center min-w-0">
                                    <Database className="w-3.5 h-3.5 text-gray-400 mr-2 shrink-0 group-hover:text-blue-500" />
                                    <div className="font-medium text-gray-700 truncate group-hover:text-blue-700" title={ds.name}>{ds.name}</div>
                                    <div className="text-[10px] text-gray-400 ml-2 shrink-0">{ds.totalCount} rows</div>
                                </div>
                                <button 
                                    onClick={(e) => { e.stopPropagation(); onOpenTableInSql(ds.name); }}
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
  );
};
