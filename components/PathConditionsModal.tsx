
import React, { useState } from 'react';
import { X, ArrowDown, Filter, GitCommit, Route, Link, ArrowDownAZ, FunctionSquare, Calculator, Sparkles, Database, Loader2 } from 'lucide-react';
import { OperationNode, Command, ApiConfig } from '../types';
import { api } from '../utils/api';

interface PathConditionsModalProps {
  isOpen: boolean;
  onClose: () => void;
  tree: OperationNode;
  targetNodeId: string;
  targetCommandId?: string;
  sessionId: string;
  apiConfig: ApiConfig;
}

export const PathConditionsModal: React.FC<PathConditionsModalProps> = ({ isOpen, onClose, tree, targetNodeId, targetCommandId, sessionId, apiConfig }) => {
  const [counts, setCounts] = useState<Record<string, number | null>>({});
  const [loadingCounts, setLoadingCounts] = useState<Record<string, boolean>>({});

  if (!isOpen) return null;

  const findPath = (node: OperationNode, targetId: string, path: OperationNode[]): OperationNode[] | null => {
    const newPath = [...path, node];
    if (node.id === targetId) return newPath;
    if (node.children) {
      for (const child of node.children) {
        const result = findPath(child, targetId, newPath);
        if (result) return result;
      }
    }
    return null;
  };

  const path = findPath(tree, targetNodeId, []) || [];
  // Filter out the root node from the display
  const displayPath = path.filter(node => node.id !== 'root');

  const handleCheckCount = async (nodeId: string) => {
    setLoadingCounts(prev => ({ ...prev, [nodeId]: true }));
    try {
        const res = await api.post(apiConfig, '/execute', {
            sessionId: sessionId,
            tree: tree, 
            targetNodeId: nodeId, 
            page: 1,
            pageSize: 1
        });
        setCounts(prev => ({ ...prev, [nodeId]: res.totalCount }));
    } catch (e: any) {
        console.error("Failed to get count", e);
        alert("Failed to calculate count: " + e.message);
    } finally {
        setLoadingCounts(prev => ({ ...prev, [nodeId]: false }));
    }
  };

  const stringifyFilterGroup = (group: any): React.ReactNode => {
      if (!group || !group.conditions || group.conditions.length === 0) return <span className="italic text-gray-400">Empty Filter</span>;
      
      return (
          <span className="flex items-center flex-wrap gap-1">
              {group.conditions.map((c: any, i: number) => (
                  <React.Fragment key={i}>
                      {i > 0 && <span className="text-blue-500 font-bold text-[10px] uppercase">{group.logicalOperator}</span>}
                      {c.type === 'group' ? (
                          <span className="bg-gray-100 rounded px-1 border border-gray-200">({stringifyFilterGroup(c)})</span>
                      ) : (
                          <span className="flex items-center gap-1 bg-white px-1 rounded border border-gray-200">
                              <span className="font-semibold text-gray-600">{c.field}</span>
                              <span className="text-blue-600 font-bold text-[10px]">{c.operator}</span>
                              <span className="text-gray-900">{String(c.value)}</span>
                          </span>
                      )}
                  </React.Fragment>
              ))}
          </span>
      );
  };

  const renderCommand = (cmd: Command, context: string) => {
      // Determine the specific table/context this command is acting on
      let specificContext = "";
      
      // If dataSource is set, it overrides everything.
      // If it is empty string, it means "None" (Invalid/Missing).
      if (cmd.config.dataSource !== undefined) {
           if (cmd.config.dataSource === '') specificContext = "None";
           else specificContext = cmd.config.dataSource;
      } else {
           // Legacy fallback for source commands
           specificContext = cmd.config.mainTable || context;
      }

      // Display Label Logic
      let contextLabel = specificContext;
      if (specificContext === 'stream') contextLabel = 'Stream'; 
      if (specificContext === '' || specificContext === 'None') contextLabel = 'Missing Source';

      const contextClass = (specificContext === '' || specificContext === 'None') ? "text-red-500 font-bold" : "text-gray-400";

      switch (cmd.type) {
          case 'filter':
              if (cmd.config.filterRoot) {
                   return (
                      <div key={cmd.id} className="flex items-center text-sm bg-gray-50 border border-gray-100 rounded px-3 py-2 text-gray-700">
                          <Filter className="w-3.5 h-3.5 text-gray-400 mr-2.5 shrink-0" />
                          <span className="font-mono text-xs truncate flex items-center flex-wrap gap-1">
                              <span className={`${contextClass} text-[10px] mr-1`}>[{contextLabel}]</span>
                              {stringifyFilterGroup(cmd.config.filterRoot)}
                          </span>
                      </div>
                   );
              }
              return (
                  <div key={cmd.id} className="flex items-center text-sm bg-gray-50 border border-gray-100 rounded px-3 py-2 text-gray-700">
                      <Filter className="w-3.5 h-3.5 text-gray-400 mr-2.5 shrink-0" />
                      <span className="font-mono text-xs truncate flex items-center flex-wrap gap-1">
                          <span className={`${contextClass} text-[10px] mr-1`}>[{contextLabel}]</span>
                          <span className="font-semibold text-gray-600">{cmd.config.field || '...'}</span>
                          <span className="text-blue-500 font-bold">{cmd.config.operator}</span>
                          <span className="font-semibold text-gray-900 bg-white px-1.5 py-0.5 rounded border border-gray-200">
                              {typeof cmd.config.value === 'object' ? JSON.stringify(cmd.config.value) : cmd.config.value}
                          </span>
                      </span>
                  </div>
              );
          case 'join':
              return (
                   <div key={cmd.id} className="flex items-center text-sm bg-purple-50 border border-purple-100 rounded px-3 py-2 text-purple-900">
                      <Link className="w-3.5 h-3.5 text-purple-400 mr-2.5 shrink-0" />
                      <span className="font-mono text-xs truncate flex items-center flex-wrap gap-1">
                          <span className={`${contextClass} text-[10px] mr-1`}>[{contextLabel}]</span>
                          <span className="font-bold">{cmd.config.joinType?.toUpperCase() || 'LEFT'} JOIN</span>
                          <span className="bg-white px-1 rounded border border-purple-100">{cmd.config.joinTable}</span>
                          <span className="text-purple-600">ON</span>
                          <span className="italic text-gray-600">{cmd.config.on}</span>
                      </span>
                  </div>
              );
          case 'transform':
               return (
                   <div key={cmd.id} className="flex items-center text-sm bg-indigo-50 border border-indigo-100 rounded px-3 py-2 text-indigo-900">
                      <FunctionSquare className="w-3.5 h-3.5 text-indigo-400 mr-2.5 shrink-0" />
                      <span className="font-mono text-xs truncate flex items-center flex-wrap gap-1">
                          <span className={`${contextClass} text-[10px] mr-1`}>[{contextLabel}]</span>
                          <span className="text-gray-500">Set</span>
                          <span className="font-bold text-indigo-700">{cmd.config.outputField || 'new_column'}</span>
                          <span className="text-gray-400">=</span>
                          <code className="bg-white px-1.5 py-0.5 rounded border border-indigo-100 text-gray-600 font-mono" title={cmd.config.expression}>
                              {cmd.config.expression || 'expression...'}
                          </code>
                      </span>
                  </div>
              );
          case 'sort':
               return (
                   <div key={cmd.id} className="flex items-center text-sm bg-yellow-50 border border-yellow-100 rounded px-3 py-2 text-yellow-900">
                      <ArrowDownAZ className="w-3.5 h-3.5 text-yellow-500 mr-2.5 shrink-0" />
                      <span className="font-mono text-xs truncate flex items-center gap-1">
                          <span className={`${contextClass} text-[10px] mr-1`}>[{contextLabel}]</span>
                          <span>Sort by</span>
                          <span className="font-bold">{cmd.config.field}</span>
                          <span className="text-gray-500 bg-white px-1 rounded border border-yellow-100">
                              {cmd.config.ascending === false ? 'DESC' : 'ASC'}
                          </span>
                      </span>
                  </div>
              );
           case 'group':
               return (
                   <div key={cmd.id} className="flex items-center text-sm bg-orange-50 border border-orange-100 rounded px-3 py-2 text-orange-900">
                      <Calculator className="w-3.5 h-3.5 text-orange-400 mr-2.5 shrink-0" />
                      <span className="font-mono text-xs truncate flex items-center gap-1">
                          <span className={`${contextClass} text-[10px] mr-1`}>[{contextLabel}]</span>
                          <span className="font-bold uppercase">{cmd.config.aggFunc}</span>
                          <span>of</span>
                          <span className="font-semibold">{cmd.config.field}</span>
                          <span className="text-gray-500">by</span>
                          <span className="bg-white px-1 rounded border border-orange-100">
                              {Array.isArray(cmd.config.groupBy) ? cmd.config.groupBy.join(', ') : cmd.config.groupBy}
                          </span>
                      </span>
                  </div>
              );
          case 'source':
              return (
                  <div key={cmd.id} className="flex items-center text-sm bg-emerald-50 border border-emerald-100 rounded px-3 py-2 text-emerald-900">
                       <Database className="w-3.5 h-3.5 text-emerald-500 mr-2.5 shrink-0" />
                       <span className="font-mono text-xs flex items-center space-x-2">
                           <span>Load Table:</span>
                           <span className="font-bold bg-white px-1 rounded border border-emerald-200">{cmd.config.mainTable}</span>
                           <span className="text-emerald-400">&rarr;</span>
                           <span className="font-bold text-emerald-700">{cmd.config.alias || 'Default'}</span>
                       </span>
                  </div>
              );
          default:
              return (
                  <div key={cmd.id} className="flex items-center text-sm bg-gray-50 border border-gray-100 rounded px-3 py-2 text-gray-500">
                       <Sparkles className="w-3.5 h-3.5 text-gray-400 mr-2.5 shrink-0" />
                       <span className="text-xs font-mono truncate">{JSON.stringify(cmd.config)}</span>
                  </div>
              );
      }
  };

  // Variable to track the active source table as we traverse the path
  let flowContext = "Input Stream";

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm p-4">
        <div className="bg-white rounded-xl shadow-2xl w-full max-w-2xl max-h-[85vh] flex flex-col animate-in fade-in zoom-in duration-200">
            {/* Header */}
            <div className="flex items-center justify-between px-6 py-4 border-b border-gray-100 shrink-0">
                <div className="flex items-center space-x-3">
                    <div className="p-2 bg-blue-50 rounded-lg text-blue-600">
                        <Route className="w-5 h-5" />
                    </div>
                    <div>
                        <h3 className="text-lg font-bold text-gray-900">Path Logic Synthesis</h3>
                        <p className="text-xs text-gray-500">Combined conditions from root to current operation</p>
                    </div>
                </div>
                <button onClick={onClose} className="text-gray-400 hover:text-gray-600 transition-colors">
                    <X className="w-5 h-5" />
                </button>
            </div>

            {/* Content */}
            <div className="flex-1 overflow-y-auto p-6 bg-gray-50/30">
                {displayPath.length === 0 ? (
                    <div className="text-center text-gray-500 py-8">Node not found or is root.</div>
                ) : (
                    <div className="relative pl-2">
                         {displayPath.map((node, index) => {
                             const isLast = index === displayPath.length - 1;
                             const count = counts[node.id];
                             const isLoading = loadingCounts[node.id];

                             return (
                             <div key={node.id} className="flex group mb-0 relative pb-8 last:pb-0">
                                {/* Connector Line */}
                                {!isLast && (
                                    <div className="absolute left-[15px] top-8 bottom-0 w-0.5 bg-gray-200" />
                                )}

                                {/* Icon */}
                                <div className={`shrink-0 w-8 h-8 rounded-full flex items-center justify-center border-2 z-10 shadow-sm transition-colors ${
                                    isLast 
                                    ? 'bg-blue-600 border-blue-600 text-white ring-4 ring-blue-50' 
                                    : 'bg-white border-gray-200 text-gray-400'
                                }`}>
                                    {index === 0 ? <GitCommit className="w-4 h-4" /> : <ArrowDown className="w-4 h-4" />}
                                </div>

                                {/* Card */}
                                <div className={`ml-6 flex-1 bg-white rounded-lg border ${isLast ? 'border-blue-200 shadow-md' : 'border-gray-200 shadow-sm'}`}>
                                    <div className={`px-4 py-3 border-b flex justify-between items-center ${isLast ? 'border-blue-100 bg-blue-50/30' : 'border-gray-100'}`}>
                                        <div className="flex flex-col">
                                            <div className="flex items-center space-x-2">
                                                <span className={`font-semibold text-sm ${isLast ? 'text-blue-800' : 'text-gray-800'}`}>
                                                    {node.name}
                                                </span>
                                                {isLast && <span className="text-[10px] font-bold bg-blue-100 text-blue-700 px-2 py-0.5 rounded-full uppercase">Current</span>}
                                            </div>
                                        </div>

                                        {/* Count Button/Display */}
                                        <div className="flex items-center">
                                            {count !== undefined && count !== null ? (
                                                <span className="text-xs font-mono font-medium text-gray-600 bg-gray-100 px-2 py-1 rounded border border-gray-200">
                                                    {count.toLocaleString()} rows
                                                </span>
                                            ) : (
                                                <button 
                                                    onClick={() => handleCheckCount(node.id)}
                                                    disabled={isLoading}
                                                    className="flex items-center space-x-1 text-[10px] font-medium text-blue-600 hover:bg-blue-50 px-2 py-1 rounded border border-blue-100 transition-colors"
                                                    title="Calculate Output Rows at this step"
                                                >
                                                    {isLoading ? <Loader2 className="w-3 h-3 animate-spin" /> : <Calculator className="w-3 h-3" />}
                                                    <span>{isLoading ? '...' : 'Count'}</span>
                                                </button>
                                            )}
                                        </div>
                                    </div>
                                    
                                    <div className="p-4 space-y-2">
                                        {node.commands.length === 0 ? (
                                            <div className="text-xs text-gray-400 italic pl-1">No command</div>
                                        ) : (
                                            (() => {
                                                let commandsToShow = node.commands;
                                                if (node.id === targetNodeId && targetCommandId) {
                                                    const cmdIndex = node.commands.findIndex(c => c.id === targetCommandId);
                                                    if (cmdIndex !== -1) {
                                                        commandsToShow = node.commands.slice(0, cmdIndex + 1);
                                                    }
                                                }

                                                return commandsToShow.map(cmd => {
                                                    // Update context based on command type logic
                                                    if (cmd.type === 'source' && cmd.config.alias) {
                                                        flowContext = cmd.config.alias;
                                                    } else if (cmd.config.dataSource && cmd.config.dataSource !== '') {
                                                        flowContext = cmd.config.dataSource;
                                                    }
                                                    
                                                    return renderCommand(cmd, flowContext);
                                                });
                                            })()
                                        )}
                                    </div>
                                </div>
                             </div>
                             );
                         })}
                    </div>
                )}
            </div>
            
            <div className="p-4 border-t border-gray-100 bg-white rounded-b-xl flex justify-between items-center">
                <div className="text-xs text-gray-400">
                    Showing {displayPath.length} steps in execution path
                </div>
                <button 
                    onClick={onClose}
                    className="px-4 py-2 bg-gray-100 text-gray-700 hover:bg-gray-200 border border-gray-200 rounded-lg text-sm font-medium transition-colors"
                >
                    Close
                </button>
            </div>
        </div>
    </div>
  );
};
