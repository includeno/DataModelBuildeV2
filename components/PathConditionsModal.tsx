import React from 'react';
import { X, ArrowDown, Filter, GitCommit, ListFilter } from 'lucide-react';
import { OperationNode, Command } from '../types';

interface PathConditionsModalProps {
  isOpen: boolean;
  onClose: () => void;
  tree: OperationNode;
  targetNodeId: string;
}

export const PathConditionsModal: React.FC<PathConditionsModalProps> = ({ isOpen, onClose, tree, targetNodeId }) => {
  if (!isOpen) return null;

  // Helper to find path from root to targetId
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

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm p-4">
        <div className="bg-white rounded-xl shadow-2xl w-full max-w-2xl max-h-[85vh] flex flex-col animate-in fade-in zoom-in duration-200">
            {/* Header */}
            <div className="flex items-center justify-between px-6 py-4 border-b border-gray-100 shrink-0">
                <div className="flex items-center space-x-3">
                    <div className="p-2 bg-blue-50 rounded-lg text-blue-600">
                        <ListFilter className="w-5 h-5" />
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
                {path.length === 0 ? (
                    <div className="text-center text-gray-500 py-8">Node not found in current tree.</div>
                ) : (
                    <div className="relative pl-2">
                         {path.map((node, index) => {
                             const isLast = index === path.length - 1;
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
                                    <div className={`px-4 py-3 border-b ${isLast ? 'border-blue-100 bg-blue-50/30' : 'border-gray-100'}`}>
                                        <div className="flex items-center justify-between">
                                            <span className={`font-semibold text-sm ${isLast ? 'text-blue-800' : 'text-gray-800'}`}>
                                                {node.name}
                                            </span>
                                            {isLast && <span className="text-[10px] font-bold bg-blue-100 text-blue-700 px-2 py-0.5 rounded-full uppercase">Current</span>}
                                        </div>
                                    </div>
                                    
                                    <div className="p-4 space-y-2">
                                        {node.commands.length === 0 ? (
                                            <div className="text-xs text-gray-400 italic pl-1">No filters applied (Pass-through)</div>
                                        ) : (
                                            node.commands.map(cmd => (
                                                <div key={cmd.id} className="flex items-center text-sm bg-gray-50 border border-gray-100 rounded px-3 py-2 text-gray-700">
                                                    <Filter className="w-3.5 h-3.5 text-gray-400 mr-2.5 shrink-0" />
                                                    <span className="font-mono text-xs truncate flex items-center flex-wrap gap-1">
                                                        <span className="font-semibold text-gray-600">{cmd.config.field || '...'}</span>
                                                        <span className="text-blue-500 font-bold">{cmd.config.operator}</span>
                                                        <span className="font-semibold text-gray-900 bg-white px-1.5 py-0.5 rounded border border-gray-200">
                                                            {typeof cmd.config.value === 'object' ? JSON.stringify(cmd.config.value) : cmd.config.value}
                                                        </span>
                                                    </span>
                                                </div>
                                            ))
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
                    Showing {path.length} steps in execution path
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