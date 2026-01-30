
import React, { useEffect, useState } from 'react';
import { ChevronRight, ChevronDown, Layers, Trash2, Plus, Power, Filter, FunctionSquare, Calculator, Link, Split, AlertTriangle, Braces, Database, ArrowDownAZ } from 'lucide-react';
import { OperationNode } from '../types';

interface OperationTreeProps {
  node: OperationNode;
  level?: number;
  selectedId: string | null;
  onSelect: (id: string) => void;
  onToggleEnabled: (id: string) => void;
  onAddChild: (parentId: string) => void;
  onDelete: (id: string) => void;
  onAnalyzeOverlap?: (id: string) => void;
  expandTrigger?: number;
  collapseTrigger?: number;
}

const getIcon = (node: OperationNode) => {
    if (node.operationType === 'dataset') return Database;
    const firstCmd = node.commands[0];
    if (!firstCmd) return Layers;
    switch (firstCmd.type) {
        case 'filter': return Filter;
        case 'transform': return FunctionSquare;
        // Fix: Changed 'aggregate' to 'group' to match CommandType
        case 'group': return Calculator;
        case 'join': return Link;
        case 'save': return Braces;
        case 'sort': return ArrowDownAZ;
        default: return Layers;
    }
};

export const OperationTree: React.FC<OperationTreeProps> = ({ 
  node, 
  level = 0, 
  selectedId, 
  onSelect, 
  onToggleEnabled,
  onAddChild,
  onDelete,
  onAnalyzeOverlap,
  expandTrigger,
  collapseTrigger
}) => {
  const [expanded, setExpanded] = useState(true);
  const isSelected = selectedId === node.id;
  const hasChildren = node.children && node.children.length > 0;
  
  useEffect(() => { if (expandTrigger && expandTrigger > 0) setExpanded(true); }, [expandTrigger]);
  useEffect(() => { if (collapseTrigger && collapseTrigger > 0) setExpanded(false); }, [collapseTrigger]);

  const handleToggle = (e: React.MouseEvent) => {
    e.stopPropagation();
    setExpanded(!expanded);
  };

  const Icon = getIcon(node);

  return (
    <div className="select-none min-w-max">
      <div 
        className={`
          flex items-center py-0.5 px-2 cursor-pointer border-l-2 transition-all duration-150 group my-0.25 rounded-r-md relative whitespace-nowrap
          ${isSelected ? 'bg-blue-50 border-blue-500' : 'border-transparent hover:bg-gray-50'}
          ${!node.enabled ? 'opacity-50 grayscale' : ''}
        `}
        style={{ paddingLeft: `${level * 12 + 6}px` }}
        onClick={() => onSelect(node.id)}
      >
        <div 
            onClick={hasChildren ? handleToggle : undefined} 
            className={`p-0.5 mr-1 rounded transition-colors ${hasChildren ? 'hover:bg-gray-200 text-gray-400 cursor-pointer' : 'invisible'}`}
        >
          {expanded ? <ChevronDown className="w-3 h-3" /> : <ChevronRight className="w-3 h-3" />}
        </div>

        <Icon className={`w-3 h-3 mr-1.5 ${isSelected ? 'text-blue-600' : 'text-gray-400'}`} />

        <span className={`text-xs flex-grow ${isSelected ? 'font-bold text-blue-900' : 'text-gray-600'}`}>
          {node.name}
        </span>

        <div className={`flex items-center space-x-0.5 ml-2 ${isSelected || 'group-hover:opacity-100 opacity-0'} transition-opacity`}>
          {hasChildren && onAnalyzeOverlap && (
             <button onClick={(e) => { e.stopPropagation(); onAnalyzeOverlap(node.id); }} className="p-1 rounded hover:bg-yellow-100 text-yellow-600" title="Overlap"><Split className="w-2.5 h-2.5" /></button>
          )}
          <button onClick={(e) => { e.stopPropagation(); onToggleEnabled(node.id); }} className={`p-1 rounded hover:bg-gray-200 ${node.enabled ? 'text-green-600' : 'text-gray-400'}`} title="Power"><Power className="w-2.5 h-2.5" /></button>
          <button onClick={(e) => { e.stopPropagation(); onAddChild(node.id); setExpanded(true); }} className="p-1 rounded hover:bg-gray-200 text-gray-500" title="Add Child"><Plus className="w-2.5 h-2.5" /></button>
          {node.id !== 'root' && <button onClick={(e) => { e.stopPropagation(); onDelete(node.id); }} className="p-1 rounded hover:bg-red-100 text-red-500" title="Delete"><Trash2 className="w-2.5 h-2.5" /></button>}
        </div>
      </div>

      {expanded && hasChildren && (
        <div className="border-l border-gray-100 ml-3 pl-0">
            {node.children!.map(child => (
                <OperationTree 
                    key={child.id} 
                    node={child} 
                    level={level + 1}
                    selectedId={selectedId}
                    onSelect={onSelect}
                    onToggleEnabled={onToggleEnabled}
                    onAddChild={onAddChild}
                    onDelete={onDelete}
                    onAnalyzeOverlap={onAnalyzeOverlap}
                    expandTrigger={expandTrigger}
                    collapseTrigger={collapseTrigger}
                />
            ))}
        </div>
      )}
    </div>
  );
};