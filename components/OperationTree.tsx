import React from 'react';
import { ChevronRight, ChevronDown, Layers, Trash2, Plus, Power } from 'lucide-react';
import { OperationNode } from '../types';

interface OperationTreeProps {
  node: OperationNode;
  level?: number;
  selectedId: string | null;
  onSelect: (id: string) => void;
  onToggleEnabled: (id: string) => void;
  onAddChild: (parentId: string) => void;
  onDelete: (id: string) => void;
}

export const OperationTree: React.FC<OperationTreeProps> = ({ 
  node, 
  level = 0, 
  selectedId, 
  onSelect, 
  onToggleEnabled,
  onAddChild,
  onDelete
}) => {
  const [expanded, setExpanded] = React.useState(true);
  const isSelected = selectedId === node.id;
  const hasChildren = node.children && node.children.length > 0;
  
  const handleToggle = (e: React.MouseEvent) => {
    e.stopPropagation();
    setExpanded(!expanded);
  };

  return (
    <div className="select-none">
      <div 
        className={`
          flex items-center py-1.5 px-2 cursor-pointer border-l-2 transition-all duration-150 group my-0.5 rounded-r-md
          ${isSelected ? 'bg-blue-50 border-blue-500' : 'border-transparent hover:bg-gray-100'}
          ${!node.enabled ? 'opacity-60 grayscale' : ''}
        `}
        style={{ paddingLeft: `${level * 16 + 8}px` }}
        onClick={() => onSelect(node.id)}
      >
        {/* Expand/Collapse Icon */}
        <div 
            onClick={hasChildren ? handleToggle : undefined} 
            className={`p-0.5 mr-1.5 rounded transition-colors ${hasChildren ? 'hover:bg-gray-200 text-gray-500 cursor-pointer' : 'invisible'}`}
        >
          {expanded ? <ChevronDown className="w-3.5 h-3.5" /> : <ChevronRight className="w-3.5 h-3.5" />}
        </div>

        {/* Node Icon */}
        <Layers className={`w-4 h-4 mr-2 ${isSelected ? 'text-blue-600' : 'text-gray-400'}`} />

        {/* Node Name */}
        <span className={`text-sm flex-grow truncate ${isSelected ? 'font-medium text-blue-900' : 'text-gray-700'}`}>
          {node.name}
        </span>

        {/* Actions (visible on hover or selected) */}
        <div className={`flex items-center space-x-1 ${isSelected || 'group-hover:opacity-100 opacity-0'} transition-opacity`}>
          <button 
            title={node.enabled ? "Disable" : "Enable"}
            onClick={(e) => { e.stopPropagation(); onToggleEnabled(node.id); }}
            className={`p-1 rounded hover:bg-gray-200 ${node.enabled ? 'text-green-600' : 'text-gray-400'}`}
          >
            <Power className="w-3 h-3" />
          </button>
          
          <button 
             title="Add Child Operation"
             onClick={(e) => { e.stopPropagation(); onAddChild(node.id); setExpanded(true); }}
             className="p-1 rounded hover:bg-gray-200 text-gray-500"
          >
            <Plus className="w-3 h-3" />
          </button>

          {node.id !== 'root' && (
            <button 
                title="Delete"
                onClick={(e) => { e.stopPropagation(); onDelete(node.id); }}
                className="p-1 rounded hover:bg-red-100 text-red-500"
            >
                <Trash2 className="w-3 h-3" />
            </button>
          )}
        </div>
      </div>

      {/* Children Recursion */}
      {expanded && hasChildren && (
        <div className="border-l border-gray-200 ml-4 pl-0">
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
                />
            ))}
        </div>
      )}
    </div>
  );
};