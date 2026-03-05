import React, { useState, useEffect } from 'react';
import { ChevronRight, ChevronDown, ChevronUp, FileText, Database, Split, Power, Plus, Trash2, Layers } from 'lucide-react';
import { OperationNode, AppearanceConfig } from '../types';

interface OperationTreeProps {
  node: OperationNode;
  selectedId: string;
  onSelect: (id: string) => void;
  onToggleEnabled: (id: string) => void;
  onAddChild: (parentId: string) => void;
  onDelete: (id: string) => void;
  onMoveNode?: (id: string, direction: 'up' | 'down') => void;
  onAnalyzeOverlap?: (id: string) => void;
  expandTrigger?: number;
  collapseTrigger?: number;
  globalAction?: 'expand' | 'collapse' | null;
  appearance: AppearanceConfig;
  level?: number;
  parentId?: string | null;
  index?: number;
  siblingCount?: number;
}

export const OperationTree: React.FC<OperationTreeProps> = ({
  node,
  selectedId,
  onSelect,
  onToggleEnabled,
  onAddChild,
  onDelete,
  onMoveNode,
  onAnalyzeOverlap,
  expandTrigger,
  collapseTrigger,
  globalAction,
  appearance,
  level = 0,
  parentId = null,
  index = 0,
  siblingCount = 0
}) => {
  const [expanded, setExpanded] = useState(true);
  
  useEffect(() => {
      if (globalAction === 'expand') setExpanded(true);
      if (globalAction === 'collapse') setExpanded(false);
  }, [expandTrigger, collapseTrigger, globalAction]);

  const hasChildren = node.children && node.children.length > 0;
  const isSelected = node.id === selectedId;
  const canMoveUp = Boolean(parentId) && index > 0;
  const canMoveDown = Boolean(parentId) && index < siblingCount - 1;

  // Determine Icon based on type
  const getIcon = () => {
      if (node.operationType === 'setup') return Database;
      if (node.operationType === 'dataset') return FileText;
      return Layers;
  };
  
  const Icon = getIcon();

  return (
    <div className="flex flex-col select-none">
      <div 
        className={`flex items-center py-1 px-2 cursor-pointer border-l-2 transition-colors group relative ${
            isSelected 
            ? 'bg-blue-50 border-blue-500 text-blue-900' 
            : 'border-transparent hover:bg-gray-100 text-gray-700'
        } ${!node.enabled ? 'opacity-60' : ''}`}
        style={{ 
            paddingLeft: `${level * 12 + 8}px`,
            fontSize: `${appearance.textSize}px`,
            color: isSelected ? undefined : appearance.textColor
        }}
        onClick={(e) => { e.stopPropagation(); onSelect(node.id); }}
      >
        {/* Guide Lines */}
        {appearance.showGuideLines && level > 0 && (
            <div 
                className="absolute left-0 top-0 bottom-0 w-px"
                style={{ left: `${level * 12}px`, backgroundColor: appearance.guideLineColor }}
            />
        )}

        <div className="flex items-center justify-center w-4 h-4 mr-1 shrink-0" onClick={(e) => {
            if (hasChildren) {
                e.stopPropagation();
                setExpanded(!expanded);
            }
        }}>
           {hasChildren && (
               expanded ? <ChevronDown className="w-3 h-3 text-gray-400" /> : <ChevronRight className="w-3 h-3 text-gray-400" />
           )}
        </div>

        <Icon className={`w-3.5 h-3.5 mr-2 shrink-0 ${isSelected ? 'text-blue-600' : (node.operationType === 'setup' ? 'text-purple-500' : 'text-gray-500')}`} />
        
        <span className={`truncate font-medium flex-1 ${!node.enabled ? 'line-through text-gray-400' : ''}`}>
            {node.name}
        </span>
        {appearance.showNodeIds && (
            <span className="ml-2 text-[9px] font-mono text-gray-400 bg-gray-50 border border-gray-200 rounded px-1.5 py-0.5 shrink-0">
                {node.id}
            </span>
        )}
        
        {/* Actions */}
        <div className={`flex items-center space-x-0.5 ml-auto pl-2 bg-gradient-to-l from-inherit to-transparent`}>
          {/* Prominent Enable Button for Disabled Nodes */}
          {!node.enabled && (
              <button 
                  onClick={(e) => { 
                      e.stopPropagation(); 
                      console.log(`[UI] Enable button clicked for node: ${node.id}`);
                      onToggleEnabled(node.id); 
                  }} 
                  className="bg-gray-200 hover:bg-gray-300 text-gray-600 text-[10px] font-medium px-2 py-0.5 rounded border border-gray-300 shadow-sm transition-colors mr-1"
                  title="Click to Enable this operation"
              >
                  Enable
              </button>
          )}

          <div className={`flex items-center space-x-0.5 ${!node.enabled ? 'hidden' : (isSelected ? 'opacity-100' : 'opacity-0 group-hover:opacity-100')} transition-opacity`}>
              {hasChildren && onAnalyzeOverlap && (
                 <button onClick={(e) => { e.stopPropagation(); onAnalyzeOverlap(node.id); }} className="p-1 rounded hover:bg-yellow-100 text-yellow-600" title="Overlap"><Split className="w-3 h-3" /></button>
              )}
              {/* Only show small power toggle if Enabled (to disable) */}
              {node.enabled && (
                  <button onClick={(e) => { 
                      e.stopPropagation(); 
                      console.log(`[UI] Disable toggle clicked for node: ${node.id}`);
                      onToggleEnabled(node.id); 
                  }} className="p-1 rounded hover:bg-gray-200 text-green-600" title="Disable"><Power className="w-3 h-3" /></button>
              )}
              <button onClick={(e) => { e.stopPropagation(); onAddChild(node.id); setExpanded(true); }} className="p-1 rounded hover:bg-gray-200 text-gray-500" title="Add Child"><Plus className="w-3 h-3" /></button>
              {onMoveNode && (
                  <>
                      <button
                          onClick={(e) => {
                              e.stopPropagation();
                              if (canMoveUp) onMoveNode(node.id, 'up');
                          }}
                          className={`p-1 rounded ${canMoveUp ? 'hover:bg-gray-200 text-gray-400 hover:text-gray-600' : 'text-gray-200 cursor-not-allowed'}`}
                          title="Move Up"
                          disabled={!canMoveUp}
                      >
                          <ChevronUp className="w-3 h-3" />
                      </button>
                      <button
                          onClick={(e) => {
                              e.stopPropagation();
                              if (canMoveDown) onMoveNode(node.id, 'down');
                          }}
                          className={`p-1 rounded ${canMoveDown ? 'hover:bg-gray-200 text-gray-400 hover:text-gray-600' : 'text-gray-200 cursor-not-allowed'}`}
                          title="Move Down"
                          disabled={!canMoveDown}
                      >
                          <ChevronDown className="w-3 h-3" />
                      </button>
                  </>
              )}
              {node.id !== 'root' && <button onClick={(e) => { e.stopPropagation(); onDelete(node.id); }} className="p-1 rounded hover:bg-red-100 text-red-500" title="Delete"><Trash2 className="w-3 h-3" /></button>}
          </div>
        </div>
      </div>

      {hasChildren && expanded && (
          <div className="relative">
              {node.children?.map((child, childIndex) => (
                  <OperationTree 
                    key={child.id} 
                    node={child} 
                    selectedId={selectedId} 
                    onSelect={onSelect} 
                    onToggleEnabled={onToggleEnabled}
                    onAddChild={onAddChild}
                    onDelete={onDelete}
                    onMoveNode={onMoveNode}
                    onAnalyzeOverlap={onAnalyzeOverlap}
                    expandTrigger={expandTrigger}
                    collapseTrigger={collapseTrigger}
                    globalAction={globalAction}
                    appearance={appearance}
                    level={level + 1}
                    parentId={node.id}
                    index={childIndex}
                    siblingCount={node.children?.length ?? 0}
                  />
              ))}
          </div>
      )}
    </div>
  );
};
