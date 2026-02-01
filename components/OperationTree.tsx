
import React, { useEffect, useState, useRef } from 'react';
import { ChevronRight, ChevronDown, Layers, Trash2, Plus, Power, Filter, FunctionSquare, Calculator, Link, Split, Braces, Database, ArrowDownAZ, Settings2, Copy } from 'lucide-react';
import { OperationNode, AppearanceConfig } from '../types';

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
  globalAction?: 'expand' | 'collapse' | null;
  appearance: AppearanceConfig;
}

const getIcon = (node: OperationNode) => {
    if (node.operationType === 'dataset' || node.operationType === 'setup') return Database;
    const firstCmd = node.commands[0];
    if (!firstCmd) return Layers;
    switch (firstCmd.type) {
        case 'filter': return Filter;
        case 'transform': return FunctionSquare;
        case 'group': return Calculator;
        case 'join': return Link;
        case 'save': return Braces;
        case 'sort': return ArrowDownAZ;
        default: return Layers;
    }
};

export const OperationTree: React.FC<OperationTreeProps> = ({ 
  node, 
  selectedId, 
  onSelect, 
  onToggleEnabled, 
  onAddChild, 
  onDelete,
  onAnalyzeOverlap,
  expandTrigger = 0,
  collapseTrigger = 0,
  globalAction = null,
  appearance
}) => {
  const [expanded, setExpanded] = useState(() => {
      if (globalAction === 'collapse') return false;
      return true;
  });

  const isSelected = selectedId === node.id;
  const hasChildren = node.children && node.children.length > 0;
  
  const prevExpandRef = useRef(expandTrigger);
  const prevCollapseRef = useRef(collapseTrigger);

  useEffect(() => {
    if (expandTrigger !== prevExpandRef.current) {
        prevExpandRef.current = expandTrigger;
        setExpanded(true);
    }
  }, [expandTrigger]);

  useEffect(() => {
    if (collapseTrigger !== prevCollapseRef.current) {
        prevCollapseRef.current = collapseTrigger;
        setExpanded(false);
    }
  }, [collapseTrigger]);

  const handleToggle = (e: React.MouseEvent) => {
    e.stopPropagation();
    setExpanded(!expanded);
  };

  const Icon = getIcon(node);
  const isSetup = node.operationType === 'setup';
  
  // Custom display logic for Setup nodes
  let displayName: React.ReactNode = node.name;
  if (isSetup) {
      const sourceCommands = node.commands.filter(c => c.type === 'source');
      // Only special display if multiple sources, otherwise use simple name
      if (sourceCommands.length > 1) {
          displayName = (
            <span className="flex items-center space-x-1">
                <span className="font-bold text-gray-800">{node.name}</span>
                <span className="text-[10px] bg-blue-100 text-blue-700 px-1.5 rounded-full border border-blue-200">
                    {sourceCommands.length} Sources
                </span>
            </span>
          );
      }
  }

  // Dynamic Styles
  const indentPixels = Math.max(16, appearance.textSize + 6); 
  const iconSize = Math.max(12, appearance.textSize);

  return (
    <div className="select-none min-w-max flex flex-col">
      {/* Row Content */}
      <div 
        className={`
          flex items-center py-1 px-1 cursor-pointer transition-all duration-150 group rounded-md relative whitespace-nowrap mb-0.5
          ${isSelected ? 'bg-blue-50 ring-1 ring-blue-500 z-10' : 'hover:bg-gray-100 border border-transparent'}
          ${!node.enabled ? 'opacity-60 grayscale' : ''}
          ${isSetup ? 'border-l-2 border-l-blue-400 pl-1' : ''}
        `}
        onClick={() => onSelect(node.id)}
      >
        {/* Toggle / Spacer */}
        <div 
            onClick={hasChildren ? handleToggle : undefined} 
            className={`
                flex items-center justify-center rounded hover:bg-black/5 transition-colors cursor-pointer mr-1
                ${!hasChildren ? 'invisible' : ''}
            `}
            style={{ width: '20px', height: '20px' }}
        >
          {expanded ? <ChevronDown className="w-3.5 h-3.5 text-gray-500" /> : <ChevronRight className="w-3.5 h-3.5 text-gray-400" />}
        </div>

        {/* Node Icon */}
        <Icon 
            className={`mr-2 shrink-0 ${isSelected ? 'text-blue-600' : 'text-gray-400 group-hover:text-gray-600'}`} 
            style={{ width: iconSize, height: iconSize }}
        />

        {/* Label */}
        <span 
            className={`flex-grow truncate mr-2 ${isSelected ? 'font-semibold text-blue-900' : ''}`}
            style={{ 
                fontSize: `${appearance.textSize}px`,
                color: isSelected ? undefined : appearance.textColor
            }}
        >
          {displayName}
        </span>

        {/* Actions - visible on hover or selected */}
        <div className={`flex items-center space-x-0.5 ml-auto pl-2 ${isSelected || 'group-hover:opacity-100 opacity-0'} transition-opacity`}>
          {hasChildren && onAnalyzeOverlap && (
             <button onClick={(e) => { e.stopPropagation(); onAnalyzeOverlap(node.id); }} className="p-1 rounded hover:bg-yellow-100 text-yellow-600" title="Overlap"><Split className="w-3 h-3" /></button>
          )}
          <button onClick={(e) => { e.stopPropagation(); onToggleEnabled(node.id); }} className={`p-1 rounded hover:bg-gray-200 ${node.enabled ? 'text-green-600' : 'text-gray-400'}`} title={node.enabled ? "Disable" : "Enable"}><Power className="w-3 h-3" /></button>
          <button onClick={(e) => { e.stopPropagation(); onAddChild(node.id); setExpanded(true); }} className="p-1 rounded hover:bg-gray-200 text-gray-500" title="Add Child"><Plus className="w-3 h-3" /></button>
          {node.id !== 'root' && <button onClick={(e) => { e.stopPropagation(); onDelete(node.id); }} className="p-1 rounded hover:bg-red-100 text-red-500" title="Delete"><Trash2 className="w-3 h-3" /></button>}
        </div>
      </div>

      {/* Children Container (Nested) */}
      {expanded && hasChildren && (
        <div 
            className="flex flex-col relative"
            style={{ 
                paddingLeft: '0px',
                // Critical for alignment
                marginLeft: '10px', 
                borderLeftWidth: appearance.showGuideLines ? '1px' : '0',
                borderLeftColor: appearance.guideLineColor,
                borderLeftStyle: 'solid',
            }}
        >
            <div style={{ paddingLeft: `${indentPixels - 10}px` }}> 
                {node.children!.map(child => (
                    <OperationTree 
                        key={child.id} 
                        node={child} 
                        selectedId={selectedId}
                        onSelect={onSelect}
                        onToggleEnabled={onToggleEnabled}
                        onAddChild={onAddChild}
                        onDelete={onDelete}
                        onAnalyzeOverlap={onAnalyzeOverlap}
                        expandTrigger={expandTrigger}
                        collapseTrigger={collapseTrigger}
                        globalAction={globalAction}
                        appearance={appearance}
                    />
                ))}
            </div>
        </div>
      )}
    </div>
  );
};
