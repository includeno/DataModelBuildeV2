

import React, { useMemo, useState, useRef, useEffect } from 'react';
import { Command, CommandType, Dataset, OperationType, AggregationConfig, OperationNode, DataType, HavingCondition, MappingRule, FilterGroup, FilterCondition, SubTableConfig, FieldInfo } from '../types';
import { Button } from './Button';
import { Trash2, Plus, GripVertical, Type, Database, Play, Layers, Braces, ArrowRight, Filter as FilterIcon, Table, Calculator, List, Check, Info, ChevronDown, Split, LayoutDashboard, AlertTriangle, Settings2, Eye, Variable } from 'lucide-react';

interface CommandEditorProps {
  operationId: string;
  operationName: string;
  operationType: OperationType;
  commands: Command[];
  datasets: Dataset[];
  inputSchema: Record<string, DataType>; 
  onUpdateCommands: (operationId: string, newCommands: Command[]) => void;
  onUpdateName: (name: string) => void;
  onUpdateType: (operationId: string, type: OperationType) => void;
  onViewPath: () => void;
  onRun?: (commandId?: string) => void;
  tree?: OperationNode; 
}

const PYTHON_TEMPLATE = `def transform(row):
    # Available: np, pd, math, datetime, re
    # Name must be 'transform', return the calculated value
    val = row.get('id', 0)
    return val * 1.1`;

// const DATA_TYPE_ICONS: Record<string, any> = {
//     string: Type,
//     number: Hash,
//     boolean: CheckCircle,
//     date: Calendar,
//     timestamp: Clock,
//     json: Code,
// };

// const OPERATION_TYPES: {value: OperationType, label: string, icon: any}[] = [
//     { value: 'setup', label: 'Setup Source', icon: Settings2 },
//     { value: 'process', label: 'Process', icon: Play },
// ];

const OPERATORS: Record<string, { value: string; label: string }[]> = {
    string: [
        { value: '=', label: 'Equals' },
        { value: '!=', label: 'Not Equals' },
        { value: 'contains', label: 'Contains' },
        { value: 'not_contains', label: 'Does Not Contain' },
        { value: 'starts_with', label: 'Starts With' },
        { value: 'ends_with', label: 'Ends With' },
        { value: 'is_empty', label: 'Is Empty' },
        { value: 'is_not_empty', label: 'Is Not Empty' },
        { value: 'in_variable', label: 'In Variable List' },
        { value: 'not_in_variable', label: 'Not In Variable List' },
    ],
    number: [
        { value: '=', label: 'Equals' },
        { value: '!=', label: 'Not Equals' },
        { value: '>', label: 'Greater Than' },
        { value: '>=', label: 'Greater/Equal' },
        { value: '<', label: 'Less Than' },
        { value: '<=', label: 'Less/Equal' },
        { value: 'is_empty', label: 'Is Null' },
        { value: 'is_not_empty', label: 'Is Not Null' },
        { value: 'in_variable', label: 'In Variable List' },
        { value: 'not_in_variable', label: 'Not In Variable List' },
    ],
    boolean: [
        { value: 'is_true', label: 'Is True' },
        { value: 'is_false', label: 'Is False' },
    ],
    date: [
        { value: '=', label: 'Is On' },
        { value: '!=', label: 'Is Not On' },
        { value: 'before', label: 'Before' },
        { value: 'after', label: 'After' },
    ],
    json: [
        { value: 'has_key', label: 'Has Key' },
        { value: 'contains', label: 'Contains Value' },
    ]
};
OPERATORS['timestamp'] = OPERATORS['date'];

const baseInputStyles = "w-full text-sm border border-gray-200 rounded-md focus:ring-2 focus:ring-blue-100 focus:border-blue-500 bg-white text-gray-900 shadow-sm transition-all hover:border-gray-300 py-1.5";
const errorInputStyles = "w-full text-sm border border-red-300 rounded-md focus:ring-2 focus:ring-red-100 focus:border-red-500 bg-red-50 text-red-900 shadow-sm transition-all py-1.5";
const codeAreaStyles = "w-full text-[11px] border border-gray-700 rounded-md focus:ring-2 focus:ring-blue-900 focus:border-blue-700 bg-[#1e1e1e] text-[#d4d4d4] font-mono shadow-sm transition-all py-2 px-3 leading-relaxed resize-none selection:bg-[#264f78]";

const flattenNodes = (root: OperationNode): OperationNode[] => {
    let result = [root];
    if (root.children) {
        root.children.forEach(child => {
            result = [...result, ...flattenNodes(child)];
        });
    }
    return result;
};

const findAncestorVariables = (root: OperationNode, currentId: string): string[] => {
    const vars: string[] = [];
    const traverse = (node: OperationNode): boolean => {
        if (node.id === currentId) return true; 
        let foundInChild = false;
        if (node.children) {
            for (const child of node.children) {
                if (traverse(child)) {
                    foundInChild = true;
                    break;
                }
            }
        }
        if (foundInChild) {
            node.commands.forEach(cmd => {
                if (cmd.type === 'save' && cmd.config.value) {
                    vars.push(String(cmd.config.value));
                }
                // Also capture definition variables from setup nodes
                if (cmd.type === 'define_variable' && cmd.config.variableName) {
                    vars.push(cmd.config.variableName);
                }
            });
            return true;
        }
        return false;
    };
    if (root) traverse(root);
    return vars;
};

const getAncestors = (node: OperationNode, targetId: string): OperationNode[] | null => {
    if (node.id === targetId) return [];
    if (node.children) {
        for (const child of node.children) {
            const res = getAncestors(child, targetId);
            if (res) return [node, ...res];
        }
    }
    return null;
};

// --- HELPER COMPONENTS ---

interface SelectOption {
    value: string;
    label: string;
    subLabel?: string;
    disabled?: boolean;
    icon?: React.ElementType;
}

const CustomSelect: React.FC<{
    value: string;
    onChange: (val: string) => void;
    options: SelectOption[];
    placeholder?: string;
    icon?: React.ElementType;
    hasError?: boolean;
    className?: string;
}> = ({ value, onChange, options, placeholder = "Select...", icon: DefaultIcon, hasError, className = "" }) => {
    const [isOpen, setIsOpen] = useState(false);
    const containerRef = useRef<HTMLDivElement>(null);

    useEffect(() => {
        const handleClickOutside = (event: MouseEvent) => {
            if (containerRef.current && !containerRef.current.contains(event.target as Node)) {
                setIsOpen(false);
            }
        };
        document.addEventListener('mousedown', handleClickOutside);
        return () => document.removeEventListener('mousedown', handleClickOutside);
    }, []);

    const selectedOption = options.find(o => o.value === value);
    const isPlaceholder = !value;
    const IconToUse = selectedOption?.icon || DefaultIcon;

    return (
        <div className={`relative w-full ${className}`} ref={containerRef}>
            <div
                onClick={() => setIsOpen(!isOpen)}
                className={`
                    w-full px-3 py-2.5 rounded-lg border flex items-center justify-between cursor-pointer transition-all bg-white
                    ${hasError ? 'border-red-300 focus:ring-2 focus:ring-red-100' : 'border-gray-200 hover:border-blue-400 focus:ring-2 focus:ring-blue-50'}
                    ${isOpen ? 'ring-2 ring-blue-100 border-blue-400' : 'shadow-sm'}
                `}
            >
                <div className="flex items-center overflow-hidden">
                    {IconToUse && <IconToUse className={`w-4 h-4 mr-2.5 shrink-0 ${selectedOption ? 'text-blue-600' : 'text-gray-400'}`} />}
                    <span className={`text-sm truncate font-medium ${isPlaceholder ? 'text-gray-400 italic' : 'text-gray-900'}`}>
                        {selectedOption ? selectedOption.label : placeholder}
                    </span>
                </div>
                <ChevronDown className={`w-4 h-4 text-gray-400 transition-transform duration-200 ${isOpen ? 'rotate-180' : ''}`} />
            </div>

            {isOpen && (
                <div className="absolute z-50 left-0 right-0 mt-1.5 bg-white border border-gray-100 rounded-xl shadow-xl max-h-60 overflow-y-auto animate-in fade-in zoom-in-95 duration-100 p-1">
                    {options.length === 0 ? (
                        <div className="px-4 py-3 text-xs text-gray-400 text-center italic">No options available</div>
                    ) : (
                        options.map((opt) => (
                            <div
                                key={opt.value}
                                onClick={() => {
                                    if (!opt.disabled) {
                                        onChange(opt.value);
                                        setIsOpen(false);
                                    }
                                }}
                                className={`
                                    flex items-center justify-between px-3 py-2.5 rounded-lg mb-0.5 transition-colors
                                    ${opt.disabled 
                                        ? 'opacity-50 cursor-not-allowed bg-gray-50' 
                                        : 'cursor-pointer hover:bg-blue-50 group'
                                    }
                                    ${opt.value === value ? 'bg-blue-50/80' : ''}
                                `}
                            >
                                <div className="flex flex-col min-w-0">
                                    <div className="flex items-center">
                                        {opt.icon && <opt.icon className={`w-3.5 h-3.5 mr-2 ${opt.value === value ? 'text-blue-700' : 'text-gray-400 group-hover:text-blue-600'}`} />}
                                        <span className={`text-sm font-medium ${opt.value === value ? 'text-blue-700' : 'text-gray-700 group-hover:text-blue-700'}`}>
                                            {opt.label}
                                        </span>
                                    </div>
                                    {opt.subLabel && (
                                        <span className="text-[10px] text-gray-400 mt-0.5 ml-5.5">
                                            {opt.subLabel}
                                        </span>
                                    )}
                                </div>
                                {opt.value === value && <Check className="w-4 h-4 text-blue-600" />}
                                {opt.disabled && opt.value !== value && (
                                    <span className="text-[10px] text-gray-400 bg-gray-100 px-1.5 py-0.5 rounded ml-2">Used</span>
                                )}
                            </div>
                        ))
                    )}
                </div>
            )}
        </div>
    );
};

// Collapsible Section Component
const CollapsibleSection: React.FC<{
    title: string;
    icon: any;
    count: number;
    children: React.ReactNode;
    color?: string; // class for text color e.g. text-blue-500
}> = ({ title, icon: Icon, count, children, color = "text-blue-500" }) => {
    const [isOpen, setIsOpen] = useState(true);

    return (
        <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden flex flex-col transition-all duration-200 group">
            <div 
                className="px-6 py-4 flex justify-between items-center cursor-pointer hover:bg-gray-50 transition-colors select-none"
                onClick={() => setIsOpen(!isOpen)}
            >
                <div className="flex items-center space-x-3">
                    <div className={`p-1.5 rounded-lg transition-colors ${isOpen ? 'bg-gray-100' : 'bg-transparent'}`}>
                        <Icon className={`w-4 h-4 ${color}`} />
                    </div>
                    <div className="flex items-center space-x-2">
                        <h3 className="text-sm font-bold text-gray-800 uppercase tracking-wider">{title}</h3>
                        {count > 0 && (
                            <span className="text-[10px] font-bold bg-gray-100 text-gray-500 px-2 py-0.5 rounded-full border border-gray-200">
                                {count}
                            </span>
                        )}
                    </div>
                </div>
                <div className={`transform transition-transform duration-200 text-gray-400 group-hover:text-gray-600 ${isOpen ? 'rotate-180' : ''}`}>
                    <ChevronDown className="w-4 h-4" />
                </div>
            </div>
            <div 
                className={`transition-all duration-300 ease-in-out overflow-hidden bg-white ${isOpen ? 'max-h-[800px] opacity-100 border-t border-gray-100' : 'max-h-0 opacity-0'}`}
            >
                <div className="p-6 overflow-y-auto max-h-[60vh] space-y-4 custom-scrollbar">
                    {children}
                </div>
            </div>
        </div>
    );
};

interface VariableInserterProps {
    variables: string[];
    onInsert: (v: string) => void;
}
const VariableInserter: React.FC<VariableInserterProps> = ({ variables, onInsert }) => {
    const [isOpen, setIsOpen] = useState(false);
    return (
        <div className="absolute right-1 top-1 z-10">
            <button 
                onClick={() => setIsOpen(!isOpen)}
                className="p-1 text-gray-400 hover:text-blue-600 rounded bg-transparent hover:bg-blue-50 transition-colors"
                title="Insert Variable"
            >
                <Braces className="w-3.5 h-3.5" />
            </button>
            {isOpen && (
                <>
                    <div className="fixed inset-0 z-20" onClick={() => setIsOpen(false)} />
                    <div className="absolute right-0 mt-1 w-48 bg-white border border-gray-200 rounded-lg shadow-xl z-30 py-1 max-h-48 overflow-y-auto animate-in fade-in zoom-in-95 duration-100">
                        <div className="px-3 py-1.5 text-[10px] font-bold text-gray-400 uppercase tracking-wider bg-gray-50 border-b border-gray-100 mb-1">
                            Available Variables
                        </div>
                        {variables.length === 0 ? (
                            <div className="px-3 py-2 text-xs text-gray-400 italic">No variables found</div>
                        ) : (
                            variables.map(v => (
                                <button
                                    key={v}
                                    onClick={() => { onInsert(v); setIsOpen(false); }}
                                    className="w-full text-left px-3 py-1.5 text-xs text-gray-700 hover:bg-blue-50 hover:text-blue-700 flex items-center"
                                >
                                    <span className="font-mono bg-gray-100 px-1 rounded mr-2 text-[10px] border border-gray-200">{v}</span>
                                </button>
                            ))
                        )}
                    </div>
                </>
            )}
        </div>
    );
};
const InsertDivider = ({ onInsert, index }: { onInsert: (i: number) => void; index: number }) => {
    return (
        <div className="relative h-5 group flex items-center justify-center my-1">
            <div className="absolute inset-x-8 top-1/2 -translate-y-1/2 h-px bg-blue-200 opacity-0 group-hover:opacity-100 transition-opacity duration-200"></div>
            <button
                onClick={() => onInsert(index)}
                className="relative z-10 bg-white border border-blue-200 text-blue-600 rounded-full p-0.5 shadow-sm opacity-0 group-hover:opacity-100 transition-all duration-200 hover:bg-blue-50 hover:scale-110"
                title="Insert Step Here"
            >
                <Plus className="w-3.5 h-3.5" />
            </button>
        </div>
    );
};

const VariableSuggestionInput: React.FC<{ value: string, onChange: (val: string) => void, variables: string[] }> = ({ value, onChange, variables }) => {
    const [showSuggestions, setShowSuggestions] = useState(false);
    const containerRef = useRef<HTMLDivElement>(null);
    const inputRef = useRef<HTMLInputElement>(null);

    // Close on click outside
    useEffect(() => {
        const handleClickOutside = (event: MouseEvent) => {
            if (containerRef.current && !containerRef.current.contains(event.target as Node)) {
                setShowSuggestions(false);
            }
        };
        document.addEventListener('mousedown', handleClickOutside);
        return () => document.removeEventListener('mousedown', handleClickOutside);
    }, []);
    
    return (
        <div className="relative w-full" ref={containerRef}>
            <input 
                ref={inputRef}
                className={`${baseInputStyles} py-1 px-2 pr-6`} 
                placeholder="Variable Name" 
                value={value} 
                onChange={(e) => onChange(e.target.value)}
                onFocus={() => setShowSuggestions(true)}
            />
            <div 
                className="absolute right-1 top-1/2 -translate-y-1/2 cursor-pointer text-gray-400 hover:text-blue-500 p-1"
                onClick={() => {
                    setShowSuggestions(!showSuggestions);
                    if (!showSuggestions) {
                        inputRef.current?.focus();
                    }
                }}
            >
                <ChevronDown className="w-3 h-3" />
            </div>
            
            {showSuggestions && (
                <div className="absolute left-0 right-0 z-[100] mt-1 bg-white border border-gray-200 rounded-md shadow-lg max-h-48 overflow-y-auto animate-in fade-in zoom-in-95 duration-100 min-w-[120px]">
                    <div className="px-2 py-1.5 text-[10px] font-bold text-gray-400 uppercase bg-gray-50 border-b border-gray-100 sticky top-0">Select Variable</div>
                    {variables.length === 0 ? (
                        <div className="px-3 py-2 text-xs text-gray-400 italic">No variables available</div>
                    ) : (
                        variables.map(v => (
                            <div 
                                key={v}
                                className="px-3 py-2 text-xs text-gray-700 hover:bg-blue-50 hover:text-blue-700 cursor-pointer flex items-center transition-colors"
                                onClick={() => { onChange(v); setShowSuggestions(false); }}
                            >
                                <Braces className="w-3 h-3 mr-2 text-blue-400 shrink-0" />
                                <span className="truncate font-medium">{v}</span>
                            </div>
                        ))
                    )}
                </div>
            )}
        </div>
    );
};

interface FilterGroupEditorProps {
    group: FilterGroup;
    activeSchema: Record<string, DataType>;
    onUpdate: (updated: FilterGroup) => void;
    onRemove: (id: string) => void;
    isRoot?: boolean;
    availableVariables: string[];
}
const FilterGroupEditor: React.FC<FilterGroupEditorProps> = ({ group, activeSchema, onUpdate, onRemove, isRoot = false, availableVariables }) => {
    const fieldNames = Object.keys(activeSchema);
    const handleUpdateCondition = (id: string, updates: Partial<FilterCondition>) => {
        const newConditions = group.conditions.map(c => {
            if (c.id === id && c.type === 'condition') return { ...c, ...updates };
            return c;
        });
        onUpdate({ ...group, conditions: newConditions });
    };
    const handleUpdateSubGroup = (id: string, updatedGroup: FilterGroup) => {
        const newConditions = group.conditions.map(c => c.id === id ? updatedGroup : c);
        onUpdate({ ...group, conditions: newConditions });
    };
    const handleAddCondition = () => {
        const newCond: FilterCondition = { id: `cond_${Date.now()}_${Math.random().toString(36).substr(2, 5)}`, type: 'condition', field: '', operator: '=', value: '' };
        onUpdate({ ...group, conditions: [...group.conditions, newCond] });
    };
    const handleAddGroup = () => {
        const newGroup: FilterGroup = { id: `group_${Date.now()}_${Math.random().toString(36).substr(2, 5)}`, type: 'group', logicalOperator: 'AND', conditions: [] };
        onUpdate({ ...group, conditions: [...group.conditions, newGroup] });
    };
    const handleRemoveChild = (id: string) => {
        onUpdate({ ...group, conditions: group.conditions.filter(c => c.id !== id) });
    };
    return (
        <div className={`space-y-3 ${isRoot ? '' : 'pl-4 border-l-2 border-blue-100 py-1'}`}>
            <div className="flex items-center space-x-3 mb-2">
                <div className="flex bg-gray-100 rounded p-0.5">
                    <button 
                        onClick={() => onUpdate({ ...group, logicalOperator: 'AND' })}
                        className={`px-2 py-0.5 text-[10px] font-bold rounded-sm transition-all ${group.logicalOperator === 'AND' ? 'bg-white shadow-sm text-blue-600' : 'text-gray-500'}`}
                    >AND</button>
                    <button 
                        onClick={() => onUpdate({ ...group, logicalOperator: 'OR' })}
                        className={`px-2 py-0.5 text-[10px] font-bold rounded-sm transition-all ${group.logicalOperator === 'OR' ? 'bg-white shadow-sm text-blue-600' : 'text-gray-500'}`}
                    >OR</button>
                </div>
                <div className="flex-1 h-px bg-gray-100"></div>
                {!isRoot && (
                    <button onClick={() => onRemove(group.id)} className="p-1 text-gray-300 hover:text-red-500 transition-colors shrink-0">
                        <Trash2 className="w-3.5 h-3.5" />
                    </button>
                )}
            </div>
            <div className="space-y-3">
                {group.conditions.map(item => (
                    item.type === 'group' ? (
                        <FilterGroupEditor 
                            key={item.id} 
                            group={item} 
                            activeSchema={activeSchema} 
                            onUpdate={(g) => handleUpdateSubGroup(item.id, g)} 
                            onRemove={handleRemoveChild}
                            availableVariables={availableVariables}
                        />
                    ) : (
                        <div key={item.id} className="grid grid-cols-12 gap-2 items-center bg-gray-50/50 p-2 rounded-md border border-gray-100 group/cond relative">
                            <div className="col-span-5 relative">
                                <select 
                                    className={`${baseInputStyles} py-1 pl-2`} 
                                    value={item.field} 
                                    onChange={(e) => handleUpdateCondition(item.id, { field: e.target.value })}
                                >
                                    <option value="">Select Field...</option>
                                    {fieldNames.map(f => <option key={f} value={f}>{f}</option>)}
                                </select>
                            </div>
                            <div className="col-span-3">
                                <select 
                                    className={`${baseInputStyles} py-1`} 
                                    value={item.operator} 
                                    onChange={(e) => handleUpdateCondition(item.id, { operator: e.target.value })}
                                >
                                    {(OPERATORS[activeSchema[item.field] || 'string'] || OPERATORS['string']).map(op => (
                                        <option key={op.value} value={op.value}>{op.label}</option>
                                    ))}
                                </select>
                            </div>
                            <div className="col-span-3 relative">
                                {(item.operator === 'in_variable' || item.operator === 'not_in_variable') ? (
                                    <VariableSuggestionInput 
                                        value={String(item.value)} 
                                        onChange={(val) => handleUpdateCondition(item.id, { value: val })}
                                        variables={availableVariables}
                                    />
                                ) : (
                                    <input 
                                        className={`${baseInputStyles} py-1 px-2`} 
                                        placeholder="Value" 
                                        value={String(item.value)} 
                                        onChange={(e) => handleUpdateCondition(item.id, { value: e.target.value })} 
                                    />
                                )}
                            </div>
                            <div className="col-span-1 flex justify-end">
                                <button onClick={() => handleRemoveChild(item.id)} className="p-1 text-gray-300 hover:text-red-500 opacity-0 group-hover/cond:opacity-100 transition-all">
                                    <Trash2 className="w-3.5 h-3.5" />
                                </button>
                            </div>
                        </div>
                    )
                ))}
            </div>
            <div className="flex items-center space-x-4 pt-1">
                <button onClick={handleAddCondition} className="text-[10px] font-bold text-blue-600 hover:underline flex items-center">
                    <Plus className="w-3 h-3 mr-1" /> Add Rule
                </button>
                <button onClick={handleAddGroup} className="text-[10px] font-bold text-gray-500 hover:underline flex items-center">
                    <Split className="w-3 h-3 mr-1" /> Add Group
                </button>
            </div>
        </div>
    );
};

// --- MAIN EDITOR ---

export const CommandEditor: React.FC<CommandEditorProps> = ({ 
  operationId, 
  operationName, 
  operationType, 
  commands, 
  datasets,
  // inputSchema, // Unused
  onUpdateCommands,
  onUpdateName,
  // onUpdateType, // Unused
  onViewPath,
  onRun,
  tree
}) => {
  
  const ancestorVariables = useMemo(() => {
     if (!tree) return [];
     return findAncestorVariables(tree, operationId);
  }, [tree, operationId]);

  const availableNodes = useMemo(() => {
      if (!tree) return [];
      return flattenNodes(tree).filter(n => n.id !== operationId);
  }, [tree, operationId]);

  // Aggregate all available source aliases from all setup nodes
  const availableSourceAliases = useMemo(() => {
      if (!tree) return [];
      const setupNodes = flattenNodes(tree).filter(n => n.operationType === 'setup');
      const aliases: { alias: string, nodeName: string, id: string, sourceTable?: string, linkId: string }[] = [];
      
      setupNodes.forEach(node => {
          const sourceCmds = node.commands.filter(c => c.type === 'source');
          sourceCmds.forEach((cmd, idx) => {
              // Determine display alias: Explicit Alias -> Node Name (if first command) -> Empty
              let effectiveAlias = cmd.config.alias;
              if (!effectiveAlias && idx === 0) {
                  effectiveAlias = node.name;
              }
              
              // Ensure we have a link ID. Fallback to cmd.id if linkId wasn't saved in older versions.
              const linkId = cmd.config.linkId || cmd.id;

              if (effectiveAlias) {
                  aliases.push({ 
                      alias: effectiveAlias, 
                      nodeName: node.name, 
                      id: node.id,
                      sourceTable: cmd.config.mainTable || '?',
                      linkId: linkId
                  });
              }
          });
      });
      return aliases;
  }, [tree]);

  const ancestors = useMemo(() => {
      if (!tree) return [];
      return getAncestors(tree, operationId) || [];
  }, [tree, operationId]);

  // Collect outputs from Ancestor nodes (Parent -> Parent -> Root)
  const ancestorOutputs = useMemo(() => {
      const outputs = new Set<string>();
      ancestors.forEach(node => {
          node.commands.forEach(cmd => {
              if (cmd.type === 'group' && cmd.config.outputTableName && cmd.config.outputTableName.trim() !== '') {
                  outputs.add(cmd.config.outputTableName.trim());
              }
          });
      });
      return Array.from(outputs);
  }, [tree, operationId]);

  // Global list only for resolving schemas if needed, though strictly we should use scoped.
  // We'll use this only for fallback in `inputSchema` resolution if necessary, 
  // but for the Dropdown we will use `ancestorOutputs` + `localOutputs`.
  const allGeneratedTablesGlobal = useMemo(() => {
      if (!tree) return [];
      const names = new Set<string>();
      const nodes = flattenNodes(tree);
      nodes.forEach(node => {
          node.commands.forEach(cmd => {
              if (cmd.type === 'group' && cmd.config.outputTableName && cmd.config.outputTableName.trim() !== '') {
                  names.add(cmd.config.outputTableName.trim());
              }
          });
      });
      return Array.from(names);
  }, [tree]);

  const hasComplexView = useMemo(() => commands.some(c => c.type === 'multi_table'), [commands]);

  // -- Setup Mode Handler --
  if (operationType === 'setup') {
      const sourceCommands = commands.filter(c => c.type === 'source');
      const variableCommands = commands.filter(c => c.type === 'define_variable');
      
      const handleAddSource = () => {
          const newCmd: Command = {
              id: `cmd_src_${Date.now()}_${Math.floor(Math.random() * 1000)}`,
              type: 'source',
              config: { 
                  mainTable: '', 
                  alias: '',
                  linkId: `link_${Date.now()}_${Math.random().toString(36).substr(2, 5)}` // Generate stable Link ID
              }, 
              order: commands.length + 1
          };
          onUpdateCommands(operationId, [...commands, newCmd]);
      };

      const handleAddVariable = () => {
          const newCmd: Command = {
              id: `cmd_var_${Date.now()}_${Math.floor(Math.random() * 1000)}`,
              type: 'define_variable',
              config: {
                  variableName: '',
                  variableType: 'text',
                  variableValue: ''
              },
              order: commands.length + 1
          };
          onUpdateCommands(operationId, [...commands, newCmd]);
      };

      const handleUpdateSourceCmd = (cmdId: string, updates: Partial<any>) => {
          const updated = commands.map(c => {
              if (c.id === cmdId) {
                  return { ...c, config: { ...c.config, ...updates } };
              }
              return c;
          });
          onUpdateCommands(operationId, updated);
      };

      const handleDatasetSelection = (cmdId: string, datasetName: string, currentAlias: string) => {
          const updates: any = { mainTable: datasetName };
          // Auto-fill alias if empty when selecting a dataset
          if (!currentAlias && datasetName) {
              updates.alias = datasetName;
          }
          handleUpdateSourceCmd(cmdId, updates);
      };

      const handleRemoveCmd = (cmdId: string) => {
          const updated = commands.filter(c => c.id !== cmdId);
          onUpdateCommands(operationId, updated);
      };

      const getValidationErrors = (cmd: Command, index: number) => {
          const errors: string[] = [];
          if (cmd.type === 'source') {
              const currentTable = cmd.config.mainTable;
              const currentAlias = cmd.config.alias;

              // 1. Duplicate Data Source Check
              if (currentTable) {
                  const isDuplicateTable = sourceCommands.some((c, idx) => 
                      idx !== index && c.config.mainTable === currentTable
                  );
                  if (isDuplicateTable) errors.push("This table is already selected.");
              }

              // 2. Duplicate Alias Check
              if (currentAlias) {
                  const isDuplicateAlias = sourceCommands.some((c, idx) => 
                      idx !== index && c.config.alias === currentAlias
                  );
                  if (isDuplicateAlias) errors.push("Alias name must be unique.");

                  // 3. Alias vs Variable Name Check (New Requirement)
                  const isConflictWithVariable = variableCommands.some(c => c.config.variableName === currentAlias);
                  if (isConflictWithVariable) errors.push("Alias conflicts with a variable name.");
              }

              // 4. Alias vs Table Name Check
              if (currentAlias) {
                  const isConflictWithDataset = datasets.some(d => d.name === currentAlias);
                  if (isConflictWithDataset && currentAlias !== currentTable) {
                       errors.push(`Alias matches another dataset '${currentAlias}'.`);
                  }
              }
          }
          return errors;
      };

      const getVariableValidationErrors = (cmd: Command) => {
          const errors: string[] = [];
          if (cmd.type === 'define_variable') {
              const name = cmd.config.variableName;
              if (!name) return errors;

              // Check against Dataset Names
              if (datasets.some(d => d.name === name)) {
                  errors.push("Conflict with dataset name.");
              }

              // Check against Source Aliases
              if (sourceCommands.some(c => c.config.alias === name)) {
                  errors.push("Conflict with source alias.");
              }

              // Check against Other Variable Names
              if (variableCommands.some(c => c.id !== cmd.id && c.config.variableName === name)) {
                  errors.push("Variable name must be unique.");
              }
          }
          return errors;
      };

      return (
        <div className="flex flex-col h-full bg-gray-50/50">
            {/* Unified Header Style */}
            <div className="px-6 py-5 border-b border-gray-200 flex justify-between items-center bg-white sticky top-0 z-10 shadow-sm">
                <div className="flex-1 min-w-0">
                   <div className="flex items-center space-x-2 mb-1">
                     <span className="text-[10px] uppercase font-bold text-gray-400 tracking-wider">Configuration</span>
                     <span className="text-[10px] font-mono text-gray-300">#{operationId}</span>
                   </div>
                   <div className="flex items-center space-x-3">
                       <div className="relative group shrink-0 text-gray-400">
                            <Settings2 className="w-6 h-6" />
                       </div>
                       <input 
                           type="text" 
                           value={operationName} 
                           onChange={(e) => onUpdateName(e.target.value)} 
                           className="text-xl font-bold text-gray-900 bg-transparent border-none focus:ring-0 p-0 hover:bg-gray-50 pl-1 rounded transition-colors placeholder-gray-300 flex-1 min-w-0" 
                           placeholder="Operation Name" 
                       />
                   </div>
                </div>
                <div className="flex items-center pl-4 border-l border-gray-200 ml-4">
                     <button onClick={onViewPath} className="p-2 text-gray-400 hover:text-blue-600 hover:bg-blue-50 rounded-md transition-colors" title="View Logic Path"><Layers className="w-5 h-5" /></button>
                </div>
            </div>
            
            <div className="p-8 max-w-4xl mx-auto w-full h-full overflow-y-auto custom-scrollbar">
                <div className="flex flex-col space-y-4 pb-20">
                    {/* Source Configuration */}
                    <CollapsibleSection title="Configured Sources" icon={Database} count={sourceCommands.length}>
                        {sourceCommands.map((cmd, idx) => {
                            const errors = getValidationErrors(cmd, idx);
                            const hasError = errors.length > 0;
                            
                            const otherSelectedTables = new Set(
                                sourceCommands
                                    .filter(other => other.id !== cmd.id)
                                    .map(other => other.config.mainTable)
                                    .filter(Boolean)
                            );

                            const currentSelection = cmd.config.mainTable;
                            const isMissing = currentSelection && !datasets.some(d => d.name === currentSelection);

                            const options: SelectOption[] = datasets.map(d => ({
                                value: d.name,
                                label: d.name,
                                subLabel: `${d.totalCount} rows`,
                                disabled: otherSelectedTables.has(d.name),
                                icon: Database
                            }));

                            if (isMissing && currentSelection) {
                                options.push({ value: currentSelection, label: `${currentSelection} (Unavailable)`, disabled: true, icon: Database });
                            }

                            return (
                            <div key={cmd.id} className={`flex flex-col p-4 bg-gray-50 border rounded-lg group transition-all ${hasError ? 'border-red-300 bg-red-50/30' : 'border-gray-200 hover:border-blue-300 hover:shadow-sm'}`}>
                                <div className="flex items-start space-x-3">
                                    <div className="shrink-0 pt-2 text-gray-400 font-mono text-xs w-6 text-center">{idx + 1}</div>
                                    <div className="flex-1 space-y-3">
                                        <div>
                                            <label className="block text-xs font-bold text-gray-500 uppercase mb-1">Dataset</label>
                                            <CustomSelect 
                                                value={cmd.config.mainTable || ''}
                                                onChange={(val) => handleDatasetSelection(cmd.id, val, cmd.config.alias || '')}
                                                options={options}
                                                placeholder="-- Select Dataset --"
                                                icon={Database}
                                                hasError={hasError && errors[0].includes('table')}
                                            />
                                        </div>
                                        <div>
                                            <label className="block text-xs font-bold text-gray-500 uppercase mb-1">Alias Name</label>
                                            <div className="flex items-center space-x-2">
                                                <ArrowRight className="w-4 h-4 text-gray-400 shrink-0" />
                                                <input 
                                                    type="text" 
                                                    className={`w-full px-3 py-2 border rounded-md focus:ring-1 text-sm font-bold bg-blue-50/50 placeholder-blue-200 ${hasError && (errors[0].includes('Alias') || errors[0].includes('unique') || errors[0].includes('conflicts')) ? 'border-red-300 text-red-700 focus:border-red-500 focus:ring-red-200' : 'border-gray-300 text-blue-700 focus:border-blue-500 focus:ring-blue-500'}`}
                                                    value={cmd.config.alias || ''}
                                                    onChange={(e) => handleUpdateSourceCmd(cmd.id, { alias: e.target.value })}
                                                    placeholder="e.g. Users"
                                                />
                                            </div>
                                        </div>
                                    </div>
                                    <button 
                                        onClick={() => handleRemoveCmd(cmd.id)}
                                        className="p-2 text-gray-300 hover:text-red-500 hover:bg-red-50 rounded-md transition-colors mt-1 opacity-0 group-hover:opacity-100"
                                        title="Remove Source"
                                    >
                                        <Trash2 className="w-4 h-4" />
                                    </button>
                                </div>
                                {hasError && (
                                    <div className="mt-3 ml-9 flex items-start text-xs text-red-600 bg-red-100/50 p-2 rounded border border-red-100">
                                        <AlertTriangle className="w-3.5 h-3.5 mr-1.5 shrink-0 mt-0.5" />
                                        <div className="space-y-0.5">
                                            {errors.map((err, i) => (
                                                <div key={i}>{err}</div>
                                            ))}
                                        </div>
                                    </div>
                                )}
                            </div>
                            );
                        })}
                        
                        <div className="pt-2">
                            <Button variant="secondary" onClick={handleAddSource} className="w-full justify-center" icon={<Plus className="w-4 h-4"/>}>
                                Add Data Source
                            </Button>
                        </div>
                    </CollapsibleSection>

                    {/* Variable Configuration */}
                    <CollapsibleSection title="Custom Variables" icon={Variable} count={variableCommands.length} color="text-purple-500">
                        {variableCommands.length === 0 && (
                            <div className="text-center py-6 text-gray-400 text-xs italic bg-gray-50 rounded-lg border border-dashed border-gray-200">
                                No variables defined.<br/>Use variables to store values for reuse in filters.
                            </div>
                        )}
                        
                        {variableCommands.map((cmd) => {
                            const errors = getVariableValidationErrors(cmd);
                            const hasError = errors.length > 0;

                            const typeOptions: SelectOption[] = [
                                { value: 'text', label: 'Single Text', icon: Type, subLabel: 'String value' },
                                { value: 'list', label: 'Text List', icon: List, subLabel: 'Array of strings' }
                            ];

                            return (
                            <div key={cmd.id} className={`flex flex-col p-4 bg-purple-50/30 border rounded-lg group transition-all ${hasError ? 'border-red-300 bg-red-50/30' : 'border-purple-100 hover:shadow-sm'}`}>
                                <div className="flex items-start space-x-3">
                                    <div className="flex-1 space-y-3">
                                        <div className="flex space-x-3">
                                            <div className="flex-1">
                                                <label className="block text-xs font-bold text-gray-500 uppercase mb-1">Name</label>
                                                <div className="relative">
                                                    <span className="absolute inset-y-0 left-0 pl-2 flex items-center text-gray-400 font-mono text-xs">{'{'}</span>
                                                    <input 
                                                        type="text" 
                                                        className={`w-full pl-6 px-3 py-2 border rounded-md focus:ring-1 text-sm font-mono ${hasError ? 'border-red-300 focus:ring-red-200 focus:border-red-500' : 'border-gray-300 focus:ring-purple-500 focus:border-purple-500'}`}
                                                        value={cmd.config.variableName || ''}
                                                        onChange={(e) => handleUpdateSourceCmd(cmd.id, { variableName: e.target.value })}
                                                        placeholder="var_name"
                                                    />
                                                    <span className="absolute inset-y-0 right-0 pr-2 flex items-center text-gray-400 font-mono text-xs">{'}'}</span>
                                                </div>
                                            </div>
                                            <div className="w-1/3">
                                                <label className="block text-xs font-bold text-gray-500 uppercase mb-1">Type</label>
                                                <CustomSelect 
                                                    value={cmd.config.variableType || 'text'}
                                                    onChange={(val) => handleUpdateSourceCmd(cmd.id, { variableType: val })}
                                                    options={typeOptions}
                                                    icon={Variable}
                                                    className="bg-white"
                                                />
                                            </div>
                                        </div>
                                        
                                        <div>
                                            <label className="block text-xs font-bold text-gray-500 uppercase mb-1">
                                                {cmd.config.variableType === 'list' ? 'Values (Comma Separated)' : 'Value'}
                                            </label>
                                            {cmd.config.variableType === 'list' ? (
                                                <textarea 
                                                    className="w-full px-3 py-2 border border-gray-300 rounded-md focus:ring-1 focus:ring-purple-500 focus:border-purple-500 text-sm font-mono min-h-[60px]"
                                                    value={Array.isArray(cmd.config.variableValue) ? cmd.config.variableValue.join(', ') : (cmd.config.variableValue || '')}
                                                    onChange={(e) => handleUpdateSourceCmd(cmd.id, { variableValue: e.target.value.split(',').map(s => s.trim()) })}
                                                    placeholder="Value 1, Value 2, Value 3"
                                                />
                                            ) : (
                                                <input 
                                                    type="text" 
                                                    className="w-full px-3 py-2 border border-gray-300 rounded-md focus:ring-1 focus:ring-purple-500 focus:border-purple-500 text-sm"
                                                    value={cmd.config.variableValue as string || ''}
                                                    onChange={(e) => handleUpdateSourceCmd(cmd.id, { variableValue: e.target.value })}
                                                    placeholder="Enter value"
                                                />
                                            )}
                                        </div>
                                    </div>
                                    <button 
                                        onClick={() => handleRemoveCmd(cmd.id)}
                                        className="p-2 text-gray-300 hover:text-red-500 hover:bg-red-50 rounded-md transition-colors mt-1 opacity-0 group-hover:opacity-100"
                                        title="Remove Variable"
                                    >
                                        <Trash2 className="w-4 h-4" />
                                    </button>
                                </div>
                                {hasError && (
                                    <div className="mt-3 flex items-start text-xs text-red-600 bg-red-50/50 p-2 rounded border border-red-100">
                                        <AlertTriangle className="w-3.5 h-3.5 mr-1.5 shrink-0 mt-0.5" />
                                        <div className="space-y-0.5">
                                            {errors.map((err, i) => (
                                                <div key={i}>{err}</div>
                                            ))}
                                        </div>
                                    </div>
                                )}
                            </div>
                            );
                        })}

                        <div className="pt-2">
                            <Button variant="secondary" onClick={handleAddVariable} className="w-full justify-center text-purple-600 hover:text-purple-700 hover:bg-purple-50 border-purple-200" icon={<Plus className="w-4 h-4"/>}>
                                Add Variable
                            </Button>
                        </div>
                    </CollapsibleSection>
                </div>
            </div>
        </div>
      );
  }

  // -- Standard Process Mode Handlers --

  const addCommand = () => {
    const newCmd: Command = {
      id: `cmd_${Date.now()}`,
      type: 'filter',
      config: { 
        filterRoot: { id: `root_${Date.now()}`, type: 'group', logicalOperator: 'AND', conditions: [] },
        dataSource: '' // Default to empty to force selection (or imply parent if left empty, but visibly distinct)
      },
      order: commands.length + 1
    };
    onUpdateCommands(operationId, [...commands, newCmd]);
  };

  const insertCommand = (index: number) => {
    const newCmd: Command = {
      id: `cmd_${Date.now()}_${Math.random().toString(36).substr(2, 5)}`,
      type: 'filter',
      config: { 
        filterRoot: { id: `root_${Date.now()}`, type: 'group', logicalOperator: 'AND', conditions: [] },
        dataSource: '' // Default to empty
      },
      order: index + 1 // Will be reordered
    };
    
    const newCommands = [...commands];
    newCommands.splice(index, 0, newCmd);
    
    // Recalculate order
    const updated = newCommands.map((c, i) => ({ ...c, order: i + 1 }));
    onUpdateCommands(operationId, updated);
  };

  const removeCommand = (id: string) => {
    onUpdateCommands(operationId, commands.filter(c => c.id !== id));
  };

  const updateCommand = (id: string, field: string, value: any) => {
    const updated = commands.map(c => {
      if (c.id === id) {
        if (field === 'type') {
            let newConfig: any = { dataSource: '' }; // Reset to empty on type change
            // Source command removed from Process type
            if (value === 'filter') newConfig = { ...newConfig, filterRoot: { id: `root_${Date.now()}`, type: 'group', logicalOperator: 'AND', conditions: [] } };
            else if (value === 'group') newConfig = { ...newConfig, groupByFields: [], aggregations: [], havingConditions: [], outputTableName: '' };
            else if (value === 'join') newConfig = { ...newConfig, joinType: 'LEFT', joinTargetType: 'table', joinSuffix: '_joined' };
            else if (value === 'save') newConfig = { ...newConfig, field: '', value: 'var_name', distinct: true };
            else if (value === 'transform') newConfig = { ...newConfig, mappings: [{ id: `m_${Date.now()}`, mode: 'simple', expression: '', outputField: 'new_column' }] };
            else if (value === 'multi_table') newConfig = { ...newConfig, subTables: [] };
            else if (value === 'view') newConfig = { ...newConfig, dataSource: '' }; // Reset for View
            return { ...c, type: value as CommandType, config: newConfig };
        }
        if (field.startsWith('config.')) {
            const configKey = field.split('.')[1];
            return { ...c, config: { ...c.config, [configKey]: value } };
        }
        return { ...c, [field]: value };
      }
      return c;
    });
    onUpdateCommands(operationId, updated);
  };

  // ... (Mapping and Aggregation helpers remain the same as original) ...
  const addMappingRule = (cmdId: string, current: MappingRule[]) => {
      updateCommand(cmdId, 'config.mappings', [...(current || []), { id: `m_${Date.now()}`, mode: 'simple', expression: '', outputField: `new_col_${(current || []).length + 1}` }]);
  };
  const removeMappingRule = (cmdId: string, current: MappingRule[], idx: number) => {
      const newList = [...(current || [])]; newList.splice(idx, 1); updateCommand(cmdId, 'config.mappings', newList);
  };
  const updateMappingRule = (cmdId: string, current: MappingRule[], idx: number, key: keyof MappingRule, val: string) => {
      const newList = [...(current || [])]; newList[idx] = { ...newList[idx], [key]: val };
      updateCommand(cmdId, 'config.mappings', newList);
  };
  const setMappingMode = (cmdId: string, current: MappingRule[], mode: 'simple' | 'python') => {
      const newList = (current || []).map(m => {
          if (m.mode === mode) return m;
          let newExpression = m.expression;
          if (mode === 'python' && !m.expression) newExpression = PYTHON_TEMPLATE;
          if (mode === 'simple' && m.expression === PYTHON_TEMPLATE) newExpression = '';
          return { ...m, mode, expression: newExpression };
      });
      updateCommand(cmdId, 'config.mappings', newList);
  };
  const addGroupField = (cmdId: string, currentFields: string[]) => updateCommand(cmdId, 'config.groupByFields', [...(currentFields || []), '']);
  const removeGroupField = (cmdId: string, currentFields: string[], idx: number) => {
      const newFields = [...(currentFields || [])]; newFields.splice(idx, 1); updateCommand(cmdId, 'config.groupByFields', newFields);
  };
  const updateGroupField = (cmdId: string, currentFields: string[], idx: number, val: string) => {
      const newFields = [...(currentFields || [])]; newFields[idx] = val; updateCommand(cmdId, 'config.groupByFields', newFields);
  };
  const addAggregation = (cmdId: string, currentAggs: AggregationConfig[]) => updateCommand(cmdId, 'config.aggregations', [...(currentAggs || []), { field: '', func: 'count', alias: '' }]);
  const removeAggregation = (cmdId: string, currentAggs: AggregationConfig[], idx: number) => {
      const newAggs = [...(currentAggs || [])]; newAggs.splice(idx, 1); updateCommand(cmdId, 'config.aggregations', newAggs);
  };
  const updateAggregation = (cmdId: string, currentAggs: AggregationConfig[], idx: number, key: keyof AggregationConfig, val: string) => {
      const newAggs = [...(currentAggs || [])]; newAggs[idx] = { ...newAggs[idx], [key]: val };
      if (key !== 'alias' && !newAggs[idx].alias && newAggs[idx].field && newAggs[idx].func) newAggs[idx].alias = `${newAggs[idx].func}_${newAggs[idx].field}`;
      updateCommand(cmdId, 'config.aggregations', newAggs);
  };
  const addHavingCondition = (cmdId: string, current: HavingCondition[]) => updateCommand(cmdId, 'config.havingConditions', [...(current || []), { id: `h_${Date.now()}`, metricAlias: '', operator: '=', value: '' }]);
  const removeHavingCondition = (cmdId: string, current: HavingCondition[], idx: number) => {
      const newList = [...(current || [])]; newList.splice(idx, 1); updateCommand(cmdId, 'config.havingConditions', newList);
  };
  const updateHavingCondition = (cmdId: string, current: HavingCondition[], idx: number, key: keyof HavingCondition, val: any) => {
      const newList = [...(current || [])]; newList[idx] = { ...newList[idx], [key]: val };
      updateCommand(cmdId, 'config.havingConditions', newList);
  };
  const addSubTable = (cmdId: string, current: SubTableConfig[]) => {
      updateCommand(cmdId, 'config.subTables', [...(current || []), { id: `sub_${Date.now()}`, table: '', on: 'main.id = sub.uid', label: 'New Sub-Table' }]);
  };
  const removeSubTable = (cmdId: string, current: SubTableConfig[], idx: number) => {
      const newList = [...(current || [])]; newList.splice(idx, 1); updateCommand(cmdId, 'config.subTables', newList);
  };
  const updateSubTable = (cmdId: string, current: SubTableConfig[], idx: number, key: keyof SubTableConfig, val: string) => {
      const newList = [...(current || [])]; newList[idx] = { ...newList[idx], [key]: val };
      updateCommand(cmdId, 'config.subTables', newList);
  };

  const getFieldStyle = (value: string, availableFields: string[]) => {
      if (!value) return baseInputStyles;
      if (value === '*') return baseInputStyles; 
      return availableFields.includes(value) ? baseInputStyles : errorInputStyles;
  };

  return (
    <div className="flex flex-col h-full bg-gray-50/50">
      <div className="px-6 py-5 border-b border-gray-200 flex justify-between items-center bg-white sticky top-0 z-10 shadow-sm">
        <div className="flex-1 min-w-0">
           <div className="flex items-center space-x-2 mb-1">
             <span className="text-[10px] uppercase font-bold text-gray-400 tracking-wider">Operation</span>
             <span className="text-[10px] font-mono text-gray-300">#{operationId}</span>
           </div>
           <div className="flex items-center space-x-3">
               <div className="relative group shrink-0">
                    <button 
                        onClick={() => onRun && onRun()}
                        className="flex items-center justify-center w-8 h-8 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors shadow-sm active:scale-95"
                        title="Run this operation"
                    >
                        <Play className="w-4 h-4 ml-0.5" />
                    </button>
               </div>
               <input type="text" value={operationName} onChange={(e) => onUpdateName(e.target.value)} className="text-xl font-bold text-gray-900 bg-transparent border-none focus:ring-0 p-0 hover:bg-gray-50 pl-1 rounded transition-colors placeholder-gray-300 flex-1 min-w-0" placeholder="Operation Name" />
           </div>
        </div>
        <div className="flex items-center pl-4 border-l border-gray-200 ml-4">
             <button onClick={onViewPath} className="p-2 text-gray-400 hover:text-blue-600 hover:bg-blue-50 rounded-md transition-colors" title="View Logic Path"><Layers className="w-5 h-5" /></button>
        </div>
      </div>

      <div className="flex-1 overflow-y-auto p-6 space-y-0">
        {commands.length === 0 ? (
          <div className="flex flex-col items-center justify-center py-12 text-gray-400 border-2 border-dashed border-gray-200 rounded-xl bg-gray-50/50 hover:bg-gray-50 transition-colors cursor-pointer" onClick={addCommand}>
            <Plus className="w-8 h-8 mb-2 opacity-50" />
            <p className="font-medium text-sm">Add your first command</p>
          </div>
        ) : (
          <>
            {commands.map((cmd, index) => {
                let activeSchema: Record<string, DataType> = {};
                
                if (cmd.config.dataSource) {
                    const sourceAlias = availableSourceAliases.find(sa => sa.linkId === cmd.config.dataSource);
                    let targetDatasetName = "";
                    if (sourceAlias) {
                        targetDatasetName = sourceAlias.sourceTable || "";
                    } else {
                        // Assume it's a direct table name (generated or otherwise)
                        targetDatasetName = cmd.config.dataSource;
                    }

                    if (targetDatasetName) {
                        const ds = datasets.find(d => d.name === targetDatasetName);
                        if (ds) {
                             if (ds.fieldTypes) {
                                 Object.entries(ds.fieldTypes).forEach(([k, v]) => activeSchema[k] = (v as FieldInfo).type);
                             } else {
                                 ds.fields.forEach(f => activeSchema[f] = 'string');
                             }
                        } else {
                            // Fallback: Check globally if it's a generated table not in datasets yet (schema might be missing but we allow selection)
                            if (allGeneratedTablesGlobal.includes(targetDatasetName)) {
                                // No schema available yet for un-executed generated tables
                            }
                        }
                    }
                }
                const activeSchemaRef = activeSchema;
                const fieldNames = Object.keys(activeSchema);

                // Calculate available tables for this specific command index
                const localPrecedingOutputs = commands
                    .slice(0, index)
                    .filter(c => c.type === 'group' && c.config.outputTableName && c.config.outputTableName.trim() !== '')
                    .map(c => c.config.outputTableName!.trim());
                
                const availableGeneratedTablesForCmd = Array.from(new Set([...ancestorOutputs, ...localPrecedingOutputs])).sort();

                // Calculate Local Variables defined in previous steps of THIS operation
                // Force convert to string to handle 'any' type from CommandConfig
                const localVariables = commands
                    .slice(0, index)
                    .filter(c => c.type === 'save' && c.config.value)
                    .map(c => String(c.config.value));
                
                // Combine Ancestor + Local Variables for this command's context and deduplicate
                const currentScopeVariables = Array.from(new Set([...ancestorVariables, ...localVariables]));

                const isSourceRequired = index === 0 && (!ancestors || ancestors.length === 0);
                const isMissingSource = isSourceRequired && !cmd.config.dataSource;

                return (
                <React.Fragment key={cmd.id}>
                    <InsertDivider index={index} onInsert={insertCommand} />
                    <div className="relative group bg-white border border-gray-200 rounded-lg shadow-sm hover:shadow-md transition-all duration-200">
                        <div className="flex items-center justify-between p-3 border-b border-gray-100 bg-white rounded-t-lg">
                            <div className="flex items-center space-x-3 overflow-hidden">
                                <div className="cursor-move text-gray-300 hover:text-gray-500"><GripVertical className="w-4 h-4" /></div>
                                <div className="flex items-center">
                                    <select value={cmd.type} onChange={(e) => updateCommand(cmd.id, 'type', e.target.value)} className="text-sm font-bold text-gray-800 bg-transparent border-none focus:ring-0 cursor-pointer hover:text-blue-600 pl-0 pr-6 py-0">
                                        <option value="filter">Filter</option>
                                        <option value="join">Join</option>
                                        <option value="sort">Sort</option>
                                        <option value="transform">Mapping</option>
                                        <option value="group">Group</option>
                                        <option value="save">Save Variable</option>
                                        <option value="view">View / Select Table</option>
                                        <option value="multi_table">Complex View (Final Step)</option>
                                    </select>
                                </div>
                            </div>
                            <div className="flex items-center space-x-2">
                                {/* Run to Step Button */}
                                <button 
                                    onClick={() => onRun && onRun(cmd.id)}
                                    className="p-1 text-gray-400 hover:text-blue-600 hover:bg-blue-50 rounded transition-colors"
                                    title="Run logic up to this step"
                                >
                                    <Play className="w-3.5 h-3.5" />
                                </button>
                                <span className="text-[10px] font-mono text-gray-300">#{index + 1}</span>
                                <button onClick={() => removeCommand(cmd.id)} className="p-1 text-gray-300 hover:text-red-500 hover:bg-red-50 rounded transition-colors"><Trash2 className="w-3.5 h-3.5" /></button>
                            </div>
                        </div>

                        {cmd.type !== 'view' && (
                            <div className={`px-3 py-1.5 border-b border-gray-100 flex items-center space-x-2 ${isMissingSource ? 'bg-red-50/50' : 'bg-gray-50'}`}>
                                <Database className={`w-3 h-3 ${isMissingSource ? 'text-red-400' : 'text-gray-400'}`} />
                                <span className="text-[10px] font-semibold text-gray-500 uppercase tracking-wide">Select Dataset:</span>
                                <select
                                        value={cmd.config.dataSource || ''}
                                        onChange={(e) => updateCommand(cmd.id, 'config.dataSource', e.target.value)}
                                        className={`bg-transparent text-xs font-medium focus:outline-none cursor-pointer border-none p-0 pr-4 hover:underline ${isMissingSource ? 'text-red-600' : 'text-blue-700'}`}
                                    >
                                        <option value="">{isSourceRequired ? "-- Select Source --" : "Inherit (Use Incoming Data)"}</option>
                                        {availableSourceAliases.length > 0 && (
                                            <optgroup label="Data Sources">
                                                {availableSourceAliases.map(sa => (
                                                    <option key={sa.linkId} value={sa.linkId}>{sa.alias} to {sa.sourceTable}</option>
                                                ))}
                                            </optgroup>
                                        )}
                                        {availableGeneratedTablesForCmd.length > 0 && (
                                            <optgroup label="Generated Datasets">
                                                {availableGeneratedTablesForCmd.map(name => (
                                                    <option key={name} value={name}>{name}</option>
                                                ))}
                                            </optgroup>
                                        )}
                                    </select>
                            </div>
                        )}

                        <div className="p-4">
                            {cmd.type === 'filter' && cmd.config.filterRoot && (
                                <div className="space-y-4">
                                    <label className="text-xs font-bold text-gray-500 uppercase flex items-center"><FilterIcon className="w-3 h-3 mr-1"/> Rule Builder</label>
                                    <FilterGroupEditor 
                                        group={cmd.config.filterRoot} 
                                        activeSchema={activeSchemaRef} 
                                        onUpdate={(g) => updateCommand(cmd.id, 'config.filterRoot', g)}
                                        onRemove={() => {}}
                                        isRoot={true}
                                        availableVariables={currentScopeVariables}
                                    />
                                </div>
                            )}

                            {/* View Command - Simple Selector */}
                            {cmd.type === 'view' && (
                                <div className="space-y-4">
                                    <div className="flex items-start space-x-3 p-3 bg-purple-50 border border-purple-100 rounded-lg text-sm text-purple-900">
                                        <Eye className="w-5 h-5 shrink-0 mt-0.5" />
                                        <div>
                                            <p className="font-semibold">Explicit View Selection</p>
                                            <p className="text-xs text-purple-700 mt-1">
                                                This step forces the pipeline to output the selected table below. Use this to verify intermediate results or saved tables.
                                            </p>
                                        </div>
                                    </div>
                                    
                                    <div className="flex flex-col">
                                        <label className="block text-xs font-bold text-gray-500 uppercase mb-2">Table to View</label>
                                        <select 
                                            className={`${baseInputStyles} py-2`}
                                            value={cmd.config.dataSource || ''} 
                                            onChange={(e) => updateCommand(cmd.id, 'config.dataSource', e.target.value)}
                                        >
                                            <option value="">-- Select Table --</option>
                                            {availableSourceAliases.length > 0 && (
                                                <optgroup label="Data Sources">
                                                    {availableSourceAliases.map(sa => (
                                                        <option key={sa.linkId} value={sa.linkId}>{sa.alias} to {sa.sourceTable}</option>
                                                    ))}
                                                </optgroup>
                                            )}
                                            {availableGeneratedTablesForCmd.length > 0 && (
                                                <optgroup label="Generated Datasets">
                                                    {availableGeneratedTablesForCmd.map(name => (
                                                        <option key={name} value={name}>{name}</option>
                                                    ))}
                                                </optgroup>
                                            )}
                                        </select>
                                    </div>
                                </div>
                            )}

                            {/* ... (Other command types rendering, identical to original but preserved) ... */}
                            {cmd.type === 'multi_table' && (
                                <div className="space-y-4">
                                    <div className="flex items-start space-x-3 p-3 bg-blue-50 border border-blue-100 rounded-lg text-sm text-blue-800">
                                        <LayoutDashboard className="w-5 h-5 shrink-0 mt-0.5" />
                                        <div>
                                            <p className="font-semibold">Complex View Configuration</p>
                                            <p className="text-xs text-blue-600 mt-1">This command displays the main result stream alongside multiple related sub-tables.</p>
                                        </div>
                                    </div>
                                    <div>
                                        <label className="text-xs font-bold text-gray-500 uppercase mb-2 flex items-center">Sub-Tables (Linked Data)</label>
                                        <div className="space-y-3">
                                            {(cmd.config.subTables || []).map((sub, i) => (
                                                <div key={sub.id} className="grid grid-cols-12 gap-2 items-center bg-gray-50 p-2 rounded border border-gray-100">
                                                    <div className="col-span-3">
                                                        <select className={baseInputStyles} value={sub.table} onChange={(e) => updateSubTable(cmd.id, cmd.config.subTables || [], i, 'table', e.target.value)}>
                                                            <option value="">Select Source...</option>
                                                            {availableSourceAliases.map(sa => (
                                                                <option key={sa.linkId} value={sa.linkId}>{sa.alias} to {sa.sourceTable}</option>
                                                            ))}
                                                            {availableGeneratedTablesForCmd.map(name => (
                                                                <option key={name} value={name}>{name}</option>
                                                            ))}
                                                        </select>
                                                    </div>
                                                    <div className="col-span-3">
                                                        <input className={baseInputStyles} placeholder="Tab Name" value={sub.label} onChange={(e) => updateSubTable(cmd.id, cmd.config.subTables || [], i, 'label', e.target.value)} />
                                                    </div>
                                                    <div className="col-span-5 relative">
                                                        <div className="relative">
                                                            <input className={`${baseInputStyles} pr-8`} placeholder="main.id = sub.user_id" value={sub.on} onChange={(e) => updateSubTable(cmd.id, cmd.config.subTables || [], i, 'on', e.target.value)} />
                                                            <VariableInserter variables={currentScopeVariables} onInsert={(v) => updateSubTable(cmd.id, cmd.config.subTables || [], i, 'on', sub.on ? `${sub.on} ${v}` : v)} />
                                                        </div>
                                                    </div>
                                                    <div className="col-span-1 flex items-end justify-center pb-1">
                                                        <button onClick={() => removeSubTable(cmd.id, cmd.config.subTables || [], i)} className="text-red-400 hover:text-red-600"><Trash2 className="w-4 h-4" /></button>
                                                    </div>
                                                </div>
                                            ))}
                                            <Button size="sm" variant="secondary" onClick={() => addSubTable(cmd.id, cmd.config.subTables || [])} icon={<Plus className="w-3 h-3"/>}>Add Sub-Table</Button>
                                        </div>
                                    </div>
                                </div>
                            )}

                            {cmd.type === 'group' && (
                                <div className="space-y-4">
                                    <div className="grid grid-cols-12 gap-6">
                                        <div className="col-span-5">
                                            <label className="text-xs font-bold text-gray-500 uppercase mb-2 flex items-center"><Layers className="w-3 h-3 mr-1"/> Group By</label>
                                            <div className="space-y-2">
                                                {(cmd.config.groupByFields || []).map((field, i) => (
                                                    <div key={i} className="flex space-x-2">
                                                        <select className={getFieldStyle(field, fieldNames)} value={field} onChange={(e) => updateGroupField(cmd.id, cmd.config.groupByFields || [], i, e.target.value)}>
                                                            <option value="">Select Field...</option>
                                                            {fieldNames.map(f => <option key={f} value={f}>{f}</option>)}
                                                        </select>
                                                        <button onClick={() => removeGroupField(cmd.id, cmd.config.groupByFields || [], i)} className="text-red-400 hover:text-red-600"><Trash2 className="w-4 h-4" /></button>
                                                    </div>
                                                ))}
                                                <button onClick={() => addGroupField(cmd.id, cmd.config.groupByFields || [])} className="text-xs text-blue-600 hover:underline flex items-center font-medium"><Plus className="w-3 h-3 mr-1" /> Add Column</button>
                                            </div>
                                        </div>
                                        <div className="col-span-7">
                                            <label className="text-xs font-bold text-gray-500 uppercase mb-2 flex items-center"><Calculator className="w-3 h-3 mr-1"/> Metrics</label>
                                            <div className="space-y-2">
                                                {(cmd.config.aggregations || []).map((agg, i) => (
                                                    <div key={i} className="flex space-x-2 items-center">
                                                        <select className={`${baseInputStyles} w-24 shrink-0`} value={agg.func} onChange={(e) => updateAggregation(cmd.id, cmd.config.aggregations || [], i, 'func', e.target.value)}>
                                                            <option value="count">Count</option><option value="sum">Sum</option><option value="mean">Avg</option><option value="min">Min</option><option value="max">Max</option>
                                                        </select>
                                                        <select className={getFieldStyle(agg.field, fieldNames)} value={agg.field} onChange={(e) => updateAggregation(cmd.id, cmd.config.aggregations || [], i, 'field', e.target.value)}>
                                                            <option value="">Field...</option><option value="*">Any (*)</option>{fieldNames.map(f => <option key={f} value={f}>{f}</option>)}
                                                        </select>
                                                        <input className={`${baseInputStyles} w-24 shrink-0`} placeholder="As..." value={agg.alias} onChange={(e) => updateAggregation(cmd.id, cmd.config.aggregations || [], i, 'alias', e.target.value)} />
                                                        <button onClick={() => removeAggregation(cmd.id, cmd.config.aggregations || [], i)} className="text-red-400 hover:text-red-600"><Trash2 className="w-4 h-4" /></button>
                                                    </div>
                                                ))}
                                                <button onClick={() => addAggregation(cmd.id, cmd.config.aggregations || [])} className="text-xs text-blue-600 hover:underline flex items-center font-medium"><Plus className="w-3 h-3 mr-1" /> Add Metric</button>
                                            </div>
                                        </div>
                                    </div>
                                    <div className="border-t border-gray-100 pt-4">
                                        <label className="text-xs font-bold text-gray-500 uppercase mb-2 flex items-center"><FilterIcon className="w-3 h-3 mr-1"/> Having</label>
                                        <div className="space-y-2">
                                            {(cmd.config.havingConditions || []).map((cond, i) => (
                                                <div key={cond.id} className="flex space-x-2 items-center">
                                                    <select className={baseInputStyles} value={cond.metricAlias} onChange={(e) => updateHavingCondition(cmd.id, cmd.config.havingConditions || [], i, 'metricAlias', e.target.value)}>
                                                        <option value="">Metric...</option>
                                                        {(cmd.config.aggregations || []).map(agg => agg.alias && <option key={agg.alias} value={agg.alias}>{agg.alias}</option>)}
                                                    </select>
                                                    <select className={`${baseInputStyles} w-24 shrink-0`} value={cond.operator} onChange={(e) => updateHavingCondition(cmd.id, cmd.config.havingConditions || [], i, 'operator', e.target.value)}>
                                                        {OPERATORS['number'].map(op => <option key={op.value} value={op.value}>{op.label}</option>)}
                                                    </select>
                                                    <input className={baseInputStyles} placeholder="Value" value={cond.value} onChange={(e) => updateHavingCondition(cmd.id, cmd.config.havingConditions || [], i, 'value', e.target.value)} />
                                                    <button onClick={() => removeHavingCondition(cmd.id, cmd.config.havingConditions || [], i)} className="text-red-400 hover:text-red-600"><Trash2 className="w-4 h-4" /></button>
                                                </div>
                                            ))}
                                            <button onClick={() => addHavingCondition(cmd.id, cmd.config.havingConditions || [])} className="text-xs text-blue-600 hover:underline flex items-center font-medium"><Plus className="w-3 h-3 mr-1" /> Add Condition</button>
                                        </div>
                                    </div>
                                    <div className="border-t border-gray-100 pt-4 flex items-center space-x-3">
                                        <Table className="w-3.5 h-3.5 text-blue-500"/>
                                        <input className={`${baseInputStyles} max-w-[200px]`} placeholder="Output Table Name" value={cmd.config.outputTableName || ''} onChange={(e) => updateCommand(cmd.id, 'config.outputTableName', e.target.value)} />
                                    </div>
                                </div>
                            )}

                            {cmd.type === 'join' && (
                                <div className="grid grid-cols-12 gap-3">
                                    <div className="col-span-4">
                                        <select className={baseInputStyles} value={cmd.config.joinTargetType || 'table'} onChange={(e) => updateCommand(cmd.id, 'config.joinTargetType', e.target.value)}>
                                            <option value="table">Table</option>
                                            <option value="node">Operation Node</option>
                                        </select>
                                    </div>
                                    <div className="col-span-8">
                                        {cmd.config.joinTargetType === 'node' ? (
                                            <select className={baseInputStyles} value={cmd.config.joinTargetNodeId || ''} onChange={(e) => updateCommand(cmd.id, 'config.joinTargetNodeId', e.target.value)}>
                                                <option value="">-- Select Operation --</option>
                                                {availableNodes.map(n => <option key={n.id} value={n.id}>{n.name}</option>)}
                                            </select>
                                        ) : (
                                            <select className={baseInputStyles} value={cmd.config.joinTable || ''} onChange={(e) => updateCommand(cmd.id, 'config.joinTable', e.target.value)}>
                                                <option value="">-- Select Source --</option>
                                                {availableSourceAliases.length > 0 && (
                                                    <optgroup label="Data Sources">
                                                        {availableSourceAliases.map(sa => (
                                                            <option key={sa.linkId} value={sa.linkId}>{sa.alias} to {sa.sourceTable}</option>
                                                        ))}
                                                    </optgroup>
                                                )}
                                                {availableGeneratedTablesForCmd.length > 0 && (
                                                    <optgroup label="Generated Datasets">
                                                        {availableGeneratedTablesForCmd.map(name => (
                                                            <option key={name} value={name}>{name}</option>
                                                        ))}
                                                    </optgroup>
                                                )}
                                            </select>
                                        )}
                                    </div>
                                    <div className="col-span-12"><input className={baseInputStyles} value={cmd.config.on || ''} onChange={(e) => updateCommand(cmd.id, 'config.on', e.target.value)} placeholder="ON Condition (e.g. id = user_id)" /></div>
                                </div>
                            )}

                            {cmd.type === 'save' && (
                                <div className="grid grid-cols-12 gap-4">
                                    <div className="col-span-4">
                                        <select className={getFieldStyle(cmd.config.field || '', fieldNames)} value={cmd.config.field || ''} onChange={(e) => updateCommand(cmd.id, 'config.field', e.target.value)}>
                                            <option value="">Select Field...</option>
                                            {fieldNames.map(f => <option key={f} value={f}>{f}</option>)}
                                        </select>
                                    </div>
                                    <div className="col-span-4">
                                        <div className="flex bg-gray-100 rounded-md p-0.5">
                                            <button onClick={() => updateCommand(cmd.id, 'config.distinct', false)} className={`flex-1 py-1 rounded-sm text-[10px] font-bold ${!cmd.config.distinct ? 'bg-white shadow text-blue-600' : 'text-gray-500'}`}>All</button>
                                            <button onClick={() => updateCommand(cmd.id, 'config.distinct', true)} className={`flex-1 py-1 rounded-sm text-[10px] font-bold ${cmd.config.distinct ? 'bg-white shadow text-blue-600' : 'text-gray-500'}`}>Distinct</button>
                                        </div>
                                    </div>
                                    <div className="col-span-4">
                                        <input className={baseInputStyles} placeholder="var_name" value={cmd.config.value || ''} onChange={(e) => updateCommand(cmd.id, 'config.value', e.target.value)} />
                                    </div>
                                </div>
                            )}

                            {cmd.type === 'transform' && (
                                <div className="space-y-4">
                                    <div className="flex bg-gray-100 rounded-md p-0.5 w-fit">
                                        <button onClick={() => setMappingMode(cmd.id, cmd.config.mappings || [], 'simple')} className={`px-3 py-1 rounded-sm text-[10px] font-bold ${!cmd.config.mappings?.some(m => m.mode === 'python') ? 'bg-white shadow text-blue-600' : 'text-gray-500'}`}>Simple</button>
                                        <button onClick={() => setMappingMode(cmd.id, cmd.config.mappings || [], 'python')} className={`px-3 py-1 rounded-sm text-[10px] font-bold ${cmd.config.mappings?.some(m => m.mode === 'python') ? 'bg-white shadow text-blue-600' : 'text-gray-500'}`}>Python</button>
                                    </div>
                                    <div className="space-y-3">
                                        {(cmd.config.mappings || []).map((mapping, i) => (
                                            <div key={mapping.id} className="flex space-x-3 items-start border p-3 rounded-lg bg-gray-50">
                                                <div className="flex-1 space-y-2">
                                                    {mapping.mode === 'python' ? (
                                                        <textarea className={codeAreaStyles} rows={3} value={mapping.expression} onChange={(e) => updateMappingRule(cmd.id, cmd.config.mappings || [], i, 'expression', e.target.value)} placeholder={PYTHON_TEMPLATE} />
                                                    ) : (
                                                        <input className={baseInputStyles} value={mapping.expression} onChange={(e) => updateMappingRule(cmd.id, cmd.config.mappings || [], i, 'expression', e.target.value)} placeholder="Expression" />
                                                    )}
                                                    <input className={baseInputStyles} value={mapping.outputField} onChange={(e) => updateMappingRule(cmd.id, cmd.config.mappings || [], i, 'outputField', e.target.value)} placeholder="Output Field" />
                                                </div>
                                                <button onClick={() => removeMappingRule(cmd.id, cmd.config.mappings || [], i)} className="text-red-400 hover:text-red-600"><Trash2 className="w-4 h-4" /></button>
                                            </div>
                                        ))}
                                    </div>
                                    <button onClick={() => addMappingRule(cmd.id, cmd.config.mappings || [])} className="text-xs text-blue-600 hover:underline"><Plus className="w-3 h-3 inline mr-1" />Add Mapping</button>
                                </div>
                            )}

                            {cmd.type === 'sort' && (
                                    <div className="grid grid-cols-12 gap-3">
                                    <div className="col-span-8">
                                        <select className={getFieldStyle(cmd.config.field || '', fieldNames)} value={cmd.config.field || ''} onChange={(e) => updateCommand(cmd.id, 'config.field', e.target.value)}>
                                            <option value="">Select Field...</option>
                                            {fieldNames.map(f => <option key={f} value={f}>{f}</option>)}
                                        </select>
                                    </div>
                                    <div className="col-span-4">
                                        <select className={baseInputStyles} value={cmd.config.ascending === false ? 'desc' : 'asc'} onChange={(e) => updateCommand(cmd.id, 'config.ascending', e.target.value === 'asc')}>
                                            <option value="asc">Asc</option><option value="desc">Desc</option>
                                        </select>
                                    </div>
                                    </div>
                            )}
                        </div>
                    </div>
                </React.Fragment>
                );
            })}
            
            {hasComplexView ? (
                <div className="flex justify-center pt-4 text-xs text-orange-500 font-medium bg-orange-50 p-2 rounded border border-orange-100 mx-4">
                    <Info className="w-4 h-4 mr-2 inline" />
                    Complex View must be the final step in this operation.
                </div>
            ) : (
                <div className="flex justify-center pt-2">
                    <Button variant="secondary" size="sm" onClick={addCommand} icon={<Plus className="w-4 h-4" />}>Add Step</Button>
                </div>
            )}
          </>
        )}
      </div>
    </div>
  );
};