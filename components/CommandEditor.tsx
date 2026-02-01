
import React, { useMemo, useState } from 'react';
import { Command, CommandType, Dataset, OperationType, AggregationConfig, OperationNode, DataType, HavingCondition, MappingRule, FilterGroup, FilterCondition, SubTableConfig } from '../types';
import { Button } from './Button';
import { Trash2, Plus, GripVertical, Type, Hash, Calendar, Clock, CheckCircle, Code, Database, Play, Layers, Braces, Save, Share2, ArrowRight, AlertCircle, Filter as FilterIcon, Table, Calculator, List, Check, Wand2, Info, ChevronRight, Split, LayoutDashboard, AlertTriangle } from 'lucide-react';

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
  tree?: OperationNode; 
}

const PYTHON_TEMPLATE = `def transform(row):
    # Available: np, pd, math, datetime, re
    # Name must be 'transform', return the calculated value
    val = row.get('id', 0)
    return val * 1.1`;

const DATA_TYPE_ICONS: Record<string, any> = {
    string: Type,
    number: Hash,
    boolean: CheckCircle,
    date: Calendar,
    timestamp: Clock,
    json: Code,
};

const OPERATION_TYPES: {value: OperationType, label: string, icon: any}[] = [
    { value: 'dataset', label: 'Data Source', icon: Database },
    { value: 'process', label: 'Process', icon: Play },
];

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
                    vars.push(cmd.config.value as string);
                }
            });
            return true;
        }
        return false;
    };
    if (root) traverse(root);
    return vars;
};

// --- HELPER COMPONENTS ---

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
            {/* The hit area is the full height, line appears in middle */}
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

// --- RECURSIVE FILTER COMPONENTS ---

interface FilterGroupEditorProps {
    group: FilterGroup;
    activeSchema: Record<string, DataType>;
    onUpdate: (updated: FilterGroup) => void;
    onRemove: (id: string) => void;
    isRoot?: boolean;
}

const FilterGroupEditor: React.FC<FilterGroupEditorProps> = ({ group, activeSchema, onUpdate, onRemove, isRoot = false }) => {
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
                        />
                    ) : (
                        <div key={item.id} className="grid grid-cols-12 gap-2 items-center bg-gray-50/50 p-2 rounded-md border border-gray-100 group/cond">
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
                            <div className="col-span-3">
                                <input 
                                    className={`${baseInputStyles} py-1 px-2`} 
                                    placeholder="Value" 
                                    value={String(item.value)} 
                                    onChange={(e) => handleUpdateCondition(item.id, { value: e.target.value })} 
                                />
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
  inputSchema,
  onUpdateCommands,
  onUpdateName,
  onUpdateType,
  onViewPath,
  tree
}) => {
  
  const availableVariables = useMemo(() => {
     if (!tree) return [];
     return findAncestorVariables(tree, operationId);
  }, [tree, operationId]);

  const availableNodes = useMemo(() => {
      if (!tree) return [];
      return flattenNodes(tree).filter(n => n.id !== operationId);
  }, [tree, operationId]);

  const hasComplexView = useMemo(() => commands.some(c => c.type === 'multi_table'), [commands]);

  const addCommand = () => {
    const newCmd: Command = {
      id: `cmd_${Date.now()}`,
      type: 'filter',
      config: { 
        filterRoot: { id: `root_${Date.now()}`, type: 'group', logicalOperator: 'AND', conditions: [] },
        dataSource: 'stream' 
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
        dataSource: 'stream' 
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
            let newConfig: any = { dataSource: 'stream' }; 
            if (value === 'source') newConfig = { ...newConfig, mainTable: '' };
            else if (value === 'filter') newConfig = { ...newConfig, filterRoot: { id: `root_${Date.now()}`, type: 'group', logicalOperator: 'AND', conditions: [] } };
            else if (value === 'group') newConfig = { ...newConfig, groupByFields: [], aggregations: [], havingConditions: [], outputTableName: '' };
            else if (value === 'join') newConfig = { ...newConfig, joinType: 'left', joinTargetType: 'table', joinSuffix: '_joined' };
            else if (value === 'save') newConfig = { ...newConfig, field: '', value: 'var_name', distinct: true };
            else if (value === 'transform') newConfig = { ...newConfig, mappings: [{ id: `m_${Date.now()}`, mode: 'simple', expression: '', outputField: 'new_column' }] };
            else if (value === 'multi_table') newConfig = { ...newConfig, subTables: [] };
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

  const addMappingRule = (cmdId: string, current: MappingRule[]) => {
      updateCommand(cmdId, 'config.mappings', [...(current || []), { id: `m_${Date.now()}`, mode: 'simple', expression: '', outputField: `new_col_${(current || []).length + 1}` }]);
  };

  const removeMappingRule = (cmdId: string, current: MappingRule[], idx: number) => {
      const newList = [...(current || [])];
      newList.splice(idx, 1);
      updateCommand(cmdId, 'config.mappings', newList);
  };

  const updateMappingRule = (cmdId: string, current: MappingRule[], idx: number, key: keyof MappingRule, val: string) => {
      const newList = [...(current || [])];
      newList[idx] = { ...newList[idx], [key]: val };
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

  // Multi-table functions
  const addSubTable = (cmdId: string, current: SubTableConfig[]) => {
      // Default to main.id = sub.uid as it is a common mock pattern
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

  const getFieldSchemaAtStep = (targetIndex: number) => {
      let currentFields: Record<string, DataType> = { ...inputSchema };

      for (let i = 0; i < targetIndex; i++) {
          const cmd = commands[i];
          const source = cmd.config.dataSource;
          
          if (source && source !== 'stream' && source !== '') {
              const ds = datasets.find(d => d.name === source);
              if (ds) {
                  // Fix: Properly map Record<string, FieldInfo> to Record<string, DataType>
                  currentFields = {};
                  if (ds.fieldTypes) {
                      Object.keys(ds.fieldTypes).forEach(f => {
                          currentFields[f] = ds.fieldTypes![f].type;
                      });
                  } else {
                      ds.fields.forEach(f => currentFields[f] = 'string');
                  }
              }
          }

          if (cmd.type === 'source' && cmd.config.mainTable) {
               const ds = datasets.find(d => d.name === cmd.config.mainTable);
               if (ds) {
                   // Fix: Properly map Record<string, FieldInfo> to Record<string, DataType>
                   currentFields = {};
                   if (ds.fieldTypes) {
                       Object.keys(ds.fieldTypes).forEach(f => {
                           currentFields[f] = ds.fieldTypes![f].type;
                       });
                   } else {
                       ds.fields.forEach(f => currentFields[f] = 'string');
                   }
               }
          }

          if (cmd.type === 'transform') {
               const mappings = cmd.config.mappings || [];
               if (mappings.length > 0) {
                   mappings.forEach(m => {
                       if (m.outputField) currentFields[m.outputField] = 'number';
                   });
               } else if (cmd.config.outputField) {
                    currentFields[cmd.config.outputField] = 'number'; 
               }
          }
          
          if (cmd.type === 'join' && cmd.config.joinTable) {
               const ds = datasets.find(d => d.name === cmd.config.joinTable);
               if (ds) {
                   const suffix = cmd.config.joinSuffix || '_joined';
                   const joinSchema = ds.fieldTypes || {};
                   (Object.keys(joinSchema).length ? Object.keys(joinSchema) : ds.fields).forEach(f => {
                       // Fix: Extract .type from FieldInfo
                       const type = joinSchema[f]?.type || 'string';
                       currentFields[currentFields[f] ? `${f}${suffix}` : f] = type as DataType;
                   });
               }
          }
          
          if (cmd.type === 'group') {
              const nextSchema: Record<string, DataType> = {};
              (cmd.config.groupByFields || []).forEach(g => { if (currentFields[g]) nextSchema[g] = currentFields[g]; });
              (cmd.config.aggregations || []).forEach(agg => { nextSchema[agg.alias || `${agg.func}_${agg.field}`] = 'number'; });
              if (Object.keys(nextSchema).length > 0) currentFields = nextSchema;
          }
      }
      return currentFields;
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
                    <button className="flex items-center justify-center w-8 h-8 bg-blue-50 text-blue-600 rounded-lg hover:bg-blue-100 transition-colors">
                        {React.createElement(OPERATION_TYPES.find(t => t.value === operationType)?.icon || Layers, { className: "w-5 h-5" })}
                    </button>
                    <div className="absolute top-full left-0 mt-1 w-48 bg-white border border-gray-200 rounded-lg shadow-xl hidden group-hover:block z-20">
                        {OPERATION_TYPES.map(type => (
                            <button key={type.value} onClick={() => onUpdateType(operationId, type.value)} className="flex items-center w-full px-4 py-2 text-sm text-gray-700 hover:bg-gray-50 text-left">
                                {React.createElement(type.icon, { className: "w-4 h-4 mr-2 text-gray-400" })}
                                {type.label}
                            </button>
                        ))}
                    </div>
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
                const stepSchema = getFieldSchemaAtStep(index);
                let activeSchema = { ...stepSchema };
                if (cmd.config.dataSource && cmd.config.dataSource !== 'stream' && cmd.config.dataSource !== '') {
                    const ds = datasets.find(d => d.name === cmd.config.dataSource);
                    if (ds) {
                        activeSchema = {};
                        if (ds.fieldTypes) {
                            Object.keys(ds.fieldTypes).forEach(f => {
                                activeSchema[f] = ds.fieldTypes![f].type;
                            });
                        } else {
                            ds.fields.forEach(f => activeSchema[f] = 'string');
                        }
                    }
                }

                const fieldNames = Object.keys(activeSchema);
                const isSource = cmd.type === 'source';

                return (
                <React.Fragment key={cmd.id}>
                    <InsertDivider index={index} onInsert={insertCommand} />
                    <div className="relative group bg-white border border-gray-200 rounded-lg shadow-sm hover:shadow-md transition-all duration-200">
                        <div className="flex items-center justify-between p-3 border-b border-gray-100 bg-white rounded-t-lg">
                            <div className="flex items-center space-x-3 overflow-hidden">
                                <div className="cursor-move text-gray-300 hover:text-gray-500"><GripVertical className="w-4 h-4" /></div>
                                <div className="flex items-center">
                                    <select value={cmd.type} onChange={(e) => updateCommand(cmd.id, 'type', e.target.value)} className="text-sm font-bold text-gray-800 bg-transparent border-none focus:ring-0 cursor-pointer hover:text-blue-600 pl-0 pr-6 py-0">
                                        <option value="source">Load Table</option>
                                        <option value="filter">Filter</option>
                                        <option value="join">Join</option>
                                        <option value="sort">Sort</option>
                                        <option value="transform">Mapping</option>
                                        <option value="group">Group</option>
                                        <option value="save">Save Variable</option>
                                        <option value="multi_table">Complex View (Final Step)</option>
                                    </select>
                                </div>
                            </div>
                            <div className="flex items-center space-x-2">
                                <span className="text-[10px] font-mono text-gray-300">#{index + 1}</span>
                                <button onClick={() => removeCommand(cmd.id)} className="p-1 text-gray-300 hover:text-red-500 hover:bg-red-50 rounded transition-colors"><Trash2 className="w-3.5 h-3.5" /></button>
                            </div>
                        </div>

                        {!isSource && (
                            <div className={`px-3 py-1.5 border-b border-gray-100 flex items-center space-x-2 ${!cmd.config.dataSource ? 'bg-red-50/50' : 'bg-gray-50'}`}>
                                <Database className={`w-3 h-3 ${!cmd.config.dataSource ? 'text-red-400' : 'text-gray-400'}`} />
                                <span className="text-[10px] font-semibold text-gray-500 uppercase tracking-wide">Apply To:</span>
                                <select
                                        value={cmd.config.dataSource || ''}
                                        onChange={(e) => updateCommand(cmd.id, 'config.dataSource', e.target.value)}
                                        className={`bg-transparent text-xs font-medium focus:outline-none cursor-pointer border-none p-0 pr-4 hover:underline ${!cmd.config.dataSource ? 'text-red-600' : 'text-blue-700'}`}
                                    >
                                        <option value="">-- Missing Source --</option>
                                        <option value="stream">Parent Stream</option>
                                        <optgroup label="Data Sources">
                                            {datasets.map(d => (
                                                <option key={d.id} value={d.name}>{d.name}</option>
                                            ))}
                                        </optgroup>
                                    </select>
                            </div>
                        )}

                        <div className="p-4">
                            {isSource && (
                                <div className="flex items-center space-x-3">
                                    <label className="text-sm text-gray-600">Select Table:</label>
                                    <select className={baseInputStyles} value={cmd.config.mainTable || ''} onChange={(e) => updateCommand(cmd.id, 'config.mainTable', e.target.value)}>
                                        <option value="">-- Choose Dataset --</option>
                                        {datasets.map(d => <option key={d.id} value={d.name}>{d.name}</option>)}
                                    </select>
                                </div>
                            )}

                            {cmd.type === 'filter' && cmd.config.filterRoot && (
                                <div className="space-y-4">
                                    <label className="text-xs font-bold text-gray-500 uppercase flex items-center"><FilterIcon className="w-3 h-3 mr-1"/> Rule Builder</label>
                                    <FilterGroupEditor 
                                        group={cmd.config.filterRoot} 
                                        activeSchema={activeSchema} 
                                        onUpdate={(g) => updateCommand(cmd.id, 'config.filterRoot', g)}
                                        onRemove={() => {}}
                                        isRoot={true}
                                    />
                                </div>
                            )}

                            {cmd.type === 'multi_table' && (
                                <div className="space-y-4">
                                    <div className="flex items-start space-x-3 p-3 bg-blue-50 border border-blue-100 rounded-lg text-sm text-blue-800">
                                        <LayoutDashboard className="w-5 h-5 shrink-0 mt-0.5" />
                                        <div>
                                            <p className="font-semibold">Complex View Configuration</p>
                                            <p className="text-xs text-blue-600 mt-1">This command displays the main result stream alongside multiple related sub-tables. Filtering applied before this step affects the main stream, which can then be used to filter sub-tables.</p>
                                        </div>
                                    </div>
                                    
                                    <div>
                                        <label className="text-xs font-bold text-gray-500 uppercase mb-2 flex items-center">Sub-Tables (Linked Data)</label>
                                        <div className="space-y-3">
                                            {(cmd.config.subTables || []).map((sub, i) => (
                                                <div key={sub.id} className="grid grid-cols-12 gap-2 items-center bg-gray-50 p-2 rounded border border-gray-100">
                                                    <div className="col-span-3">
                                                        <label className="text-[10px] text-gray-400 font-bold uppercase">Table</label>
                                                        <select className={baseInputStyles} value={sub.table} onChange={(e) => updateSubTable(cmd.id, cmd.config.subTables || [], i, 'table', e.target.value)}>
                                                            <option value="">Select...</option>
                                                            {datasets.map(d => <option key={d.id} value={d.name}>{d.name}</option>)}
                                                        </select>
                                                    </div>
                                                    <div className="col-span-3">
                                                        <label className="text-[10px] text-gray-400 font-bold uppercase">Display Label</label>
                                                        <input className={baseInputStyles} placeholder="Tab Name" value={sub.label} onChange={(e) => updateSubTable(cmd.id, cmd.config.subTables || [], i, 'label', e.target.value)} />
                                                    </div>
                                                    <div className="col-span-5 relative">
                                                        <label className="text-[10px] text-gray-400 font-bold uppercase">Join Condition (WHERE EXISTS)</label>
                                                        <div className="relative">
                                                            <input 
                                                                className={`${baseInputStyles} pr-8`} 
                                                                placeholder="e.g. main.id = sub.user_id" 
                                                                value={sub.on} 
                                                                onChange={(e) => updateSubTable(cmd.id, cmd.config.subTables || [], i, 'on', e.target.value)} 
                                                            />
                                                            <VariableInserter 
                                                                variables={availableVariables} 
                                                                onInsert={(v) => updateSubTable(cmd.id, cmd.config.subTables || [], i, 'on', sub.on ? `${sub.on} ${v}` : v)} 
                                                            />
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
                                    
                                    <div className="flex items-center text-xs text-orange-600 bg-orange-50 p-2 rounded border border-orange-100">
                                        <AlertTriangle className="w-3.5 h-3.5 mr-2 shrink-0" />
                                        <span className="font-medium">Terminal Step: No further commands can be added after a Complex View.</span>
                                    </div>
                                </div>
                            )}
                            
                            {/* Other command renders remain unchanged */}
                            {cmd.type !== 'multi_table' && cmd.type !== 'filter' && cmd.type !== 'source' && (
                                <div className="space-y-4">
                                    {cmd.type === 'group' && (
                                        <div className="space-y-4">
                                            <div className="grid grid-cols-12 gap-6">
                                                <div className="col-span-5">
                                                    <label className="text-xs font-bold text-gray-500 uppercase mb-2 flex items-center"><Layers className="w-3 h-3 mr-1"/> Group By</label>
                                                    <div className="space-y-2">
                                                        {(cmd.config.groupByFields || []).map((field, i) => (
                                                            <div key={i} className="flex space-x-2">
                                                                <select className={getFieldStyle(field, Object.keys(activeSchema))} value={field} onChange={(e) => updateGroupField(cmd.id, cmd.config.groupByFields || [], i, e.target.value)}>
                                                                    <option value="">Select Field...</option>
                                                                    {Object.keys(activeSchema).map(f => <option key={f} value={f}>{f}</option>)}
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
                                                                <select className={getFieldStyle(agg.field, Object.keys(activeSchema))} value={agg.field} onChange={(e) => updateAggregation(cmd.id, cmd.config.aggregations || [], i, 'field', e.target.value)}>
                                                                    <option value="">Field...</option><option value="*">Any (*)</option>{Object.keys(activeSchema).map(f => <option key={f} value={f}>{f}</option>)}
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
                                                        <option value="">-- Select Table --</option>
                                                        {datasets.map(d => <option key={d.id} value={d.name}>{d.name}</option>)}
                                                    </select>
                                                )}
                                            </div>
                                            <div className="col-span-12"><input className={baseInputStyles} value={cmd.config.on || ''} onChange={(e) => updateCommand(cmd.id, 'config.on', e.target.value)} placeholder="ON Condition (e.g. id = user_id)" /></div>
                                        </div>
                                    )}

                                    {cmd.type === 'save' && (
                                        <div className="grid grid-cols-12 gap-4">
                                            <div className="col-span-4">
                                                <select className={getFieldStyle(cmd.config.field || '', Object.keys(activeSchema))} value={cmd.config.field || ''} onChange={(e) => updateCommand(cmd.id, 'config.field', e.target.value)}>
                                                    <option value="">Select Field...</option>
                                                    {Object.keys(activeSchema).map(f => <option key={f} value={f}>{f}</option>)}
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
                                                <select className={getFieldStyle(cmd.config.field || '', Object.keys(activeSchema))} value={cmd.config.field || ''} onChange={(e) => updateCommand(cmd.id, 'config.field', e.target.value)}>
                                                    <option value="">Select Field...</option>
                                                    {Object.keys(activeSchema).map(f => <option key={f} value={f}>{f}</option>)}
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
