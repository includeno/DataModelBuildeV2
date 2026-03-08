import React from 'react';
import { Trash2, Plus, Split } from 'lucide-react';
import { DataType, FilterGroup, FilterCondition } from '../../types';
import { OPERATORS, baseInputStyles } from './constants';
import { VariableSuggestionInput } from './VariableSuggestionInput';

interface FilterGroupEditorProps {
    group: FilterGroup;
    activeSchema: Record<string, DataType>;
    onUpdate: (updated: FilterGroup) => void;
    onRemove: (id: string) => void;
    isRoot?: boolean;
    availableVariables: string[];
}

export const FilterGroupEditor: React.FC<FilterGroupEditorProps> = ({ group, activeSchema, onUpdate, onRemove, isRoot = false, availableVariables }) => {
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
                            <div className="col-span-3 relative">
                                <select 
                                    className={`${baseInputStyles} py-1 pl-2 text-xs`} 
                                    value={item.field} 
                                    onChange={(e) => handleUpdateCondition(item.id, { field: e.target.value })}
                                >
                                    <option value="">Field...</option>
                                    {fieldNames.map(f => <option key={f} value={f}>{f}</option>)}
                                </select>
                            </div>
                            <div className="col-span-3">
                                <select 
                                    className={`${baseInputStyles} py-1 text-xs`} 
                                    value={item.operator} 
                                    onChange={(e) => handleUpdateCondition(item.id, { operator: e.target.value })}
                                >
                                    {(OPERATORS[activeSchema[item.field] || 'string'] || OPERATORS['string']).map(op => (
                                        <option key={op.value} value={op.value}>{op.label}</option>
                                    ))}
                                </select>
                            </div>
                            <div className="col-span-2">
                                <select
                                    className={`${baseInputStyles} py-1 text-xs font-mono text-blue-600 bg-blue-50/50 border-blue-100`}
                                    value={item.valueType || 'raw'}
                                    onChange={(e) => handleUpdateCondition(item.id, { valueType: e.target.value as 'raw' | 'variable' })}
                                >
                                    <option value="raw">Raw</option>
                                    <option value="variable">Variable</option>
                                </select>
                            </div>
                            <div className="col-span-3 relative">
                                {(item.valueType === 'variable' || item.operator === 'in_variable' || item.operator === 'not_in_variable') ? (
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
