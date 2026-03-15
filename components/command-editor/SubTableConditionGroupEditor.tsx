import React from 'react';
import { Trash2, Plus, Split } from 'lucide-react';
import { SubTableConditionGroup, SubTableLinkCondition } from '../../types';
import { baseInputStyles, errorInputStyles } from './constants';

interface SubTableConditionGroupEditorProps {
  group: SubTableConditionGroup;
  subFields: string[];
  mainFields: string[];
  onUpdate: (updated: SubTableConditionGroup) => void;
  onRemove: (id: string) => void;
  isRoot?: boolean;
  subAliasLabel?: string;
  mainAliasLabel?: string;
}

const SUB_TABLE_LINK_OPERATORS: Array<{ value: string; label: string }> = [
  { value: '=', label: '=' },
  { value: '!=', label: '!=' },
  { value: '>', label: '>' },
  { value: '>=', label: '>=' },
  { value: '<', label: '<' },
  { value: '<=', label: '<=' },
  { value: 'contains', label: 'Contains' },
  { value: 'not_contains', label: 'Not Contains' },
  { value: 'starts_with', label: 'Starts With' },
  { value: 'ends_with', label: 'Ends With' },
  { value: 'is_null', label: 'Is Null' },
  { value: 'is_not_null', label: 'Is Not Null' },
  { value: 'is_empty', label: 'Is Empty' },
  { value: 'is_not_empty', label: 'Is Not Empty' }
];

const UNARY_OPERATORS = new Set(['is_null', 'is_not_null', 'is_empty', 'is_not_empty']);

export const SubTableConditionGroupEditor: React.FC<SubTableConditionGroupEditorProps> = ({
  group,
  subFields,
  mainFields,
  onUpdate,
  onRemove,
  isRoot = false,
  subAliasLabel = 'sub',
  mainAliasLabel = 'main'
}) => {
  const subPrefix = subAliasLabel && String(subAliasLabel).trim() ? String(subAliasLabel).trim() : 'sub';
  const mainPrefix = mainAliasLabel && String(mainAliasLabel).trim() ? String(mainAliasLabel).trim() : 'main';
  const handleUpdateCondition = (id: string, updates: Partial<SubTableLinkCondition>) => {
    const next = group.conditions.map((item) => {
      if (item.id === id && item.type === 'condition') {
        return { ...item, ...updates };
      }
      return item;
    });
    onUpdate({ ...group, conditions: next });
  };

  const handleUpdateSubGroup = (id: string, updatedGroup: SubTableConditionGroup) => {
    const next = group.conditions.map((item) => item.id === id ? updatedGroup : item);
    onUpdate({ ...group, conditions: next });
  };

  const handleRemoveChild = (id: string) => {
    onUpdate({ ...group, conditions: group.conditions.filter(item => item.id !== id) });
  };

  const handleAddCondition = () => {
    const nextCond: SubTableLinkCondition = {
      id: `sub_cond_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`,
      type: 'condition',
      field: '',
      operator: '=',
      mainField: ''
    };
    onUpdate({ ...group, conditions: [...group.conditions, nextCond] });
  };

  const handleAddGroup = () => {
    const nextGroup: SubTableConditionGroup = {
      id: `sub_group_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`,
      type: 'group',
      logicalOperator: 'AND',
      conditions: []
    };
    onUpdate({ ...group, conditions: [...group.conditions, nextGroup] });
  };

  return (
    <div className={`space-y-3 ${isRoot ? '' : 'pl-4 border-l-2 border-blue-100 py-1'}`}>
      <div className="flex items-center space-x-3 mb-2">
        <div className="flex bg-gray-100 rounded p-0.5">
          <button
            onClick={() => onUpdate({ ...group, logicalOperator: 'AND' })}
            className={`px-2 py-0.5 text-[10px] font-bold rounded-sm transition-all ${group.logicalOperator === 'AND' ? 'bg-white shadow-sm text-blue-600' : 'text-gray-500'}`}
          >
            AND
          </button>
          <button
            onClick={() => onUpdate({ ...group, logicalOperator: 'OR' })}
            className={`px-2 py-0.5 text-[10px] font-bold rounded-sm transition-all ${group.logicalOperator === 'OR' ? 'bg-white shadow-sm text-blue-600' : 'text-gray-500'}`}
          >
            OR
          </button>
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
            <SubTableConditionGroupEditor
              key={item.id}
              group={item}
              subFields={subFields}
              mainFields={mainFields}
              subAliasLabel={subPrefix}
              mainAliasLabel={mainPrefix}
              onUpdate={(g) => handleUpdateSubGroup(item.id, g)}
              onRemove={handleRemoveChild}
            />
          ) : (
            <div key={item.id} className="space-y-1">
              {(() => {
                const isUnary = UNARY_OPERATORS.has(item.operator);
                const isMissingSubField = !!item.field && !subFields.includes(item.field);
                const isMissingMainField = !isUnary && !!item.mainField && !mainFields.includes(item.mainField);

                if (isUnary) {
                  return (
                    <div className="grid grid-cols-12 gap-2 items-center p-2 rounded-md border group/cond relative bg-gray-50/50 border-gray-100">
                      <div className="col-span-5">
                        <select
                          className={`${isMissingSubField ? errorInputStyles : baseInputStyles} py-1 pl-2 text-xs`}
                          value={item.field}
                          onChange={(e) => handleUpdateCondition(item.id, { field: e.target.value })}
                        >
                          <option value="">Sub Field...</option>
                          {isMissingSubField && <option value={item.field}>{item.field} (Missing)</option>}
                          {subFields.map(f => <option key={f} value={f}>{subPrefix}.{f}</option>)}
                        </select>
                      </div>
                      <div className="col-span-5">
                        <select
                          className={`${baseInputStyles} py-1 text-xs`}
                          value={item.operator}
                          onChange={(e) => handleUpdateCondition(item.id, { operator: e.target.value })}
                        >
                          {SUB_TABLE_LINK_OPERATORS.map(op => <option key={op.value} value={op.value}>{op.label}</option>)}
                        </select>
                      </div>
                      <div className="col-span-2 flex justify-end">
                        <button onClick={() => handleRemoveChild(item.id)} className="p-1 text-gray-300 hover:text-red-500 opacity-0 group-hover/cond:opacity-100 transition-all">
                          <Trash2 className="w-3.5 h-3.5" />
                        </button>
                      </div>
                    </div>
                  );
                }

                return (
                  <div className="grid grid-cols-12 gap-2 items-center p-2 rounded-md border group/cond relative bg-gray-50/50 border-gray-100">
                    <div className="col-span-4">
                      <select
                        className={`${isMissingSubField ? errorInputStyles : baseInputStyles} py-1 pl-2 text-xs`}
                        value={item.field}
                        onChange={(e) => handleUpdateCondition(item.id, { field: e.target.value })}
                      >
                        <option value="">Sub Field...</option>
                        {isMissingSubField && <option value={item.field}>{item.field} (Missing)</option>}
                        {subFields.map(f => <option key={f} value={f}>{subPrefix}.{f}</option>)}
                      </select>
                    </div>
                    <div className="col-span-3">
                      <select
                        className={`${baseInputStyles} py-1 text-xs`}
                        value={item.operator}
                        onChange={(e) => handleUpdateCondition(item.id, { operator: e.target.value })}
                      >
                        {SUB_TABLE_LINK_OPERATORS.map(op => <option key={op.value} value={op.value}>{op.label}</option>)}
                      </select>
                    </div>
                    <div className="col-span-4">
                      <select
                        className={`${isMissingMainField ? errorInputStyles : baseInputStyles} py-1 pl-2 text-xs`}
                        value={item.mainField}
                        onChange={(e) => handleUpdateCondition(item.id, { mainField: e.target.value })}
                      >
                        <option value="">Main Field...</option>
                        {isMissingMainField && <option value={item.mainField}>{item.mainField} (Missing)</option>}
                        {mainFields.map(f => <option key={f} value={f}>{mainPrefix}.{f}</option>)}
                      </select>
                    </div>
                    <div className="col-span-1 flex justify-end">
                      <button onClick={() => handleRemoveChild(item.id)} className="p-1 text-gray-300 hover:text-red-500 opacity-0 group-hover/cond:opacity-100 transition-all">
                        <Trash2 className="w-3.5 h-3.5" />
                      </button>
                    </div>
                  </div>
                );
              })()}
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
