import React from 'react';
import { Command, CommandType, Dataset } from '../types';
import { Button } from './Button';
import { Trash2, Plus, GripVertical, Type, Hash, Calendar, Clock, CheckSquare, Code, AlertCircle, Map, ArrowDownAZ, ArrowUpAZ, Calculator } from 'lucide-react';

interface CommandEditorProps {
  operationId: string;
  operationName: string;
  commands: Command[];
  datasets: Dataset[];
  onUpdateCommands: (operationId: string, newCommands: Command[]) => void;
  onUpdateName: (name: string) => void;
  onViewPath: () => void;
}

// --- CONFIGURATION CONSTANTS ---

const DATA_TYPES = [
    { value: 'string', label: 'String', icon: Type },
    { value: 'number', label: 'Number', icon: Hash },
    { value: 'boolean', label: 'Boolean', icon: CheckSquare },
    { value: 'date', label: 'Date', icon: Calendar },
    { value: 'timestamp', label: 'Timestamp', icon: Clock },
    { value: 'json', label: 'JSON', icon: Code },
];

const OPERATORS: Record<string, { value: string; label: string }[]> = {
    string: [
        { value: '=', label: 'Equals' },
        { value: '!=', label: 'Not Equals' },
        { value: 'contains', label: 'Contains' },
        { value: 'not_contains', label: 'Does Not Contain' },
        { value: 'starts_with', label: 'Starts With' },
        { value: 'ends_with', label: 'Ends With' },
        { value: 'regex', label: 'Matches Regex' },
    ],
    number: [
        { value: '=', label: 'Equals (=)' },
        { value: '!=', label: 'Not Equals (!=)' },
        { value: '>', label: 'Greater Than (>)' },
        { value: '>=', label: 'Greater/Equal (>=)' },
        { value: '<', label: 'Less Than (<)' },
        { value: '<=', label: 'Less/Equal (<=)' },
    ],
    boolean: [
        { value: 'true', label: 'Is True' },
        { value: 'false', label: 'Is False' },
    ],
    date: [
        { value: '=', label: 'Is On' },
        { value: '!=', label: 'Is Not On' },
        { value: 'before', label: 'Before' },
        { value: 'after', label: 'After' },
    ],
    timestamp: [
        { value: 'before', label: 'Before' },
        { value: 'after', label: 'After' },
        { value: 'window', label: 'In Time Window' },
    ],
    json: [
        { value: 'has_key', label: 'Has Key' },
        { value: 'contains_value', label: 'Contains Value' },
        { value: 'equals', label: 'Equals JSON' },
    ]
};

export const CommandEditor: React.FC<CommandEditorProps> = ({ 
  operationId, 
  operationName, 
  commands, 
  datasets,
  onUpdateCommands,
  onUpdateName,
  onViewPath
}) => {
  
  const addCommand = () => {
    const newCmd: Command = {
      id: `cmd_${Date.now()}`,
      type: 'filter',
      config: { field: '', dataType: 'string', operator: '=', value: '' },
      order: commands.length + 1
    };
    onUpdateCommands(operationId, [...commands, newCmd]);
  };

  const removeCommand = (id: string) => {
    onUpdateCommands(operationId, commands.filter(c => c.id !== id));
  };

  const updateCommand = (id: string, field: string, value: any) => {
    const updated = commands.map(c => {
      if (c.id === id) {
        if (field === 'type') {
            // Reset config on type change
            return { ...c, type: value as CommandType, config: { dataType: 'string', operator: '=', value: '' } };
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

  const handleDataTypeChange = (id: string, newType: string) => {
      const defaultOp = OPERATORS[newType]?.[0]?.value || '=';
      const updated = commands.map(c => {
          if (c.id === id) {
              return {
                  ...c,
                  config: {
                      ...c.config,
                      dataType: newType,
                      operator: defaultOp,
                      value: '' // Reset value to avoid type mismatch
                  }
              };
          }
          return c;
      });
      onUpdateCommands(operationId, updated);
  };

  // Helper to get fields from all datasets
  const allFields = Array.from(new Set(datasets.flatMap(d => d.fields)));

  return (
    <div className="flex flex-col h-full bg-gray-50">
      {/* Header */}
      <div className="p-4 border-b border-gray-200 flex justify-between items-center bg-white shadow-sm z-10">
        <div>
           <div className="flex items-center space-x-2 mb-1">
             <label className="text-xs font-semibold text-gray-500 uppercase tracking-wider">
               Operation Name
             </label>
             <span className="text-[10px] text-gray-400 font-mono bg-gray-100 px-1.5 py-0.5 rounded">
               ID: {operationId}
             </span>
           </div>
           <input 
             type="text" 
             value={operationName}
             onChange={(e) => onUpdateName(e.target.value)}
             className="text-lg font-bold text-gray-900 bg-transparent border-none focus:ring-0 p-0 hover:bg-gray-50 focus:bg-gray-50 transition-colors rounded"
           />
        </div>
        <div className="flex items-center space-x-3">
             <button 
                onClick={onViewPath}
                className="flex items-center space-x-1.5 px-3 py-1.5 bg-indigo-50 text-indigo-600 hover:bg-indigo-100 rounded-md text-xs font-medium transition-colors border border-indigo-100"
                title="View accumulated logic path"
             >
                <Map className="w-3.5 h-3.5" />
                <span>View Path</span>
             </button>
             <div className="text-xs text-gray-500 border-l border-gray-200 pl-3">
                {commands.length} Commands configured
             </div>
        </div>
      </div>

      {/* Scrollable Command List */}
      <div className="flex-1 overflow-y-auto p-6 space-y-4">
        {commands.length === 0 ? (
          <div className="text-center py-10 text-gray-400 border-2 border-dashed border-gray-300 rounded-lg bg-white/50">
            <p>No commands in this operation.</p>
            <Button variant="secondary" size="sm" className="mt-4" onClick={addCommand}>
                <Plus className="w-4 h-4 mr-2" /> Add Command
            </Button>
          </div>
        ) : (
          commands.map((cmd, index) => {
            const currentDataType = cmd.config.dataType || 'string';
            const availableOps = OPERATORS[currentDataType] || OPERATORS['string'];

            return (
            <div key={cmd.id} className="relative group bg-white border border-gray-200 rounded-lg shadow-sm hover:shadow-md transition-all">
              {/* Command Header */}
              <div className="flex items-center justify-between p-3 bg-white rounded-t-lg border-b border-gray-100">
                <div className="flex items-center space-x-3">
                    <GripVertical className="w-4 h-4 text-gray-400 cursor-move" />
                    <span className="text-xs font-mono bg-gray-100 px-2 py-0.5 rounded text-gray-600">#{index + 1}</span>
                    <select 
                        value={cmd.type}
                        onChange={(e) => updateCommand(cmd.id, 'type', e.target.value)}
                        className="text-sm font-semibold text-gray-700 bg-transparent border-none focus:ring-0 cursor-pointer hover:text-blue-600"
                    >
                        <option value="filter">Filter</option>
                        <option value="join">Join</option>
                        <option value="sort">Sort</option>
                        <option value="transform">Transform (Synthetic Column)</option>
                        <option value="aggregate">Aggregate</option>
                    </select>
                </div>
                <button onClick={() => removeCommand(cmd.id)} className="text-gray-400 hover:text-red-500">
                    <Trash2 className="w-4 h-4" />
                </button>
              </div>

              {/* Command Config Form */}
              <div className="p-4 grid grid-cols-12 gap-4">
                {cmd.type === 'filter' && (
                    <>
                        {/* Row 1: Field and Type */}
                        <div className="col-span-6">
                            <label className="block text-xs text-gray-500 mb-1">Field</label>
                            <select 
                                className="w-full text-sm border-gray-300 rounded-md focus:ring-blue-500 focus:border-blue-500 bg-white text-gray-900 shadow-sm"
                                value={cmd.config.field || ''}
                                onChange={(e) => updateCommand(cmd.id, 'config.field', e.target.value)}
                            >
                                <option value="">Select Field...</option>
                                {allFields.map(f => <option key={f} value={f}>{f}</option>)}
                            </select>
                        </div>
                        <div className="col-span-6">
                            <label className="block text-xs text-gray-500 mb-1">Data Type</label>
                            <div className="relative">
                                <select 
                                    className="w-full pl-9 text-sm border-gray-300 rounded-md focus:ring-blue-500 focus:border-blue-500 bg-white text-gray-900 appearance-none shadow-sm"
                                    value={currentDataType}
                                    onChange={(e) => handleDataTypeChange(cmd.id, e.target.value)}
                                >
                                    {DATA_TYPES.map(t => (
                                        <option key={t.value} value={t.value}>{t.label}</option>
                                    ))}
                                </select>
                                <div className="absolute left-3 top-1/2 -translate-y-1/2 pointer-events-none text-gray-400">
                                    {React.createElement(DATA_TYPES.find(t => t.value === currentDataType)?.icon || AlertCircle, { size: 16 })}
                                </div>
                            </div>
                        </div>

                        {/* Row 2: Operator and Value */}
                        <div className="col-span-4">
                            <label className="block text-xs text-gray-500 mb-1">Operator</label>
                            <select 
                                className="w-full text-sm border-gray-300 rounded-md focus:ring-blue-500 focus:border-blue-500 bg-white text-gray-900 shadow-sm"
                                value={cmd.config.operator || availableOps[0].value}
                                onChange={(e) => updateCommand(cmd.id, 'config.operator', e.target.value)}
                            >
                                {availableOps.map(op => (
                                    <option key={op.value} value={op.value}>{op.label}</option>
                                ))}
                            </select>
                        </div>
                        <div className="col-span-8">
                            <label className="block text-xs text-gray-500 mb-1">Value</label>
                            {currentDataType === 'boolean' ? (
                                <div className="text-sm text-gray-500 italic py-2 bg-gray-50 rounded px-3 border border-dashed border-gray-200">
                                    Boolean check determined by operator.
                                </div>
                            ) : currentDataType === 'date' ? (
                                <input 
                                    type="date"
                                    className="w-full text-sm border-gray-300 rounded-md focus:ring-blue-500 focus:border-blue-500 bg-white text-gray-900 shadow-sm"
                                    value={cmd.config.value || ''}
                                    onChange={(e) => updateCommand(cmd.id, 'config.value', e.target.value)}
                                />
                            ) : currentDataType === 'timestamp' ? (
                                <input 
                                    type="datetime-local"
                                    className="w-full text-sm border-gray-300 rounded-md focus:ring-blue-500 focus:border-blue-500 bg-white text-gray-900 shadow-sm"
                                    value={cmd.config.value || ''}
                                    onChange={(e) => updateCommand(cmd.id, 'config.value', e.target.value)}
                                />
                            ) : currentDataType === 'number' ? (
                                <input 
                                    type="number"
                                    className="w-full text-sm border-gray-300 rounded-md focus:ring-blue-500 focus:border-blue-500 bg-white text-gray-900 shadow-sm"
                                    placeholder="0"
                                    value={cmd.config.value || ''}
                                    onChange={(e) => updateCommand(cmd.id, 'config.value', e.target.value)}
                                />
                            ) : (
                                <input 
                                    type="text"
                                    className="w-full text-sm border-gray-300 rounded-md focus:ring-blue-500 focus:border-blue-500 bg-white text-gray-900 shadow-sm"
                                    placeholder={currentDataType === 'json' ? '{"key": "value"}' : 'Value'}
                                    value={cmd.config.value || ''}
                                    onChange={(e) => updateCommand(cmd.id, 'config.value', e.target.value)}
                                />
                            )}
                        </div>
                    </>
                )}

                {cmd.type === 'join' && (
                     <>
                        <div className="col-span-4">
                            <label className="block text-xs text-gray-500 mb-1">Join Type</label>
                            <select 
                                className="w-full text-sm border-gray-300 rounded-md bg-white text-gray-900 shadow-sm"
                                value={cmd.config.joinType || 'LEFT'}
                                onChange={(e) => updateCommand(cmd.id, 'config.joinType', e.target.value)}
                            >
                                <option value="LEFT">Left Join</option>
                                <option value="INNER">Inner Join</option>
                                <option value="FULL">Full Outer</option>
                            </select>
                        </div>
                        <div className="col-span-4">
                            <label className="block text-xs text-gray-500 mb-1">Target Dataset</label>
                            <select 
                                className="w-full text-sm border-gray-300 rounded-md bg-white text-gray-900 shadow-sm"
                                value={cmd.config.joinTable || ''}
                                onChange={(e) => updateCommand(cmd.id, 'config.joinTable', e.target.value)}
                            >
                                <option value="">Select Dataset...</option>
                                {datasets.map(d => <option key={d.id} value={d.name}>{d.name}</option>)}
                            </select>
                        </div>
                         <div className="col-span-4">
                            <label className="block text-xs text-gray-500 mb-1">On (Key)</label>
                            <input 
                                type="text" 
                                className="w-full text-sm border-gray-300 rounded-md bg-white text-gray-900 shadow-sm" 
                                placeholder="id = user_id"
                                value={cmd.config.on || ''} 
                                onChange={(e) => updateCommand(cmd.id, 'config.on', e.target.value)}
                            />
                        </div>
                    </>
                )}

                {cmd.type === 'sort' && (
                    <>
                        <div className="col-span-6">
                            <label className="block text-xs text-gray-500 mb-1">Field to Sort By</label>
                            <select 
                                className="w-full text-sm border-gray-300 rounded-md focus:ring-blue-500 focus:border-blue-500 bg-white text-gray-900 shadow-sm"
                                value={cmd.config.field || ''}
                                onChange={(e) => updateCommand(cmd.id, 'config.field', e.target.value)}
                            >
                                <option value="">Select Field...</option>
                                {allFields.map(f => <option key={f} value={f}>{f}</option>)}
                            </select>
                        </div>
                        <div className="col-span-6">
                            <label className="block text-xs text-gray-500 mb-1">Order</label>
                            <div className="relative">
                                <select 
                                    className="w-full text-sm border-gray-300 rounded-md focus:ring-blue-500 focus:border-blue-500 bg-white text-gray-900 shadow-sm pl-8"
                                    value={cmd.config.ascending === false ? 'false' : 'true'}
                                    onChange={(e) => updateCommand(cmd.id, 'config.ascending', e.target.value === 'true')}
                                >
                                    <option value="true">Ascending (A-Z, 0-9)</option>
                                    <option value="false">Descending (Z-A, 9-0)</option>
                                </select>
                                <div className="absolute left-2.5 top-1/2 -translate-y-1/2 pointer-events-none text-gray-400">
                                    {cmd.config.ascending !== false ? <ArrowDownAZ className="w-4 h-4" /> : <ArrowUpAZ className="w-4 h-4" />}
                                </div>
                            </div>
                        </div>
                    </>
                )}

                {cmd.type === 'transform' && (
                    <>
                        <div className="col-span-12 bg-blue-50 border border-blue-100 rounded-md p-3 mb-2 flex items-start space-x-3">
                             <Calculator className="w-5 h-5 text-blue-500 shrink-0 mt-0.5" />
                             <div className="text-xs text-blue-700">
                                 <p className="font-semibold mb-1">Synthetic Column via Python Lambda</p>
                                 <p className="opacity-90">
                                     Enter a Python expression. Access values using <code>row['column_name']</code>.
                                     <br/>Available modules: <code>math</code>, <code>np</code> (numpy).
                                 </p>
                             </div>
                        </div>
                        <div className="col-span-4">
                            <label className="block text-xs text-gray-500 mb-1">New Column Name</label>
                            <input 
                                type="text"
                                className="w-full text-sm border-gray-300 rounded-md focus:ring-blue-500 focus:border-blue-500 bg-white text-gray-900 shadow-sm"
                                placeholder="e.g. total_price"
                                value={cmd.config.outputField || ''}
                                onChange={(e) => updateCommand(cmd.id, 'config.outputField', e.target.value)}
                            />
                        </div>
                        <div className="col-span-8">
                            <label className="block text-xs text-gray-500 mb-1">Python Expression (lambda row: ...)</label>
                            <div className="relative">
                                <Code className="absolute left-2.5 top-2.5 w-4 h-4 text-gray-400" />
                                <input 
                                    type="text"
                                    className="w-full pl-9 text-sm font-mono border-gray-300 rounded-md focus:ring-blue-500 focus:border-blue-500 bg-white text-gray-900 shadow-sm"
                                    placeholder="row['price'] * 1.2"
                                    value={cmd.config.expression || ''}
                                    onChange={(e) => updateCommand(cmd.id, 'config.expression', e.target.value)}
                                />
                            </div>
                        </div>
                    </>
                )}

                {(cmd.type !== 'filter' && cmd.type !== 'join' && cmd.type !== 'sort' && cmd.type !== 'transform') && (
                     <div className="col-span-12">
                         <label className="block text-xs text-gray-500 mb-1">JSON Configuration</label>
                         <textarea 
                            className="w-full text-xs font-mono border-gray-300 rounded-md bg-gray-50 text-gray-900 shadow-sm"
                            rows={3}
                            placeholder='{"groupBy": ["col"], "aggFunc": "mean", "field": "val"}'
                            value={JSON.stringify(cmd.config, null, 2)}
                            readOnly
                         />
                         <div className="text-xs text-gray-400 mt-1 italic">
                            {cmd.type === 'aggregate' ? 'Use raw JSON to configure aggregation for now.' : 'Configuration UI coming soon.'}
                         </div>
                     </div>
                )}
              </div>
            </div>
            );
          })
        )}
        
        {commands.length > 0 && (
             <div className="flex justify-center pt-2 pb-6">
                <Button variant="secondary" size="sm" onClick={addCommand}>
                    <Plus className="w-4 h-4 mr-2" /> Add Next Command
                </Button>
            </div>
        )}
      </div>
    </div>
  );
};