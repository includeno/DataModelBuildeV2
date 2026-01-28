import React from 'react';
import { Command, CommandType, Dataset } from '../types';
import { Button } from './Button';
import { Trash2, Plus, GripVertical, Type, Hash, Calendar, Clock, CheckSquare, Code, AlertCircle, Map, ArrowDownAZ, ArrowUpAZ, Calculator, Database, ArrowRight, Link, FunctionSquare, Sparkles } from 'lucide-react';

interface CommandEditorProps {
  operationId: string;
  operationName: string;
  commands: Command[];
  datasets: Dataset[];
  inheritedDataSource?: string; 
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
    ],
    json: [
        { value: 'has_key', label: 'Has Key' },
    ]
};

const TRANSFORM_SNIPPETS = [
    { label: 'To Uppercase', code: "str(row['col_name']).upper()", type: 'string' },
    { label: 'Concat Strings', code: "row['first'] + ' ' + row['last']", type: 'string' },
    { label: 'Math Add', code: "row['price'] + 100", type: 'number' },
    { label: 'Multiply Cols', code: "row['price'] * row['qty']", type: 'number' },
    { label: 'Round Number', code: "round(row['val'], 2)", type: 'number' },
    { label: 'Logic (If/Else)', code: "'High' if row['val'] > 100 else 'Low'", type: 'logic' },
    { label: 'Parse Date', code: "pd.to_datetime(row['date_str'])", type: 'date' },
];

export const CommandEditor: React.FC<CommandEditorProps> = ({ 
  operationId, 
  operationName, 
  commands, 
  datasets,
  inheritedDataSource,
  onUpdateCommands,
  onUpdateName,
  onViewPath
}) => {
  
  const addCommand = () => {
    const previousCmd = commands.length > 0 ? commands[commands.length - 1] : null;
    const defaultSource = previousCmd?.config.mainTable || inheritedDataSource || '';

    const newCmd: Command = {
      id: `cmd_${Date.now()}`,
      type: 'filter',
      config: { 
          field: '', 
          dataType: 'string', 
          operator: '=', 
          value: '',
          mainTable: defaultSource 
      },
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
            const currentTable = c.config.mainTable;
            // Provide intelligent defaults when switching types
            let newConfig: any = { mainTable: currentTable };
            
            if (value === 'filter') {
                newConfig = { ...newConfig, dataType: 'string', operator: '=', value: '' };
            } else if (value === 'join') {
                newConfig = { ...newConfig, joinType: 'left', on: '' };
            } else if (value === 'sort') {
                newConfig = { ...newConfig, ascending: true };
            } else if (value === 'transform') {
                newConfig = { ...newConfig, outputField: 'new_column', expression: '' };
            }

            return { 
                ...c, 
                type: value as CommandType, 
                config: newConfig
            };
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
                      value: '' 
                  }
              };
          }
          return c;
      });
      onUpdateCommands(operationId, updated);
  };

  const insertSnippet = (cmdId: string, currentExpression: string, snippet: string) => {
      updateCommand(cmdId, 'config.expression', (currentExpression || '') + snippet);
  };

  const allFields = Array.from(new Set(datasets.flatMap(d => d.fields)));

  const baseInputStyles = "w-full text-sm border border-gray-200 rounded-md focus:ring-2 focus:ring-blue-100 focus:border-blue-500 bg-white text-gray-900 shadow-sm transition-all hover:border-gray-300 py-1.5";

  return (
    <div className="flex flex-col h-full bg-gray-50/50">
      {/* Header */}
      <div className="px-6 py-4 border-b border-gray-200 flex justify-between items-center bg-white sticky top-0 z-10">
        <div>
           <div className="flex items-center space-x-2 mb-1.5">
             <span className="text-[10px] uppercase font-bold text-gray-400 tracking-wider">Operation</span>
             <span className="text-[10px] text-gray-300 font-mono px-1">|</span>
             <span className="text-[10px] text-gray-400 font-mono bg-gray-100 px-1.5 py-0.5 rounded">
               ID: {operationId}
             </span>
             {inheritedDataSource && (
                 <span className="flex items-center text-[10px] text-blue-600 bg-blue-50 px-1.5 py-0.5 rounded border border-blue-100 ml-2" title="Inherited Context">
                    <ArrowRight className="w-3 h-3 mr-1" />
                    Source: {inheritedDataSource}
                 </span>
             )}
           </div>
           <input 
             type="text" 
             value={operationName}
             onChange={(e) => onUpdateName(e.target.value)}
             className="text-xl font-bold text-gray-900 bg-transparent border-none focus:ring-0 p-0 hover:bg-gray-50 -ml-1 pl-1 rounded transition-colors placeholder-gray-300"
             placeholder="Operation Name"
           />
        </div>
        <div className="flex items-center space-x-3">
             <button 
                onClick={onViewPath}
                className="flex items-center space-x-1.5 px-3 py-1.5 bg-white hover:bg-gray-50 text-gray-600 rounded-md text-xs font-medium transition-colors border border-gray-200 shadow-sm"
             >
                <Map className="w-3.5 h-3.5 mr-1" />
                View Logic Path
             </button>
             <div className="text-xs font-medium text-gray-400 border-l border-gray-200 pl-4">
                {commands.length} Command{commands.length !== 1 ? 's' : ''}
             </div>
        </div>
      </div>

      {/* Scrollable Command List */}
      <div className="flex-1 overflow-y-auto p-6 space-y-5">
        {commands.length === 0 ? (
          <div className="flex flex-col items-center justify-center py-12 text-gray-400 border-2 border-dashed border-gray-200 rounded-xl bg-gray-50/50">
            <p className="mb-3 font-medium text-sm">No commands configured yet</p>
            {inheritedDataSource && (
                <p className="text-xs text-blue-500 mb-5 bg-blue-50 px-3 py-1 rounded-full border border-blue-100">
                    Input Data: <span className="font-mono font-bold">{inheritedDataSource}</span>
                </p>
            )}
            <Button variant="secondary" size="sm" onClick={addCommand}>
                <Plus className="w-4 h-4 mr-2" /> Add First Command
            </Button>
          </div>
        ) : (
          commands.map((cmd, index) => {
            const currentDataType = cmd.config.dataType || 'string';
            const availableOps = OPERATORS[currentDataType] || OPERATORS['string'];
            const selectedDataSource = cmd.config.mainTable;
            const targetDataset = datasets.find(d => d.name === selectedDataSource);
            const availableFields = targetDataset ? targetDataset.fields : allFields;

            return (
            <div key={cmd.id} className="relative group bg-white border border-gray-200 rounded-xl shadow-sm hover:shadow-md transition-all duration-200">
              
              {/* Command Header */}
              <div className="flex items-center justify-between p-3 pl-4 border-b border-gray-100">
                <div className="flex items-center space-x-3">
                    <div className="cursor-move text-gray-300 hover:text-gray-500">
                        <GripVertical className="w-4 h-4" />
                    </div>
                    <span className="text-xs font-mono bg-gray-100 text-gray-500 px-2 py-0.5 rounded border border-gray-200">
                        #{index + 1}
                    </span>
                    
                    <div className="h-4 w-px bg-gray-200 mx-1"></div>

                    <select 
                        value={cmd.type}
                        onChange={(e) => updateCommand(cmd.id, 'type', e.target.value)}
                        className="text-sm font-bold text-gray-800 bg-transparent border-none focus:ring-0 cursor-pointer hover:text-blue-600 pl-0 pr-8"
                    >
                        <option value="filter">Filter</option>
                        <option value="join">Join Table</option>
                        <option value="sort">Sort Data</option>
                        <option value="transform">Calculate / Transform</option>
                        <option value="aggregate">Group & Aggregate</option>
                    </select>
                    
                    {cmd.config.mainTable && (
                        <div className="hidden sm:flex items-center text-[10px] px-2 py-0.5 bg-blue-50 text-blue-700 rounded border border-blue-100 font-medium max-w-[150px]" title={`Source: ${cmd.config.mainTable}`}>
                            <Database className="w-3 h-3 mr-1.5" />
                            <span className="truncate">{cmd.config.mainTable}</span>
                        </div>
                    )}
                </div>
                <button 
                    onClick={() => removeCommand(cmd.id)} 
                    className="p-1.5 text-gray-400 hover:text-red-600 hover:bg-red-50 rounded transition-colors"
                >
                    <Trash2 className="w-4 h-4" />
                </button>
              </div>

              {/* Command Config Form */}
              <div className="p-5 grid grid-cols-12 gap-5">
                {cmd.type === 'filter' && (
                    <>
                        <div className="col-span-12 md:col-span-4">
                            <label className="block text-xs font-medium text-gray-500 mb-1.5">Data Source</label>
                            <select 
                                className={`${baseInputStyles} ${!cmd.config.mainTable ? 'text-gray-400 border-dashed' : ''}`}
                                value={cmd.config.mainTable || ''}
                                onChange={(e) => updateCommand(cmd.id, 'config.mainTable', e.target.value)}
                            >
                                <option value="" disabled>Select Source...</option>
                                <option value="">-- Use Input Stream --</option>
                                {datasets.map(ds => (
                                    <option key={ds.id} value={ds.name}>{ds.name}</option>
                                ))}
                            </select>
                        </div>
                        <div className="col-span-12 md:col-span-4">
                            <label className="block text-xs font-medium text-gray-500 mb-1.5">Field</label>
                            <select 
                                className={baseInputStyles}
                                value={cmd.config.field || ''}
                                onChange={(e) => updateCommand(cmd.id, 'config.field', e.target.value)}
                            >
                                <option value="">Select Field...</option>
                                {availableFields.map(f => <option key={f} value={f}>{f}</option>)}
                            </select>
                        </div>
                        <div className="col-span-12 md:col-span-4">
                            <label className="block text-xs font-medium text-gray-500 mb-1.5">Data Type</label>
                            <div className="relative">
                                <select 
                                    className={`${baseInputStyles} pl-9`}
                                    value={currentDataType}
                                    onChange={(e) => handleDataTypeChange(cmd.id, e.target.value)}
                                >
                                    {DATA_TYPES.map(t => (
                                        <option key={t.value} value={t.value}>{t.label}</option>
                                    ))}
                                </select>
                                <div className="absolute left-3 top-1/2 -translate-y-1/2 pointer-events-none text-gray-400">
                                    {React.createElement(DATA_TYPES.find(t => t.value === currentDataType)?.icon || AlertCircle, { size: 14 })}
                                </div>
                            </div>
                        </div>

                        <div className="col-span-12 md:col-span-4">
                            <label className="block text-xs font-medium text-gray-500 mb-1.5">Operator</label>
                            <select 
                                className={baseInputStyles}
                                value={cmd.config.operator || availableOps[0].value}
                                onChange={(e) => updateCommand(cmd.id, 'config.operator', e.target.value)}
                            >
                                {availableOps.map(op => (
                                    <option key={op.value} value={op.value}>{op.label}</option>
                                ))}
                            </select>
                        </div>
                        <div className="col-span-12 md:col-span-8">
                            <label className="block text-xs font-medium text-gray-500 mb-1.5">Value</label>
                            {currentDataType === 'boolean' ? (
                                <div className="text-sm text-gray-500 italic py-2 bg-gray-50 rounded px-3 border border-dashed border-gray-200">
                                    Check is determined by the operator selection (True/False).
                                </div>
                            ) : currentDataType === 'number' ? (
                                <input 
                                    type="number"
                                    className={baseInputStyles}
                                    placeholder="0"
                                    value={cmd.config.value || ''}
                                    onChange={(e) => updateCommand(cmd.id, 'config.value', e.target.value)}
                                />
                            ) : (
                                <input 
                                    type={currentDataType === 'date' ? 'date' : 'text'}
                                    className={baseInputStyles}
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
                        <div className="col-span-12 md:col-span-4">
                            <label className="block text-xs font-medium text-gray-500 mb-1.5 flex items-center">
                                <Link className="w-3 h-3 mr-1" /> Join With Table
                            </label>
                            <select 
                                className={baseInputStyles}
                                value={cmd.config.joinTable || ''}
                                onChange={(e) => updateCommand(cmd.id, 'config.joinTable', e.target.value)}
                            >
                                <option value="" disabled>Select Table...</option>
                                {datasets.filter(d => d.name !== cmd.config.mainTable).map(ds => (
                                    <option key={ds.id} value={ds.name}>{ds.name}</option>
                                ))}
                                {datasets.length === 0 && <option value="" disabled>No other tables available</option>}
                            </select>
                        </div>
                        <div className="col-span-6 md:col-span-3">
                            <label className="block text-xs font-medium text-gray-500 mb-1.5">Join Type</label>
                            <select 
                                className={baseInputStyles}
                                value={cmd.config.joinType?.toLowerCase() || 'left'}
                                onChange={(e) => updateCommand(cmd.id, 'config.joinType', e.target.value)}
                            >
                                <option value="left">Left Join</option>
                                <option value="inner">Inner Join</option>
                                <option value="right">Right Join</option>
                                <option value="full">Full Outer</option>
                            </select>
                        </div>
                        <div className="col-span-12 md:col-span-5">
                            <label className="block text-xs font-medium text-gray-500 mb-1.5">Match Condition (On)</label>
                            <input 
                                type="text"
                                className={baseInputStyles}
                                placeholder="e.g. id = user_id"
                                value={cmd.config.on || ''}
                                onChange={(e) => updateCommand(cmd.id, 'config.on', e.target.value)}
                            />
                            <p className="mt-1 text-[10px] text-gray-400">Format: column_in_source = column_in_target</p>
                        </div>
                    </>
                )}

                {cmd.type === 'transform' && (
                    <>
                        <div className="col-span-12 md:col-span-4">
                            <label className="block text-xs font-medium text-gray-500 mb-1.5 flex items-center">
                                <FunctionSquare className="w-3 h-3 mr-1" /> Output Column Name
                            </label>
                            <input 
                                type="text"
                                className={baseInputStyles}
                                placeholder="e.g. total_price"
                                value={cmd.config.outputField || ''}
                                onChange={(e) => updateCommand(cmd.id, 'config.outputField', e.target.value)}
                            />
                        </div>
                        <div className="col-span-12 md:col-span-8">
                             <label className="block text-xs font-medium text-gray-500 mb-1.5 flex items-center justify-between">
                                <span className="flex items-center"><Sparkles className="w-3 h-3 mr-1 text-purple-500"/> Expression (Python-syntax)</span>
                                <span className="text-[10px] text-gray-400 font-normal">Use row['col'] to access values</span>
                            </label>
                            <textarea 
                                className="w-full text-sm font-mono border border-gray-200 rounded-md bg-gray-50 focus:ring-2 focus:ring-blue-100 focus:border-blue-500 p-2 min-h-[80px]"
                                placeholder="e.g. row['price'] * row['quantity']"
                                value={cmd.config.expression || ''}
                                onChange={(e) => updateCommand(cmd.id, 'config.expression', e.target.value)}
                            />
                            
                            <div className="mt-2 flex flex-wrap gap-2">
                                {TRANSFORM_SNIPPETS.map((snip, i) => (
                                    <button
                                        key={i}
                                        onClick={() => insertSnippet(cmd.id, cmd.config.expression || '', snip.code)}
                                        className="text-[10px] px-2 py-1 bg-white border border-gray-200 rounded-full text-gray-600 hover:border-blue-300 hover:text-blue-600 transition-colors flex items-center"
                                        title={snip.code}
                                    >
                                        <Plus className="w-2 h-2 mr-1" />
                                        {snip.label}
                                    </button>
                                ))}
                            </div>
                        </div>
                    </>
                )}

                {/* Other types logic preserved but styled consistently */}
                {cmd.type === 'sort' && (
                    <>
                         <div className="col-span-6">
                            <label className="block text-xs font-medium text-gray-500 mb-1.5">Field</label>
                            <select 
                                className={baseInputStyles}
                                value={cmd.config.field || ''}
                                onChange={(e) => updateCommand(cmd.id, 'config.field', e.target.value)}
                            >
                                <option value="">Select Field...</option>
                                {availableFields.map(f => <option key={f} value={f}>{f}</option>)}
                            </select>
                        </div>
                        <div className="col-span-6">
                            <label className="block text-xs font-medium text-gray-500 mb-1.5">Direction</label>
                            <div className="relative">
                                <select 
                                    className={`${baseInputStyles} pl-9`}
                                    value={cmd.config.ascending === false ? 'false' : 'true'}
                                    onChange={(e) => updateCommand(cmd.id, 'config.ascending', e.target.value === 'true')}
                                >
                                    <option value="true">Ascending (A-Z)</option>
                                    <option value="false">Descending (Z-A)</option>
                                </select>
                                <div className="absolute left-3 top-1/2 -translate-y-1/2 pointer-events-none text-gray-400">
                                    {cmd.config.ascending !== false ? <ArrowDownAZ className="w-4 h-4" /> : <ArrowUpAZ className="w-4 h-4" />}
                                </div>
                            </div>
                        </div>
                    </>
                )}
                
                {/* Fallback for other complex types */}
                {cmd.type !== 'filter' && cmd.type !== 'sort' && cmd.type !== 'join' && cmd.type !== 'transform' && (
                    <div className="col-span-12">
                        <textarea 
                             className="w-full text-xs font-mono border-gray-200 rounded-md bg-gray-50 text-gray-700 shadow-inner p-3 focus:outline-none focus:ring-1 focus:ring-blue-500"
                             rows={3}
                             value={JSON.stringify(cmd.config, null, 2)}
                             readOnly
                        />
                        <div className="text-[10px] text-gray-400 mt-1.5 flex justify-end">
                            Raw JSON Configuration View
                        </div>
                    </div>
                )}

              </div>
            </div>
            );
          })
        )}
        
        {commands.length > 0 && (
             <div className="flex justify-center pt-2 pb-10">
                <Button variant="secondary" size="sm" onClick={addCommand}>
                    <Plus className="w-3.5 h-3.5 mr-2 text-gray-500" /> Add Next Command
                </Button>
            </div>
        )}
      </div>
    </div>
  );
};