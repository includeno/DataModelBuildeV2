
import React, { useMemo, useState, useRef, useEffect } from 'react';
import { Command, CommandType, Dataset, OperationType, AggregationConfig, OperationNode, DataType, HavingCondition, MappingRule, SubTableConfig, FieldInfo, AppearanceConfig, SubTableConditionGroup, ValidationRule } from '../types';
import { Button } from './Button';
import { Trash2, Plus, GripVertical, Type, Database, Play, Layers, ArrowRight, Filter as FilterIcon, Table, Calculator, List, Check, ChevronDown, ChevronUp, Split, LayoutDashboard, AlertTriangle, Settings2, Eye, Variable, Route, Code, Pin, PinOff, GitBranch } from 'lucide-react';
import { Reorder } from 'framer-motion';
import { CustomSelect, SelectOption } from './command-editor/CustomSelect';
import { CollapsibleSection } from './command-editor/CollapsibleSection';
import { SqlPreviewModal } from './command-editor/SqlPreviewModal';
import { SqlBuilderModal } from './command-editor/SqlBuilderModal';
import { COMMAND_LABELS, OPERATORS, PYTHON_TEMPLATE, baseInputStyles, errorInputStyles, codeAreaStyles } from './command-editor/constants';
import { DraggableItem } from './command-editor/DraggableItem';
import { VariableInserter } from './command-editor/VariableInserter';
import { InsertDivider } from './command-editor/InsertDivider';
import { FilterGroupEditor } from './command-editor/FilterGroupEditor';
import { SubTableConditionGroupEditor } from './command-editor/SubTableConditionGroupEditor';
import { flattenNodes, findAncestorVariables, getAncestors } from './command-editor/treeUtils';
import { parseSqlToCommands } from './command-editor/sqlParser';
import { StepOutline } from './command-editor/StepOutline';
import { getDatasetFieldNames, getSourceLabel, formatSourceOptionLabel, resolveDataSource, renderSqlCommandSummary, SourceAlias } from './command-editor/helpers';

interface CommandEditorProps {
  operationId: string;
  operationName: string;
  operationType: OperationType;
  commands: Command[];
  datasets: Dataset[];
  inputSchema: Record<string, DataType>; 
  appearance?: AppearanceConfig;
  onUpdateCommands: (operationId: string, newCommands: Command[]) => void;
  onUpdateName: (name: string) => void;
  onUpdateType: (operationId: string, type: OperationType) => void;
  onViewPath: (commandId?: string) => void;
  onRun?: (commandId?: string) => void;
  onGenerateSql?: (commandId: string) => Promise<string>;
  onViewLineage?: (commandId: string, sourceTable?: string) => void;
  tree?: OperationNode; 
  canRun?: boolean;
}


export const CommandEditor: React.FC<CommandEditorProps> = ({ 
  operationId, 
  operationName, 
  operationType, 
  commands, 
  datasets,
  inputSchema,
  appearance,
  onUpdateCommands,
  onUpdateName,
  onViewPath,
  onRun,
  onGenerateSql,
  onViewLineage,
  tree,
  canRun = true
}) => {
  
  const [sqlModalOpen, setSqlModalOpen] = useState(false);
  const [sqlContent, setSqlContent] = useState('');
  const [sqlLoading, setSqlLoading] = useState(false);
  const [collapsedSteps, setCollapsedSteps] = useState<Record<string, boolean>>({});
  const stepRefs = useRef<Record<string, HTMLDivElement | null>>({});
  const [sqlBuilderOpen, setSqlBuilderOpen] = useState(false);
  const [sqlBuilderInput, setSqlBuilderInput] = useState('');
  const [sqlBuilderCommands, setSqlBuilderCommands] = useState<Command[]>([]);
  const [sqlBuilderWarnings, setSqlBuilderWarnings] = useState<string[]>([]);
  const [sqlBuilderError, setSqlBuilderError] = useState<string | null>(null);
  const [sqlInsertIndex, setSqlInsertIndex] = useState<number | null>(null);
  const [outlinePinned, setOutlinePinned] = useState(false);

  const toggleStepCollapsed = (id: string) => {
      setCollapsedSteps(prev => ({ ...prev, [id]: !prev[id] }));
  };

  const collapseAllSteps = () => {
      const next: Record<string, boolean> = {};
      commands.forEach(c => { next[c.id] = true; });
      setCollapsedSteps(next);
  };

  const expandAllSteps = () => {
      const next: Record<string, boolean> = {};
      commands.forEach(c => { next[c.id] = false; });
      setCollapsedSteps(next);
  };

  const scrollToStep = (id: string) => {
      const el = stepRefs.current[id];
      if (el) el.scrollIntoView({ behavior: 'smooth', block: 'start' });
  };

  const handleGenerateSql = async (cmdId: string) => {
      if (!onGenerateSql) return;
      const target = commands.find(c => c.id === cmdId);
      const requiresSource = target ? (target.type !== 'source' && target.type !== 'multi_table') : false;
      const rawSource = target?.config?.dataSource;
      const isMissingSource = requiresSource && (!rawSource || String(rawSource).trim() === '' || rawSource === 'stream');
      if (target && isMissingSource) {
          setSqlModalOpen(true);
          setSqlLoading(false);
          setSqlContent('-- Error: No data source selected. Please choose a source first.');
          return;
      }
      setSqlModalOpen(true);
      setSqlLoading(true);
      setSqlContent('');
      try {
          const sql = await onGenerateSql(cmdId);
          setSqlContent(sql);
      } catch (e) {
          setSqlContent(`-- Error generating SQL: ${e}`);
      } finally {
          setSqlLoading(false);
      }
  };

  const ancestorVariables = useMemo(() => {
     if (!tree) return [];
     return findAncestorVariables(tree, operationId);
  }, [tree, operationId]);

  const availableNodes = useMemo(() => {
      if (!tree) return [];
      return flattenNodes(tree).filter(n => n.id !== operationId);
  }, [tree, operationId]);

  const scopedSetupIds = useMemo(() => {
      if (!tree) return null;
      if (operationType === 'setup') return new Set([operationId]);
      const ancestorNodes = getAncestors(tree, operationId) || [];
      const nearestSetup = [...ancestorNodes].reverse().find(n => n.operationType === 'setup');
      if (!nearestSetup) return null;
      return new Set([nearestSetup.id]);
  }, [tree, operationId, operationType]);

  const availableSourceAliases = useMemo(() => {
      if (!tree) return [];
      const setupNodes = flattenNodes(tree).filter(n => {
          if (n.operationType !== 'setup') return false;
          if (!scopedSetupIds) return true;
          return scopedSetupIds.has(n.id);
      });
      const aliases: SourceAlias[] = [];
      
      setupNodes.forEach(node => {
          const sourceCmds = node.commands.filter(c => c.type === 'source');
          sourceCmds.forEach((cmd) => {
              const effectiveAlias = cmd.config.alias || cmd.config.mainTable;
              
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
  }, [tree, scopedSetupIds]);

  const handleParseSql = () => {
      const result = parseSqlToCommands(sqlBuilderInput, (name) => resolveDataSource(availableSourceAliases, name));
      setSqlBuilderWarnings(result.warnings);
      setSqlBuilderError(result.error);
      setSqlBuilderCommands(result.commands);
  };

  const handleApplySqlCommands = () => {
      if (sqlBuilderCommands.length === 0) return;
      const insertIndex = sqlInsertIndex ?? commands.length;
      const merged = [
          ...commands.slice(0, insertIndex),
          ...sqlBuilderCommands,
          ...commands.slice(insertIndex)
      ].map((cmd, idx) => ({ ...cmd, order: idx + 1 }));
      onUpdateCommands(operationId, merged);
      setSqlBuilderOpen(false);
      setSqlInsertIndex(null);
      setSqlBuilderInput('');
      setSqlBuilderCommands([]);
      setSqlBuilderWarnings([]);
      setSqlBuilderError(null);
  };

  const openSqlBuilder = (insertIndex: number | null = null) => {
      setSqlInsertIndex(insertIndex);
      setSqlBuilderOpen(true);
  };

  const ancestors = useMemo(() => {
      if (!tree) return [];
      return getAncestors(tree, operationId) || [];
  }, [tree, operationId]);

  const compareCommands = useMemo(() => {
      const ancestorCommands = ancestors.flatMap(node => node.commands);
      const localBefore = sqlInsertIndex != null ? commands.slice(0, sqlInsertIndex) : commands;
      return [...ancestorCommands, ...localBefore];
  }, [ancestors, commands, sqlInsertIndex]);

  const showStepOutline = commands.length >= 4;

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

  const normalizeSourceToken = (value: unknown): string => {
      if (value === null || value === undefined) return '';
      return String(value).trim();
  };
  const withCurrentOption = (options: string[], currentValue?: string): string[] => {
      const current = normalizeSourceToken(currentValue);
      if (!current) return options;
      return options.includes(current) ? options : [current, ...options];
  };
  const datasetNameSet = useMemo(() => new Set(datasets.map(d => String(d.name))), [datasets]);
  const getKnownSourceTable = (table?: string): string | undefined => {
      const normalized = normalizeSourceToken(table);
      if (!normalized) return undefined;
      return datasetNameSet.has(normalized) ? normalized : undefined;
  };
  const unresolvedSourceOptionStyle: React.CSSProperties = { color: '#dc2626', fontWeight: 600 };
  const resolveSourceAlias = (sourceValue: string) => (
      availableSourceAliases.find(sa =>
          sa.linkId === sourceValue || sa.alias === sourceValue || sa.sourceTable === sourceValue
      )
  );
  const isKnownSourceValue = (sourceValue: string, generatedTables: string[]): boolean => {
      if (!sourceValue || sourceValue === 'stream') return false;
      if (generatedTables.includes(sourceValue)) return true;
      if (datasetNameSet.has(sourceValue)) return true;
      const sourceAlias = resolveSourceAlias(sourceValue);
      if (sourceAlias) return !!getKnownSourceTable(sourceAlias.sourceTable);
      return false;
  };
  const hasMissingCommandSources = useMemo(() => (
      commands.some(cmd => {
          const requiresSource = cmd.type !== 'source' && cmd.type !== 'multi_table';
          if (!requiresSource) return false;
          const sourceValue = normalizeSourceToken(cmd.config?.dataSource);
          if (!sourceValue || sourceValue === 'stream') return true;
          return !isKnownSourceValue(sourceValue, allGeneratedTablesGlobal);
      })
  ), [commands, availableSourceAliases, allGeneratedTablesGlobal, datasetNameSet]);

  const RESERVED_WORDS = useMemo(() => new Set([
      'select', 'from', 'where', 'order', 'group', 'by', 'join', 'left', 'right',
      'inner', 'outer', 'full', 'on', 'limit', 'offset', 'union', 'distinct',
      'having', 'as', 'and', 'or', 'not', 'null', 'is', 'like', 'in', 'table', 'view'
  ]), []);

  const isReservedName = (name?: string) => {
      if (!name) return false;
      return RESERVED_WORDS.has(String(name).trim().toLowerCase());
  };

  if (operationType === 'setup') {
      const sourceCommands = commands.filter(c => c.type === 'source');
      const configuredSourceCount = sourceCommands.filter(c => c.config.mainTable && String(c.config.mainTable).trim().length > 0).length;
      const variableCommands = commands.filter(c => c.type === 'define_variable');
      
      const handleAddSource = () => {
          const newCmd: Command = {
              id: `cmd_src_${Date.now()}_${Math.floor(Math.random() * 1000)}`,
              type: 'source',
              config: { 
                  mainTable: '', 
                  alias: '',
                  note: '',
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
                  variableValue: '',
                  note: ''
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

              if (!currentTable || String(currentTable).trim().length === 0) {
                  errors.push("Dataset is required.");
                  return errors;
              }

              const isDuplicateTable = sourceCommands.some((c, idx) => 
                  idx !== index && c.config.mainTable === currentTable
              );
              if (isDuplicateTable) errors.push("This table is already selected.");

              const isMissingDataset = !datasets.some(d => d.name === currentTable);
              if (isMissingDataset) errors.push("Selected dataset is unavailable.");
              if (currentTable && isReservedName(currentTable)) {
                  errors.push(`Dataset name '${currentTable}' is a reserved keyword. Please rename or re-import.`);
              }

              if (currentAlias) {
                  const isDuplicateAlias = sourceCommands.some((c, idx) => 
                      idx !== index && c.config.alias === currentAlias
                  );
                  if (isDuplicateAlias) errors.push("Alias name must be unique.");

                  const isConflictWithVariable = variableCommands.some(c => c.config.variableName === currentAlias);
                  if (isConflictWithVariable) errors.push("Alias conflicts with a variable name.");

                  if (isReservedName(currentAlias)) {
                      errors.push(`Alias '${currentAlias}' is a reserved keyword.`);
                  }
              }

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

              if (isReservedName(name)) {
                  errors.push("Variable name is a reserved keyword.");
              }

              if (datasets.some(d => d.name === name)) {
                  errors.push("Conflict with dataset name.");
              }

              if (sourceCommands.some(c => c.config.alias === name)) {
                  errors.push("Conflict with source alias.");
              }

              if (variableCommands.some(c => c.id !== cmd.id && c.config.variableName === name)) {
                  errors.push("Variable name must be unique.");
              }
          }
          return errors;
      };

      return (
        <div className="flex flex-col h-full">
            {/* Unified Header Style */}
            <div className="px-6 py-5 border-b border-gray-200 flex justify-between items-center bg-white sticky top-0 z-10 shadow-sm">
                <div className="flex-1 min-w-0">
                   <div className="flex items-center space-x-2 mb-1">
                     <span className="text-[10px] uppercase font-bold text-gray-400 tracking-wider">Configuration</span>
                     {appearance?.showOperationIds && (
                         <span className="text-[10px] font-mono text-gray-300">#{operationId}</span>
                     )}
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
                     <button onClick={() => onViewPath()} className="p-2 text-gray-400 hover:text-blue-600 hover:bg-blue-50 rounded-md transition-colors" title="View Logic Path"><Layers className="w-5 h-5" /></button>
                </div>
            </div>
            
            <div className="p-6 w-full h-full overflow-y-auto custom-scrollbar">
                <div className="flex flex-col gap-4 min-h-full">
                    {/* Source Configuration */}
                    {datasets.some(d => isReservedName(d.name)) && (
                        <div className="mb-4 p-3 bg-red-50 text-red-700 text-xs rounded-lg border border-red-100">
                            Reserved keyword dataset detected: {datasets.filter(d => isReservedName(d.name)).map(d => d.name).join(', ')}. Please re-import with a different name.
                        </div>
                    )}
                    <CollapsibleSection title="Configured Sources" icon={Database} count={configuredSourceCount}>
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
                                disabled: otherSelectedTables.has(d.name) || isReservedName(d.name),
                                icon: Database
                            }));

                            if (isMissing && currentSelection) {
                                options.push({ value: currentSelection, label: currentSelection, disabled: true, icon: Database });
                            }
                            if (currentSelection && isReservedName(currentSelection)) {
                                options.push({ value: currentSelection, label: `${currentSelection} (Reserved)`, disabled: true, icon: Database });
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
                                                hasError={hasError && errors.some(e => {
                                                    const msg = e.toLowerCase();
                                                    return msg.includes('dataset') || msg.includes('table');
                                                })}
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
                                        <div>
                                            <label className="block text-xs font-bold text-gray-500 uppercase mb-1">Notes</label>
                                            <textarea
                                                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:ring-1 focus:border-blue-500 focus:ring-blue-500 text-sm min-h-[64px]"
                                                value={cmd.config.note || ''}
                                                onChange={(e) => handleUpdateSourceCmd(cmd.id, { note: e.target.value })}
                                                placeholder="Describe this data source..."
                                            />
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
                                        <div>
                                            <label className="block text-xs font-bold text-gray-500 uppercase mb-1">Notes</label>
                                            <textarea
                                                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:ring-1 focus:ring-purple-500 focus:border-purple-500 text-sm min-h-[64px]"
                                                value={cmd.config.note || ''}
                                                onChange={(e) => handleUpdateSourceCmd(cmd.id, { note: e.target.value })}
                                                placeholder="Describe this variable..."
                                            />
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
  const normalizeOnField = (field: unknown): string => {
      if (field === null || field === undefined) return '';
      const raw = String(field).trim();
      if (!raw) return '';
      if (raw.includes('.')) return raw.split('.').pop() || '';
      return raw;
  };
  const isSubTableOnGroupEmpty = (group?: SubTableConditionGroup | null): boolean => {
      if (!group || !Array.isArray(group.conditions) || group.conditions.length === 0) return true;
      return group.conditions.every((item: any) => item?.type === 'group'
          ? isSubTableOnGroupEmpty(item as SubTableConditionGroup)
          : false);
  };
  const buildSubTableOnExpressionFromGroup = (group?: SubTableConditionGroup | null): string => {
      if (!group || !Array.isArray(group.conditions) || group.conditions.length === 0) return '';
      const parts = group.conditions.map((item: any): string => {
          if (item?.type === 'group') {
              const nested = buildSubTableOnExpressionFromGroup(item as SubTableConditionGroup);
              return nested ? `(${nested})` : '';
          }
          const leftField = normalizeOnField(item?.field);
          const op = String(item?.operator || '=').toLowerCase();
          if (!leftField) return '';
          const leftExpr = `sub.${leftField}`;
          if (op === 'is_null') return `${leftExpr} IS NULL`;
          if (op === 'is_not_null') return `${leftExpr} IS NOT NULL`;
          if (op === 'is_empty') return `(${leftExpr} IS NULL OR ${leftExpr} = '')`;
          if (op === 'is_not_empty') return `(${leftExpr} IS NOT NULL AND ${leftExpr} != '')`;
          const rightField = normalizeOnField(item?.mainField ?? item?.value);
          if (!rightField) return '';
          const rightExpr = `main.${rightField}`;
          if (op === 'contains') return `CAST(${leftExpr} AS VARCHAR) ILIKE '%' || CAST(${rightExpr} AS VARCHAR) || '%'`;
          if (op === 'not_contains') return `CAST(${leftExpr} AS VARCHAR) NOT ILIKE '%' || CAST(${rightExpr} AS VARCHAR) || '%'`;
          if (op === 'starts_with') return `CAST(${leftExpr} AS VARCHAR) ILIKE CAST(${rightExpr} AS VARCHAR) || '%'`;
          if (op === 'ends_with') return `CAST(${leftExpr} AS VARCHAR) ILIKE '%' || CAST(${rightExpr} AS VARCHAR)`;
          const safeOp = ['=', '!=', '>', '>=', '<', '<='].includes(op) ? op : '=';
          return `${leftExpr} ${safeOp} ${rightExpr}`;
      }).filter(Boolean);
      if (parts.length === 0) return '';
      const logical = group.logicalOperator === 'OR' ? 'OR' : 'AND';
      return parts.join(` ${logical} `);
  };
  const countSubTableOnRules = (group?: SubTableConditionGroup | null): number => {
      if (!group || !Array.isArray(group.conditions) || group.conditions.length === 0) return 0;
      return group.conditions.reduce((count, item: any) => {
          if (item?.type === 'group') return count + countSubTableOnRules(item as SubTableConditionGroup);
          return count + 1;
      }, 0);
  };
  const updateSubTableOnConditionGroup = (cmdId: string, current: SubTableConfig[], idx: number, group: SubTableConditionGroup) => {
      const newList = [...(current || [])];
      const normalizedGroup = isSubTableOnGroupEmpty(group) ? undefined : group;
      const nextOn = normalizedGroup ? buildSubTableOnExpressionFromGroup(normalizedGroup) : '';
      newList[idx] = {
          ...newList[idx],
          on: nextOn,
          onConditionGroup: normalizedGroup,
          conditionGroup: undefined
      };
      updateCommand(cmdId, 'config.subTables', newList);
  };
  const createDefaultSubTableConditionGroup = (): SubTableConditionGroup => ({
      id: `sub_group_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`,
      type: 'group',
      logicalOperator: 'AND',
      conditions: []
  });
  const addSubTable = (cmdId: string, current: SubTableConfig[]) => {
      updateCommand(cmdId, 'config.subTables', [
          ...(current || []),
          {
              id: `sub_${Date.now()}`,
              table: '',
              on: '',
              label: 'New Sub-Table',
              onConditionGroup: createDefaultSubTableConditionGroup()
          }
      ]);
  };
  const removeSubTable = (cmdId: string, current: SubTableConfig[], idx: number) => {
      const newList = [...(current || [])]; newList.splice(idx, 1); updateCommand(cmdId, 'config.subTables', newList);
  };
  const updateSubTable = <K extends keyof SubTableConfig>(cmdId: string, current: SubTableConfig[], idx: number, key: K, val: SubTableConfig[K]) => {
      const newList = [...(current || [])]; newList[idx] = { ...newList[idx], [key]: val };
      updateCommand(cmdId, 'config.subTables', newList);
  };
  const addViewField = (cmdId: string, current: any[]) => {
      updateCommand(cmdId, 'config.viewFields', [...(current || []), { field: '', distinct: false }]);
  };
  const removeViewField = (cmdId: string, current: any[], idx: number) => {
      const newList = [...(current || [])]; newList.splice(idx, 1); updateCommand(cmdId, 'config.viewFields', newList);
  };
  const moveViewField = (cmdId: string, current: any[], idx: number, dir: 'up' | 'down') => {
      const newList = [...(current || [])];
      const swapIdx = dir === 'up' ? idx - 1 : idx + 1;
      if (swapIdx < 0 || swapIdx >= newList.length) return;
      const tmp = newList[idx];
      newList[idx] = newList[swapIdx];
      newList[swapIdx] = tmp;
      updateCommand(cmdId, 'config.viewFields', newList);
  };
  const updateViewField = (cmdId: string, current: any[], idx: number, key: string, val: any) => {
      const newList = [...(current || [])]; newList[idx] = { ...newList[idx], [key]: val };
      updateCommand(cmdId, 'config.viewFields', newList);
  };
  const addViewSort = (cmdId: string, current: any[]) => {
      updateCommand(cmdId, 'config.viewSorts', [...(current || []), { field: '', ascending: true }]);
  };
  const removeViewSort = (cmdId: string, current: any[], idx: number) => {
      const newList = [...(current || [])]; newList.splice(idx, 1); updateCommand(cmdId, 'config.viewSorts', newList);
  };
  const moveViewSort = (cmdId: string, current: any[], idx: number, dir: 'up' | 'down') => {
      const newList = [...(current || [])];
      const swapIdx = dir === 'up' ? idx - 1 : idx + 1;
      if (swapIdx < 0 || swapIdx >= newList.length) return;
      const tmp = newList[idx];
      newList[idx] = newList[swapIdx];
      newList[swapIdx] = tmp;
      updateCommand(cmdId, 'config.viewSorts', newList);
  };
  const updateViewSort = (cmdId: string, current: any[], idx: number, key: string, val: any) => {
      const newList = [...(current || [])]; newList[idx] = { ...newList[idx], [key]: val };
      updateCommand(cmdId, 'config.viewSorts', newList);
  };
  const updateJoinConditionFromBuilder = (
      cmdId: string,
      leftField: string,
      operator: string,
      rightField: string,
      leftAlias: string,
      rightAlias: string
  ) => {
      const safeOp = ['=', '!=', '>', '>=', '<', '<='].includes(operator) ? operator : '=';
      const leftExpr = leftField ? `${leftAlias}.${leftField}` : '';
      const rightExpr = rightField ? `${rightAlias}.${rightField}` : '';
      const onExpr = leftExpr && rightExpr ? `${leftExpr} ${safeOp} ${rightExpr}` : '';
      const updated = commands.map(c => {
          if (c.id !== cmdId) return c;
          return {
              ...c,
              config: {
                  ...c.config,
                  joinLeftField: leftField,
                  joinOperator: safeOp,
                  joinRightField: rightField,
                  on: onExpr
              }
          };
      });
      onUpdateCommands(operationId, updated);
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
             {appearance?.showOperationIds && (
                 <span className="text-[10px] font-mono text-gray-300">#{operationId}</span>
             )}
           </div>
           <div className="flex items-center space-x-3">
               <div className="relative group shrink-0">
                    <button 
                        onClick={() => onRun && onRun()}
                        disabled={!canRun || hasMissingCommandSources}
                        className={`flex items-center justify-center w-8 h-8 rounded-lg transition-colors shadow-sm active:scale-95 ${
                            canRun && !hasMissingCommandSources ? 'bg-blue-600 text-white hover:bg-blue-700' : 'bg-gray-300 text-white cursor-not-allowed'
                        }`}
                        title={
                            !canRun
                            ? "Configure data sources before running"
                            : hasMissingCommandSources
                            ? "Select a valid data source for each step before running"
                            : "Run this operation"
                        }
                    >
                        <Play className="w-4 h-4 ml-0.5" />
                    </button>
               </div>
               <input type="text" value={operationName} onChange={(e) => onUpdateName(e.target.value)} className="text-xl font-bold text-gray-900 bg-transparent border-none focus:ring-0 p-0 hover:bg-gray-50 pl-1 rounded transition-colors placeholder-gray-300 flex-1 min-w-0" placeholder="Operation Name" />
           </div>
        </div>
        <div className="flex items-center pl-4 border-l border-gray-200 ml-4 space-x-2">
             <button
                 onClick={() => setOutlinePinned(prev => !prev)}
                 className={`p-2 rounded-md transition-colors ${
                     outlinePinned ? 'text-blue-600 bg-blue-50' : 'text-gray-400 hover:text-blue-600 hover:bg-blue-50'
                 }`}
                 title={outlinePinned ? 'Unpin step outline' : 'Pin step outline'}
             >
                 {outlinePinned ? <PinOff className="w-5 h-5" /> : <Pin className="w-5 h-5" />}
             </button>
             <button onClick={() => onViewPath()} className="p-2 text-gray-400 hover:text-blue-600 hover:bg-blue-50 rounded-md transition-colors" title="View Logic Path"><Layers className="w-5 h-5" /></button>
        </div>
      </div>

      <div className="flex-1 overflow-y-auto p-6 space-y-0">
        {commands.length === 0 ? (
          <div className="flex flex-col items-center justify-center py-12 text-gray-400 border-2 border-dashed border-gray-200 rounded-xl bg-gray-50/50 hover:bg-gray-50 transition-colors">
            <div className="flex flex-col items-center cursor-pointer" onClick={addCommand}>
                <Plus className="w-8 h-8 mb-2 opacity-50" />
                <p className="font-medium text-sm">Add your first command</p>
            </div>
            {operationType !== 'setup' && (
                <div className="mt-4">
                    <Button variant="secondary" size="sm" onClick={() => openSqlBuilder(null)}>
                        Build from SQL
                    </Button>
                </div>
            )}
          </div>
        ) : (
          <>
            {showStepOutline && (
                <StepOutline
                    commands={commands}
                    onJump={scrollToStep}
                    onCollapseAll={collapseAllSteps}
                    onExpandAll={expandAllSteps}
                    isPinned={outlinePinned}
                />
            )}
            <Reorder.Group axis="y" values={commands} onReorder={(newCommands) => {
                const updated = newCommands.map((c, i) => ({ ...c, order: i + 1 }));
                onUpdateCommands(operationId, updated);
            }}>
            {commands.map((cmd, index) => {
                let activeSchema: Record<string, DataType> = {};
                const normalizedDataSource = cmd.config.dataSource && cmd.config.dataSource !== 'stream'
                    ? (availableSourceAliases.find(sa =>
                        sa.linkId === cmd.config.dataSource ||
                        sa.alias === cmd.config.dataSource ||
                        sa.sourceTable === cmd.config.dataSource
                    )?.linkId || cmd.config.dataSource)
                    : '';
                
                if (normalizedDataSource) {
                    const sourceAlias = availableSourceAliases.find(sa => sa.linkId === normalizedDataSource);
                    let targetDatasetName = "";
                    if (sourceAlias) {
                        targetDatasetName = sourceAlias.sourceTable || "";
                    } else {
                        targetDatasetName = normalizedDataSource;
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
                            if (allGeneratedTablesGlobal.includes(targetDatasetName)) {
                            }
                        }
                    }
                }
                const activeSchemaRef = activeSchema;
                const fieldNames = Object.keys(activeSchema);
                const isCollapsed = !!collapsedSteps[cmd.id];

                const sourceLabel = getSourceLabel(availableSourceAliases, normalizedDataSource)
                    || (allGeneratedTablesGlobal.includes(normalizedDataSource) ? normalizedDataSource : '');
                const joinTargetType = cmd.config.joinTargetType || 'table';
                const joinTargetKey = joinTargetType === 'node' ? cmd.config.joinTargetNodeId : cmd.config.joinTable;
                const joinTargetNode = joinTargetType === 'node'
                    ? availableNodes.find(n => n.id === cmd.config.joinTargetNodeId)
                    : null;
                const joinTargetSource = joinTargetType === 'table'
                    ? availableSourceAliases.find(sa =>
                        sa.linkId === joinTargetKey ||
                        sa.alias === joinTargetKey ||
                        sa.sourceTable === joinTargetKey
                    )
                    : null;
                const joinTargetLabel = joinTargetType === 'node'
                    ? (joinTargetNode?.name || '')
                    : (
                        joinTargetSource?.alias
                        || joinTargetSource?.sourceTable
                        || getSourceLabel(availableSourceAliases, joinTargetKey || '')
                        || (allGeneratedTablesGlobal.includes(joinTargetKey || '') ? (joinTargetKey || '') : '')
                    );
                const joinTargetDatasetName = joinTargetType === 'table'
                    ? (joinTargetSource?.sourceTable || (joinTargetKey || ''))
                    : '';
                const joinTargetFields = getDatasetFieldNames(datasets, joinTargetDatasetName);
                const canUseJoinBuilder = joinTargetType === 'table' && fieldNames.length > 0 && joinTargetFields.length > 0;

                const joinLeftField = cmd.config.joinLeftField || '';
                const joinRightField = cmd.config.joinRightField || '';
                const joinOperator = cmd.config.joinOperator || '=';
                const joinLeftAliasDisplay = sourceLabel || 'main';
                const joinRightAliasDisplay = joinTargetLabel || 'sub';

                const summaryParts: string[] = [];
                if (sourceLabel) summaryParts.push(`Source: ${sourceLabel}`);
                if (cmd.type === 'join' && joinTargetLabel) summaryParts.push(`Join: ${joinTargetLabel}`);
                if (cmd.type === 'join' && cmd.config.on) summaryParts.push(`ON: ${cmd.config.on}`);
                if (cmd.type === 'group' && cmd.config.outputTableName) summaryParts.push(`Output: ${cmd.config.outputTableName}`);
                if (cmd.type === 'view' && sourceLabel) summaryParts.push(`View: ${sourceLabel}`);
                const stepSummary = summaryParts.length > 0 ? summaryParts.join(' • ') : 'No details';

                const localPrecedingOutputs = commands
                    .slice(0, index)
                    .filter(c => c.type === 'group' && c.config.outputTableName && c.config.outputTableName.trim() !== '')
                    .map(c => c.config.outputTableName!.trim());
                
                const availableGeneratedTablesForCmd = Array.from(new Set([...ancestorOutputs, ...localPrecedingOutputs])).sort();
                const normalizedJoinTarget = cmd.config.joinTable
                    ? (availableSourceAliases.find(sa =>
                        sa.linkId === cmd.config.joinTable ||
                        sa.alias === cmd.config.joinTable ||
                        sa.sourceTable === cmd.config.joinTable
                    )?.linkId || cmd.config.joinTable)
                    : '';
                const isInvalidJoinTarget = joinTargetType === 'table'
                    && !!normalizedJoinTarget
                    && !isKnownSourceValue(normalizedJoinTarget, availableGeneratedTablesForCmd);
                const unresolvedJoinTargetLabel = isInvalidJoinTarget && !resolveSourceAlias(normalizedJoinTarget)
                    ? normalizedJoinTarget
                    : '';

                const localVariables = commands
                    .slice(0, index)
                    .filter(c => c.type === 'save' && c.config.value)
                    .map(c => String(c.config.value));
                
                const currentScopeVariables = Array.from(new Set([...ancestorVariables, ...localVariables]));

                const showSourcePlaceholder = index === 0 && (!ancestors || ancestors.length === 0);
                const isSourceRequired = cmd.type !== 'source' && cmd.type !== 'multi_table';
                const rawSource = normalizeSourceToken(cmd.config?.dataSource);
                const normalizedSourceToken = normalizeSourceToken(normalizedDataSource);
                const sourceTokenToValidate = normalizedSourceToken || rawSource;
                const isMissingSource = isSourceRequired && (!rawSource || rawSource === 'stream');
                const isInvalidSource = isSourceRequired
                    && !isMissingSource
                    && !isKnownSourceValue(sourceTokenToValidate, availableGeneratedTablesForCmd);
                const hasSourceError = isMissingSource || isInvalidSource;
                const unresolvedSourceLabel = isInvalidSource && !resolveSourceAlias(sourceTokenToValidate)
                    ? sourceTokenToValidate
                    : '';

                const isDraggable = cmd.type !== 'source';
                return (
                <DraggableItem key={cmd.id} cmd={cmd}>
                    {(dragControls) => (
                    <React.Fragment>
                        <InsertDivider
                            index={index}
                            onInsert={insertCommand}
                            onOpenBuilder={operationType !== 'setup' ? (i) => openSqlBuilder(i) : undefined}
                        />
                        <div
                            ref={(el) => { stepRefs.current[cmd.id] = el; }}
                            className="relative group bg-white border border-gray-200 rounded-lg shadow-sm hover:shadow-md transition-all duration-200"
                        >
                            <div className="flex items-center justify-between p-3 border-b border-gray-100 bg-white rounded-t-lg">
                                <div className="flex items-center space-x-3 overflow-hidden">
                                    <div 
                                        className={`cursor-move text-gray-300 hover:text-gray-500 ${!isDraggable ? 'cursor-not-allowed opacity-50' : ''}`}
                                        onPointerDown={(e) => isDraggable && dragControls.start(e)}
                                    >
                                        <GripVertical className="w-4 h-4" />
                                    </div>
                                <div className="flex items-center">
                                    <select value={cmd.type} onChange={(e) => updateCommand(cmd.id, 'type', e.target.value)} className="text-sm font-bold text-gray-800 bg-transparent border-none focus:ring-0 cursor-pointer hover:text-blue-600 pl-0 pr-6 py-0">
                                        <option value="filter">Filter</option>
                                        <option value="join">Join</option>
                                        <option value="sort">Sort</option>
                                        <option value="transform">Mapping</option>
                                        <option value="group">Group</option>
                                        <option value="save">Save Variable</option>
                                        <option value="view">View / Select Table</option>
                                        <option value="multi_table">Complex View</option>
                                    </select>
                                    {appearance?.showCommandIds && (
                                        <span className="ml-2 text-[10px] font-mono text-gray-400 bg-gray-50 border border-gray-200 rounded px-1.5 py-0.5">
                                            {cmd.id}
                                        </span>
                                    )}
                                </div>
                            </div>
                            <div className="flex items-center space-x-2">
                                {/* Run to Step Button */}
                                <button
                                    onClick={() => onViewPath(cmd.id)}
                                    className="p-1 text-gray-400 hover:text-purple-600 hover:bg-purple-50 rounded transition-colors"
                                    title="View Path Logic Synthesis"
                                >
                                    <Route className="w-3.5 h-3.5" />
                                </button>
                                {onViewLineage && (() => {
                                    const cmdSourceTable =
                                        (cmd.config.dataSource && cmd.config.dataSource !== 'stream')
                                            ? cmd.config.dataSource
                                            : cmd.config.mainTable || undefined;
                                    return (
                                        <button
                                            onClick={() => onViewLineage(cmd.id, cmdSourceTable)}
                                            className="p-1 text-gray-400 hover:text-blue-600 hover:bg-blue-50 rounded transition-colors"
                                            title={cmdSourceTable ? `查看字段血缘（表：${cmdSourceTable}）` : '查看此步骤的字段血缘'}
                                        >
                                            <GitBranch className="w-3.5 h-3.5" />
                                        </button>
                                    );
                                })()}
                                <button 
                                    onClick={() => handleGenerateSql(cmd.id)}
                                    disabled={
                                        (cmd.type === 'transform' && cmd.config.mappings?.some(m => m.mode === 'python')) ||
                                        (cmd.type === 'join' && cmd.config.joinTargetType === 'node')
                                    }
                                    className={`p-1 rounded transition-colors ${
                                        (cmd.type === 'transform' && cmd.config.mappings?.some(m => m.mode === 'python')) ||
                                        (cmd.type === 'join' && cmd.config.joinTargetType === 'node')
                                        ? 'text-gray-300 cursor-not-allowed'
                                        : 'text-gray-400 hover:text-blue-600 hover:bg-blue-50'
                                    }`}
                                    title={
                                        (cmd.type === 'transform' && cmd.config.mappings?.some(m => m.mode === 'python'))
                                        ? "SQL generation not supported for Python transformations"
                                        : (cmd.type === 'join' && cmd.config.joinTargetType === 'node')
                                        ? "SQL generation not supported for dynamic Node joins"
                                        : "Generate SQL"
                                    }
                                >
                                    <Code className="w-3.5 h-3.5" />
                                </button>
                                <button 
                                    onClick={() => onRun && onRun(cmd.id)}
                                    disabled={!canRun || hasSourceError}
                                    className={`p-1 rounded transition-colors ${
                                        canRun && !hasSourceError ? 'text-gray-400 hover:text-blue-600 hover:bg-blue-50' : 'text-gray-300 cursor-not-allowed'
                                    }`}
                                    title={
                                        !canRun
                                        ? "Configure data sources before running"
                                        : isMissingSource
                                        ? "Select a data source for this step before running"
                                        : isInvalidSource
                                        ? "Selected data source is unavailable. Import it first or choose another source."
                                        : "Run logic up to this step"
                                    }
                                >
                                    <Play className="w-3.5 h-3.5" />
                                </button>
                                <button
                                    onClick={() => toggleStepCollapsed(cmd.id)}
                                    className="p-1 text-gray-400 hover:text-gray-700 hover:bg-gray-100 rounded transition-colors"
                                    title={isCollapsed ? "Expand step" : "Collapse step"}
                                >
                                    {isCollapsed ? <ChevronDown className="w-3.5 h-3.5" /> : <ChevronUp className="w-3.5 h-3.5" />}
                                </button>
                                <span className="text-[10px] font-mono text-gray-300">#{index + 1}</span>
                                <button onClick={() => removeCommand(cmd.id)} className="p-1 text-gray-300 hover:text-red-500 hover:bg-red-50 rounded transition-colors"><Trash2 className="w-3.5 h-3.5" /></button>
                            </div>
                        </div>

                        {!isCollapsed && (
                        <>
                        {cmd.type !== 'view' && (
                            <div className={`px-3 py-1.5 border-b border-gray-100 flex items-center space-x-2 ${hasSourceError ? 'bg-red-50/50' : 'bg-gray-50'}`}>
                                <Database className={`w-3 h-3 ${hasSourceError ? 'text-red-400' : 'text-gray-400'}`} />
                                <span className="text-[10px] font-semibold text-gray-500 uppercase tracking-wide">Select Dataset:</span>
                                <select
                                        value={normalizedDataSource}
                                        onChange={(e) => updateCommand(cmd.id, 'config.dataSource', e.target.value)}
                                        className={`bg-transparent text-xs font-medium focus:outline-none cursor-pointer border-none p-0 pr-4 hover:underline ${hasSourceError ? 'text-red-600' : 'text-blue-700'}`}
                                    >
                                        <option value="">{showSourcePlaceholder ? "-- Select Source --" : ""}</option>
                                        {availableSourceAliases.length > 0 && (
                                            <optgroup label="Data Sources">
                                                {availableSourceAliases.map(sa => (
                                                    <option
                                                        key={sa.linkId}
                                                        value={sa.linkId}
                                                        style={!getKnownSourceTable(sa.sourceTable) ? unresolvedSourceOptionStyle : undefined}
                                                    >
                                                        {formatSourceOptionLabel(sa.alias, getKnownSourceTable(sa.sourceTable), sa.linkId)}
                                                    </option>
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
                                        {unresolvedSourceLabel && (
                                            <option value={unresolvedSourceLabel} disabled>
                                                {unresolvedSourceLabel}
                                            </option>
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
                                            className={`${isInvalidSource ? errorInputStyles : baseInputStyles} py-2`}
                                            value={normalizedDataSource} 
                                            onChange={(e) => updateCommand(cmd.id, 'config.dataSource', e.target.value)}
                                        >
                                            <option value="">-- Select Table --</option>
                                            {availableSourceAliases.length > 0 && (
                                                <optgroup label="Data Sources">
                                                    {availableSourceAliases.map(sa => (
                                                        <option
                                                            key={sa.linkId}
                                                            value={sa.linkId}
                                                            style={!getKnownSourceTable(sa.sourceTable) ? unresolvedSourceOptionStyle : undefined}
                                                        >
                                                            {formatSourceOptionLabel(sa.alias, getKnownSourceTable(sa.sourceTable), sa.linkId)}
                                                        </option>
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
                                            {unresolvedSourceLabel && (
                                                <option value={unresolvedSourceLabel} disabled>
                                                    {unresolvedSourceLabel}
                                                </option>
                                            )}
                                        </select>
                                    </div>

                                    <div className="border-t border-purple-100 pt-4 space-y-3">
                                        <div className="flex items-center justify-between">
                                            <label className="text-xs font-bold text-gray-500 uppercase">Fields</label>
                                            <button onClick={() => addViewField(cmd.id, cmd.config.viewFields || [])} className="text-xs text-blue-600 hover:underline flex items-center font-medium"><Plus className="w-3 h-3 mr-1" /> Add Field</button>
                                        </div>
                                        {(cmd.config.viewFields || []).map((vf: any, i: number, list: any[]) => {
                                            const selectedFields = list.map(s => s.field).filter(Boolean);
                                            return (
                                            <div key={`${vf.field}-${i}`} className="grid grid-cols-12 gap-2 items-center">
                                                <div className="col-span-1 flex flex-col items-center space-y-1 -ml-1">
                                                    <button onClick={() => moveViewField(cmd.id, cmd.config.viewFields || [], i, 'up')} className="text-gray-400 hover:text-gray-600" title="Move Up">
                                                        <ChevronUp className="w-4 h-4" />
                                                    </button>
                                                    <button onClick={() => moveViewField(cmd.id, cmd.config.viewFields || [], i, 'down')} className="text-gray-400 hover:text-gray-600" title="Move Down">
                                                        <ChevronDown className="w-4 h-4" />
                                                    </button>
                                                </div>
                                                <div className="col-span-5">
                                                <select className={baseInputStyles} value={vf.field || ''} onChange={(e) => updateViewField(cmd.id, cmd.config.viewFields || [], i, 'field', e.target.value)}>
                                                    <option value="">Select Field...</option>
                                                    {withCurrentOption(fieldNames, vf.field).map(f => {
                                                        const disabled = selectedFields.includes(f) && f !== vf.field;
                                                        return <option key={f} value={f} disabled={disabled}>{f}</option>;
                                                    })}
                                                </select>
                                                </div>
                                                <div className="col-span-3 flex items-center">
                                                <label className="flex items-center text-xs text-gray-600 space-x-1">
                                                    <input type="checkbox" checked={!!vf.distinct} onChange={(e) => updateViewField(cmd.id, cmd.config.viewFields || [], i, 'distinct', e.target.checked)} />
                                                    <span>Distinct</span>
                                                </label>
                                                </div>
                                                <div className="col-span-3 flex items-center justify-end space-x-1">
                                                    <button onClick={() => removeViewField(cmd.id, cmd.config.viewFields || [], i)} className="text-red-400 hover:text-red-600"><Trash2 className="w-4 h-4" /></button>
                                                </div>
                                            </div>
                                        )})}
                                        <div className="text-[11px] text-gray-500">If any field is marked distinct, only those fields are returned.</div>
                                    </div>

                                    <div className="border-t border-purple-100 pt-4 space-y-3">
                                        <div className="flex items-center justify-between">
                                            <label className="text-xs font-bold text-gray-500 uppercase">Sort By</label>
                                            <button onClick={() => addViewSort(cmd.id, cmd.config.viewSorts || (cmd.config.viewSortField ? [{ field: cmd.config.viewSortField, ascending: cmd.config.viewSortAscending !== false }] : []))} className="text-xs text-blue-600 hover:underline flex items-center font-medium"><Plus className="w-3 h-3 mr-1" /> Add Sort</button>
                                        </div>
                                        {(cmd.config.viewSorts || (cmd.config.viewSortField ? [{ field: cmd.config.viewSortField, ascending: cmd.config.viewSortAscending !== false }] : [])).map((vs: any, i: number, list: any[]) => {
                                            const selectedFields = list.map(s => s.field).filter(Boolean);
                                            const currentList = (cmd.config.viewSorts || (cmd.config.viewSortField ? [{ field: cmd.config.viewSortField, ascending: cmd.config.viewSortAscending !== false }] : []));
                                            return (
                                            <div key={`${vs.field}-${i}`} className="grid grid-cols-12 gap-2 items-center">
                                                <div className="col-span-1 flex flex-col items-center space-y-1 -ml-1">
                                                    <button onClick={() => moveViewSort(cmd.id, currentList, i, 'up')} className="text-gray-400 hover:text-gray-600" title="Move Up">
                                                        <ChevronUp className="w-4 h-4" />
                                                    </button>
                                                    <button onClick={() => moveViewSort(cmd.id, currentList, i, 'down')} className="text-gray-400 hover:text-gray-600" title="Move Down">
                                                        <ChevronDown className="w-4 h-4" />
                                                    </button>
                                                </div>
                                                <div className="col-span-6">
                                                    <select className={baseInputStyles} value={vs.field || ''} onChange={(e) => updateViewSort(cmd.id, currentList, i, 'field', e.target.value)}>
                                                        <option value="">-- Field --</option>
                                                        {withCurrentOption(fieldNames, vs.field).map(f => {
                                                            const disabled = selectedFields.includes(f) && f !== vs.field;
                                                            return <option key={f} value={f} disabled={disabled}>{f}</option>;
                                                        })}
                                                    </select>
                                                </div>
                                                <div className="col-span-3">
                                                    <select className={baseInputStyles} value={vs.ascending === false ? 'desc' : 'asc'} onChange={(e) => updateViewSort(cmd.id, currentList, i, 'ascending', e.target.value !== 'desc')}>
                                                        <option value="asc">ASC</option>
                                                        <option value="desc">DESC</option>
                                                    </select>
                                                </div>
                                                <div className="col-span-2 flex justify-end">
                                                    <button onClick={() => removeViewSort(cmd.id, currentList, i)} className="text-red-400 hover:text-red-600"><Trash2 className="w-4 h-4" /></button>
                                                </div>
                                            </div>
                                        )})}
                                    </div>

                                    <div className="border-t border-purple-100 pt-4">
                                        <label className="block text-xs font-bold text-gray-500 uppercase mb-2">Limit</label>
                                        <input className={baseInputStyles} type="number" min={0} value={cmd.config.viewLimit ?? ''} onChange={(e) => updateCommand(cmd.id, 'config.viewLimit', e.target.value ? Number(e.target.value) : undefined)} placeholder="e.g. 100" />
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
                                            {(cmd.config.subTables || []).map((sub, i) => {
                                                const sourceAlias = availableSourceAliases.find(sa =>
                                                    sa.linkId === sub.table ||
                                                    sa.alias === sub.table ||
                                                    sa.sourceTable === sub.table
                                                );
                                                const normalizedSubTable = sub.table
                                                    ? (sourceAlias?.linkId || sub.table)
                                                    : '';
                                                const isInvalidSubTable = !!normalizedSubTable
                                                    && !isKnownSourceValue(normalizedSubTable, availableGeneratedTablesForCmd);
                                                const unresolvedSubTableLabel = isInvalidSubTable && !resolveSourceAlias(normalizedSubTable)
                                                    ? normalizedSubTable
                                                    : '';
                                                const resolvedSubTableName = sourceAlias?.sourceTable || normalizedSubTable;
                                                const subTableFields = getDatasetFieldNames(datasets, resolvedSubTableName);
                                                const mainFieldCandidates = Array.from(new Set([
                                                    ...fieldNames,
                                                    ...Object.keys(inputSchema || {})
                                                ]));
                                                const mainAliasLabel = sourceLabel || 'main';
                                                const subAliasLabel = sourceAlias?.alias || sourceAlias?.sourceTable || 'sub';
                                                const onConditionGroup: SubTableConditionGroup = sub.onConditionGroup || sub.conditionGroup || {
                                                    id: `sub_group_${sub.id}`,
                                                    type: 'group',
                                                    logicalOperator: 'AND',
                                                    conditions: []
                                                };
                                                const onRuleCount = countSubTableOnRules(onConditionGroup);
                                                return (
                                                <div key={sub.id} className="bg-gray-50 p-2 rounded border border-gray-100 space-y-2">
                                                    <div className="grid grid-cols-12 gap-2 items-center">
                                                        <div className="col-span-5">
                                                            <select className={isInvalidSubTable ? errorInputStyles : baseInputStyles} value={normalizedSubTable} onChange={(e) => updateSubTable(cmd.id, cmd.config.subTables || [], i, 'table', e.target.value)}>
                                                                <option value="">Select Source...</option>
                                                                {availableSourceAliases.map(sa => (
                                                                    <option
                                                                        key={sa.linkId}
                                                                        value={sa.linkId}
                                                                        style={!getKnownSourceTable(sa.sourceTable) ? unresolvedSourceOptionStyle : undefined}
                                                                    >
                                                                        {formatSourceOptionLabel(sa.alias, getKnownSourceTable(sa.sourceTable), sa.linkId)}
                                                                    </option>
                                                                ))}
                                                                {availableGeneratedTablesForCmd.map(name => (
                                                                    <option key={name} value={name}>{name}</option>
                                                                ))}
                                                                {unresolvedSubTableLabel && (
                                                                    <option value={unresolvedSubTableLabel} disabled>
                                                                        {unresolvedSubTableLabel}
                                                                    </option>
                                                                )}
                                                            </select>
                                                        </div>
                                                        <div className="col-span-6">
                                                            <input className={baseInputStyles} placeholder="Tab Name" value={sub.label} onChange={(e) => updateSubTable(cmd.id, cmd.config.subTables || [], i, 'label', e.target.value)} />
                                                        </div>
                                                        <div className="col-span-1 flex items-end justify-center pb-1">
                                                            <button onClick={() => removeSubTable(cmd.id, cmd.config.subTables || [], i)} className="text-red-400 hover:text-red-600"><Trash2 className="w-4 h-4" /></button>
                                                        </div>
                                                    </div>

                                                    <div className="rounded-md border border-gray-200 bg-white p-3">
                                                        <div className="flex items-center justify-between mb-2">
                                                            <div className="text-[10px] font-bold text-gray-500 uppercase">ON Condition Builder</div>
                                                            <div className="text-[10px] font-medium text-gray-400">{onRuleCount} rule{onRuleCount === 1 ? '' : 's'}</div>
                                                        </div>
                                                        <div className="mb-2 flex items-center gap-2 text-[10px]">
                                                            <span className="px-1.5 py-0.5 rounded bg-blue-50 text-blue-700 border border-blue-100">Sub: {subAliasLabel}</span>
                                                            <span className="px-1.5 py-0.5 rounded bg-emerald-50 text-emerald-700 border border-emerald-100">Main: {mainAliasLabel}</span>
                                                        </div>
                                                        <SubTableConditionGroupEditor
                                                            group={onConditionGroup}
                                                            subFields={subTableFields}
                                                            mainFields={mainFieldCandidates}
                                                            subAliasLabel={subAliasLabel}
                                                            mainAliasLabel={mainAliasLabel}
                                                            onUpdate={(group) => updateSubTableOnConditionGroup(cmd.id, cmd.config.subTables || [], i, group)}
                                                            onRemove={() => {}}
                                                            isRoot={true}
                                                        />
                                                    </div>
                                                </div>
                                            )})}
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
                                                            {withCurrentOption(fieldNames, field).map(f => <option key={f} value={f}>{f}</option>)}
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
                                                            <option value="">Field...</option><option value="*">Any (*)</option>{withCurrentOption(fieldNames, agg.field === '*' ? '' : agg.field).map(f => <option key={f} value={f}>{f}</option>)}
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
                                            <select
                                                className={isInvalidJoinTarget ? errorInputStyles : baseInputStyles}
                                                value={normalizedJoinTarget}
                                                onChange={(e) => updateCommand(cmd.id, 'config.joinTable', e.target.value)}
                                            >
                                                <option value="">-- Select Source --</option>
                                                {availableSourceAliases.length > 0 && (
                                                    <optgroup label="Data Sources">
                                                        {availableSourceAliases.map(sa => (
                                                            <option
                                                                key={sa.linkId}
                                                                value={sa.linkId}
                                                                style={!getKnownSourceTable(sa.sourceTable) ? unresolvedSourceOptionStyle : undefined}
                                                            >
                                                                {formatSourceOptionLabel(sa.alias, getKnownSourceTable(sa.sourceTable), sa.linkId)}
                                                            </option>
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
                                                {unresolvedJoinTargetLabel && (
                                                    <option value={unresolvedJoinTargetLabel} disabled>
                                                        {unresolvedJoinTargetLabel}
                                                    </option>
                                                )}
                                            </select>
                                        )}
                                    </div>
                                    <div className="col-span-12">
                                        <div className="p-3 bg-gray-50 border border-gray-200 rounded-lg">
                                            <div className="flex items-center justify-between mb-2">
                                                <div className="text-[10px] font-bold text-gray-500 uppercase">ON Condition Builder</div>
                                                <div className="text-[10px] font-mono text-gray-500">{cmd.config.on || 'No condition'}</div>
                                            </div>
                                            <div className="mb-2 flex items-center gap-2 text-[10px]">
                                                <span className="px-1.5 py-0.5 rounded bg-blue-50 text-blue-700 border border-blue-100">Left: {joinLeftAliasDisplay}</span>
                                                <span className="px-1.5 py-0.5 rounded bg-emerald-50 text-emerald-700 border border-emerald-100">Right: {joinRightAliasDisplay}</span>
                                            </div>
                                            {cmd.config.joinTargetType === 'node' && (
                                                <div className="text-xs text-gray-500">
                                                    Select a table join target to use the builder.
                                                </div>
                                            )}
                                            {cmd.config.joinTargetType !== 'node' && !canUseJoinBuilder && (
                                                <div className="text-xs text-gray-500">
                                                    Select source datasets to enable field pickers.
                                                </div>
                                            )}
                                            {cmd.config.joinTargetType !== 'node' && canUseJoinBuilder && (
                                                <>
                                                <div className="grid grid-cols-12 gap-2 items-center">
                                                    <div className="col-span-5">
                                                        <select
                                                            className={getFieldStyle(joinLeftField, fieldNames)}
                                                            value={joinLeftField}
                                                            onChange={(e) => updateJoinConditionFromBuilder(
                                                                cmd.id,
                                                                e.target.value,
                                                                joinOperator,
                                                                joinRightField,
                                                                joinLeftAliasDisplay,
                                                                joinRightAliasDisplay
                                                            )}
                                                        >
                                                            <option value="">Left Field...</option>
                                                            {withCurrentOption(fieldNames, joinLeftField).map(f => <option key={f} value={f}>{joinLeftAliasDisplay}.{f}</option>)}
                                                        </select>
                                                    </div>
                                                    <div className="col-span-2">
                                                        <select
                                                            className={baseInputStyles}
                                                            value={joinOperator}
                                                            onChange={(e) => updateJoinConditionFromBuilder(
                                                                cmd.id,
                                                                joinLeftField,
                                                                e.target.value,
                                                                joinRightField,
                                                                joinLeftAliasDisplay,
                                                                joinRightAliasDisplay
                                                            )}
                                                        >
                                                            <option value="=">=</option>
                                                            <option value="!=">!=</option>
                                                            <option value=">">&gt;</option>
                                                            <option value="<">&lt;</option>
                                                            <option value=">=">&gt;=</option>
                                                            <option value="<=">&lt;=</option>
                                                        </select>
                                                    </div>
                                                    <div className="col-span-5">
                                                        <select
                                                            className={getFieldStyle(joinRightField, joinTargetFields)}
                                                            value={joinRightField}
                                                            onChange={(e) => updateJoinConditionFromBuilder(
                                                                cmd.id,
                                                                joinLeftField,
                                                                joinOperator,
                                                                e.target.value,
                                                                joinLeftAliasDisplay,
                                                                joinRightAliasDisplay
                                                            )}
                                                        >
                                                            <option value="">Right Field...</option>
                                                            {withCurrentOption(joinTargetFields, joinRightField).map(f => <option key={f} value={f}>{joinRightAliasDisplay}.{f}</option>)}
                                                        </select>
                                                    </div>
                                                </div>
                                                </>
                                            )}
                                        </div>
                                    </div>
                                </div>
                            )}

                            {cmd.type === 'save' && (
                                <div className="grid grid-cols-12 gap-4">
                                    <div className="col-span-4">
                                        <select className={getFieldStyle(cmd.config.field || '', fieldNames)} value={cmd.config.field || ''} onChange={(e) => updateCommand(cmd.id, 'config.field', e.target.value)}>
                                            <option value="">Select Field...</option>
                                            {withCurrentOption(fieldNames, cmd.config.field || '').map(f => <option key={f} value={f}>{f}</option>)}
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
                                                        <textarea className={codeAreaStyles} rows={5} value={mapping.expression} onChange={(e) => updateMappingRule(cmd.id, cmd.config.mappings || [], i, 'expression', e.target.value)} placeholder={PYTHON_TEMPLATE} />
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
                                            {withCurrentOption(fieldNames, cmd.config.field || '').map(f => <option key={f} value={f}>{f}</option>)}
                                        </select>
                                    </div>
                                    <div className="col-span-4">
                                        <select className={baseInputStyles} value={cmd.config.ascending === false ? 'desc' : 'asc'} onChange={(e) => updateCommand(cmd.id, 'config.ascending', e.target.value === 'asc')}>
                                            <option value="asc">Asc</option><option value="desc">Desc</option>
                                        </select>
                                    </div>
                                    </div>
                            )}

                            {cmd.type === 'validate' && (() => {
                                const rules: ValidationRule[] = cmd.config.validationRules || [];
                                const mode = cmd.config.validationMode || 'warn';
                                const addRule = () => {
                                    const newRule: ValidationRule = { id: `vr_${Date.now()}`, field: fieldNames[0] || '', rule: 'not_null' };
                                    updateCommand(cmd.id, 'config.validationRules', [...rules, newRule]);
                                };
                                const removeRule = (ruleId: string) => {
                                    updateCommand(cmd.id, 'config.validationRules', rules.filter(r => r.id !== ruleId));
                                };
                                const updateRule = (ruleId: string, key: keyof ValidationRule, val: any) => {
                                    updateCommand(cmd.id, 'config.validationRules', rules.map(r => r.id === ruleId ? { ...r, [key]: val } : r));
                                };
                                return (
                                    <div className="space-y-3">
                                        {rules.map((rule) => (
                                            <div key={rule.id} className="border border-gray-200 rounded-md p-2 space-y-2 bg-white">
                                                <div className="grid grid-cols-12 gap-2">
                                                    <div className="col-span-5">
                                                        <select className={baseInputStyles} value={rule.field} onChange={(e) => updateRule(rule.id, 'field', e.target.value)}>
                                                            <option value="">Select Field...</option>
                                                            {fieldNames.map(f => <option key={f} value={f}>{f}</option>)}
                                                        </select>
                                                    </div>
                                                    <div className="col-span-5">
                                                        <select className={baseInputStyles} value={rule.rule} onChange={(e) => updateRule(rule.id, 'rule', e.target.value as ValidationRule['rule'])}>
                                                            <option value="not_null">Not Null</option>
                                                            <option value="unique">Unique</option>
                                                            <option value="range">Range</option>
                                                            <option value="regex">Regex</option>
                                                            <option value="enum">Enum</option>
                                                            <option value="type_check">Type Check</option>
                                                        </select>
                                                    </div>
                                                    <div className="col-span-2 flex justify-end">
                                                        <button onClick={() => removeRule(rule.id)} className="text-gray-400 hover:text-red-500 p-1"><Trash2 className="w-3.5 h-3.5" /></button>
                                                    </div>
                                                </div>
                                                {rule.rule === 'range' && (
                                                    <div className="grid grid-cols-2 gap-2">
                                                        <input type="number" className={baseInputStyles} placeholder="Min" value={rule.min ?? ''} onChange={(e) => updateRule(rule.id, 'min', e.target.value === '' ? undefined : Number(e.target.value))} />
                                                        <input type="number" className={baseInputStyles} placeholder="Max" value={rule.max ?? ''} onChange={(e) => updateRule(rule.id, 'max', e.target.value === '' ? undefined : Number(e.target.value))} />
                                                    </div>
                                                )}
                                                {rule.rule === 'regex' && (
                                                    <input type="text" className={baseInputStyles} placeholder="Regex pattern (e.g. ^\d+$)" value={rule.pattern || ''} onChange={(e) => updateRule(rule.id, 'pattern', e.target.value)} />
                                                )}
                                                {rule.rule === 'enum' && (
                                                    <input type="text" className={baseInputStyles} placeholder="Allowed values, comma-separated" value={(rule.enumValues || []).join(',')} onChange={(e) => updateRule(rule.id, 'enumValues', e.target.value.split(',').map(s => s.trim()).filter(Boolean))} />
                                                )}
                                                {rule.rule === 'type_check' && (
                                                    <select className={baseInputStyles} value={rule.expectedType || ''} onChange={(e) => updateRule(rule.id, 'expectedType', e.target.value as DataType)}>
                                                        <option value="">Select Type...</option>
                                                        <option value="string">String</option>
                                                        <option value="number">Number</option>
                                                        <option value="boolean">Boolean</option>
                                                        <option value="date">Date</option>
                                                    </select>
                                                )}
                                                <input type="text" className={baseInputStyles} placeholder="Custom error message (optional)" value={rule.message || ''} onChange={(e) => updateRule(rule.id, 'message', e.target.value || undefined)} />
                                            </div>
                                        ))}
                                        <button onClick={addRule} className="text-xs text-blue-600 hover:underline"><Plus className="w-3 h-3 inline mr-1" />Add Rule</button>
                                        <div className="flex items-center gap-2 pt-1 border-t border-gray-100 mt-2">
                                            <span className="text-xs text-gray-500">On failure:</span>
                                            {(['warn', 'fail', 'flag'] as const).map(m => (
                                                <button key={m} onClick={() => updateCommand(cmd.id, 'config.validationMode', m)} className={`text-xs px-2 py-0.5 rounded border ${mode === m ? 'bg-blue-600 text-white border-blue-600' : 'text-gray-600 border-gray-300 hover:bg-gray-50'}`}>
                                                    {m === 'warn' ? 'Warn' : m === 'fail' ? 'Fail (Stop)' : 'Flag Rows'}
                                                </button>
                                            ))}
                                        </div>
                                    </div>
                                );
                            })()}
                        </div>
                        </>
                        )}

                        {isCollapsed && (
                            <div className="px-3 py-2 text-xs text-gray-500 bg-gray-50 border-t border-gray-100">
                                <span className="font-semibold text-gray-700 mr-2">{COMMAND_LABELS[cmd.type] || cmd.type}</span>
                                <span className="text-gray-600">{stepSummary}</span>
                            </div>
                        )}
                    </div>
                </React.Fragment>
                    )}
                </DraggableItem>
                );
            })}
            </Reorder.Group>
            
            <div className="flex justify-center pt-2">
                <div className="flex items-center space-x-2">
                    <Button variant="secondary" size="sm" onClick={addCommand} icon={<Plus className="w-4 h-4" />}>Add Step</Button>
                    {operationType !== 'setup' && (
                        <Button variant="secondary" size="sm" onClick={() => openSqlBuilder(null)}>
                            Build from SQL
                        </Button>
                    )}
                </div>
            </div>
          </>
        )}
      </div>
      <SqlPreviewModal 
          isOpen={sqlModalOpen} 
          onClose={() => setSqlModalOpen(false)} 
          sql={sqlContent} 
          loading={sqlLoading} 
      />
      <SqlBuilderModal
          isOpen={sqlBuilderOpen}
          sqlInput={sqlBuilderInput}
          onSqlInputChange={setSqlBuilderInput}
          onParse={handleParseSql}
          onApply={handleApplySqlCommands}
          onClose={() => {
              setSqlBuilderOpen(false);
              setSqlInsertIndex(null);
          }}
          warnings={sqlBuilderWarnings}
          error={sqlBuilderError}
          commands={sqlBuilderCommands}
          datasets={datasets}
          availableSourceAliases={availableSourceAliases}
          onUpdateCommands={setSqlBuilderCommands}
          existingCommands={compareCommands}
          renderSummary={renderSqlCommandSummary}
      />
    </div>
  );
};
