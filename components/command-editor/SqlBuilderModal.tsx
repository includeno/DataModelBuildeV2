import React, { useEffect, useMemo, useState } from 'react';
import { ChevronDown, ChevronUp, X } from 'lucide-react';
import { Button } from '../Button';
import { Command, DataType, Dataset, FilterGroup } from '../../types';
import { FilterGroupEditor } from './FilterGroupEditor';
import { baseInputStyles, errorInputStyles } from './constants';
import { formatSourceOptionLabel, getDatasetFieldNames, SourceAlias } from './helpers';

interface SqlBuilderModalProps {
    isOpen: boolean;
    sqlInput: string;
    onSqlInputChange: (value: string) => void;
    onParse: () => void;
    onApply: () => void;
    onClose: () => void;
    warnings: string[];
    error?: string | null;
    commands: Command[];
    datasets: Dataset[];
    availableSourceAliases: SourceAlias[];
    onUpdateCommands: (commands: Command[]) => void;
    existingCommands?: Command[];
    renderSummary: (cmd: Command) => string;
}

export const SqlBuilderModal: React.FC<SqlBuilderModalProps> = ({
    isOpen,
    sqlInput,
    onSqlInputChange,
    onParse,
    onApply,
    onClose,
    warnings,
    error,
    commands,
    datasets,
    availableSourceAliases,
    onUpdateCommands,
    existingCommands = [],
    renderSummary
}) => {
    if (!isOpen) return null;

    const [expanded, setExpanded] = useState<Record<string, boolean>>({});
    const [compareWithExisting, setCompareWithExisting] = useState(false);
    const [prunedNotice, setPrunedNotice] = useState<string | null>(null);

    useEffect(() => {
        if (!isOpen) setExpanded({});
    }, [isOpen]);

    useEffect(() => {
        if (compareWithExisting) {
            pruneAgainstExisting();
        }
    }, [compareWithExisting]);

    const updateCommand = (id: string, updater: (cmd: Command) => Command) => {
        const next = commands.map(cmd => (cmd.id === id ? updater(cmd) : cmd));
        onUpdateCommands(next);
    };

    const flattenFilterConditions = (cmds: Command[]) => {
        const list: FilterCondition[] = [];
        const walk = (group: FilterGroup) => {
            group.conditions.forEach((item) => {
                if (item.type === 'group') walk(item);
                else list.push(item);
            });
        };
        cmds.forEach(cmd => {
            if (cmd.type === 'filter' && cmd.config?.filterRoot) {
                walk(cmd.config.filterRoot as FilterGroup);
            }
        });
        return list;
    };

    const normalizeValue = (val: any) => {
        if (Array.isArray(val)) {
            return [...val].map(v => String(v)).sort().join('|');
        }
        if (val === null || val === undefined) return 'null';
        return String(val);
    };

    const conditionSignature = (cond: FilterCondition) => {
        return `${cond.field}|${cond.operator}|${normalizeValue(cond.value)}`;
    };

    const existingConditions = useMemo(() => flattenFilterConditions(existingCommands), [existingCommands]);
    const existingByField = useMemo(() => {
        const map: Record<string, FilterCondition[]> = {};
        existingConditions.forEach(cond => {
            if (!map[cond.field]) map[cond.field] = [];
            map[cond.field].push(cond);
        });
        return map;
    }, [existingConditions]);

    const existingSignatureSet = useMemo(() => {
        return new Set(existingConditions.map(conditionSignature));
    }, [existingConditions]);

    const viewFieldsSignature = (fields: any[] | undefined) => {
        if (!Array.isArray(fields) || fields.length === 0) return '*';
        const parts = fields
            .map(f => ({ field: String(f.field || ''), distinct: !!f.distinct }))
            .filter(f => f.field);
        if (parts.length === 0) return '*';
        return parts
            .map(p => `${p.field}:${p.distinct ? '1' : '0'}`)
            .sort()
            .join(',');
    };

    const commandSignature = (cmd: Command) => {
        if (cmd.type === 'sort') {
            const asc = cmd.config.ascending === false ? 'desc' : 'asc';
            return `sort|${cmd.config.field || ''}|${asc}`;
        }
        if (cmd.type === 'view') {
            const fields = viewFieldsSignature(cmd.config.viewFields);
            const limit = cmd.config.viewLimit ?? '';
            return `view|${fields}|${limit}`;
        }
        return cmd.type;
    };

    const existingCommandSignatures = useMemo(() => {
        const set = new Set<string>();
        existingCommands.forEach(cmd => {
            if (cmd.type === 'sort' || cmd.type === 'view') {
                set.add(commandSignature(cmd));
            }
        });
        return set;
    }, [existingCommands]);

    const pruneFilterGroup = (group: FilterGroup) => {
        let removed = 0;
        const nextConditions: (FilterCondition | FilterGroup)[] = [];
        group.conditions.forEach((item) => {
            if (item.type === 'group') {
                const result = pruneFilterGroup(item);
                removed += result.removed;
                if (result.group) nextConditions.push(result.group);
                else removed += 1;
            } else {
                const sig = conditionSignature(item);
                if (existingSignatureSet.has(sig)) {
                    removed += 1;
                } else {
                    nextConditions.push(item);
                }
            }
        });
        if (nextConditions.length === 0) return { group: null as FilterGroup | null, removed };
        return { group: { ...group, conditions: nextConditions }, removed };
    };

    const pruneAgainstExisting = () => {
        let removedConditions = 0;
        let removedCommands = 0;
        const next = commands.map(cmd => {
            if (cmd.type === 'filter' && cmd.config?.filterRoot) {
                const result = pruneFilterGroup(cmd.config.filterRoot as FilterGroup);
                removedConditions += result.removed;
                if (!result.group) {
                    removedCommands += 1;
                    return null;
                }
                return { ...cmd, config: { ...cmd.config, filterRoot: result.group } };
            }
            if (cmd.type === 'sort' || cmd.type === 'view') {
                const sig = commandSignature(cmd);
                if (existingCommandSignatures.has(sig)) {
                    removedCommands += 1;
                    return null;
                }
            }
            return cmd;
        }).filter(Boolean) as Command[];

        if (removedConditions > 0 || removedCommands > 0) {
            const parts: string[] = [];
            if (removedConditions > 0) parts.push(`${removedConditions} condition(s)`);
            if (removedCommands > 0) parts.push(`${removedCommands} command(s)`);
            setPrunedNotice(`Omitted ${parts.join(' and ')} already defined.`);
        } else {
            setPrunedNotice('No redundant steps found.');
        }
        onUpdateCommands(next);
    };

    useEffect(() => {
        if (!compareWithExisting) {
            setPrunedNotice(null);
        }
    }, [compareWithExisting]);

    const findConflict = (cond: FilterCondition): string | undefined => {
        const peers = existingByField[cond.field] || [];
        const val = normalizeValue(cond.value);
        const toNumber = (v: any) => {
            const n = Number(v);
            return Number.isFinite(n) ? n : null;
        };
        const condValNum = toNumber(cond.value);

        for (const peer of peers) {
            if (peer.operator === 'is_null' && cond.operator === 'is_not_null') return `Conflicts with existing: ${cond.field} is null`;
            if (peer.operator === 'is_not_null' && cond.operator === 'is_null') return `Conflicts with existing: ${cond.field} is not null`;
            if (peer.operator === 'is_empty' && cond.operator === 'is_not_empty') return `Conflicts with existing: ${cond.field} is null or empty`;
            if (peer.operator === 'is_not_empty' && cond.operator === 'is_empty') return `Conflicts with existing: ${cond.field} is not null and not empty`;

            if (peer.operator === '=' && cond.operator === '!=') {
                if (normalizeValue(peer.value) === val) return `Conflicts with existing: ${cond.field} = ${normalizeValue(peer.value)}`;
            }
            if (peer.operator === '!=' && cond.operator === '=') {
                if (normalizeValue(peer.value) === val) return `Conflicts with existing: ${cond.field} != ${normalizeValue(peer.value)}`;
            }

            if (peer.operator === 'in_list' && cond.operator === 'not_in_list') {
                const overlap = Array.isArray(peer.value) && Array.isArray(cond.value)
                    ? peer.value.some(v => normalizeValue(v) === normalizeValue((cond.value as any[]).find(cv => normalizeValue(cv) === normalizeValue(v))))
                    : false;
                if (overlap) return `Conflicts with existing: ${cond.field} in list`;
            }
            if (peer.operator === 'not_in_list' && cond.operator === 'in_list') {
                const overlap = Array.isArray(peer.value) && Array.isArray(cond.value)
                    ? peer.value.some(v => normalizeValue(v) === normalizeValue((cond.value as any[]).find(cv => normalizeValue(cv) === normalizeValue(v))))
                    : false;
                if (overlap) return `Conflicts with existing: ${cond.field} not in list`;
            }
            if (peer.operator === '=' && cond.operator === 'in_list') {
                if (Array.isArray(cond.value) && !cond.value.map(normalizeValue).includes(normalizeValue(peer.value))) {
                    return `Conflicts with existing: ${cond.field} = ${normalizeValue(peer.value)}`;
                }
            }
            if (peer.operator === 'in_list' && cond.operator === '=') {
                if (Array.isArray(peer.value) && !peer.value.map(normalizeValue).includes(normalizeValue(cond.value))) {
                    return `Conflicts with existing: ${cond.field} in list`;
                }
            }
            if (peer.operator === 'not_in_list' && cond.operator === '=') {
                if (Array.isArray(peer.value) && peer.value.map(normalizeValue).includes(normalizeValue(cond.value))) {
                    return `Conflicts with existing: ${cond.field} not in list`;
                }
            }
            if (peer.operator === '=' && cond.operator === 'not_in_list') {
                if (Array.isArray(cond.value) && cond.value.map(normalizeValue).includes(normalizeValue(peer.value))) {
                    return `Conflicts with existing: ${cond.field} = ${normalizeValue(peer.value)}`;
                }
            }

            if (peer.operator === 'contains' && cond.operator === 'not_contains') {
                if (normalizeValue(peer.value) === val) return `Conflicts with existing: ${cond.field} contains ${normalizeValue(peer.value)}`;
            }
            if (peer.operator === 'not_contains' && cond.operator === 'contains') {
                if (normalizeValue(peer.value) === val) return `Conflicts with existing: ${cond.field} not contains ${normalizeValue(peer.value)}`;
            }

            const peerValNum = toNumber(peer.value);
            if (condValNum !== null && peerValNum !== null) {
                const rangeFrom = (op: string, v: number) => {
                    if (op === '=') return { min: v, max: v, minInc: true, maxInc: true };
                    if (op === '>') return { min: v, max: Infinity, minInc: false, maxInc: true };
                    if (op === '>=') return { min: v, max: Infinity, minInc: true, maxInc: true };
                    if (op === '<') return { min: -Infinity, max: v, minInc: true, maxInc: false };
                    if (op === '<=') return { min: -Infinity, max: v, minInc: true, maxInc: true };
                    return null;
                };
                const r1 = rangeFrom(peer.operator, peerValNum);
                const r2 = rangeFrom(cond.operator, condValNum);
                if (r1 && r2) {
                    const overlap = !(r2.max < r1.min || r1.max < r2.min);
                    if (!overlap) return `Conflicts with existing numeric range on ${cond.field}`;
                }
            }
        }
        return undefined;
    };

    const buildActiveSchema = (datasetName?: string) => {
        const schema: Record<string, DataType> = {};
        if (!datasetName) return schema;
        const ds = datasets.find(d => d.name === datasetName);
        if (!ds) return schema;
        if (ds.fieldTypes) {
            Object.entries(ds.fieldTypes).forEach(([k, v]) => {
                schema[k] = (v as any).type || 'string';
            });
        } else {
            ds.fields.forEach(f => { schema[f] = 'string'; });
        }
        return schema;
    };

    const resolveDatasetName = (sourceId?: string) => {
        if (!sourceId) return '';
        const match = availableSourceAliases.find(sa =>
            sa.linkId === sourceId || sa.alias === sourceId || sa.sourceTable === sourceId
        );
        return match?.sourceTable || match?.alias || sourceId;
    };

    const isMissingDataset = (datasetName?: string) => {
        if (!datasetName) return false;
        return !datasets.some(d => d.name === datasetName);
    };

    const getSourceOptions = (currentValue?: string) => {
        const options = availableSourceAliases.length > 0
            ? availableSourceAliases.map(sa => ({
                value: sa.linkId,
                label: formatSourceOptionLabel(
                    sa.alias,
                    datasets.some(d => d.name === sa.sourceTable) ? sa.sourceTable : undefined,
                    sa.linkId
                ),
                unresolved: !datasets.some(d => d.name === sa.sourceTable)
            }))
            : datasets.map(d => ({ value: d.name, label: d.name, unresolved: false }));

        if (currentValue && !options.some(opt => opt.value === currentValue)) {
            options.push({ value: currentValue, label: currentValue, unresolved: true });
        }
        return options;
    };

    const ensureFilterRoot = (cmd: Command): FilterGroup => {
        if (cmd.config?.filterRoot) return cmd.config.filterRoot as FilterGroup;
        return {
            id: `group_${Date.now()}`,
            type: 'group',
            logicalOperator: 'AND',
            conditions: []
        };
    };

    const expandAll = () => {
        const next: Record<string, boolean> = {};
        commands.forEach(c => { next[c.id] = true; });
        setExpanded(next);
    };

    const collapseAll = () => setExpanded({});

    const detailLinesById = useMemo(() => {
        const buildLines = (cmd: Command) => {
            const lines: string[] = [];
            const cfg: any = cmd.config || {};
            if (cfg.dataSource) lines.push(`Source: ${cfg.dataSource}`);

            if (cmd.type === 'filter') {
                const root = cfg.filterRoot;
                const logical = root?.logicalOperator || 'AND';
                const conditions = (root?.conditions || []).filter((c: any) => c.type === 'condition');
                lines.push(`Logic: ${logical}`);
                if (conditions.length > 0) {
                    conditions.forEach((c: any) => {
                        const field = c.field || '';
                        const op = c.operator || '';
                        const val = c.valueType === 'variable' ? `{${c.value}}` : String(c.value ?? '');
                        lines.push(`• ${field} ${op} ${val}`.trim());
                    });
                }
            }

            if (cmd.type === 'sort') {
                if (cfg.field) lines.push(`Field: ${cfg.field}`);
                if (typeof cfg.ascending === 'boolean') lines.push(`Order: ${cfg.ascending ? 'ASC' : 'DESC'}`);
            }

            if (cmd.type === 'view') {
                if (Array.isArray(cfg.viewFields) && cfg.viewFields.length > 0) {
                    const list = cfg.viewFields.map((f: any) => f.field).filter(Boolean).join(', ');
                    if (list) lines.push(`Fields: ${list}`);
                }
                if (cfg.viewLimit !== undefined && cfg.viewLimit !== null) lines.push(`Limit: ${cfg.viewLimit}`);
            }

            if (cmd.type === 'group') {
                if (Array.isArray(cfg.groupByFields) && cfg.groupByFields.length > 0) {
                    lines.push(`Group By: ${cfg.groupByFields.join(', ')}`);
                }
                if (Array.isArray(cfg.aggregations) && cfg.aggregations.length > 0) {
                    cfg.aggregations.forEach((agg: any) => {
                        const alias = agg.alias ? ` as ${agg.alias}` : '';
                        lines.push(`• ${agg.func}(${agg.field || '*'})${alias}`);
                    });
                }
                if (cfg.outputTableName) lines.push(`Output: ${cfg.outputTableName}`);
            }

            if (cmd.type === 'save') {
                if (cfg.field) lines.push(`Field: ${cfg.field}`);
                if (cfg.value) lines.push(`Variable: ${cfg.value}`);
                if (typeof cfg.distinct === 'boolean') lines.push(`Distinct: ${cfg.distinct ? 'Yes' : 'No'}`);
            }

            if (cmd.type === 'transform') {
                if (Array.isArray(cfg.mappings)) {
                    cfg.mappings.forEach((m: any) => {
                        const expr = m.mode === 'python' ? '[python]' : m.expression || '';
                        lines.push(`• ${expr} -> ${m.outputField || ''}`.trim());
                    });
                }
            }

            if (cmd.type === 'join') {
                if (cfg.joinType) lines.push(`Join Type: ${cfg.joinType}`);
                if (cfg.joinTable || cfg.joinTargetNodeId) lines.push(`Target: ${cfg.joinTable || cfg.joinTargetNodeId}`);
                if (cfg.on) lines.push(`On: ${cfg.on}`);
            }

            if (lines.length === 0) lines.push('No details.');
            return lines;
        };

        const map: Record<string, string[]> = {};
        commands.forEach(cmd => { map[cmd.id] = buildLines(cmd); });
        return map;
    }, [commands]);

    const validationIssues = useMemo(() => {
        const issues: { id: string; message: string }[] = [];

        const collectFilterIssues = (group: FilterGroup, fieldNames: string[], commandId: string) => {
            group.conditions.forEach((cond) => {
                if (cond.type === 'group') {
                    collectFilterIssues(cond, fieldNames, commandId);
                    return;
                }
                if (cond.field && !fieldNames.includes(cond.field)) {
                    issues.push({ id: commandId, message: `Missing field: ${cond.field}` });
                }
                if (compareWithExisting) {
                    const conflict = findConflict(cond);
                    if (conflict) issues.push({ id: commandId, message: conflict });
                }
            });
        };

        commands.forEach((cmd) => {
            const cfg: any = cmd.config || {};
            const datasetName = resolveDatasetName(cfg.dataSource);
            const missingDataset = isMissingDataset(datasetName);
            if (missingDataset) {
                issues.push({ id: cmd.id, message: `Missing dataset: ${datasetName}` });
                return;
            }

            const fieldNames = getDatasetFieldNames(datasets, datasetName);

            if (cmd.type === 'filter' && cfg.filterRoot) {
                collectFilterIssues(cfg.filterRoot as FilterGroup, fieldNames, cmd.id);
            }
            if (cmd.type === 'sort' && cfg.field && !fieldNames.includes(cfg.field)) {
                issues.push({ id: cmd.id, message: `Missing field: ${cfg.field}` });
            }
            if (cmd.type === 'view' && Array.isArray(cfg.viewFields)) {
                cfg.viewFields.forEach((vf: any) => {
                    if (vf.field && !fieldNames.includes(vf.field)) {
                        issues.push({ id: cmd.id, message: `Missing field: ${vf.field}` });
                    }
                });
            }
        });

        return issues;
    }, [commands, datasets, availableSourceAliases, compareWithExisting, existingCommands]);

    const hasValidationErrors = validationIssues.length > 0;

    const renderCommandEditor = (cmd: Command) => {
        const cfg: any = cmd.config || {};
        const sourceOptions = getSourceOptions(cfg.dataSource);
        const datasetName = resolveDatasetName(cfg.dataSource);
        const fieldNames = getDatasetFieldNames(datasets, datasetName);
        const activeSchema = buildActiveSchema(datasetName);
        const missingDataset = isMissingDataset(datasetName);
        const updateConfig = (partial: Record<string, any>) => updateCommand(cmd.id, (current) => ({
            ...current,
            config: { ...current.config, ...partial }
        }));

        const renderSourceSelect = () => (
            <div className="flex flex-col space-y-1">
                <div className="flex items-center space-x-2 text-[10px] uppercase font-semibold text-gray-400">
                    <span>Source</span>
                    <select
                        className={`${missingDataset ? errorInputStyles : baseInputStyles} text-[11px] font-medium py-1 px-2`}
                        value={cfg.dataSource || ''}
                        onChange={(e) => updateCommand(cmd.id, (current) => ({
                            ...current,
                            config: { ...current.config, dataSource: e.target.value }
                        }))}
                    >
                        <option value="">{sourceOptions.length > 0 ? '-- Select Source --' : 'No sources'}</option>
                        {sourceOptions.map(opt => (
                            <option
                                key={opt.value}
                                value={opt.value}
                                style={opt.unresolved ? { color: '#dc2626', fontWeight: 600 } : undefined}
                            >
                                {opt.label}
                            </option>
                        ))}
                    </select>
                </div>
                {missingDataset && (
                    <div className="text-[11px] text-red-600">Source table not found.</div>
                )}
            </div>
        );

        if (cmd.type === 'filter') {
            const root = ensureFilterRoot(cmd);
            return (
                <div className="space-y-3">
                    {renderSourceSelect()}
                    <FilterGroupEditor
                        group={root}
                        activeSchema={activeSchema}
                        onUpdate={(updated) => updateCommand(cmd.id, (current) => ({
                            ...current,
                            config: { ...current.config, filterRoot: updated }
                        }))}
                        onRemove={() => {}}
                        isRoot
                        availableVariables={[]}
                        getConditionIssue={compareWithExisting ? (cond) => findConflict(cond) : undefined}
                    />
                </div>
            );
        }

        if (cmd.type === 'sort') {
            return (
                <div className="space-y-3">
                    {renderSourceSelect()}
                    <div className="grid grid-cols-2 gap-3">
                        <div>
                            <label className="block text-[10px] font-bold text-gray-500 uppercase mb-1">Field</label>
                            {(() => {
                                const missingField = !!cfg.field && !fieldNames.includes(cfg.field);
                                return (
                                    <select
                                        className={`${missingField ? errorInputStyles : baseInputStyles} text-xs px-2 py-1`}
                                        value={cfg.field || ''}
                                        onChange={(e) => updateCommand(cmd.id, (current) => ({
                                            ...current,
                                            config: { ...current.config, field: e.target.value }
                                        }))}
                                    >
                                        <option value="">Select Field...</option>
                                        {missingField && (
                                            <option value={cfg.field}>{cfg.field} (Missing)</option>
                                        )}
                                        {fieldNames.map(f => <option key={f} value={f}>{f}</option>)}
                                    </select>
                                );
                            })()}
                        </div>
                        <div>
                            <label className="block text-[10px] font-bold text-gray-500 uppercase mb-1">Order</label>
                            <select
                                className="w-full text-xs border border-gray-200 rounded-md px-2 py-1"
                                value={cfg.ascending === false ? 'desc' : 'asc'}
                                onChange={(e) => updateCommand(cmd.id, (current) => ({
                                    ...current,
                                    config: { ...current.config, ascending: e.target.value !== 'desc' }
                                }))}
                            >
                                <option value="asc">Asc</option>
                                <option value="desc">Desc</option>
                            </select>
                        </div>
                    </div>
                </div>
            );
        }

        if (cmd.type === 'view') {
            const viewFields = Array.isArray(cfg.viewFields) ? cfg.viewFields : [];
            return (
                <div className="space-y-3">
                    {renderSourceSelect()}
                    <div className="space-y-2">
                        <div className="flex items-center justify-between">
                            <label className="text-[10px] font-bold text-gray-500 uppercase">Fields</label>
                            <button
                                onClick={() => updateCommand(cmd.id, (current) => ({
                                    ...current,
                                    config: { ...current.config, viewFields: [...viewFields, { field: '', distinct: false }] }
                                }))}
                                className="text-[10px] font-semibold text-blue-600 hover:underline"
                            >
                                Add Field
                            </button>
                        </div>
                        {viewFields.length === 0 && (
                            <div className="text-[11px] text-gray-400">All fields selected.</div>
                        )}
                        {viewFields.map((vf: any, idx: number) => (
                            <div key={idx} className="grid grid-cols-12 gap-2 items-center bg-gray-50 p-2 rounded border border-gray-100">
                                <div className="col-span-6">
                                    {(() => {
                                        const missingField = !!vf.field && !fieldNames.includes(vf.field);
                                        return (
                                            <select
                                                className={`${missingField ? errorInputStyles : baseInputStyles} text-xs px-2 py-1`}
                                                value={vf.field || ''}
                                                onChange={(e) => {
                                                    const next = [...viewFields];
                                                    next[idx] = { ...next[idx], field: e.target.value };
                                                    updateCommand(cmd.id, (current) => ({
                                                        ...current,
                                                        config: { ...current.config, viewFields: next }
                                                    }));
                                                }}
                                            >
                                                <option value="">Select Field...</option>
                                                {missingField && (
                                                    <option value={vf.field}>{vf.field} (Missing)</option>
                                                )}
                                                {fieldNames.map(f => <option key={f} value={f}>{f}</option>)}
                                            </select>
                                        );
                                    })()}
                                </div>
                                <div className="col-span-4 flex items-center space-x-2">
                                    <input
                                        type="checkbox"
                                        checked={!!vf.distinct}
                                        onChange={(e) => {
                                            const next = [...viewFields];
                                            next[idx] = { ...next[idx], distinct: e.target.checked };
                                            updateCommand(cmd.id, (current) => ({
                                                ...current,
                                                config: { ...current.config, viewFields: next }
                                            }));
                                        }}
                                    />
                                    <span className="text-[11px] text-gray-500">Distinct</span>
                                </div>
                                <div className="col-span-2 text-right">
                                    <button
                                        onClick={() => {
                                            const next = [...viewFields];
                                            next.splice(idx, 1);
                                            updateCommand(cmd.id, (current) => ({
                                                ...current,
                                                config: { ...current.config, viewFields: next }
                                            }));
                                        }}
                                        className="text-[11px] text-red-500 hover:underline"
                                    >
                                        Remove
                                    </button>
                                </div>
                            </div>
                        ))}
                    </div>
                    <div className="grid grid-cols-3 gap-3">
                        <div className="col-span-1">
                            <label className="block text-[10px] font-bold text-gray-500 uppercase mb-1">Limit</label>
                            <input
                                type="number"
                                className="w-full text-xs border border-gray-200 rounded-md px-2 py-1"
                                value={cfg.viewLimit ?? ''}
                                onChange={(e) => updateCommand(cmd.id, (current) => ({
                                    ...current,
                                    config: { ...current.config, viewLimit: e.target.value ? Number(e.target.value) : undefined }
                                }))}
                                placeholder="0"
                            />
                        </div>
                    </div>
                </div>
            );
        }

        if (cmd.type === 'join') {
            const joinTargetType = cfg.joinTargetType || 'table';
            const joinTargetDatasetName = resolveDatasetName(cfg.joinTable);
            const joinFieldNames = getDatasetFieldNames(datasets, joinTargetDatasetName);
            const joinLeftField = cfg.joinLeftField || '';
            const joinRightField = cfg.joinRightField || '';
            const joinOperator = cfg.joinOperator || '=';
            const leftLabel = datasetName || 'left';
            const rightLabel = joinTargetDatasetName || 'right';
            const canBuildOn = joinTargetType === 'table' && !!joinLeftField && !!joinRightField;

            return (
                <div className="space-y-3">
                    {renderSourceSelect()}
                    <div className="grid grid-cols-3 gap-3">
                        <div>
                            <label className="block text-[10px] font-bold text-gray-500 uppercase mb-1">Target Type</label>
                            <select
                                className="w-full text-xs border border-gray-200 rounded-md px-2 py-1"
                                value={joinTargetType}
                                onChange={(e) => updateConfig({ joinTargetType: e.target.value })}
                            >
                                <option value="table">Table</option>
                                <option value="node">Node</option>
                            </select>
                        </div>
                        <div>
                            <label className="block text-[10px] font-bold text-gray-500 uppercase mb-1">Join Type</label>
                            <select
                                className="w-full text-xs border border-gray-200 rounded-md px-2 py-1"
                                value={cfg.joinType || 'LEFT'}
                                onChange={(e) => updateConfig({ joinType: e.target.value })}
                            >
                                <option value="INNER">INNER</option>
                                <option value="LEFT">LEFT</option>
                                <option value="RIGHT">RIGHT</option>
                                <option value="FULL">FULL</option>
                            </select>
                        </div>
                        <div>
                            <label className="block text-[10px] font-bold text-gray-500 uppercase mb-1">Target</label>
                            {joinTargetType === 'node' ? (
                                <input
                                    className={baseInputStyles}
                                    value={cfg.joinTargetNodeId || ''}
                                    onChange={(e) => updateConfig({ joinTargetNodeId: e.target.value })}
                                    placeholder="Node ID"
                                />
                            ) : (
                                <select
                                    className={baseInputStyles}
                                    value={cfg.joinTable || ''}
                                    onChange={(e) => updateConfig({ joinTable: e.target.value })}
                                >
                                    <option value="">-- Select Source --</option>
                                    {getSourceOptions(cfg.joinTable).map(opt => (
                                        <option
                                            key={opt.value}
                                            value={opt.value}
                                            style={opt.unresolved ? { color: '#dc2626', fontWeight: 600 } : undefined}
                                        >
                                            {opt.label}
                                        </option>
                                    ))}
                                </select>
                            )}
                        </div>
                    </div>
                    <div>
                        <label className="block text-[10px] font-bold text-gray-500 uppercase mb-1">ON Condition</label>
                        <input
                            className={baseInputStyles}
                            value={cfg.on || ''}
                            onChange={(e) => updateConfig({ on: e.target.value })}
                            placeholder="left.id = right.user_id"
                        />
                    </div>
                    {joinTargetType === 'table' && fieldNames.length > 0 && joinFieldNames.length > 0 && (
                        <div className="border border-gray-100 rounded-md p-2 bg-gray-50">
                            <div className="text-[10px] font-bold text-gray-500 uppercase mb-2">ON Builder</div>
                            <div className="grid grid-cols-12 gap-2 items-center">
                                <div className="col-span-5">
                                    <select
                                        className={baseInputStyles}
                                        value={joinLeftField}
                                        onChange={(e) => updateConfig({ joinLeftField: e.target.value })}
                                    >
                                        <option value="">Left Field...</option>
                                        {fieldNames.map(f => <option key={f} value={f}>{f}</option>)}
                                    </select>
                                </div>
                                <div className="col-span-2">
                                    <select
                                        className={baseInputStyles}
                                        value={joinOperator}
                                        onChange={(e) => updateConfig({ joinOperator: e.target.value })}
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
                                        className={baseInputStyles}
                                        value={joinRightField}
                                        onChange={(e) => updateConfig({ joinRightField: e.target.value })}
                                    >
                                        <option value="">Right Field...</option>
                                        {joinFieldNames.map(f => <option key={f} value={f}>{f}</option>)}
                                    </select>
                                </div>
                            </div>
                            <div className="mt-2 flex items-center justify-between">
                                <div className="text-[10px] text-gray-500">
                                    {leftLabel}.{joinLeftField || '...'} {joinOperator} {rightLabel}.{joinRightField || '...'}
                                </div>
                                <button
                                    className="text-[10px] font-semibold text-blue-600 hover:underline disabled:text-gray-300 disabled:no-underline"
                                    disabled={!canBuildOn}
                                    onClick={() => updateConfig({ on: `${leftLabel}.${joinLeftField} ${joinOperator} ${rightLabel}.${joinRightField}` })}
                                >
                                    Apply to ON
                                </button>
                            </div>
                        </div>
                    )}
                </div>
            );
        }

        if (cmd.type === 'group') {
            const groupByFields = Array.isArray(cfg.groupByFields) ? cfg.groupByFields : [];
            const aggregations = Array.isArray(cfg.aggregations) ? cfg.aggregations : [];
            const havingConditions = Array.isArray(cfg.havingConditions) ? cfg.havingConditions : [];

            return (
                <div className="space-y-3">
                    {renderSourceSelect()}
                    <div className="space-y-2">
                        <div className="flex items-center justify-between">
                            <label className="text-[10px] font-bold text-gray-500 uppercase">Group By</label>
                            <button
                                className="text-[10px] font-semibold text-blue-600 hover:underline"
                                onClick={() => updateConfig({ groupByFields: [...groupByFields, ''] })}
                            >
                                Add Field
                            </button>
                        </div>
                        {groupByFields.map((field: string, idx: number) => (
                            <div key={idx} className="grid grid-cols-12 gap-2 items-center">
                                <div className="col-span-10">
                                    <select
                                        className={baseInputStyles}
                                        value={field || ''}
                                        onChange={(e) => {
                                            const next = [...groupByFields];
                                            next[idx] = e.target.value;
                                            updateConfig({ groupByFields: next });
                                        }}
                                    >
                                        <option value="">Select Field...</option>
                                        {fieldNames.map(f => <option key={f} value={f}>{f}</option>)}
                                    </select>
                                </div>
                                <div className="col-span-2 text-right">
                                    <button
                                        className="text-[11px] text-red-500 hover:underline"
                                        onClick={() => {
                                            const next = [...groupByFields];
                                            next.splice(idx, 1);
                                            updateConfig({ groupByFields: next });
                                        }}
                                    >
                                        Remove
                                    </button>
                                </div>
                            </div>
                        ))}
                    </div>
                    <div className="space-y-2">
                        <div className="flex items-center justify-between">
                            <label className="text-[10px] font-bold text-gray-500 uppercase">Aggregations</label>
                            <button
                                className="text-[10px] font-semibold text-blue-600 hover:underline"
                                onClick={() => updateConfig({ aggregations: [...aggregations, { field: '', func: 'count', alias: '' }] })}
                            >
                                Add Metric
                            </button>
                        </div>
                        {aggregations.map((agg: any, idx: number) => (
                            <div key={idx} className="grid grid-cols-12 gap-2 items-center">
                                <div className="col-span-3">
                                    <select
                                        className={baseInputStyles}
                                        value={agg.func || 'count'}
                                        onChange={(e) => {
                                            const next = [...aggregations];
                                            next[idx] = { ...next[idx], func: e.target.value };
                                            updateConfig({ aggregations: next });
                                        }}
                                    >
                                        <option value="count">count</option>
                                        <option value="sum">sum</option>
                                        <option value="mean">mean</option>
                                        <option value="min">min</option>
                                        <option value="max">max</option>
                                        <option value="first">first</option>
                                        <option value="last">last</option>
                                    </select>
                                </div>
                                <div className="col-span-4">
                                    <select
                                        className={baseInputStyles}
                                        value={agg.field || ''}
                                        onChange={(e) => {
                                            const next = [...aggregations];
                                            next[idx] = { ...next[idx], field: e.target.value };
                                            updateConfig({ aggregations: next });
                                        }}
                                    >
                                        <option value="">Field...</option>
                                        <option value="*">*</option>
                                        {fieldNames.map(f => <option key={f} value={f}>{f}</option>)}
                                    </select>
                                </div>
                                <div className="col-span-4">
                                    <input
                                        className={baseInputStyles}
                                        value={agg.alias || ''}
                                        onChange={(e) => {
                                            const next = [...aggregations];
                                            next[idx] = { ...next[idx], alias: e.target.value };
                                            updateConfig({ aggregations: next });
                                        }}
                                        placeholder="Alias"
                                    />
                                </div>
                                <div className="col-span-1 text-right">
                                    <button
                                        className="text-[11px] text-red-500 hover:underline"
                                        onClick={() => {
                                            const next = [...aggregations];
                                            next.splice(idx, 1);
                                            updateConfig({ aggregations: next });
                                        }}
                                    >
                                        ×
                                    </button>
                                </div>
                            </div>
                        ))}
                    </div>
                    <div className="space-y-2">
                        <div className="flex items-center justify-between">
                            <label className="text-[10px] font-bold text-gray-500 uppercase">Having</label>
                            <button
                                className="text-[10px] font-semibold text-blue-600 hover:underline"
                                onClick={() => updateConfig({ havingConditions: [...havingConditions, { id: `having_${Date.now()}`, metricAlias: '', operator: '=', value: '' }] })}
                            >
                                Add Condition
                            </button>
                        </div>
                        {havingConditions.map((cond: any, idx: number) => (
                            <div key={cond.id || idx} className="grid grid-cols-12 gap-2 items-center">
                                <div className="col-span-4">
                                    <select
                                        className={baseInputStyles}
                                        value={cond.metricAlias || ''}
                                        onChange={(e) => {
                                            const next = [...havingConditions];
                                            next[idx] = { ...next[idx], metricAlias: e.target.value };
                                            updateConfig({ havingConditions: next });
                                        }}
                                    >
                                        <option value="">Metric...</option>
                                        {aggregations.map((agg: any, i: number) => (
                                            agg.alias ? <option key={`${agg.alias}_${i}`} value={agg.alias}>{agg.alias}</option> : null
                                        ))}
                                    </select>
                                </div>
                                <div className="col-span-2">
                                    <select
                                        className={baseInputStyles}
                                        value={cond.operator || '='}
                                        onChange={(e) => {
                                            const next = [...havingConditions];
                                            next[idx] = { ...next[idx], operator: e.target.value };
                                            updateConfig({ havingConditions: next });
                                        }}
                                    >
                                        <option value="=">=</option>
                                        <option value="!=">!=</option>
                                        <option value=">">&gt;</option>
                                        <option value=">=">&gt;=</option>
                                        <option value="<">&lt;</option>
                                        <option value="<=">&lt;=</option>
                                    </select>
                                </div>
                                <div className="col-span-5">
                                    <input
                                        className={baseInputStyles}
                                        value={cond.value ?? ''}
                                        onChange={(e) => {
                                            const next = [...havingConditions];
                                            next[idx] = { ...next[idx], value: e.target.value };
                                            updateConfig({ havingConditions: next });
                                        }}
                                        placeholder="Value"
                                    />
                                </div>
                                <div className="col-span-1 text-right">
                                    <button
                                        className="text-[11px] text-red-500 hover:underline"
                                        onClick={() => {
                                            const next = [...havingConditions];
                                            next.splice(idx, 1);
                                            updateConfig({ havingConditions: next });
                                        }}
                                    >
                                        ×
                                    </button>
                                </div>
                            </div>
                        ))}
                    </div>
                    <div>
                        <label className="block text-[10px] font-bold text-gray-500 uppercase mb-1">Output Table Name</label>
                        <input
                            className={baseInputStyles}
                            value={cfg.outputTableName || ''}
                            onChange={(e) => updateConfig({ outputTableName: e.target.value })}
                            placeholder="optional"
                        />
                    </div>
                </div>
            );
        }

        if (cmd.type === 'transform') {
            const mappings = Array.isArray(cfg.mappings) ? cfg.mappings : [];
            return (
                <div className="space-y-3">
                    {renderSourceSelect()}
                    <div className="flex items-center justify-between">
                        <label className="text-[10px] font-bold text-gray-500 uppercase">Mappings</label>
                        <button
                            className="text-[10px] font-semibold text-blue-600 hover:underline"
                            onClick={() => updateConfig({ mappings: [...mappings, { id: `map_${Date.now()}`, mode: 'simple', expression: '', outputField: '' }] })}
                        >
                            Add Mapping
                        </button>
                    </div>
                    {mappings.map((m: any, idx: number) => (
                        <div key={m.id || idx} className="border border-gray-100 rounded-md p-2 bg-gray-50 space-y-2">
                            <div className="grid grid-cols-12 gap-2 items-center">
                                <div className="col-span-4">
                                    <label className="block text-[10px] font-bold text-gray-500 uppercase mb-1">Mode</label>
                                    <select
                                        className={baseInputStyles}
                                        value={m.mode || 'simple'}
                                        onChange={(e) => {
                                            const next = [...mappings];
                                            next[idx] = { ...next[idx], mode: e.target.value };
                                            updateConfig({ mappings: next });
                                        }}
                                    >
                                        <option value="simple">simple</option>
                                        <option value="python">python</option>
                                    </select>
                                </div>
                                <div className="col-span-7">
                                    <label className="block text-[10px] font-bold text-gray-500 uppercase mb-1">Output Field</label>
                                    <input
                                        className={baseInputStyles}
                                        value={m.outputField || ''}
                                        onChange={(e) => {
                                            const next = [...mappings];
                                            next[idx] = { ...next[idx], outputField: e.target.value };
                                            updateConfig({ mappings: next });
                                        }}
                                        placeholder="new_field"
                                    />
                                </div>
                                <div className="col-span-1 text-right">
                                    <button
                                        className="text-[11px] text-red-500 hover:underline mt-5"
                                        onClick={() => {
                                            const next = [...mappings];
                                            next.splice(idx, 1);
                                            updateConfig({ mappings: next });
                                        }}
                                    >
                                        ×
                                    </button>
                                </div>
                            </div>
                            <div>
                                <label className="block text-[10px] font-bold text-gray-500 uppercase mb-1">Expression</label>
                                {m.mode === 'python' ? (
                                    <textarea
                                        className={`${baseInputStyles} min-h-[90px] font-mono`}
                                        value={m.expression || ''}
                                        onChange={(e) => {
                                            const next = [...mappings];
                                            next[idx] = { ...next[idx], expression: e.target.value };
                                            updateConfig({ mappings: next });
                                        }}
                                        placeholder="def transform(row): ..."
                                    />
                                ) : (
                                    <input
                                        className={baseInputStyles}
                                        value={m.expression || ''}
                                        onChange={(e) => {
                                            const next = [...mappings];
                                            next[idx] = { ...next[idx], expression: e.target.value };
                                            updateConfig({ mappings: next });
                                        }}
                                        placeholder="amount * 1.1"
                                    />
                                )}
                            </div>
                        </div>
                    ))}
                </div>
            );
        }

        if (cmd.type === 'save') {
            return (
                <div className="space-y-3">
                    {renderSourceSelect()}
                    <div className="grid grid-cols-3 gap-3">
                        <div>
                            <label className="block text-[10px] font-bold text-gray-500 uppercase mb-1">Field</label>
                            <select
                                className={baseInputStyles}
                                value={cfg.field || ''}
                                onChange={(e) => updateConfig({ field: e.target.value })}
                            >
                                <option value="">Select Field...</option>
                                {fieldNames.map(f => <option key={f} value={f}>{f}</option>)}
                            </select>
                        </div>
                        <div>
                            <label className="block text-[10px] font-bold text-gray-500 uppercase mb-1">Distinct</label>
                            <select
                                className={baseInputStyles}
                                value={cfg.distinct ? 'true' : 'false'}
                                onChange={(e) => updateConfig({ distinct: e.target.value === 'true' })}
                            >
                                <option value="true">true</option>
                                <option value="false">false</option>
                            </select>
                        </div>
                        <div>
                            <label className="block text-[10px] font-bold text-gray-500 uppercase mb-1">Variable</label>
                            <input
                                className={baseInputStyles}
                                value={cfg.value || ''}
                                onChange={(e) => updateConfig({ value: e.target.value })}
                                placeholder="var_name"
                            />
                        </div>
                    </div>
                </div>
            );
        }

        return (
            <div className="text-[11px] text-gray-400">
                {detailLinesById[cmd.id]?.map((line, i) => (
                    <div key={i}>{line}</div>
                ))}
            </div>
        );
    };

    return (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm p-4" data-testid="sql-builder-modal">
            <div className="bg-white rounded-xl shadow-2xl w-full max-w-5xl max-h-[90vh] overflow-hidden flex flex-col">
                <div className="flex items-center justify-between px-6 py-4 border-b border-gray-100 bg-gray-50">
                    <div>
                        <h3 className="text-lg font-bold text-gray-900">Build Commands from SQL</h3>
                        <p className="text-xs text-gray-500">Paste SQL and generate a command list you can edit before applying.</p>
                    </div>
                    <button onClick={onClose} className="text-gray-400 hover:text-gray-600 transition-colors">
                        <X className="w-5 h-5" />
                    </button>
                </div>

                <div className="p-6 space-y-4 overflow-y-auto">
                    <div>
                        <label className="block text-xs font-bold text-gray-500 uppercase mb-2">SQL</label>
                        <p className="text-[11px] text-gray-500 mb-2">
                            Supports plain SQL input.
                        </p>
                        <textarea
                            data-testid="sql-builder-input"
                            value={sqlInput}
                            onChange={(e) => onSqlInputChange(e.target.value)}
                            placeholder="SELECT * FROM my_table WHERE status = 'active' ORDER BY created_at DESC LIMIT 50"
                            className="w-full min-h-[140px] border border-gray-200 rounded-lg p-3 font-mono text-sm focus:ring-2 focus:ring-blue-100 focus:border-blue-400"
                        />
                    </div>

                    {error && (
                        <div className="text-sm text-red-600 bg-red-50 border border-red-100 rounded-md px-3 py-2">
                            {error}
                        </div>
                    )}

                    {warnings.length > 0 && (
                        <div className="text-xs text-yellow-700 bg-yellow-50 border border-yellow-100 rounded-md px-3 py-2">
                            {warnings.map((w, i) => (
                                <div key={i}>{w}</div>
                            ))}
                        </div>
                    )}
                    {prunedNotice && (
                        <div className="text-xs text-blue-700 bg-blue-50 border border-blue-100 rounded-md px-3 py-2">
                            {prunedNotice}
                        </div>
                    )}
                    {hasValidationErrors && (
                        <div className="text-xs text-red-700 bg-red-50 border border-red-100 rounded-md px-3 py-2">
                            Fix highlighted fields before applying.
                        </div>
                    )}

                    <div className="border-t border-gray-100 pt-3">
                        <div className="flex items-center justify-between mb-2">
                            <label className="block text-xs font-bold text-gray-500 uppercase">Generated Commands</label>
                            {commands.length > 0 && (
                                <div className="flex items-center space-x-2">
                                    <button onClick={expandAll} className="text-[10px] font-medium text-gray-500 hover:text-gray-700">Expand</button>
                                    <span className="text-gray-300">|</span>
                                    <button onClick={collapseAll} className="text-[10px] font-medium text-gray-500 hover:text-gray-700">Collapse</button>
                                </div>
                            )}
                        </div>
                        {commands.length === 0 ? (
                            <div className="text-xs text-gray-400 italic">No commands parsed yet.</div>
                        ) : (
                            <div className="space-y-2">
                                {commands.map((cmd, idx) => (
                                    <div key={cmd.id} className="border border-gray-100 rounded-md bg-gray-50 text-xs overflow-hidden">
                                        <button
                                            onClick={() => setExpanded(prev => ({ ...prev, [cmd.id]: !prev[cmd.id] }))}
                                            className="w-full flex items-center justify-between px-3 py-2 hover:bg-gray-100"
                                        >
                                            <div className="flex items-center min-w-0">
                                                <span className="font-mono text-gray-400">#{idx + 1}</span>
                                                <span className="ml-3 text-gray-700 truncate">{renderSummary(cmd)}</span>
                                            </div>
                                            <div className="flex items-center space-x-2">
                                                <span className="text-[10px] text-gray-400">{cmd.type}</span>
                                                {expanded[cmd.id] ? <ChevronUp className="w-3.5 h-3.5 text-gray-400" /> : <ChevronDown className="w-3.5 h-3.5 text-gray-400" />}
                                            </div>
                                        </button>
                                        {expanded[cmd.id] && (
                                            <div className="px-3 pb-3 text-[11px] text-gray-600 space-y-3 bg-white border-t border-gray-100">
                                                {renderCommandEditor(cmd)}
                                            </div>
                                        )}
                                    </div>
                                ))}
                            </div>
                        )}
                    </div>
                </div>

                <div className="p-4 bg-gray-50 border-t border-gray-200 flex justify-between items-center">
                    <div className="flex items-center space-x-3">
                        <Button variant="secondary" onClick={onParse}>Parse</Button>
                        <label className="flex items-center space-x-2 text-xs text-gray-600">
                            <input
                                type="checkbox"
                                checked={compareWithExisting}
                                onChange={(e) => {
                                    const checked = e.target.checked;
                                    setCompareWithExisting(checked);
                                    if (!checked) {
                                        onParse();
                                        setPrunedNotice('Consider Existing disabled. Commands regenerated from SQL.');
                                    }
                                }}
                            />
                            <span>Consider Existing</span>
                        </label>
                    </div>
                    <div className="flex items-center space-x-2">
                        <Button variant="secondary" onClick={onClose}>Cancel</Button>
                        <Button variant="primary" onClick={onApply} disabled={commands.length === 0 || hasValidationErrors}>Apply</Button>
                    </div>
                </div>
            </div>
        </div>
    );
};
