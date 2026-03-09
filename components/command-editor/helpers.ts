import { Command, Dataset } from '../../types';

export interface SourceAlias {
    alias: string;
    nodeName: string;
    id: string;
    sourceTable?: string;
    linkId: string;
}

export const getDatasetFieldNames = (datasets: Dataset[], datasetName: string | undefined): string[] => {
    if (!datasetName) return [];
    const ds = datasets.find(d => d.name === datasetName);
    if (!ds) return [];
    if (ds.fieldTypes) return Object.keys(ds.fieldTypes);
    return ds.fields || [];
};

export const getSourceLabel = (availableSourceAliases: SourceAlias[], sourceId?: string): string => {
    if (!sourceId) return '';
    const sa = availableSourceAliases.find(s =>
        s.linkId === sourceId || s.alias === sourceId || s.sourceTable === sourceId
    );
    if (sa?.alias) return sa.alias;
    if (sa?.sourceTable) return sa.sourceTable;
    return sourceId;
};

export const formatSourceOptionLabel = (alias: string, table: string | undefined, linkId: string) => {
    const base = `${alias} to ${table || '?'}`;
    return `${base} · ${linkId}`;
};

export const resolveDataSource = (availableSourceAliases: SourceAlias[], tableName: string) => {
    if (!tableName) return '';
    const match = availableSourceAliases.find(sa =>
        sa.sourceTable === tableName || sa.alias === tableName || sa.linkId === tableName
    );
    return match ? match.linkId : tableName;
};

export const renderSqlCommandSummary = (cmd: Command) => {
    if (cmd.type === 'filter') {
        const count = cmd.config.filterRoot?.conditions?.length || 0;
        return `Filter (${count} conditions)`;
    }
    if (cmd.type === 'join') {
        const target = cmd.config.joinTable || cmd.config.joinTargetNodeId || '';
        const joinType = (cmd.config.joinType || 'LEFT').toUpperCase();
        return `Join ${joinType} ${target}`.trim();
    }
    if (cmd.type === 'group') {
        const dims = (cmd.config.groupByFields || []).filter(Boolean);
        const aggs = (cmd.config.aggregations || []).length;
        return `Group ${dims.length > 0 ? `by ${dims.join(', ')}` : ''} ${aggs > 0 ? `(${aggs} metrics)` : ''}`.trim();
    }
    if (cmd.type === 'sort') {
        return `Sort ${cmd.config.field || ''} ${cmd.config.ascending === false ? 'DESC' : 'ASC'}`.trim();
    }
    if (cmd.type === 'transform') {
        const count = (cmd.config.mappings || []).length;
        return `Mapping (${count})`;
    }
    if (cmd.type === 'save') {
        const distinct = cmd.config.distinct ? 'Distinct ' : '';
        return `Save ${distinct}${cmd.config.field || ''} -> ${cmd.config.value || ''}`.trim();
    }
    if (cmd.type === 'view') {
        const fields = (cmd.config.viewFields || []).map(f => f.field).filter(Boolean);
        const hasLimit = cmd.config.viewLimit !== undefined && cmd.config.viewLimit !== null;
        const limit = hasLimit ? `Limit ${cmd.config.viewLimit}` : '';
        return `View ${fields.length > 0 ? fields.join(', ') : 'All Fields'} ${limit}`.trim();
    }
    if (cmd.type === 'source') {
        return `Source ${cmd.config.mainTable || ''}`.trim();
    }
    if (cmd.type === 'define_variable') {
        return `Define Variable ${cmd.config.variableName || ''}`.trim();
    }
    if (cmd.type === 'multi_table') {
        const count = (cmd.config.subTables || []).length;
        return `Complex View (${count} sub-table${count === 1 ? '' : 's'})`;
    }
    return cmd.type;
};
