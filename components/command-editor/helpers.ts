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
    if (cmd.type === 'sort') {
        return `Sort ${cmd.config.field || ''} ${cmd.config.ascending === false ? 'DESC' : 'ASC'}`.trim();
    }
    if (cmd.type === 'view') {
        const fields = (cmd.config.viewFields || []).map(f => f.field).filter(Boolean);
        const limit = cmd.config.viewLimit ? `Limit ${cmd.config.viewLimit}` : '';
        return `View ${fields.length > 0 ? fields.join(', ') : 'All Fields'} ${limit}`.trim();
    }
    return cmd.type;
};
