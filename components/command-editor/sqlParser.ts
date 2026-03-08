import { Command, FilterCondition } from '../../types';

export const parseSqlToCommands = (rawSql: string, resolveDataSource: (tableName: string) => string) => {
    const warnings: string[] = [];
    const sql = rawSql.trim().replace(/;$/, '');
    if (!sql) {
        return { commands: [] as Command[], warnings, error: "SQL is empty." };
    }

    let remaining = sql;
    let limitValue: number | undefined;
    let orderByClause: string | undefined;
    let whereClause: string | undefined;

    const limitMatch = remaining.match(/\blimit\s+(\d+)\s*$/i);
    if (limitMatch) {
        limitValue = Number(limitMatch[1]);
        remaining = remaining.slice(0, limitMatch.index).trim();
    }

    const orderMatch = remaining.match(/\border\s+by\s+(.+)$/i);
    if (orderMatch) {
        orderByClause = orderMatch[1].trim();
        remaining = remaining.slice(0, orderMatch.index).trim();
    }

    const whereMatch = remaining.match(/\bwhere\s+(.+)$/i);
    if (whereMatch) {
        whereClause = whereMatch[1].trim();
        remaining = remaining.slice(0, whereMatch.index).trim();
    }

    const selectMatch = remaining.match(/select\s+(.+)\s+from\s+(.+)$/i);
    if (!selectMatch) {
        return { commands: [] as Command[], warnings, error: "Only simple SELECT ... FROM ... queries are supported." };
    }

    const selectPart = selectMatch[1].trim();
    const fromPart = selectMatch[2].trim().split(/\s+/)[0];
    const dataSource = resolveDataSource(fromPart);

    const commands: Command[] = [];
    const makeId = (prefix: string) => `${prefix}_${Date.now()}_${Math.random().toString(36).substr(2, 5)}`;

    const parseValue = (val: string) => {
        const trimmed = val.trim();
        const unquoted = trimmed.replace(/^['"]|['"]$/g, '');
        if (/^(true|false)$/i.test(unquoted)) return unquoted.toLowerCase() === 'true';
        if (/^-?\d+(\.\d+)?$/.test(unquoted)) return Number(unquoted);
        return unquoted;
    };

    const parseCondition = (chunk: string) => {
        const match = chunk.match(/^(.+?)(=|!=|<>|>=|<=|>|<|\s+like\s+)(.+)$/i);
        if (!match) return null;
        const fieldRaw = match[1].trim();
        const opRaw = match[2].trim();
        const valueRaw = match[3].trim();

        const field = fieldRaw.includes('.') ? fieldRaw.split('.').pop() || fieldRaw : fieldRaw;
        const valueParsed = parseValue(valueRaw);

        if (/like/i.test(opRaw)) {
            const pattern = String(valueParsed);
            if (pattern.startsWith('%') && pattern.endsWith('%')) return { field, operator: 'contains', value: pattern.slice(1, -1) };
            if (pattern.startsWith('%')) return { field, operator: 'ends_with', value: pattern.slice(1) };
            if (pattern.endsWith('%')) return { field, operator: 'starts_with', value: pattern.slice(0, -1) };
            return { field, operator: 'contains', value: pattern };
        }

        const opMap: Record<string, string> = { '=': '=', '!=': '!=', '<>': '!=', '>': '>', '>=': '>=', '<': '<', '<=': '<=' };
        const mapped = opMap[opRaw] || '=';
        return { field, operator: mapped, value: valueParsed };
    };

    if (whereClause) {
        const hasAnd = /\s+and\s+/i.test(whereClause);
        const hasOr = /\s+or\s+/i.test(whereClause);
        let logicalOperator: 'AND' | 'OR' = hasOr ? 'OR' : 'AND';
        if (hasAnd && hasOr) {
            warnings.push("WHERE contains both AND/OR. Parsed as AND only.");
            logicalOperator = 'AND';
        }
        const splitter = new RegExp(`\\s+${logicalOperator}\\s+`, 'i');
        const parts = whereClause.split(splitter).map(p => p.trim()).filter(Boolean);
        const conditions = parts.map(part => {
            const parsed = parseCondition(part);
            if (!parsed) {
                warnings.push(`Unsupported condition: ${part}`);
                return null;
            }
            return {
                id: `cond_${Date.now()}_${Math.random().toString(36).substr(2, 5)}`,
                type: 'condition' as const,
                field: parsed.field,
                operator: parsed.operator,
                value: parsed.value
            };
        }).filter(Boolean) as FilterCondition[];

        if (conditions.length > 0) {
            commands.push({
                id: makeId('cmd_filter'),
                type: 'filter',
                order: 0,
                config: {
                    dataSource: dataSource,
                    filterRoot: {
                        id: `group_${Date.now()}_${Math.random().toString(36).substr(2, 5)}`,
                        type: 'group',
                        logicalOperator,
                        conditions
                    }
                }
            });
        }
    }

    if (orderByClause) {
        const parts = orderByClause.split(',').map(p => p.trim()).filter(Boolean);
        if (parts.length > 1) warnings.push("ORDER BY has multiple fields. Only the first one is used.");
        const [fieldToken, dirToken] = parts[0].split(/\s+/);
        const field = fieldToken.includes('.') ? fieldToken.split('.').pop() || fieldToken : fieldToken;
        const ascending = !(dirToken && /desc/i.test(dirToken));
        commands.push({
            id: makeId('cmd_sort'),
            type: 'sort',
            order: 0,
            config: {
                field,
                ascending
            }
        });
    }

    let viewFields: { field: string; distinct?: boolean }[] | undefined;
    if (selectPart && selectPart !== '*') {
        const fields = selectPart.split(',').map(f => f.trim()).filter(Boolean);
        const cleaned = fields.map(f => f.split(/\s+as\s+/i)[0].trim());
        const simpleFields = cleaned.filter(f => !/[()]/.test(f));
        if (simpleFields.length !== cleaned.length) {
            warnings.push("Some selected fields contain functions/expressions and were ignored.");
        }
        viewFields = simpleFields.map(f => ({
            field: f.includes('.') ? f.split('.').pop() || f : f
        }));
        if (viewFields.length === 0) {
            warnings.push("No valid fields found in SELECT list.");
        }
    }

    if (viewFields || limitValue) {
        commands.push({
            id: makeId('cmd_view'),
            type: 'view',
            order: 0,
            config: {
                viewFields,
                viewLimit: limitValue
            }
        });
    }

    if (commands.length === 0) {
        commands.push({
            id: makeId('cmd_view'),
            type: 'view',
            order: 0,
            config: {
                dataSource: dataSource
            }
        });
    }

    if (commands.length > 0) {
        commands[0] = {
            ...commands[0],
            config: { ...commands[0].config, dataSource }
        };
    }

    return { commands, warnings, error: null };
};
