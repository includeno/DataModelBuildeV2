import { Command, FilterCondition, FilterGroup } from '../../types';

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

    const whereMatch = remaining.match(/\bwhere\s+([\s\S]+)$/i);
    if (whereMatch) {
        whereClause = whereMatch[1].trim();
        remaining = remaining.slice(0, whereMatch.index).trim();
    }

    const selectMatch = remaining.match(/select\s+([\s\S]+?)\s+from\s+([\s\S]+)$/i);
    if (!selectMatch) {
        return { commands: [] as Command[], warnings, error: "Only simple SELECT ... FROM ... queries are supported." };
    }

    const selectPart = selectMatch[1].trim();
    const fromPart = selectMatch[2].trim().split(/\s+/)[0];
    const dataSource = resolveDataSource(fromPart);

    const commands: Command[] = [];
    const makeId = (prefix: string) => `${prefix}_${Date.now()}_${Math.random().toString(36).substr(2, 5)}`;

    const parseValueLiteral = (val: string) => {
        const trimmed = val.trim();
        const unquoted = trimmed.replace(/^['"]|['"]$/g, '');
        if (/^(true|false)$/i.test(unquoted)) return unquoted.toLowerCase() === 'true';
        if (/^-?\d+(\.\d+)?$/.test(unquoted)) return Number(unquoted);
        if (/^null$/i.test(unquoted)) return null;
        return unquoted;
    };

    type Token =
        | { type: 'lparen' | 'rparen' | 'comma' }
        | { type: 'operator'; value: string }
        | { type: 'keyword'; value: string }
        | { type: 'identifier'; value: string }
        | { type: 'number'; value: number }
        | { type: 'string'; value: string };

    const tokenizeWhere = (input: string): Token[] => {
        const tokens: Token[] = [];
        let i = 0;
        const len = input.length;
        const isWordChar = (c: string) => /[A-Za-z0-9_.]/.test(c);

        while (i < len) {
            const ch = input[i];
            if (/\s/.test(ch)) { i += 1; continue; }
            if (ch === '(') { tokens.push({ type: 'lparen' }); i += 1; continue; }
            if (ch === ')') { tokens.push({ type: 'rparen' }); i += 1; continue; }
            if (ch === ',') { tokens.push({ type: 'comma' }); i += 1; continue; }

            if (ch === '\'' || ch === '"') {
                const quote = ch;
                let j = i + 1;
                let value = '';
                while (j < len) {
                    const cj = input[j];
                    if (cj === quote) {
                        if (input[j + 1] === quote) {
                            value += quote;
                            j += 2;
                            continue;
                        }
                        j += 1;
                        break;
                    }
                    value += cj;
                    j += 1;
                }
                tokens.push({ type: 'string', value });
                i = j;
                continue;
            }

            if (/[0-9]/.test(ch) || (ch === '.' && /[0-9]/.test(input[i + 1] || ''))) {
                let j = i;
                let num = '';
                while (j < len && /[0-9.]/.test(input[j])) { num += input[j]; j += 1; }
                tokens.push({ type: 'number', value: Number(num) });
                i = j;
                continue;
            }

            if (isWordChar(ch)) {
                let j = i;
                let word = '';
                while (j < len && isWordChar(input[j])) { word += input[j]; j += 1; }
                const lower = word.toLowerCase();
                if (['and', 'or', 'not', 'in', 'is', 'null', 'like'].includes(lower)) {
                    tokens.push({ type: 'keyword', value: lower });
                } else {
                    tokens.push({ type: 'identifier', value: word });
                }
                i = j;
                continue;
            }

            const twoChar = input.slice(i, i + 2);
            if (['!=', '<=', '>=', '<>'].includes(twoChar)) {
                tokens.push({ type: 'operator', value: twoChar });
                i += 2;
                continue;
            }
            if (['=', '<', '>'].includes(ch)) {
                tokens.push({ type: 'operator', value: ch });
                i += 1;
                continue;
            }

            i += 1;
        }

        return tokens;
    };

    const toFieldName = (raw: string) => raw.includes('.') ? raw.split('.').pop() || raw : raw;

    const parseWhereExpression = (input: string): { group?: FilterGroup; error?: string } => {
        const tokens = tokenizeWhere(input);
        let pos = 0;

        const peek = () => tokens[pos];
        const consume = () => tokens[pos++];
        const matchKeyword = (value: string) => {
            const t = peek();
            if (t && t.type === 'keyword' && t.value === value) { pos += 1; return true; }
            return false;
        };
        const matchToken = (type: Token['type']) => {
            const t = peek();
            if (t && t.type === type) { pos += 1; return true; }
            return false;
        };

        const combineGroup = (op: 'AND' | 'OR', left: FilterGroup | FilterCondition, right: FilterGroup | FilterCondition): FilterGroup => {
            if (left.type === 'group' && left.logicalOperator === op) {
                return { ...left, conditions: [...left.conditions, right] };
            }
            if (right.type === 'group' && right.logicalOperator === op) {
                return { ...right, conditions: [left, ...right.conditions] };
            }
            return {
                id: makeId('group'),
                type: 'group',
                logicalOperator: op,
                conditions: [left, right]
            };
        };

        const parseValueToken = () => {
            const t = peek();
            if (!t) return '';
            consume();
            if (t.type === 'number') return t.value;
            if (t.type === 'string') return t.value;
            if (t.type === 'keyword' && t.value === 'null') return null;
            if (t.type === 'identifier') return parseValueLiteral(t.value);
            return '';
        };

        const parseListValues = (): (string | number | null)[] => {
            const values: (string | number | null)[] = [];
            if (!matchToken('lparen')) return values;
            while (pos < tokens.length) {
                const t = peek();
                if (!t) break;
                if (t.type === 'rparen') { consume(); break; }
                if (t.type === 'comma') { consume(); continue; }
                if (t.type === 'number') { consume(); values.push(t.value); continue; }
                if (t.type === 'string') { consume(); values.push(t.value); continue; }
                if (t.type === 'keyword' && t.value === 'null') { consume(); values.push(null); continue; }
                if (t.type === 'identifier') { consume(); values.push(parseValueLiteral(t.value)); continue; }
                consume();
            }
            return values;
        };

        const buildLikeCondition = (field: string, rawValue: any, isNot: boolean): FilterCondition => {
            const pattern = String(rawValue ?? '');
            let operator = 'contains';
            let value = pattern;
            if (pattern.startsWith('%') && pattern.endsWith('%')) {
                operator = 'contains';
                value = pattern.slice(1, -1);
            } else if (pattern.startsWith('%')) {
                operator = 'ends_with';
                value = pattern.slice(1);
            } else if (pattern.endsWith('%')) {
                operator = 'starts_with';
                value = pattern.slice(0, -1);
            }
            if (isNot) {
                operator = 'not_contains';
            }
            return {
                id: makeId('cond'),
                type: 'condition',
                field,
                operator,
                value
            };
        };

        const parseCondition = (): FilterCondition | null => {
            const fieldToken = peek();
            if (!fieldToken || fieldToken.type !== 'identifier') return null;
            consume();
            const field = toFieldName(fieldToken.value);

            if (matchKeyword('is')) {
                const isNot = matchKeyword('not');
                if (matchKeyword('null')) {
                    return {
                        id: makeId('cond'),
                        type: 'condition',
                        field,
                        operator: isNot ? 'is_not_empty' : 'is_empty',
                        value: ''
                    };
                }
                if (matchKeyword('in')) {
                    const values = parseListValues();
                    return {
                        id: makeId('cond'),
                        type: 'condition',
                        field,
                        operator: isNot ? 'not_in_list' : 'in_list',
                        value: values
                    };
                }
                return null;
            }

            if (matchKeyword('not')) {
                if (matchKeyword('in')) {
                    const values = parseListValues();
                    return {
                        id: makeId('cond'),
                        type: 'condition',
                        field,
                        operator: 'not_in_list',
                        value: values
                    };
                }
                if (matchKeyword('like')) {
                    const value = parseValueToken();
                    return buildLikeCondition(field, value, true);
                }
            }

            if (matchKeyword('in')) {
                const values = parseListValues();
                return {
                    id: makeId('cond'),
                    type: 'condition',
                    field,
                    operator: 'in_list',
                    value: values
                };
            }

            if (matchKeyword('like')) {
                const value = parseValueToken();
                return buildLikeCondition(field, value, false);
            }

            const opToken = peek();
            if (opToken && opToken.type === 'operator') {
                consume();
                const value = parseValueToken();
                const opMap: Record<string, string> = { '=': '=', '!=': '!=', '<>': '!=', '>': '>', '>=': '>=', '<': '<', '<=': '<=' };
                const mapped = opMap[opToken.value] || '=';
                return {
                    id: makeId('cond'),
                    type: 'condition',
                    field,
                    operator: mapped,
                    value
                };
            }

            return null;
        };

        const parsePrimary = (): FilterGroup | FilterCondition | null => {
            const t = peek();
            if (!t) return null;
            if (t.type === 'lparen') {
                consume();
                const expr = parseOr();
                if (!matchToken('rparen')) return null;
                return expr;
            }
            return parseCondition();
        };

        const parseAnd = (): FilterGroup | FilterCondition => {
            let left = parsePrimary()!;
            while (matchKeyword('and')) {
                const right = parsePrimary()!;
                left = combineGroup('AND', left, right);
            }
            return left;
        };

        const parseOr = (): FilterGroup | FilterCondition => {
            let left = parseAnd();
            while (matchKeyword('or')) {
                const right = parseAnd();
                left = combineGroup('OR', left, right);
            }
            return left;
        };

        try {
            const expr = parseOr();
            if (!expr) return { error: 'Failed to parse WHERE clause.' };
            if (pos < tokens.length) {
                return { error: 'Unsupported tokens in WHERE clause.' };
            }
            const root: FilterGroup = expr.type === 'group'
                ? expr
                : { id: makeId('group'), type: 'group', logicalOperator: 'AND', conditions: [expr] };
            return { group: root };
        } catch {
            return { error: 'Failed to parse WHERE clause.' };
        }
    };

    if (whereClause) {
        const parsed = parseWhereExpression(whereClause);
        if (parsed.error) {
            warnings.push(parsed.error);
        } else if (parsed.group) {
            commands.push({
                id: makeId('cmd_filter'),
                type: 'filter',
                order: 0,
                config: {
                    dataSource: dataSource,
                    filterRoot: parsed.group
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
        commands.forEach((cmd, idx) => {
            if (!cmd.config.dataSource) {
                commands[idx] = {
                    ...cmd,
                    config: { ...cmd.config, dataSource }
                };
            }
        });
    }

    return { commands, warnings, error: null };
};
