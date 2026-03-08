import { Command, FilterCondition, FilterGroup } from '../../types';

export const parseSqlToCommands = (rawSql: string, resolveDataSource: (tableName: string) => string) => {
    const warnings: string[] = [];
    const sql = rawSql.trim().replace(/;+\s*$/, '');
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

    let selectPart = selectMatch[1].trim();
    const rawFromPart = selectMatch[2].trim().split(/\s+/)[0];
    const fromPart = normalizeIdentifier(rawFromPart);
    const dataSource = resolveDataSource(fromPart);

    const fromRemainder = selectMatch[2].trim();
    const hasUnsupportedFromClause = /\b(group\s+by|having|union|join|offset)\b/i.test(fromRemainder);
    if (hasUnsupportedFromClause) {
        warnings.push("Unsupported clause detected after FROM.");
    }
    if (limitValue === undefined && /\blimit\b/i.test(fromRemainder)) {
        warnings.push("LIMIT clause is not supported.");
    }

    let distinctFlag = false;
    if (/^distinct\s+/i.test(selectPart)) {
        distinctFlag = true;
        selectPart = selectPart.replace(/^distinct\s+/i, '').trim();
        if (selectPart === '*') {
            warnings.push("DISTINCT * is not supported. Treated as SELECT *.");
        }
    }

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
        const isQuote = (c: string) => c === '"' || c === '`';

        const readWord = (start: number) => {
            let j = start;
            let word = '';
            while (j < len && isWordChar(input[j])) { word += input[j]; j += 1; }
            return { word, end: j };
        };

        const readQuotedIdentifier = (start: number) => {
            const quote = input[start];
            let j = start + 1;
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
            return { value, end: j };
        };

        while (i < len) {
            const ch = input[i];
            if (/\s/.test(ch)) { i += 1; continue; }
            if (ch === '(') { tokens.push({ type: 'lparen' }); i += 1; continue; }
            if (ch === ')') { tokens.push({ type: 'rparen' }); i += 1; continue; }
            if (ch === ',') { tokens.push({ type: 'comma' }); i += 1; continue; }

            if (ch === '\'') {
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

            if (isQuote(ch)) {
                const first = readQuotedIdentifier(i);
                let combined = first.value;
                let j = first.end;

                while (j < len) {
                    let k = j;
                    while (k < len && /\s/.test(input[k])) k += 1;
                    if (input[k] !== '.') break;
                    k += 1;
                    while (k < len && /\s/.test(input[k])) k += 1;
                    if (k >= len) break;
                    if (isQuote(input[k])) {
                        const nextQuoted = readQuotedIdentifier(k);
                        combined = `${combined}.${nextQuoted.value}`;
                        j = nextQuoted.end;
                        continue;
                    }
                    const nextWord = readWord(k);
                    if (!nextWord.word) break;
                    combined = `${combined}.${nextWord.word}`;
                    j = nextWord.end;
                }

                tokens.push({ type: 'identifier', value: combined });
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
                const parsed = readWord(i);
                const word = parsed.word;
                const j = parsed.end;
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

    function stripIdentifierQuotes(value: string) {
        const trimmed = value.trim();
        if ((trimmed.startsWith('"') && trimmed.endsWith('"')) ||
            (trimmed.startsWith('`') && trimmed.endsWith('`')) ||
            (trimmed.startsWith('[') && trimmed.endsWith(']'))) {
            return trimmed.slice(1, -1).replace(/""/g, '"');
        }
        return trimmed;
    }

    function normalizeIdentifier(raw: string) {
        if (!raw) return raw;
        const parts = raw.split('.').map(p => stripIdentifierQuotes(p));
        return parts.join('.');
    }

    function toFieldName(raw: string) {
        const normalized = normalizeIdentifier(raw);
        return normalized.includes('.') ? normalized.split('.').pop() || normalized : normalized;
    }

    type ExprNode = FilterGroup | FilterCondition | { type: 'const'; value: boolean };

    const parseWhereExpression = (input: string): { group?: FilterGroup; error?: string; warning?: string } => {
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

        const isConst = (node: ExprNode): node is { type: 'const'; value: boolean } => (node as any).type === 'const';

        const combineExpr = (op: 'AND' | 'OR', left: ExprNode, right: ExprNode): ExprNode => {
            if (isConst(left) && isConst(right)) {
                return { type: 'const', value: op === 'AND' ? (left.value && right.value) : (left.value || right.value) };
            }
            if (op === 'AND') {
                if (isConst(left)) return left.value ? right : { type: 'const', value: false };
                if (isConst(right)) return right.value ? left : { type: 'const', value: false };
            }
            if (op === 'OR') {
                if (isConst(left)) return left.value ? { type: 'const', value: true } : right;
                if (isConst(right)) return right.value ? { type: 'const', value: true } : left;
            }
            return combineGroup(op, left as FilterGroup | FilterCondition, right as FilterGroup | FilterCondition);
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

        const parseCondition = (): ExprNode | null => {
            const fieldToken = peek();
            if (!fieldToken || (fieldToken.type !== 'identifier' && fieldToken.type !== 'number')) return null;
            consume();
            const field = fieldToken.type === 'number'
                ? String(fieldToken.value)
                : toFieldName(fieldToken.value);

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
                // Constant predicate folding for numeric-only comparisons
                if (fieldToken.type === 'number' && typeof value === 'number') {
                    const leftVal = Number(field);
                    let boolVal: boolean | null = null;
                    if (opToken.value === '=') boolVal = leftVal === value;
                    if (opToken.value === '!=' || opToken.value === '<>') boolVal = leftVal !== value;
                    if (opToken.value === '>') boolVal = leftVal > value;
                    if (opToken.value === '>=') boolVal = leftVal >= value;
                    if (opToken.value === '<') boolVal = leftVal < value;
                    if (opToken.value === '<=') boolVal = leftVal <= value;
                    if (boolVal !== null) {
                        return { type: 'const', value: boolVal };
                    }
                }
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

        const parsePrimary = (): ExprNode | null => {
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

        const parseAnd = (): ExprNode | null => {
            let left = parsePrimary();
            while (matchKeyword('and')) {
                const right = parsePrimary();
                if (!left && !right) left = null;
                else if (!left) left = right;
                else if (!right) left = left;
                else left = combineExpr('AND', left, right);
            }
            return left;
        };

        const parseOr = (): ExprNode | null => {
            let left = parseAnd();
            while (matchKeyword('or')) {
                const right = parseAnd();
                if (!left && !right) left = null;
                else if (!left) left = right;
                else if (!right) left = left;
                else left = combineExpr('OR', left, right);
            }
            return left;
        };

        try {
            const expr = parseOr();
            if (!expr) return { error: 'Failed to parse WHERE clause.' };
            if (pos < tokens.length) {
                return { error: 'Unsupported tokens in WHERE clause.' };
            }
            if ((expr as any).type === 'const') {
                const constNode = expr as { type: 'const'; value: boolean };
                if (constNode.value) {
                    return { warning: 'Constant TRUE predicate removed from WHERE clause.' };
                }
                const alwaysFalse: FilterCondition = {
                    id: makeId('cond'),
                    type: 'condition',
                    field: '__const__',
                    operator: 'always_false',
                    value: ''
                };
                return {
                    group: {
                        id: makeId('group'),
                        type: 'group',
                        logicalOperator: 'AND',
                        conditions: [alwaysFalse]
                    }
                };
            }
            const root: FilterGroup = (expr as any).type === 'group'
                ? (expr as FilterGroup)
                : { id: makeId('group'), type: 'group', logicalOperator: 'AND', conditions: [expr as FilterCondition] };
            return { group: root };
        } catch {
            return { error: 'Failed to parse WHERE clause.' };
        }
    };

    if (whereClause) {
        const parsed = parseWhereExpression(whereClause);
        if (parsed.error) {
            warnings.push(parsed.error);
        } else if (parsed.warning) {
            warnings.push(parsed.warning);
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
        const cleaned = fields.map(f => normalizeIdentifier(f.split(/\s+as\s+/i)[0].trim()));
        const simpleFields = cleaned.filter(f => !/[()]/.test(f) && !/\s[+\-*/]\s/.test(f));
        if (simpleFields.length !== cleaned.length) {
            warnings.push("Some selected fields contain functions/expressions and were ignored.");
        }
        viewFields = simpleFields.map(f => ({
            field: f.includes('.') ? f.split('.').pop() || f : f,
            distinct: distinctFlag || undefined
        }));
        if (viewFields.length === 0) {
            warnings.push("No valid fields found in SELECT list.");
        }
    }

    if ((viewFields || limitValue !== undefined || distinctFlag) && selectPart !== '*') {
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
