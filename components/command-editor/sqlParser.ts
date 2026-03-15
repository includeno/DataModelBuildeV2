import { Command, FilterCondition, FilterGroup } from '../../types';

export const parseSqlToCommands = (rawSql: string, resolveDataSource: (tableName: string) => string) => {
    const warnings: string[] = [];
    const makeId = (prefix: string) => `${prefix}_${Date.now()}_${Math.random().toString(36).substr(2, 5)}`;

    const normalizeDirectiveCommand = (payload: any, lineNo: number): Command | null => {
        const raw = payload && typeof payload === 'object' ? payload : {};
        let type = String(raw.type || '').trim();
        if (type === 'aggregate') type = 'group';

        const supportedTypes = new Set([
            'source',
            'define_variable',
            'filter',
            'join',
            'sort',
            'group',
            'transform',
            'save',
            'view',
            'multi_table'
        ]);

        if (!supportedTypes.has(type)) {
            warnings.push(`Unsupported DMB_COMMAND type at line ${lineNo}: ${type || '(empty)'}`);
            return null;
        }

        const config = raw.config && typeof raw.config === 'object' ? JSON.parse(JSON.stringify(raw.config)) : {};

        if (type !== 'source' && type !== 'define_variable') {
            const ds = typeof config.dataSource === 'string' ? config.dataSource.trim() : '';
            if (ds && ds !== 'stream') {
                config.dataSource = resolveDataSource(ds);
            }
        }

        if (type === 'join' && (config.joinTargetType || 'table') !== 'node') {
            const joinTable = typeof config.joinTable === 'string' ? config.joinTable.trim() : '';
            if (joinTable) {
                config.joinTable = resolveDataSource(joinTable);
            }
        }

        return {
            id: makeId(`cmd_${type}`),
            type: type as Command['type'],
            order: 0,
            config
        };
    };

    const parseDirectiveCommands = () => {
        const commandLines = rawSql.split(/\r?\n/);
        const directiveRegex = /^\s*--\s*DMB_COMMAND\s*:\s*(\{.*\})\s*$/i;
        const parsed: Command[] = [];
        let foundDirective = false;

        commandLines.forEach((line, idx) => {
            const match = line.match(directiveRegex);
            if (!match) return;
            foundDirective = true;
            try {
                const payload = JSON.parse(match[1]);
                const cmd = normalizeDirectiveCommand(payload, idx + 1);
                if (cmd) parsed.push(cmd);
            } catch {
                warnings.push(`Invalid DMB_COMMAND metadata at line ${idx + 1}.`);
            }
        });

        if (!foundDirective) return null;
        const ordered = parsed.map((cmd, idx) => ({ ...cmd, order: idx + 1 }));
        return ordered;
    };

    const directiveCommands = parseDirectiveCommands();
    if (directiveCommands && directiveCommands.length > 0) {
        return { commands: directiveCommands, warnings, error: null };
    }

    const sql = rawSql.trim().replace(/;+\s*$/, '');
    if (!sql) {
        return { commands: [] as Command[], warnings, error: "SQL is empty." };
    }

    const isWordBoundary = (ch: string | undefined) => !ch || !/[A-Za-z0-9_]/.test(ch);

    const findTopLevelKeyword = (input: string, keyword: string, start = 0): number => {
        const lower = input.toLowerCase();
        const key = keyword.toLowerCase();
        const keyLen = key.length;
        let paren = 0;
        let singleQuote = false;
        let doubleQuote = false;
        let backtick = false;
        let bracketQuote = false;

        for (let i = start; i <= input.length - keyLen; i++) {
            const ch = input[i];
            const prev = input[i - 1];

            if (singleQuote) {
                if (ch === "'" && input[i + 1] === "'") { i += 1; continue; }
                if (ch === "'") singleQuote = false;
                continue;
            }
            if (doubleQuote) {
                if (ch === '"' && input[i + 1] === '"') { i += 1; continue; }
                if (ch === '"') doubleQuote = false;
                continue;
            }
            if (backtick) {
                if (ch === '`') backtick = false;
                continue;
            }
            if (bracketQuote) {
                if (ch === ']') bracketQuote = false;
                continue;
            }

            if (ch === "'") { singleQuote = true; continue; }
            if (ch === '"') { doubleQuote = true; continue; }
            if (ch === '`') { backtick = true; continue; }
            if (ch === '[') { bracketQuote = true; continue; }
            if (ch === '(') { paren += 1; continue; }
            if (ch === ')') { paren = Math.max(0, paren - 1); continue; }
            if (paren > 0) continue;

            if (lower.startsWith(key, i)) {
                const before = input[i - 1];
                const after = input[i + keyLen];
                if (isWordBoundary(before) && isWordBoundary(after)) return i;
            }
            if (ch && !isWordBoundary(ch) && isWordBoundary(prev)) {
                // continue scanning
            }
        }
        return -1;
    };

    const splitTopLevelComma = (input: string): string[] => {
        const out: string[] = [];
        let cur = '';
        let paren = 0;
        let singleQuote = false;
        let doubleQuote = false;
        let backtick = false;
        let bracketQuote = false;

        for (let i = 0; i < input.length; i++) {
            const ch = input[i];
            if (singleQuote) {
                cur += ch;
                if (ch === "'" && input[i + 1] === "'") { cur += input[i + 1]; i += 1; continue; }
                if (ch === "'") singleQuote = false;
                continue;
            }
            if (doubleQuote) {
                cur += ch;
                if (ch === '"' && input[i + 1] === '"') { cur += input[i + 1]; i += 1; continue; }
                if (ch === '"') doubleQuote = false;
                continue;
            }
            if (backtick) {
                cur += ch;
                if (ch === '`') backtick = false;
                continue;
            }
            if (bracketQuote) {
                cur += ch;
                if (ch === ']') bracketQuote = false;
                continue;
            }

            if (ch === "'") { singleQuote = true; cur += ch; continue; }
            if (ch === '"') { doubleQuote = true; cur += ch; continue; }
            if (ch === '`') { backtick = true; cur += ch; continue; }
            if (ch === '[') { bracketQuote = true; cur += ch; continue; }
            if (ch === '(') { paren += 1; cur += ch; continue; }
            if (ch === ')') { paren = Math.max(0, paren - 1); cur += ch; continue; }

            if (ch === ',' && paren === 0) {
                out.push(cur.trim());
                cur = '';
                continue;
            }
            cur += ch;
        }
        if (cur.trim()) out.push(cur.trim());
        return out;
    };

    const splitTopLevelAnd = (input: string): string[] => {
        const out: string[] = [];
        let start = 0;
        let i = 0;
        while (i < input.length) {
            const idx = findTopLevelKeyword(input, 'and', i);
            if (idx < 0) break;
            out.push(input.slice(start, idx).trim());
            i = idx + 3;
            start = i;
        }
        out.push(input.slice(start).trim());
        return out.filter(Boolean);
    };

    const selectIdx = findTopLevelKeyword(sql, 'select', 0);
    if (selectIdx !== 0) {
        return { commands: [] as Command[], warnings, error: "Only simple SELECT ... FROM ... queries are supported." };
    }

    const fromIdx = findTopLevelKeyword(sql, 'from', 6);
    if (fromIdx < 0) {
        return { commands: [] as Command[], warnings, error: "Only simple SELECT ... FROM ... queries are supported." };
    }

    const whereIdx = findTopLevelKeyword(sql, 'where', fromIdx + 4);
    const groupIdx = findTopLevelKeyword(sql, 'group by', fromIdx + 4);
    const havingIdx = findTopLevelKeyword(sql, 'having', fromIdx + 4);
    const orderIdx = findTopLevelKeyword(sql, 'order by', fromIdx + 4);
    const limitIdx = findTopLevelKeyword(sql, 'limit', fromIdx + 4);

    const clauseIndices = [whereIdx, groupIdx, havingIdx, orderIdx, limitIdx].filter(i => i >= 0).sort((a, b) => a - b);
    const fromEnd = clauseIndices.length > 0 ? clauseIndices[0] : sql.length;

    let selectPart = sql.slice(selectIdx + 6, fromIdx).trim();
    const fromClause = sql.slice(fromIdx + 4, fromEnd).trim();
    if (!fromClause) {
        return { commands: [] as Command[], warnings, error: "Only simple SELECT ... FROM ... queries are supported." };
    }

    const nextClauseStart = (idx: number, keyLen: number) => {
        const afterIdx = [whereIdx, groupIdx, havingIdx, orderIdx, limitIdx]
            .filter(i => i > idx)
            .sort((a, b) => a - b)[0];
        const end = afterIdx !== undefined ? afterIdx : sql.length;
        return sql.slice(idx + keyLen, end).trim();
    };

    const whereClause = whereIdx >= 0 ? nextClauseStart(whereIdx, 5) : undefined;
    const groupByClause = groupIdx >= 0 ? nextClauseStart(groupIdx, 8) : undefined;
    const havingClause = havingIdx >= 0 ? nextClauseStart(havingIdx, 6) : undefined;
    const orderByClause = orderIdx >= 0 ? nextClauseStart(orderIdx, 8) : undefined;
    const limitClause = limitIdx >= 0 ? nextClauseStart(limitIdx, 5) : undefined;

    let limitValue: number | undefined;
    if (limitClause !== undefined) {
        const match = limitClause.match(/^(-?\d+)\s*$/);
        if (match) {
            const val = Number(match[1]);
            if (Number.isFinite(val) && val >= 0) limitValue = val;
            else warnings.push("LIMIT clause is invalid and was ignored.");
        } else {
            warnings.push("LIMIT clause is invalid and was ignored.");
        }
    }

    if (findTopLevelKeyword(sql, 'union', 0) >= 0) warnings.push("UNION clause is not supported.");
    if (findTopLevelKeyword(sql, 'offset', 0) >= 0) warnings.push("OFFSET clause is not supported.");

    let distinctFlag = false;
    if (/^distinct\s+/i.test(selectPart)) {
        distinctFlag = true;
        selectPart = selectPart.replace(/^distinct\s+/i, '').trim();
        if (selectPart === '*') {
            warnings.push("DISTINCT * is not supported. Treated as SELECT *.");
        }
    }

    const commands: Command[] = [];

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
                        operator: isNot ? 'is_not_null' : 'is_null',
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
            if (!left) return null;
            while (matchKeyword('and')) {
                const right = parsePrimary();
                if (!right) return null;
                left = combineExpr('AND', left, right);
            }
            return left;
        };

        const parseOr = (): ExprNode | null => {
            let left = parseAnd();
            if (!left) return null;
            while (matchKeyword('or')) {
                const right = parseAnd();
                if (!right) return null;
                left = combineExpr('OR', left, right);
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

    const escapeRegExp = (value: string) => value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

    const parseTableSpec = (segment: string): { table: string; alias?: string } | null => {
        const trimmed = segment.trim();
        if (!trimmed) return null;
        if (trimmed.startsWith('(')) {
            warnings.push("Subquery in FROM/JOIN is not supported.");
            return null;
        }

        let tablePart = trimmed;
        let alias: string | undefined;

        const asMatch = trimmed.match(/^(.*)\s+as\s+([^\s]+)$/i);
        if (asMatch) {
            tablePart = asMatch[1].trim();
            alias = stripIdentifierQuotes(asMatch[2]);
        } else {
            const tokens = trimmed.split(/\s+/);
            if (tokens.length >= 2) {
                const aliasCandidate = tokens[tokens.length - 1];
                if (/^("([^"]|"")+"|`[^`]+`|\[[^\]]+\]|[A-Za-z_][A-Za-z0-9_$]*)$/.test(aliasCandidate)) {
                    alias = stripIdentifierQuotes(aliasCandidate);
                    tablePart = tokens.slice(0, -1).join(' ').trim();
                }
            }
        }

        const table = normalizeIdentifier(tablePart);
        if (!table) return null;
        return { table, alias };
    };

    const stripTrailingJoinTypeWords = (input: string) =>
        input
            .replace(/\s+(left|right|inner|full|cross)(\s+outer)?\s*$/i, '')
            .trim();

    const detectJoinType = (prefix: string): 'LEFT' | 'RIGHT' | 'INNER' | 'FULL' | 'CROSS' => {
        const text = prefix.trim().toLowerCase();
        if (/(^|\s)left(\s+outer)?$/.test(text)) return 'LEFT';
        if (/(^|\s)right(\s+outer)?$/.test(text)) return 'RIGHT';
        if (/(^|\s)full(\s+outer)?$/.test(text)) return 'FULL';
        if (/(^|\s)cross$/.test(text)) return 'CROSS';
        return 'INNER';
    };

    const parseFromAndJoins = (input: string) => {
        const firstJoinIdx = findTopLevelKeyword(input, 'join', 0);
        const baseSegment = firstJoinIdx >= 0
            ? stripTrailingJoinTypeWords(input.slice(0, firstJoinIdx))
            : input.trim();
        const baseSpec = parseTableSpec(baseSegment);
        if (!baseSpec) return null;

        const aliasToTable: Record<string, string> = {
            [baseSpec.table]: baseSpec.table
        };
        if (baseSpec.alias) aliasToTable[baseSpec.alias] = baseSpec.table;

        const joins: Array<{ joinType: 'LEFT' | 'RIGHT' | 'INNER' | 'FULL'; table: string; alias?: string; on: string }> = [];

        const rewriteAliasPrefixes = (text: string) => {
            let out = text;
            const pairs = Object.entries(aliasToTable).sort((a, b) => b[0].length - a[0].length);
            pairs.forEach(([alias, table]) => {
                if (!alias || alias === table) return;
                out = out.replace(new RegExp(`\\b${escapeRegExp(alias)}\\.`, 'g'), `${table}.`);
            });
            return out;
        };

        let joinIdx = firstJoinIdx;
        while (joinIdx >= 0) {
            const prefix = input.slice(0, joinIdx);
            let joinType = detectJoinType(prefix);

            const nextJoinIdx = findTopLevelKeyword(input, 'join', joinIdx + 4);
            const onIdx = findTopLevelKeyword(input, 'on', joinIdx + 4);

            let targetSegment = '';
            let onClause = '1=1';
            if (onIdx >= 0 && (nextJoinIdx < 0 || onIdx < nextJoinIdx)) {
                targetSegment = input.slice(joinIdx + 4, onIdx).trim();
                const onEnd = nextJoinIdx >= 0 ? nextJoinIdx : input.length;
                onClause = input.slice(onIdx + 2, onEnd).trim();
            } else {
                targetSegment = input.slice(joinIdx + 4, nextJoinIdx >= 0 ? nextJoinIdx : input.length).trim();
                if (joinType !== 'CROSS') {
                    warnings.push("JOIN without ON detected. Using ON 1=1.");
                }
            }

            const targetSpec = parseTableSpec(targetSegment);
            if (!targetSpec) {
                warnings.push("Failed to parse JOIN target.");
                break;
            }

            aliasToTable[targetSpec.table] = targetSpec.table;
            if (targetSpec.alias) aliasToTable[targetSpec.alias] = targetSpec.table;

            onClause = rewriteAliasPrefixes(onClause);

            if (joinType === 'CROSS') {
                warnings.push("CROSS JOIN is converted to INNER JOIN ON 1=1.");
                joinType = 'INNER';
                onClause = '1=1';
            }

            joins.push({
                joinType,
                table: targetSpec.table,
                alias: targetSpec.alias,
                on: onClause || '1=1'
            });

            joinIdx = nextJoinIdx;
        }

        return {
            base: baseSpec,
            joins,
            aliasToTable
        };
    };

    const fromParsed = parseFromAndJoins(fromClause);
    if (!fromParsed) {
        return { commands: [] as Command[], warnings, error: "Only simple SELECT ... FROM ... queries are supported." };
    }

    const dataSource = resolveDataSource(fromParsed.base.table);

    fromParsed.joins.forEach((j) => {
        commands.push({
            id: makeId('cmd_join'),
            type: 'join',
            order: 0,
            config: {
                dataSource,
                joinTargetType: 'table',
                joinTable: resolveDataSource(j.table),
                joinType: j.joinType,
                on: j.on
            }
        });
    });

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
                    dataSource,
                    filterRoot: parsed.group
                }
            });
        }
    }

    const stripKnownPrefixes = (expr: string) => {
        let out = expr;
        const keys = Array.from(new Set([
            ...Object.keys(fromParsed.aliasToTable),
            ...Object.values(fromParsed.aliasToTable)
        ])).sort((a, b) => b.length - a.length);
        keys.forEach((k) => {
            out = out.replace(new RegExp(`\\b${escapeRegExp(k)}\\.`, 'g'), '');
        });
        return out;
    };

    const parseProjection = (token: string) => {
        const trimmed = token.trim();
        if (!trimmed) return null;
        if (trimmed === '*') return { kind: 'star' as const };

        let expr = trimmed;
        let alias: string | undefined;
        const asMatch = trimmed.match(/^(.*)\s+as\s+([^\s]+)$/i);
        if (asMatch) {
            expr = asMatch[1].trim();
            alias = stripIdentifierQuotes(asMatch[2].trim());
        } else {
            const implicitAlias = trimmed.match(/^(.*\S)\s+([A-Za-z_][A-Za-z0-9_$]*|"([^"]|"")+"|`[^`]+`|\[[^\]]+\])$/);
            if (implicitAlias) {
                expr = implicitAlias[1].trim();
                alias = stripIdentifierQuotes(implicitAlias[2].trim());
            }
        }

        const aggMatch = expr.match(/^([A-Za-z_][A-Za-z0-9_]*)\s*\(([\s\S]*)\)$/);
        if (aggMatch) {
            const fnRaw = aggMatch[1].toLowerCase();
            const fnMap: Record<string, string> = {
                count: 'count',
                sum: 'sum',
                avg: 'mean',
                mean: 'mean',
                min: 'min',
                max: 'max',
                first: 'first',
                last: 'last',
            };
            const mapped = fnMap[fnRaw];
            if (mapped) {
                const rawField = aggMatch[2].trim();
                const field = rawField === '*' ? '*' : toFieldName(rawField);
                const outAlias = alias || `${mapped}_${field === '*' ? 'all' : field}`;
                return { kind: 'agg' as const, func: mapped, field, alias: outAlias };
            }
        }

        const normalizedExpr = normalizeIdentifier(expr);
        const simpleIdentifierPattern = /^([A-Za-z_][A-Za-z0-9_$]*|"([^"]|"")+"|`[^`]+`|\[[^\]]+\])(\.([A-Za-z_][A-Za-z0-9_$]*|"([^"]|"")+"|`[^`]+`|\[[^\]]+\]))*$/;
        const isSimpleField = normalizedExpr === '*' || simpleIdentifierPattern.test(expr.trim());
        if (isSimpleField) {
            const field = normalizedExpr === '*' ? '*' : toFieldName(normalizedExpr);
            const saveVarName = alias && alias.toLowerCase().startsWith('save_') ? alias.slice(5) : undefined;
            return { kind: 'field' as const, field, alias, saveVarName };
        }

        if (!alias) {
            warnings.push(`Expression "${trimmed}" requires an alias and was ignored.`);
            return null;
        }

        return {
            kind: 'expr' as const,
            expression: stripKnownPrefixes(expr),
            alias
        };
    };

    const selectTokens = selectPart ? splitTopLevelComma(selectPart) : [];
    const parsedProjections = selectTokens.map(parseProjection).filter(Boolean) as Array<
        | { kind: 'star' }
        | { kind: 'field'; field: string; alias?: string; saveVarName?: string }
        | { kind: 'agg'; func: string; field: string; alias: string }
        | { kind: 'expr'; expression: string; alias: string }
    >;

    const groupByFields: string[] = [];
    if (groupByClause) {
        splitTopLevelComma(groupByClause).forEach((raw) => {
            const trimmed = raw.trim();
            if (!trimmed) return;
            if (/[()]/.test(trimmed)) {
                warnings.push(`GROUP BY expression "${trimmed}" is not supported and was ignored.`);
                return;
            }
            groupByFields.push(toFieldName(trimmed));
        });
    }

    const aggregations = parsedProjections
        .filter((p): p is { kind: 'agg'; func: string; field: string; alias: string } => p.kind === 'agg')
        .map((p) => ({ func: p.func, field: p.field, alias: p.alias }));

    const havingConditions: Array<{ id: string; metricAlias: string; operator: string; value: string | number }> = [];
    if (havingClause) {
        if (findTopLevelKeyword(havingClause, 'or', 0) >= 0) {
            warnings.push("HAVING with OR is not supported. Parsed as AND conditions where possible.");
        }
        splitTopLevelAnd(havingClause).forEach((term) => {
            const m = term.match(/^(.+?)\s*(<=|>=|<>|!=|=|<|>)\s*(.+)$/);
            if (!m) {
                warnings.push(`Unsupported HAVING condition: ${term}`);
                return;
            }
            const opMap: Record<string, string> = { '=': '=', '!=': '!=', '<>': '!=', '>': '>', '>=': '>=', '<': '<', '<=': '<=' };
            const metricAlias = toFieldName(m[1].trim());
            const rawVal = m[3].trim();
            const value = parseValueLiteral(rawVal) as any;
            havingConditions.push({
                id: makeId('having'),
                metricAlias,
                operator: opMap[m[2]] || '=',
                value
            });
        });
    }

    const needsGroup = groupByFields.length > 0 || aggregations.length > 0 || havingConditions.length > 0;
    if (needsGroup) {
        commands.push({
            id: makeId('cmd_group'),
            type: 'group',
            order: 0,
            config: {
                dataSource,
                groupByFields,
                aggregations,
                havingConditions
            }
        });
    }

    const mappings = parsedProjections
        .filter((p): p is { kind: 'expr'; expression: string; alias: string } => p.kind === 'expr')
        .map((p) => ({
            id: makeId('map'),
            mode: 'simple' as const,
            expression: p.expression,
            outputField: p.alias
        }));
    if (mappings.length > 0) {
        commands.push({
            id: makeId('cmd_transform'),
            type: 'transform',
            order: 0,
            config: {
                dataSource,
                mappings
            }
        });
    }

    let saveConfig: { field: string; distinct: boolean; value: string } | null = null;
    if (parsedProjections.length === 1 && parsedProjections[0].kind === 'field') {
        const proj = parsedProjections[0];
        if (proj.saveVarName) {
            if (!proj.saveVarName) {
                warnings.push("save_ alias must include a variable name suffix.");
            } else {
                saveConfig = {
                    field: proj.field,
                    distinct: !!distinctFlag,
                    value: proj.saveVarName
                };
            }
        }
    }
    if (saveConfig) {
        commands.push({
            id: makeId('cmd_save'),
            type: 'save',
            order: 0,
            config: {
                dataSource,
                ...saveConfig
            }
        });
    }

    if (orderByClause) {
        const parts = splitTopLevelComma(orderByClause).map(p => p.trim()).filter(Boolean);
        if (parts.length > 1) warnings.push("ORDER BY has multiple fields. Only the first one is used.");
        const [fieldTokenRaw, dirToken] = parts[0].split(/\s+/);
        const fieldToken = normalizeIdentifier(fieldTokenRaw || '');
        const field = toFieldName(fieldToken);
        const ascending = !(dirToken && /desc/i.test(dirToken));
        commands.push({
            id: makeId('cmd_sort'),
            type: 'sort',
            order: 0,
            config: {
                dataSource,
                field,
                ascending
            }
        });
    }

    const projectionViewFields = parsedProjections
        .filter((p): p is Exclude<typeof p, { kind: 'star' }> => p.kind !== 'star')
        .map((p) => {
            if (p.kind === 'field') return p.field;
            if (p.kind === 'agg') return p.alias;
            return p.alias;
        })
        .filter(Boolean);

    const hasStar = parsedProjections.some(p => p.kind === 'star');
    let viewFields: { field: string; distinct?: boolean }[] | undefined;
    if (!hasStar && projectionViewFields.length > 0) {
        const seen = new Set<string>();
        viewFields = projectionViewFields.filter((f) => {
            if (seen.has(f)) return false;
            seen.add(f);
            return true;
        }).map((f) => ({ field: f, distinct: distinctFlag || undefined }));
    } else if (!hasStar && selectPart !== '*' && projectionViewFields.length === 0) {
        warnings.push("No valid fields found in SELECT list.");
    }

    if (viewFields || limitValue !== undefined || distinctFlag) {
        commands.push({
            id: makeId('cmd_view'),
            type: 'view',
            order: 0,
            config: {
                dataSource,
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
                dataSource
            }
        });
    }

    commands.forEach((cmd, idx) => {
        if (!cmd.config.dataSource && cmd.type !== 'source' && cmd.type !== 'define_variable') {
            commands[idx] = {
                ...cmd,
                config: { ...cmd.config, dataSource }
            };
        }
    });

    commands.forEach((cmd, idx) => {
        commands[idx] = { ...cmd, order: idx + 1 };
    });

    return { commands, warnings, error: null };
};
