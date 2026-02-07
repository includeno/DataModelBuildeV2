
import { ApiConfig, Dataset, ExecutionResult, SessionMetadata, OperationNode, Command, DataType, MappingRule, FilterGroup, FilterCondition, FieldInfo } from "../types";

// --- MOCK DATA GENERATORS ---

const inferType = (val: any): DataType => {
    if (val === null || val === undefined) return 'string';
    if (typeof val === 'number') return 'number';
    if (typeof val === 'boolean') return 'boolean';
    if (typeof val === 'object') return 'json';
    if (typeof val === 'string' && !isNaN(Date.parse(val)) && val.includes('-')) return 'date';
    return 'string';
};

const generateEmployees = (count: number) => {
    const depts = ["Engineering", "HR", "Sales", "Marketing", "Finance"];
    const rows = [];
    for (let i = 1; i <= count; i++) {
        rows.push({
            id: i,
            name: ["Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Heidi", "Ivan", "Judy"][i % 10] + (Math.floor(i/10) > 0 ? ` ${Math.floor(i/10)}` : ""),
            dept: depts[i % depts.length],
            salary: 50000 + (i * 1000) % 100000,
            active: i % 5 !== 0,
            hire_date: new Date(2020, i % 12, (i % 28) + 1).toISOString().split('T')[0],
            metadata: { level: i % 3, performance: "A" } 
        });
    }
    return rows;
};

const generateSales = (count: number) => {
    const rows = [];
    for (let i = 1; i <= count; i++) {
        // Generate uid between 1 and 50 to match employee ids
        const uid = (i % 50) + 1;
        rows.push({
            order_id: 1000 + i,
            uid: uid, 
            amount: Math.round(Math.random() * 1000),
            date: new Date(2023, i % 12, (i % 28) + 1).toISOString(),
            status: i % 4 === 0 ? "Refunded" : "Completed"
        });
    }
    return rows;
};

const generateFieldTypes = (rows: any[]): Record<string, FieldInfo> => {
    if (rows.length === 0) return {};
    const types: Record<string, FieldInfo> = {};
    const sample = rows[0];
    Object.keys(sample).forEach(key => {
        types[key] = { type: inferType(sample[key]) };
    });
    return types;
};

const empRows = generateEmployees(50);
const salesRows = generateSales(200); // Increased sales count for better density

const MOCK_DATASETS: Dataset[] = [
    { 
        id: "mock_employees", 
        name: "employees.csv", 
        totalCount: 50, 
        fields: ["id", "name", "dept", "salary", "active", "hire_date", "metadata"], 
        fieldTypes: generateFieldTypes(empRows),
        rows: empRows 
    },
    { 
        id: "mock_sales", 
        name: "sales_data.csv", 
        totalCount: 200, 
        fields: ["order_id", "uid", "amount", "date", "status"], 
        fieldTypes: generateFieldTypes(salesRows),
        rows: salesRows 
    }
];

const MOCK_SESSIONS: SessionMetadata[] = [
    { sessionId: "mock-session-demo", displayName: "Demo Session", createdAt: Date.now() - 100000 },
    { sessionId: "mock-session-archive", displayName: "", createdAt: Date.now() - 86400000 }
];

const MOCK_SESSION_STATES: Record<string, any> = {};
const MOCK_SESSION_METADATA: Record<string, any> = {};

// --- MOCK ENGINE LOGIC ---

const findPathToNode = (root: OperationNode, targetId: string): OperationNode[] | null => {
    if (root.id === targetId) return [root];
    if (root.children) {
        for (const child of root.children) {
            const path = findPathToNode(child, targetId);
            if (path) return [root, ...path];
        }
    }
    return null;
};

// Helper to map Link IDs (and aliases) back to actual table names
const buildSourceMap = (root: OperationNode): Record<string, string> => {
    const map: Record<string, string> = {};
    const traverse = (node: OperationNode) => {
        if (node.operationType === 'setup') {
            node.commands.forEach(cmd => {
                if (cmd.type === 'source' && cmd.config.mainTable) {
                    // Map linkId to table name
                    if (cmd.config.linkId) {
                        map[cmd.config.linkId] = cmd.config.mainTable;
                    }
                    // Map command ID to table name (fallback)
                    map[cmd.id] = cmd.config.mainTable;
                    // Map alias to table name
                    if (cmd.config.alias) {
                        map[cmd.config.alias] = cmd.config.mainTable;
                    }
                }
            });
        }
        if (node.children) {
            node.children.forEach(traverse);
        }
    };
    traverse(root);
    return map;
};

const evaluateCondition = (row: any, cond: FilterCondition, variables: Record<string, any[]>): boolean => {
    const val = row[cond.field];
    
    // Resolve variable placeholder in target value if present (e.g., "{my_var}")
    let target = cond.value;
    if (typeof target === 'string' && target.startsWith('{') && target.endsWith('}')) {
        const varName = target.slice(1, -1);
        // We look up in variables. 
        // Note: variables map currently stores ANY value (array or string). 
        // Typescript signature says any[] but it's really any.
        if (variables[varName] !== undefined) {
            target = variables[varName] as any;
        }
    }
    
    switch (cond.operator) {
        case '=': return String(val) == String(target); 
        case '!=': return String(val) != String(target);
        case '>': return Number(val) > Number(target);
        case '>=': return Number(val) >= Number(target);
        case '<': return Number(val) < Number(target);
        case '<=': return Number(val) <= Number(target);
        case 'contains': return String(val).toLowerCase().includes(String(target).toLowerCase());
        case 'not_contains': return !String(val).toLowerCase().includes(String(target).toLowerCase());
        case 'starts_with': return String(val).startsWith(String(target));
        case 'ends_with': return String(val).endsWith(String(target));
        case 'is_empty': return val === '' || val === null || val === undefined;
        case 'is_not_empty': return val !== '' && val !== null && val !== undefined;
        case 'is_true': return val === true;
        case 'is_false': return val === false;
        case 'has_key': return typeof val === 'object' && val !== null && String(target) in val;
        case 'in_variable': 
            // Target is expected to be a variable name string here from the dropdown config
            // OR if it was resolved from {var}, it might be the array itself. 
            // Standard 'in_variable' operator flow usually takes the variable NAME as value.
            let list: any[] = [];
            if (Array.isArray(target)) {
                list = target;
            } else {
                list = variables[String(target)] || [];
            }
            // Loose matching: compare strings to handle number/string id mismatches
            return list.some(v => String(v) === String(val));
        case 'not_in_variable': 
            let listNot: any[] = [];
            if (Array.isArray(target)) {
                listNot = target;
            } else {
                listNot = variables[String(target)] || [];
            }
            return !listNot.some(v => String(v) === String(val));
        default: return true;
    }
};

const evaluateFilterGroup = (row: any, group: FilterGroup, variables: Record<string, any[]>): boolean => {
    if (!group.conditions || group.conditions.length === 0) return true;

    if (group.logicalOperator === 'AND') {
        return group.conditions.every(item => {
            if (item.type === 'group') return evaluateFilterGroup(row, item, variables);
            return evaluateCondition(row, item, variables);
        });
    } else {
        return group.conditions.some(item => {
            if (item.type === 'group') return evaluateFilterGroup(row, item, variables);
            return evaluateCondition(row, item, variables);
        });
    }
};

const executeMockLogic = (tree: OperationNode, targetNodeId: string): any => {
    const sourceMap = buildSourceMap(tree); // Build map first
    const path = findPathToNode(tree, targetNodeId);
    if (!path) throw new Error("Target node not found in tree");

    let currentData: any[] = [];
    let hasLoadedSource = false;
    let variables: Record<string, any[]> = {};

    for (const node of path) {
        if (!node.enabled) continue;

        for (const cmd of node.commands) {
            // Note: multi_table commands are pass-throughs for the main stream, 
            // but we MUST NOT skip them entirely if they are responsible for loading the data source (Apply To).
            
            if (cmd.type === 'source' || (cmd.type !== 'join' && cmd.type !== 'group' && cmd.config.mainTable)) {
                const tableName = cmd.config.mainTable;
                const ds = MOCK_DATASETS.find(d => d.name === tableName);
                if (ds) {
                    currentData = [...ds.rows];
                    hasLoadedSource = true;
                }
            } else {
                if (!hasLoadedSource && currentData.length === 0) {
                     if (MOCK_DATASETS.length > 0) {
                        currentData = [...MOCK_DATASETS[0].rows];
                        hasLoadedSource = true;
                     }
                }
                
                if (cmd.config.dataSource && cmd.config.dataSource !== 'stream') {
                    // Resolve table name: Check map first (for Link IDs), then use value directly
                    const resolvedName = sourceMap[cmd.config.dataSource] || cmd.config.dataSource;
                    
                    const ds = MOCK_DATASETS.find(d => d.name === resolvedName);
                    if (ds) {
                        currentData = [...ds.rows];
                        // If we loaded data, we mark it.
                        hasLoadedSource = true; 
                    }
                }

                if (cmd.type === 'save') {
                    const { field, value: varName, distinct } = cmd.config;
                    if (field && varName && currentData.length > 0) {
                        let extracted = currentData.map(r => r[field]);
                        if (distinct !== false) extracted = Array.from(new Set(extracted));
                        variables[String(varName)] = extracted;
                    }
                } else if (cmd.type === 'define_variable') {
                    const { variableName, variableValue } = cmd.config;
                    if (variableName) {
                        // Store as is (string or array of strings)
                        // Cast to any[] to satisfy TS constraint of map, though it might be a string
                        variables[variableName] = variableValue as any;
                    }
                } else {
                    currentData = applyMockCommand(currentData, cmd, variables);
                }
            }
        }
    }

    const columns = currentData.length > 0 ? Object.keys(currentData[0]) : [];
    return { rows: currentData, totalCount: currentData.length, columns: columns };
};

const applyMockCommand = (data: any[], cmd: Command, variables: Record<string, any[]>): any[] => {
    const { config } = cmd;

    if (cmd.type === 'multi_table') {
        // Pass-through: multi_table command does not transform data in the main stream
        return data;
    }

    if (cmd.type === 'filter') {
        if (!config.filterRoot) return data;
        return data.filter(row => evaluateFilterGroup(row, config.filterRoot!, variables));
    }

    if (cmd.type === 'sort') {
        if (!config.field) return data;
        return [...data].sort((a, b) => {
            const valA = a[config.field!];
            const valB = b[config.field!];
            if (valA < valB) return config.ascending ? -1 : 1;
            if (valA > valB) return config.ascending ? 1 : -1;
            return 0;
        });
    }

    if (cmd.type === 'join') {
        const targetTable = config.joinTable;
        const targetDs = MOCK_DATASETS.find(d => d.name === targetTable);
        if (!targetDs) return data;
        
        const joinType = (config.joinType || 'left').toLowerCase();
        const on = config.on || '';
        
        // Simple mock join supporting equality: "table.col = other.col" or "col1 = col2"
        if (!on.includes('=')) return data; 
        
        // Extract field names. Assumes structure like "left.id = right.uid"
        const parts = on.split('=').map(s => s.trim());
        const leftKeyPart = parts[0];
        const rightKeyPart = parts[1];
        
        // Strip table prefixes if present to get raw field name
        const leftField = leftKeyPart.includes('.') ? leftKeyPart.split('.').pop()! : leftKeyPart;
        const rightField = rightKeyPart.includes('.') ? rightKeyPart.split('.').pop()! : rightKeyPart;
        
        const suffix = config.joinSuffix || '_joined';

        // Index right table
        const rightMap = new Map<string, any[]>();
        targetDs.rows.forEach(r => {
            const val = String(r[rightField]);
            if (!rightMap.has(val)) rightMap.set(val, []);
            rightMap.get(val)!.push(r);
        });

        const result: any[] = [];
        data.forEach(leftRow => {
            const key = String(leftRow[leftField]);
            const matches = rightMap.get(key);

            if (matches && matches.length > 0) {
                matches.forEach(matchRow => {
                    const merged = { ...leftRow };
                    Object.keys(matchRow).forEach(k => {
                        // Handle collision
                        if (k in merged) {
                            merged[`${k}${suffix}`] = matchRow[k];
                        } else {
                            merged[k] = matchRow[k];
                        }
                    });
                    result.push(merged);
                });
            } else if (joinType === 'left') {
                result.push({ ...leftRow });
            }
            // Inner join omits rows with no match
        });
        
        return result;
    }

    if (cmd.type === 'transform') {
        const mappings = config.mappings || [];
        if (mappings.length > 0) {
            return data.map(row => {
                const newRow = { ...row };
                mappings.forEach(m => {
                    const { expression, outputField } = m;
                    if (outputField && expression) {
                        try {
                            const keys = Object.keys(row);
                            const values = Object.values(row);
                            const func = new Function(...keys, `return ${expression}`);
                            newRow[outputField] = func(...values);
                        } catch (e) { newRow[outputField] = "Calc Error"; }
                    }
                });
                return newRow;
            });
        }
    }

    if (cmd.type === 'group') {
        const groupFields = config.groupByFields || [];
        const aggregations = config.aggregations || [];
        if (groupFields.length === 0 && aggregations.length === 0) return data;

        const groups: Record<string, any[]> = {};
        data.forEach(row => {
            const key = groupFields.map(f => String(row[f])).join('|');
            if (!groups[key]) groups[key] = [];
            groups[key].push(row);
        });

        let result = Object.entries(groups).map(([key, rows]) => {
            const resultRow: any = {};
            const keyParts = key.split('|');
            groupFields.forEach((f, i) => { resultRow[f] = rows[0][f]; });
            aggregations.forEach(agg => {
                const { field, func, alias } = agg;
                const fieldName = alias || `${func}_${field}`;
                if (func === 'count') resultRow[fieldName] = rows.length;
                else if (field && field !== '*') {
                    const values = rows.map(r => Number(r[field])).filter(v => !isNaN(v));
                    if (values.length === 0) resultRow[fieldName] = 0;
                    else {
                        switch (func) {
                            case 'sum': resultRow[fieldName] = values.reduce((a, b) => a + b, 0); break;
                            case 'mean': resultRow[fieldName] = values.reduce((a, b) => a + b, 0) / values.length; break;
                            case 'min': resultRow[fieldName] = Math.min(...values); break;
                            case 'max': resultRow[fieldName] = Math.max(...values); break;
                            default: resultRow[fieldName] = rows.length;
                        }
                    }
                }
            });
            return resultRow;
        });

        const having = config.havingConditions || [];
        if (having.length > 0) {
            result = result.filter(row => {
                return having.every(cond => {
                    const val = row[cond.metricAlias];
                    const target = cond.value;
                    const op = cond.operator;
                    const numVal = Number(val);
                    const numTarget = Number(target);
                    if (!isNaN(numVal) && !isNaN(numTarget)) {
                        if (op === '=') return numVal == numTarget;
                        if (op === '!=') return numVal != numTarget;
                        if (op === '>') return numVal > numTarget;
                        if (op === '>=') return numVal >= numTarget;
                        if (op === '<') return numVal < numTarget;
                        if (op === '<=') return numVal <= numTarget;
                    }
                    const sVal = String(val).toLowerCase();
                    const sTarget = String(target).toLowerCase();
                    if (op === '=') return sVal == sTarget;
                    if (op === '!=') return sVal != sTarget;
                    if (op === 'contains') return sVal.includes(sTarget);
                    return true;
                });
            });
        }
        return result;
    }
    return data;
};

// --- API EXPORT ---

export const api = {
    async get(config: ApiConfig, endpoint: string) {
        if (config.isMock) {
            await new Promise(r => setTimeout(r, 400));
            if (endpoint === '/sessions') return MOCK_SESSIONS;
            if (endpoint.includes('/datasets')) return [...MOCK_DATASETS];
            if (endpoint.match(/\/sessions\/.*\/state/)) return MOCK_SESSION_STATES[endpoint.split('/')[2]] || {};
            if (endpoint.match(/\/sessions\/.*\/metadata/)) return MOCK_SESSION_METADATA[endpoint.split('/')[2]] || { displayName: "", settings: { cascadeDisable: false, panelPosition: 'right' }};
            return {};
        }
        const res = await fetch(`${config.baseUrl}${endpoint}`);
        if (!res.ok) throw new Error(`API Error: ${res.statusText}`);
        return res.json();
    },

    async post(config: ApiConfig, endpoint: string, body: any) {
        if (config.isMock) {
            await new Promise(r => setTimeout(r, 600));
            if (endpoint === '/sessions') return { sessionId: `mock-sess-${Date.now()}` };
            if (endpoint.match(/\/sessions\/.*\/datasets\/update/)) {
                 const { datasetId, fieldTypes } = body;
                 const ds = MOCK_DATASETS.find(d => d.id === datasetId || d.name === datasetId);
                 if (ds) ds.fieldTypes = fieldTypes;
                 return { status: "ok" };
            }
            if (endpoint === '/execute') {
                const { tree, targetNodeId, page = 1, pageSize = 50, viewId = "main" } = body;
                
                // 1. Get Main Flow Result
                const mainResult = executeMockLogic(tree, targetNodeId);
                
                let finalData = mainResult.rows;
                let columns = mainResult.columns;

                // 2. Handle Multi-Table Sub Views
                if (viewId !== "main") {
                    // Start with empty result for safety
                    finalData = [];
                    columns = [];

                    const path = findPathToNode(tree, targetNodeId);
                    const targetNode = path ? path[path.length - 1] : null;
                    const multiCmd = targetNode?.commands.find(c => c.type === 'multi_table');
                    
                    if (multiCmd && multiCmd.config.subTables) {
                        const subConfig = multiCmd.config.subTables.find((s: any) => s.id === viewId);
                        if (subConfig) {
                            const subDs = MOCK_DATASETS.find(d => d.name === subConfig.table);
                            if (subDs) {
                                // MOCK JOIN LOGIC
                                const subRows = subDs.rows;
                                const onCondition = subConfig.on; 
                                
                                if (onCondition && onCondition.includes('=')) {
                                    const parts = onCondition.split('=').map((s: string) => s.trim());
                                    let mainCol = '', subCol = '';
                                    
                                    parts.forEach((p: string) => {
                                        if (p.startsWith('main.')) mainCol = p.replace('main.', '');
                                        else if (p.startsWith('sub.')) subCol = p.replace('sub.', '');
                                        // Fallback logic for aliases
                                        else if (p.startsWith(subConfig.table + '.')) subCol = p.replace(subConfig.table + '.', '');
                                    });

                                    // Fallback defaults for mock ease-of-use
                                    if (!mainCol) mainCol = 'id';
                                    if (!subCol) subCol = 'uid';

                                    if (mainCol && subCol) {
                                        const mainValues = new Set(mainResult.rows.map((r: any) => String(r[mainCol])));
                                        finalData = subRows.filter((r: any) => mainValues.has(String(r[subCol])));
                                        columns = subDs.fields;
                                    } else {
                                        // Condition exists but couldn't parse columns, return nothing to avoid confusion
                                        finalData = [];
                                    }
                                } else {
                                    // No valid condition: return full sub table
                                    finalData = subRows;
                                    columns = subDs.fields;
                                }
                            }
                        }
                    }
                }

                const totalCount = finalData.length;
                const start = (page - 1) * pageSize;
                return { 
                    rows: finalData.slice(start, start + pageSize), 
                    totalCount: totalCount, 
                    columns: columns, 
                    page, 
                    pageSize,
                    activeViewId: viewId
                };
            }
            if (endpoint === '/analyze') return { report: ["⚠️ Mock Analysis: Overlap detected in 2 branches."] };
            if (endpoint.match(/\/sessions\/.*\/state/)) { MOCK_SESSION_STATES[endpoint.split('/')[2]] = body; return { status: "ok" }; }
            if (endpoint.match(/\/sessions\/.*\/metadata/)) {
                const sessId = endpoint.split('/')[2];
                const current = MOCK_SESSION_METADATA[sessId] || {};
                MOCK_SESSION_METADATA[sessId] = { ...current, ...body };
                const listIdx = MOCK_SESSIONS.findIndex(s => s.sessionId === sessId);
                if (listIdx >= 0 && body.displayName !== undefined) MOCK_SESSIONS[listIdx].displayName = body.displayName;
                return { status: "ok" };
            }
            if (endpoint === '/query') {
                const { query = "", page = 1, pageSize = 50 } = body;
                const match = query.match(/select \* from (.*)/i);
                if (match) {
                    const tableName = match[1].trim();
                    const ds = MOCK_DATASETS.find(d => d.name === tableName);
                    if (ds) {
                        const start = (page - 1) * pageSize;
                        return { rows: ds.rows.slice(start, start + pageSize), totalCount: ds.totalCount, columns: ds.fields, page, pageSize };
                    }
                }
                return { rows: [], totalCount: 0, columns: [], page: 1, pageSize: 50 };
            }
            return {};
        }

        const res = await fetch(`${config.baseUrl}${endpoint}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body)
        });
        if (!res.ok) { const err = await res.json(); throw new Error(err.detail || `API Error: ${res.statusText}`); }
        return res.json();
    },

    async export(config: ApiConfig, endpoint: string, body: any) {
        if (config.isMock) return;
        const res = await fetch(`${config.baseUrl}${endpoint}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body)
        });
        if (!res.ok) throw new Error("Export failed");
        const blob = await res.blob();
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `export_${Date.now()}.csv`;
        document.body.appendChild(a);
        a.click();
        a.remove();
    },

    async delete(config: ApiConfig, endpoint: string) {
        if (config.isMock) return { status: "ok" };
        const res = await fetch(`${config.baseUrl}${endpoint}`, { method: 'DELETE' });
        if (!res.ok) throw new Error(`API Error: ${res.statusText}`);
        return res.json();
    },

    async upload(config: ApiConfig, endpoint: string, formData: FormData) {
        if (config.isMock) {
            await new Promise(r => setTimeout(r, 1000));
            const file = formData.get('file') as File;
            const name = formData.get('name') as string;
            const rows = [{ col1: "A", col2: 100, col3: true }, { col1: "B", col2: 200, col3: false }];
            const newDs: Dataset = { id: `mock_table_${Date.now()}`, name: name || file.name, fields: ["col1", "col2", "col3"], rows: rows, fieldTypes: generateFieldTypes(rows), totalCount: 2 };
            MOCK_DATASETS.push(newDs);
            return newDs;
        }
        const res = await fetch(`${config.baseUrl}${endpoint}`, { method: 'POST', body: formData });
        if (!res.ok) throw new Error(`Upload Error: ${res.statusText}`);
        return res.json();
    }
};
