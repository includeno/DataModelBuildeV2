
import {
    ApiConfig,
    Dataset,
    ProjectJob,
    ProjectMember,
    ProjectMetadata,
    ProjectMetadataDetail,
    SessionMetadata,
    OperationNode,
    Command,
    DataType,
    FilterGroup,
    FilterCondition,
    FieldInfo,
    CleanPreviewReport,
    SubTableConfig,
    SubTableConditionGroup
} from "../types";
import { applyPatches } from "./collabSync";

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
    { sessionId: "mock-session-demo", displayName: "Mock Demo Session", createdAt: Date.now() },
];

const MOCK_IMPORT_HISTORY = [
    {
        timestamp: Date.now() - 1000 * 60 * 60 * 2,
        originalFileName: "employees.csv",
        datasetName: "employees.csv",
        tableName: "employees.csv",
        rows: 50
    },
    {
        timestamp: Date.now() - 1000 * 60 * 30,
        originalFileName: "sales_data.csv",
        datasetName: "sales_data.csv",
        tableName: "sales_data.csv",
        rows: 200
    }
];

const MOCK_SESSION_STATES: Record<string, any> = {};
const MOCK_SESSION_METADATA: Record<string, any> = {};
const clone = <T,>(value: T): T => JSON.parse(JSON.stringify(value));

const DEFAULT_MOCK_PROJECT_ID = 'prj_mock_demo';
const DEFAULT_MOCK_MEMBER: ProjectMember = {
    userId: 'usr_mock_owner',
    email: 'mock.owner@example.com',
    displayName: 'Mock Owner',
    role: 'owner',
    createdAt: Date.now(),
    updatedAt: Date.now(),
};

const createMockProjectMetadata = (projectId: string, name: string): ProjectMetadata => ({
    id: projectId,
    orgId: 'org_mock_default',
    name,
    description: 'Mock collaborative workspace',
    role: 'owner',
    archived: false,
    createdAt: Date.now(),
    updatedAt: Date.now(),
});

const createMockProjectMetadataDetail = (displayName: string): ProjectMetadataDetail => ({
    displayName,
    settings: {
        cascadeDisable: false,
        panelPosition: 'right',
    },
});

const MOCK_PROJECTS: ProjectMetadata[] = [
    createMockProjectMetadata(DEFAULT_MOCK_PROJECT_ID, 'Mock Demo Project'),
];

const MOCK_PROJECT_STATES: Record<string, { version: number; state: any }> = {
    [DEFAULT_MOCK_PROJECT_ID]: {
        version: 0,
        state: {},
    },
};

const MOCK_PROJECT_METADATA: Record<string, ProjectMetadataDetail> = {
    [DEFAULT_MOCK_PROJECT_ID]: createMockProjectMetadataDetail('Mock Demo Project'),
};

const MOCK_PROJECT_DATASETS: Record<string, Dataset[]> = {
    [DEFAULT_MOCK_PROJECT_ID]: clone(MOCK_DATASETS),
};

const MOCK_PROJECT_IMPORTS: Record<string, typeof MOCK_IMPORT_HISTORY> = {
    [DEFAULT_MOCK_PROJECT_ID]: clone(MOCK_IMPORT_HISTORY),
};

const MOCK_PROJECT_MEMBERS: Record<string, ProjectMember[]> = {
    [DEFAULT_MOCK_PROJECT_ID]: [clone(DEFAULT_MOCK_MEMBER)],
};

const MOCK_PROJECT_JOBS: Record<string, ProjectJob[]> = {
    [DEFAULT_MOCK_PROJECT_ID]: [],
};

const ensureMockProject = (projectId: string) => {
    const cleanId = projectId || DEFAULT_MOCK_PROJECT_ID;
    const existing = MOCK_PROJECTS.find((project) => project.id === cleanId);
    if (!existing) {
        MOCK_PROJECTS.push(createMockProjectMetadata(cleanId, `Mock Project ${MOCK_PROJECTS.length + 1}`));
    }
    if (!MOCK_PROJECT_STATES[cleanId]) {
        MOCK_PROJECT_STATES[cleanId] = { version: 0, state: {} };
    }
    if (!MOCK_PROJECT_METADATA[cleanId]) {
        const project = MOCK_PROJECTS.find((item) => item.id === cleanId)!;
        MOCK_PROJECT_METADATA[cleanId] = createMockProjectMetadataDetail(project.name);
    }
    if (!MOCK_PROJECT_DATASETS[cleanId]) {
        MOCK_PROJECT_DATASETS[cleanId] = clone(MOCK_DATASETS);
    }
    if (!MOCK_PROJECT_IMPORTS[cleanId]) {
        MOCK_PROJECT_IMPORTS[cleanId] = clone(MOCK_IMPORT_HISTORY);
    }
    if (!MOCK_PROJECT_MEMBERS[cleanId]) {
        MOCK_PROJECT_MEMBERS[cleanId] = [clone(DEFAULT_MOCK_MEMBER)];
    }
    if (!MOCK_PROJECT_JOBS[cleanId]) {
        MOCK_PROJECT_JOBS[cleanId] = [];
    }
    return cleanId;
};

const getMockProjectDatasets = (projectId: string): Dataset[] => {
    return MOCK_PROJECT_DATASETS[ensureMockProject(projectId)];
};

const setMockProjectState = (projectId: string, nextState: any) => {
    const ensuredId = ensureMockProject(projectId);
    const current = MOCK_PROJECT_STATES[ensuredId] || { version: 0, state: {} };
    MOCK_PROJECT_STATES[ensuredId] = {
        version: current.version + 1,
        state: clone(nextState || {}),
    };
    const project = MOCK_PROJECTS.find((item) => item.id === ensuredId);
    if (project) project.updatedAt = Date.now();
    return MOCK_PROJECT_STATES[ensuredId];
};

const buildMockDiagnostics = (sessionId: string, datasets: Dataset[] = MOCK_DATASETS) => {
    const state = MOCK_SESSION_STATES[sessionId] || {};
    const tree = state.tree;
    const sources: any[] = [];
    const operations: any[] = [];
    const warnings: string[] = [];

    const walkSources = (node: any) => {
        if (!node) return;
        (node.commands || []).forEach((cmd: any) => {
            if (cmd.type === 'source') sources.push(cmd);
        });
        (node.children || []).forEach(walkSources);
    };

    const walkOperations = (node: any) => {
        if (!node) return;
        if (node.type === 'operation') {
            operations.push({
                id: node.id,
                name: node.name,
                operationType: node.operationType,
                commands: (node.commands || []).map((cmd: any) => ({
                    id: cmd.id,
                    type: cmd.type,
                    order: cmd.order ?? 0,
                    dataSource: cmd.config?.dataSource
                }))
            });
        }
        (node.children || []).forEach(walkOperations);
    };

    if (tree) {
        walkSources(tree);
        walkOperations(tree);
    } else {
        warnings.push("No tree found in mock session state.");
    }

    const missingSources: Array<{ id: string; type: string; opName: string }> = [];
    operations.forEach(op => {
        const opName = op.name || op.id || 'unknown-operation';
        (op.commands || []).forEach(cmd => {
            const cmdType = cmd.type || 'unknown';
            if (cmdType === 'source' || cmdType === 'multi_table') return;
            const raw = cmd.dataSource;
            if (raw === null || raw === undefined) {
                missingSources.push({ id: cmd.id, type: cmdType, opName });
                return;
            }
            if (typeof raw === 'string' && raw.trim() === '') {
                missingSources.push({ id: cmd.id, type: cmdType, opName });
                return;
            }
            if (raw === 'stream') return;
        });
    });

    missingSources.forEach(m => {
        warnings.push(`Missing data source: command ${m.id} (${m.type}) in operation ${m.opName}.`);
    });

    return {
        sessionId,
        generatedAt: new Date().toISOString(),
        sources: sources.map(s => ({
            id: s.id,
            mainTable: s.config?.mainTable,
            alias: s.config?.alias,
            linkId: s.config?.linkId
        })),
        sourceMap: [],
        datasets: datasets.map(d => ({
            id: d.id,
            name: d.name,
            totalCount: d.totalCount,
            fieldCount: d.fields?.length || 0
        })),
        operations,
        dataSourceResolution: [],
        warnings
    };
};

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
        case 'is_null': return val === null || val === undefined;
        case 'is_not_null': return val !== null && val !== undefined;
        case 'is_empty': return val === '';
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

const normalizeFieldName = (value: any): string => {
    if (value === null || value === undefined) return '';
    const raw = String(value).trim();
    if (!raw) return '';
    const dotIdx = raw.lastIndexOf('.');
    return dotIdx >= 0 ? raw.slice(dotIdx + 1) : raw;
};

const evaluateSubTableLinkCondition = (mainRow: any, subRow: any, cond: any): boolean => {
    const op = String(cond?.operator || '=').toLowerCase();
    const leftField = normalizeFieldName(cond?.field);
    const leftVal = leftField ? subRow?.[leftField] : undefined;

    if (op === 'is_null') return leftVal === null || leftVal === undefined;
    if (op === 'is_not_null') return leftVal !== null && leftVal !== undefined;
    if (op === 'is_empty') return leftVal === '' || leftVal === null || leftVal === undefined;
    if (op === 'is_not_empty') return leftVal !== '' && leftVal !== null && leftVal !== undefined;

    if (!leftField) return true;
    const rightField = normalizeFieldName(cond?.mainField ?? cond?.value);
    if (!rightField) return true;
    const rightVal = mainRow?.[rightField];

    switch (op) {
        case '=': return String(leftVal) === String(rightVal);
        case '!=': return String(leftVal) !== String(rightVal);
        case '>': return Number(leftVal) > Number(rightVal);
        case '>=': return Number(leftVal) >= Number(rightVal);
        case '<': return Number(leftVal) < Number(rightVal);
        case '<=': return Number(leftVal) <= Number(rightVal);
        case 'contains': return String(leftVal ?? '').toLowerCase().includes(String(rightVal ?? '').toLowerCase());
        case 'not_contains': return !String(leftVal ?? '').toLowerCase().includes(String(rightVal ?? '').toLowerCase());
        case 'starts_with': return String(leftVal ?? '').startsWith(String(rightVal ?? ''));
        case 'ends_with': return String(leftVal ?? '').endsWith(String(rightVal ?? ''));
        default: return String(leftVal) === String(rightVal);
    }
};

const evaluateSubTableConditionGroup = (mainRow: any, subRow: any, group?: SubTableConditionGroup | null): boolean => {
    if (!group || !Array.isArray(group.conditions) || group.conditions.length === 0) return true;
    if (group.logicalOperator === 'OR') {
        return group.conditions.some((item: any) => {
            if (item?.type === 'group') return evaluateSubTableConditionGroup(mainRow, subRow, item as SubTableConditionGroup);
            return evaluateSubTableLinkCondition(mainRow, subRow, item);
        });
    }
    return group.conditions.every((item: any) => {
        if (item?.type === 'group') return evaluateSubTableConditionGroup(mainRow, subRow, item as SubTableConditionGroup);
        return evaluateSubTableLinkCondition(mainRow, subRow, item);
    });
};

const evaluateSubTableOnCondition = (mainRow: any, subRow: any, subConfig: SubTableConfig): boolean => {
    const onCondition = String(subConfig?.on || '').trim();
    if (!onCondition || !onCondition.includes('=')) return true;

    const [leftRaw, rightRaw] = onCondition.split('=').map((s) => s.trim());
    const leftToken = leftRaw || '';
    const rightToken = rightRaw || '';

    let mainField = '';
    let subField = '';
    const unresolvedTokens: string[] = [];

    const assignSide = (token: string) => {
        if (token.startsWith('main.')) {
            mainField = normalizeFieldName(token);
            return;
        }
        if (token.startsWith('sub.')) {
            subField = normalizeFieldName(token);
            return;
        }
        if (subConfig?.table && token.startsWith(`${subConfig.table}.`)) {
            subField = normalizeFieldName(token);
            return;
        }
        unresolvedTokens.push(token);
    };

    assignSide(leftToken);
    assignSide(rightToken);

    unresolvedTokens.forEach((token) => {
        const field = normalizeFieldName(token);
        const inMain = field in (mainRow || {});
        const inSub = field in (subRow || {});
        if (!mainField && inMain && !inSub) {
            mainField = field;
            return;
        }
        if (!subField && inSub && !inMain) {
            subField = field;
            return;
        }
        if (!mainField && inMain) {
            mainField = field;
            return;
        }
        if (!subField && inSub) {
            subField = field;
        }
    });

    if (!mainField || !subField) return true;
    const mainVal = mainRow?.[mainField];
    const subVal = subRow?.[subField];
    if (mainVal === undefined || subVal === undefined) return false;
    return String(mainVal) === String(subVal);
};

const executeMockLogic = (tree: OperationNode, targetNodeId: string, datasets: Dataset[] = MOCK_DATASETS): any => {
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
                const ds = datasets.find(d => d.name === tableName);
                if (ds) {
                    currentData = [...ds.rows];
                    hasLoadedSource = true;
                }
            } else {
                if (!hasLoadedSource && currentData.length === 0) {
                     if (datasets.length > 0) {
                        currentData = [...datasets[0].rows];
                        hasLoadedSource = true;
                     }
                }
                
                if (cmd.config.dataSource && cmd.config.dataSource !== 'stream') {
                    // Resolve table name: Check map first (for Link IDs), then use value directly
                    const resolvedName = sourceMap[cmd.config.dataSource] || cmd.config.dataSource;
                    
                    const ds = datasets.find(d => d.name === resolvedName);
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
                    currentData = applyMockCommand(currentData, cmd, variables, datasets);
                }
            }
        }
    }

    const columns = currentData.length > 0 ? Object.keys(currentData[0]) : [];
    return { rows: currentData, totalCount: currentData.length, columns: columns };
};

const applyMockCommand = (data: any[], cmd: Command, variables: Record<string, any[]>, datasets: Dataset[] = MOCK_DATASETS): any[] => {
    const { config } = cmd;

    if (cmd.type === 'multi_table') {
        // Pass-through: multi_table command does not transform data in the main stream
        return data;
    }

    if (cmd.type === 'filter') {
        if (!config.filterRoot) return data;
        return data.filter(row => evaluateFilterGroup(row, config.filterRoot!, variables));
    }

    if (cmd.type === 'view') {
        let result = [...data];
        const viewFields = config.viewFields || [];
        const fieldsRaw = viewFields.map((vf: any) => vf.field).filter(Boolean);
        const distinctRaw = viewFields.filter((vf: any) => vf.distinct && vf.field).map((vf: any) => vf.field);
        const dedupe = (arr: string[]) => {
            const seen = new Set<string>();
            return arr.filter(f => {
                if (seen.has(f)) return false;
                seen.add(f);
                return true;
            });
        };
        const fields = dedupe(fieldsRaw);
        const distinctFields = dedupe(distinctRaw);
        const selectFields = distinctFields.length > 0 ? distinctFields : fields;

        if (selectFields.length > 0) {
            result = result.map(r => {
                const out: any = {};
                selectFields.forEach(f => { out[f] = r[f]; });
                return out;
            });
        }

        if (distinctFields.length > 0) {
            const seen = new Set<string>();
            result = result.filter(r => {
                const key = distinctFields.map(f => String(r[f])).join('|');
                if (seen.has(key)) return false;
                seen.add(key);
                return true;
            });
        }

        const sorters = (config.viewSorts && config.viewSorts.length > 0)
            ? config.viewSorts
            : (config.viewSortField ? [{ field: config.viewSortField, ascending: config.viewSortAscending !== false }] : []);

        if (sorters.length > 0) {
            const seen = new Set<string>();
            const deduped = sorters.filter(s => {
                if (!s.field) return false;
                if (seen.has(s.field)) return false;
                seen.add(s.field);
                return true;
            });
            result = [...result].sort((a, b) => {
                for (const s of deduped) {
                    const field = s.field;
                    const asc = s.ascending !== false;
                    const valA = a[field];
                    const valB = b[field];
                    if (valA < valB) return asc ? -1 : 1;
                    if (valA > valB) return asc ? 1 : -1;
                }
                return 0;
            });
        }

        if (config.viewLimit && config.viewLimit > 0) {
            result = result.slice(0, config.viewLimit);
        }

        return result;
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
        const targetDs = datasets.find(d => d.name === targetTable);
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

        let result = Object.entries(groups).map(([_key, rows]) => {
            const resultRow: any = {};
            // const keyParts = key.split('|');
            groupFields.forEach((f) => { resultRow[f] = rows[0][f]; });
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

type AuthTokens = {
    accessToken: string;
    refreshToken?: string;
    expiresAt?: number;
};

type AuthStorageMode = 'cookie_preferred' | 'local_storage';

const AUTH_STORAGE_KEY = 'dmb_auth_tokens_v1';
let AUTH_STORAGE_MODE: AuthStorageMode = 'cookie_preferred';
let RUNTIME_AUTH: AuthTokens | null = null;
let AUTH_API_ENABLED = true;

const loadStoredAuth = (): AuthTokens | null => {
    try {
        if (typeof window === 'undefined' || !window.localStorage) return null;
        const raw = window.localStorage.getItem(AUTH_STORAGE_KEY);
        if (!raw) return null;
        const parsed = JSON.parse(raw);
        if (!parsed?.accessToken) return null;
        return parsed as AuthTokens;
    } catch {
        return null;
    }
};

const saveStoredAuth = (tokens: AuthTokens | null) => {
    try {
        if (typeof window === 'undefined' || !window.localStorage) return;
        if (!tokens) {
            window.localStorage.removeItem(AUTH_STORAGE_KEY);
            return;
        }
        window.localStorage.setItem(AUTH_STORAGE_KEY, JSON.stringify(tokens));
    } catch {
        // Ignore storage errors in restricted environments.
    }
};

const getAuth = (): AuthTokens | null => {
    if (RUNTIME_AUTH) return RUNTIME_AUTH;
    RUNTIME_AUTH = loadStoredAuth();
    return RUNTIME_AUTH;
};

const setAuth = (tokens: AuthTokens | null) => {
    RUNTIME_AUTH = tokens;
    // Prefer HttpOnly/Secure cookie session when available, while keeping
    // localStorage as a fallback for token-based backends.
    if (AUTH_STORAGE_MODE === 'local_storage' || tokens) {
        saveStoredAuth(tokens);
    } else if (!tokens) {
        saveStoredAuth(null);
    }
};

const headersWithAuth = (headers?: HeadersInit): HeadersInit => {
    const auth = getAuth();
    const next: Record<string, string> = {};
    if (headers && typeof headers === 'object') {
        Object.assign(next, headers as Record<string, string>);
    }
    if (auth?.accessToken) {
        next.Authorization = `Bearer ${auth.accessToken}`;
    }
    return next;
};

const requestJson = async (config: ApiConfig, endpoint: string, init?: RequestInit, allowRefresh = true): Promise<Response> => {
    const res = await fetch(`${config.baseUrl}${endpoint}`, {
        ...init,
        credentials: 'include',
        headers: headersWithAuth(init?.headers)
    });

    if (res.status !== 401 || !allowRefresh || config.isMock || !AUTH_API_ENABLED) return res;
    const auth = getAuth();
    if (!auth?.refreshToken) return res;

    try {
        const refreshRes = await fetch(`${config.baseUrl}/auth/refresh`, {
            method: 'POST',
            credentials: 'include',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ refreshToken: auth.refreshToken })
        });
        if (!refreshRes.ok) {
            setAuth(null);
            return res;
        }
        const payload = await refreshRes.json();
        if (!payload?.accessToken) {
            setAuth(null);
            return res;
        }
        setAuth({
            accessToken: payload.accessToken,
            refreshToken: payload.refreshToken || auth.refreshToken,
            expiresAt: payload.expiresAt
        });
    } catch {
        setAuth(null);
        return res;
    }

    return fetch(`${config.baseUrl}${endpoint}`, {
        ...init,
        credentials: 'include',
        headers: headersWithAuth(init?.headers)
    });
};

const isEnvelope = (body: any): body is { data: any; error: any; meta: any; request_id?: string } => {
    return Boolean(body)
        && typeof body === 'object'
        && Object.prototype.hasOwnProperty.call(body, 'data')
        && Object.prototype.hasOwnProperty.call(body, 'error')
        && Object.prototype.hasOwnProperty.call(body, 'meta');
};

const unwrapBody = <T,>(body: any): T => {
    if (isEnvelope(body)) return body.data as T;
    return body as T;
};

const extractErrorMessage = (body: any, fallback: string): string => {
    if (isEnvelope(body) && body?.error) {
        return body.error.message || fallback;
    }
    return body?.detail?.message || body?.detail || body?.error?.message || fallback;
};

export const api = {
    setAuthApiEnabled(enabled: boolean) {
        AUTH_API_ENABLED = enabled;
        if (!enabled) {
            setAuth(null);
        }
    },
    isAuthApiEnabled(): boolean {
        return AUTH_API_ENABLED;
    },
    setAuthStorageMode(mode: AuthStorageMode) {
        AUTH_STORAGE_MODE = mode;
    },
    setAuthTokens(tokens: AuthTokens | null) {
        setAuth(tokens);
    },
    getAuthTokens(): AuthTokens | null {
        return getAuth();
    },
    clearAuthTokens() {
        setAuth(null);
    },
    async authRegister(config: ApiConfig, payload: { email: string; password: string; displayName?: string }) {
        if (!AUTH_API_ENABLED) throw new Error('Authentication is disabled on this server');
        const res = await fetch(`${config.baseUrl}/auth/register`, {
            method: 'POST',
            credentials: 'include',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });
        const body = await res.json();
        if (!res.ok) throw new Error(extractErrorMessage(body, `API Error: ${res.statusText}`));
        return unwrapBody(body);
    },
    async authLogin(config: ApiConfig, payload: { email: string; password: string }) {
        if (!AUTH_API_ENABLED) throw new Error('Authentication is disabled on this server');
        const res = await fetch(`${config.baseUrl}/auth/login`, {
            method: 'POST',
            credentials: 'include',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });
        const body = await res.json();
        if (!res.ok) throw new Error(extractErrorMessage(body, `API Error: ${res.statusText}`));
        const loginPayload = unwrapBody<any>(body);
        if (loginPayload?.accessToken) {
            setAuth({ accessToken: loginPayload.accessToken, refreshToken: loginPayload.refreshToken, expiresAt: loginPayload.expiresAt });
        }
        return loginPayload;
    },
    async authMe(config: ApiConfig) {
        if (!AUTH_API_ENABLED) throw new Error('Authentication is disabled on this server');
        const res = await requestJson(config, '/auth/me', { method: 'GET' });
        if (!res.ok) {
            const err = await res.json().catch(() => ({}));
            throw new Error(extractErrorMessage(err, `API Error: ${res.statusText}`));
        }
        return unwrapBody(await res.json());
    },
    async authLogout(config: ApiConfig) {
        if (!AUTH_API_ENABLED) {
            setAuth(null);
            return { status: 'skipped', reason: 'auth_disabled' };
        }
        const res = await requestJson(config, '/auth/logout', { method: 'POST' }, false);
        setAuth(null);
        if (!res.ok) throw new Error(`API Error: ${res.statusText}`);
        return res.json();
    },
    async ping(config: ApiConfig, timeoutMs = 2000) {
        if (config.isMock) return true;
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
        try {
            const res = await fetch(`${config.baseUrl}/sessions`, {
                signal: controller.signal,
                credentials: 'include',
                headers: headersWithAuth()
            });
            return res.ok;
        } catch {
            return false;
        } finally {
            clearTimeout(timeoutId);
        }
    },
    async get(config: ApiConfig, endpoint: string) {
        if (config.isMock) {
            await new Promise(r => setTimeout(r, 400));
            if (endpoint === '/sessions') return MOCK_SESSIONS;
            if (endpoint === '/projects') return clone(MOCK_PROJECTS);
            if (endpoint.startsWith('/projects/query')) {
                return {
                    items: clone(MOCK_PROJECTS),
                    page: 1,
                    pageSize: 20,
                    total: MOCK_PROJECTS.length,
                    hasMore: false,
                    search: '',
                };
            }
            if (endpoint === '/datasets') return [...MOCK_DATASETS];
            if (endpoint.match(/\/projects\/[^/]+\/datasets\/[^/]+\/preview/)) {
                const parts = endpoint.split('/');
                const projectId = ensureMockProject(parts[2] || DEFAULT_MOCK_PROJECT_ID);
                const datasetName = decodeURIComponent((parts[4] || '').split('?')[0]);
                const datasets = getMockProjectDatasets(projectId);
                const ds = datasets.find(d => d.name === datasetName || d.id === datasetName);
                return { rows: ds?.rows || [], totalCount: ds?.totalCount || 0 };
            }
            if (endpoint.match(/\/projects\/[^/]+\/imports/)) {
                const projectId = ensureMockProject(endpoint.split('/')[2] || DEFAULT_MOCK_PROJECT_ID);
                return clone(MOCK_PROJECT_IMPORTS[projectId] || []);
            }
            if (endpoint.match(/\/projects\/[^/]+\/datasets$/)) {
                const projectId = ensureMockProject(endpoint.split('/')[2] || DEFAULT_MOCK_PROJECT_ID);
                return clone(getMockProjectDatasets(projectId));
            }
            if (endpoint.match(/\/projects\/[^/]+\/state/)) {
                const projectId = ensureMockProject(endpoint.split('/')[2] || DEFAULT_MOCK_PROJECT_ID);
                const state = MOCK_PROJECT_STATES[projectId] || { version: 0, state: {} };
                return {
                    projectId,
                    version: state.version,
                    state: clone(state.state || {}),
                    updatedAt: Date.now(),
                };
            }
            if (endpoint.match(/\/projects\/[^/]+\/metadata/)) {
                const projectId = ensureMockProject(endpoint.split('/')[2] || DEFAULT_MOCK_PROJECT_ID);
                return clone(MOCK_PROJECT_METADATA[projectId] || createMockProjectMetadataDetail(''));
            }
            if (endpoint.match(/\/projects\/[^/]+\/diagnostics/)) {
                const projectId = ensureMockProject(endpoint.split('/')[2] || DEFAULT_MOCK_PROJECT_ID);
                const state = MOCK_PROJECT_STATES[projectId]?.state || {};
                const diagnostics = buildMockDiagnostics(projectId, getMockProjectDatasets(projectId));
                return {
                    ...diagnostics,
                    projectId,
                    sessionId: undefined,
                    sources: diagnostics.sources,
                    datasets: getMockProjectDatasets(projectId).map(d => ({
                        id: d.id,
                        name: d.name,
                        totalCount: d.totalCount,
                        fieldCount: d.fields?.length || 0,
                    })),
                    operations: diagnostics.operations,
                    generatedAt: new Date().toISOString(),
                    state,
                };
            }
            if (endpoint.match(/\/projects\/[^/]+\/members/)) {
                const projectId = ensureMockProject(endpoint.split('/')[2] || DEFAULT_MOCK_PROJECT_ID);
                return clone(MOCK_PROJECT_MEMBERS[projectId] || []);
            }
            if (endpoint.match(/\/projects\/[^/]+\/jobs/)) {
                const projectId = ensureMockProject(endpoint.split('/')[2] || DEFAULT_MOCK_PROJECT_ID);
                return clone(MOCK_PROJECT_JOBS[projectId] || []);
            }
            if (endpoint.match(/\/projects\/[^/]+$/)) {
                const projectId = ensureMockProject(endpoint.split('/')[2] || DEFAULT_MOCK_PROJECT_ID);
                return clone(MOCK_PROJECTS.find(project => project.id === projectId) || MOCK_PROJECTS[0]);
            }
            if (endpoint.match(/\/jobs\/[^/]+$/)) {
                const jobId = endpoint.split('/')[2];
                const job = Object.values(MOCK_PROJECT_JOBS).flat().find(item => item.id === jobId);
                return clone(job || {});
            }
            if (endpoint.match(/\/sessions\/.*\/datasets\/.*\/preview/)) {
                const parts = endpoint.split('/');
                const datasetName = decodeURIComponent((parts[4] || '').split('?')[0]);
                const ds = MOCK_DATASETS.find(d => d.name === datasetName || d.id === datasetName);
                return { rows: ds?.rows || [], totalCount: ds?.totalCount || 0 };
            }
            if (endpoint.match(/\/sessions\/.*\/imports/)) return [...MOCK_IMPORT_HISTORY];
            if (endpoint.match(/\/sessions\/.*\/datasets$/)) return [...MOCK_DATASETS];
            if (endpoint.match(/\/sessions\/.*\/state/)) return MOCK_SESSION_STATES[endpoint.split('/')[2]] || {};
            if (endpoint.match(/\/sessions\/.*\/metadata/)) return MOCK_SESSION_METADATA[endpoint.split('/')[2]] || { displayName: "", settings: { cascadeDisable: false, panelPosition: 'right' }};
            if (endpoint.match(/\/sessions\/.*\/diagnostics/)) {
                const sessionId = endpoint.split('/')[2];
                return buildMockDiagnostics(sessionId);
            }
            return {};
        }
        const res = await requestJson(config, endpoint, { method: 'GET' });
        if (!res.ok) throw new Error(`API Error: ${res.statusText}`);
        return unwrapBody(await res.json());
    },

    async post(config: ApiConfig, endpoint: string, body: any) {
        if (config.isMock) {
            await new Promise(r => setTimeout(r, 600));
            if (endpoint === '/projects') {
                const projectId = `prj_mock_${Date.now()}`;
                const project = createMockProjectMetadata(projectId, body?.name || `Mock Project ${MOCK_PROJECTS.length + 1}`);
                project.description = body?.description || '';
                MOCK_PROJECTS.unshift(project);
                ensureMockProject(projectId);
                MOCK_PROJECT_METADATA[projectId] = createMockProjectMetadataDetail(project.name);
                MOCK_PROJECT_DATASETS[projectId] = [];
                MOCK_PROJECT_IMPORTS[projectId] = [];
                MOCK_PROJECT_STATES[projectId] = { version: 0, state: {} };
                MOCK_PROJECT_MEMBERS[projectId] = [clone(DEFAULT_MOCK_MEMBER)];
                MOCK_PROJECT_JOBS[projectId] = [];
                return clone(project);
            }
            if (endpoint.match(/\/projects\/[^/]+\/archive/)) {
                const projectId = ensureMockProject(endpoint.split('/')[2] || DEFAULT_MOCK_PROJECT_ID);
                const project = MOCK_PROJECTS.find(item => item.id === projectId);
                if (project) {
                    project.archived = Boolean(body?.archived ?? true);
                    project.updatedAt = Date.now();
                }
                return clone(project || {});
            }
            if (endpoint.match(/\/projects\/[^/]+\/members$/)) {
                const projectId = ensureMockProject(endpoint.split('/')[2] || DEFAULT_MOCK_PROJECT_ID);
                const nextMember: ProjectMember = {
                    userId: `usr_mock_${Date.now()}`,
                    email: String(body?.memberEmail || `member${Date.now()}@mock.local`),
                    displayName: String(body?.memberEmail || 'Mock Member').split('@')[0],
                    role: body?.role || 'viewer',
                    createdAt: Date.now(),
                    updatedAt: Date.now(),
                };
                MOCK_PROJECT_MEMBERS[projectId] = [...(MOCK_PROJECT_MEMBERS[projectId] || []), nextMember];
                return clone(nextMember);
            }
            if (endpoint.match(/\/projects\/[^/]+\/state\/commit/)) {
                const projectId = ensureMockProject(endpoint.split('/')[2] || DEFAULT_MOCK_PROJECT_ID);
                const current = MOCK_PROJECT_STATES[projectId] || { version: 0, state: {} };
                const baseVersion = Number(body?.baseVersion || 0);
                if (baseVersion !== current.version) {
                    return {
                        conflict: true,
                        latestVersion: current.version,
                        state: clone(current.state || {}),
                    };
                }
                const nextState = body?.state
                    ? clone(body.state)
                    : applyPatches(clone(current.state || {}), body?.patches || []);
                const saved = setMockProjectState(projectId, nextState);
                return {
                    projectId,
                    version: saved.version,
                    state: clone(saved.state || {}),
                    conflict: false,
                    updatedAt: Date.now(),
                };
            }
            if (endpoint.match(/\/projects\/[^/]+\/datasets\/update/)) {
                const projectId = ensureMockProject(endpoint.split('/')[2] || DEFAULT_MOCK_PROJECT_ID);
                const { datasetId, fieldTypes } = body;
                const datasets = getMockProjectDatasets(projectId);
                const target = datasets.find(d => d.id === datasetId || d.name === datasetId);
                if (target) target.fieldTypes = fieldTypes;
                return { status: "ok" };
            }
            if (endpoint.match(/\/projects\/[^/]+\/metadata/)) {
                const projectId = ensureMockProject(endpoint.split('/')[2] || DEFAULT_MOCK_PROJECT_ID);
                const current = MOCK_PROJECT_METADATA[projectId] || createMockProjectMetadataDetail('');
                MOCK_PROJECT_METADATA[projectId] = { ...current, ...body };
                const project = MOCK_PROJECTS.find(item => item.id === projectId);
                if (project && body?.displayName !== undefined) {
                    project.name = body.displayName || project.name;
                    project.updatedAt = Date.now();
                }
                return { status: "ok" };
            }
            if (endpoint.match(/\/projects\/[^/]+\/execute/)) {
                const projectId = ensureMockProject(endpoint.split('/')[2] || DEFAULT_MOCK_PROJECT_ID);
                const { tree, targetNodeId, page = 1, pageSize = 50, viewId = "main" } = body;
                const datasets = getMockProjectDatasets(projectId);
                const mainResult = executeMockLogic(tree, targetNodeId, datasets);
                let finalData = mainResult.rows;
                let columns = mainResult.columns;

                if (viewId !== "main") {
                    finalData = [];
                    columns = [];
                    const path = findPathToNode(tree, targetNodeId);
                    const targetNode = path ? path[path.length - 1] : null;
                    const multiCmd = targetNode?.commands.find(c => c.type === 'multi_table');
                    if (multiCmd && multiCmd.config.subTables) {
                        const subConfig = multiCmd.config.subTables.find((s: any) => s.id === viewId);
                        if (subConfig) {
                            const subDs = datasets.find(d => d.name === subConfig.table);
                            if (subDs) {
                                finalData = subDs.rows.filter((subRow: any) =>
                                    mainResult.rows.some((mainRow: any) =>
                                        evaluateSubTableOnCondition(mainRow, subRow, subConfig as SubTableConfig)
                                        && evaluateSubTableConditionGroup(
                                            mainRow,
                                            subRow,
                                            (subConfig as SubTableConfig).onConditionGroup || (subConfig as SubTableConfig).conditionGroup || null
                                        )
                                    )
                                );
                                columns = subDs.fields;
                            }
                        }
                    }
                }

                const totalCount = finalData.length;
                const start = (page - 1) * pageSize;
                return {
                    rows: finalData.slice(start, start + pageSize),
                    totalCount,
                    columns,
                    page,
                    pageSize,
                    activeViewId: viewId,
                };
            }
            if (endpoint.match(/\/projects\/[^/]+\/jobs\/execute/)) {
                const projectId = ensureMockProject(endpoint.split('/')[2] || DEFAULT_MOCK_PROJECT_ID);
                const result = await this.post(config, `/projects/${projectId}/execute`, body);
                const job: ProjectJob = {
                    id: `job_mock_${Date.now()}`,
                    projectId,
                    type: 'execute',
                    status: 'completed',
                    progress: 100,
                    payload: clone(body || {}),
                    result: clone(result),
                    createdAt: Date.now(),
                    updatedAt: Date.now(),
                    startedAt: Date.now(),
                    finishedAt: Date.now(),
                };
                MOCK_PROJECT_JOBS[projectId] = [job, ...(MOCK_PROJECT_JOBS[projectId] || [])];
                return clone(job);
            }
            if (endpoint.match(/\/projects\/[^/]+\/export/)) {
                const projectId = ensureMockProject(endpoint.split('/')[2] || DEFAULT_MOCK_PROJECT_ID);
                const job: ProjectJob = {
                    id: `job_export_${Date.now()}`,
                    projectId,
                    type: 'export',
                    status: 'completed',
                    progress: 100,
                    payload: clone(body || {}),
                    result: {
                        downloadUrl: `/jobs/job_export_${Date.now()}/result`,
                        fileName: `export_${projectId}.csv`,
                        contentType: 'text/csv',
                    },
                    createdAt: Date.now(),
                    updatedAt: Date.now(),
                    startedAt: Date.now(),
                    finishedAt: Date.now(),
                };
                MOCK_PROJECT_JOBS[projectId] = [job, ...(MOCK_PROJECT_JOBS[projectId] || [])];
                return clone(job);
            }
            if (endpoint.match(/\/projects\/[^/]+\/generate_sql/)) {
                const query = body?.targetCommandId
                    ? `-- mock SQL for ${body.targetCommandId}\nSELECT * FROM demo_table`
                    : '-- mock SQL\nSELECT * FROM demo_table';
                return { sql: query };
            }
            if (endpoint.match(/\/projects\/[^/]+\/analyze/)) return { report: ["⚠️ Mock Analysis: Overlap detected in 2 branches."] };
            if (endpoint.match(/\/projects\/[^/]+\/query/)) {
                const projectId = ensureMockProject(endpoint.split('/')[2] || DEFAULT_MOCK_PROJECT_ID);
                const { query = "", page = 1, pageSize = 50 } = body;
                const datasets = getMockProjectDatasets(projectId);
                const match = query.match(/select \\* from (.*)/i);
                if (match) {
                    const rawTableName = match[1].trim().replace(/['"`]/g, '');
                    const ds = datasets.find(d => d.name === rawTableName || d.id === rawTableName);
                    if (ds) {
                        const start = (page - 1) * pageSize;
                        return { rows: ds.rows.slice(start, start + pageSize), totalCount: ds.totalCount, columns: ds.fields, page, pageSize };
                    }
                }
                return { rows: [], totalCount: 0, columns: [], page: 1, pageSize: 50 };
            }
            if (endpoint.match(/\/jobs\/[^/]+:cancel/)) {
                const jobId = endpoint.split('/')[2]?.replace(':cancel', '');
                for (const projectId of Object.keys(MOCK_PROJECT_JOBS)) {
                    const nextJobs = (MOCK_PROJECT_JOBS[projectId] || []).map(job => (
                        job.id === jobId
                            ? { ...job, status: 'canceled', cancelRequested: true, updatedAt: Date.now(), finishedAt: Date.now() }
                            : job
                    ));
                    MOCK_PROJECT_JOBS[projectId] = nextJobs;
                }
                return { status: 'ok' };
            }
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
                                const subRows = subDs.rows;
                                finalData = subRows.filter((subRow: any) =>
                                    mainResult.rows.some((mainRow: any) =>
                                        evaluateSubTableOnCondition(mainRow, subRow, subConfig as SubTableConfig)
                                        && evaluateSubTableConditionGroup(
                                            mainRow,
                                            subRow,
                                            (subConfig as SubTableConfig).onConditionGroup || (subConfig as SubTableConfig).conditionGroup || null
                                        )
                                    )
                                );
                                columns = subDs.fields;
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

        const res = await requestJson(config, endpoint, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body)
        });
        if (!res.ok) {
            const err = await res.json();
            throw new Error(extractErrorMessage(err, `API Error: ${res.statusText}`));
        }
        return unwrapBody(await res.json());
    },

    async patch(config: ApiConfig, endpoint: string, body: any) {
        if (config.isMock) {
            await new Promise(r => setTimeout(r, 300));
            if (endpoint.match(/\/projects\/[^/]+\/members\/[^/]+/)) {
                const parts = endpoint.split('/');
                const projectId = ensureMockProject(parts[2] || DEFAULT_MOCK_PROJECT_ID);
                const memberUserId = parts[4];
                MOCK_PROJECT_MEMBERS[projectId] = (MOCK_PROJECT_MEMBERS[projectId] || []).map(member => (
                    member.userId === memberUserId
                        ? { ...member, role: body?.role || member.role, updatedAt: Date.now() }
                        : member
                ));
                return clone((MOCK_PROJECT_MEMBERS[projectId] || []).find(member => member.userId === memberUserId) || {});
            }
            return {};
        }
        const res = await requestJson(config, endpoint, {
            method: 'PATCH',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body)
        });
        if (!res.ok) {
            const err = await res.json().catch(() => ({}));
            throw new Error(extractErrorMessage(err, `API Error: ${res.statusText}`));
        }
        return unwrapBody(await res.json());
    },

    async export(config: ApiConfig, endpoint: string, body: any) {
        if (config.isMock) return;
        const res = await requestJson(config, endpoint, {
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
        if (config.isMock) {
            if (endpoint.match(/\/projects\/[^/]+\/datasets\/(.*)/)) {
                const parts = endpoint.split('/');
                const projectId = ensureMockProject(parts[2] || DEFAULT_MOCK_PROJECT_ID);
                const datasetName = decodeURIComponent(parts[4] || '');
                const datasets = getMockProjectDatasets(projectId);
                const idx = datasets.findIndex(d => d.name === datasetName || d.id === datasetName);
                if (idx >= 0) datasets.splice(idx, 1);
                return { status: "ok" };
            }
            if (endpoint.match(/\/projects\/[^/]+\/members\/[^/]+/)) {
                const parts = endpoint.split('/');
                const projectId = ensureMockProject(parts[2] || DEFAULT_MOCK_PROJECT_ID);
                const memberUserId = parts[4];
                MOCK_PROJECT_MEMBERS[projectId] = (MOCK_PROJECT_MEMBERS[projectId] || []).filter(member => member.userId !== memberUserId);
                return { status: 'ok' };
            }
            if (endpoint.match(/\/projects\/[^/]+$/)) {
                const projectId = endpoint.split('/')[2];
                const index = MOCK_PROJECTS.findIndex(project => project.id === projectId);
                if (index >= 0) MOCK_PROJECTS.splice(index, 1);
                delete MOCK_PROJECT_STATES[projectId];
                delete MOCK_PROJECT_METADATA[projectId];
                delete MOCK_PROJECT_DATASETS[projectId];
                delete MOCK_PROJECT_IMPORTS[projectId];
                delete MOCK_PROJECT_MEMBERS[projectId];
                delete MOCK_PROJECT_JOBS[projectId];
                return { status: 'ok' };
            }
            const match = endpoint.match(/\/sessions\/.*\/datasets\/(.*)/);
            if (match) {
                const datasetName = decodeURIComponent(match[1]);
                const idx = MOCK_DATASETS.findIndex(d => d.name === datasetName || d.id === datasetName);
                if (idx >= 0) MOCK_DATASETS.splice(idx, 1);
                return { status: "ok" };
            }
            return { status: "ok" };
        }
        const res = await requestJson(config, endpoint, { method: 'DELETE' });
        if (!res.ok) throw new Error(`API Error: ${res.statusText}`);
        return unwrapBody(await res.json());
    },

    async upload(config: ApiConfig, endpoint: string, formData: FormData) {
        if (config.isMock) {
            await new Promise(r => setTimeout(r, 1000));
            const file = formData.get('file') as File;
            const name = formData.get('name') as string;
            const rows = [{ col1: "A", col2: 100, col3: true }, { col1: "B", col2: 200, col3: false }];
            const newDs: Dataset = { id: `mock_table_${Date.now()}`, name: name || file.name, fields: ["col1", "col2", "col3"], rows: rows, fieldTypes: generateFieldTypes(rows), totalCount: 2 };
            if (endpoint.match(/\/projects\/[^/]+\/upload/)) {
                const projectId = ensureMockProject(endpoint.split('/')[2] || DEFAULT_MOCK_PROJECT_ID);
                getMockProjectDatasets(projectId).push(newDs);
                MOCK_PROJECT_IMPORTS[projectId] = [
                    ...(MOCK_PROJECT_IMPORTS[projectId] || []),
                    {
                        timestamp: Date.now(),
                        originalFileName: file?.name || 'mock.csv',
                        datasetName: newDs.name,
                        tableName: newDs.name,
                        rows: newDs.totalCount || 0,
                    }
                ];
                return newDs;
            }
            MOCK_DATASETS.push(newDs);
            return newDs;
        }
        const res = await requestJson(config, endpoint, { method: 'POST', body: formData });
        if (!res.ok) throw new Error(`Upload Error: ${res.statusText}`);
        return unwrapBody(await res.json());
    },

    async uploadPreview(config: ApiConfig, projectId: string, file: File): Promise<{
        previewToken: string;
        fields: string[];
        fieldTypes: Record<string, FieldInfo>;
        rows: any[];
        totalCount: number;
        cleanReport: CleanPreviewReport;
    }> {
        if (config.isMock) {
            await new Promise(r => setTimeout(r, 500));
            const rows = [{ col1: "A", col2: 100, col3: true }, { col1: "B", col2: 200, col3: false }];
            return {
                previewToken: `mock_preview_${Date.now()}`,
                fields: ["col1", "col2", "col3"],
                fieldTypes: { col1: { type: "string" }, col2: { type: "number" }, col3: { type: "boolean" } },
                rows,
                totalCount: 2,
                cleanReport: {
                    duplicateRowCount: 0,
                    missingValueCounts: {},
                    outlierCounts: {},
                    whitespaceFieldCount: 0,
                },
            };
        }
        const formData = new FormData();
        formData.append('file', file);
        const res = await requestJson(config, `/projects/${projectId}/upload/preview`, { method: 'POST', body: formData });
        if (!res.ok) throw new Error(`Preview Error: ${res.statusText}`);
        return unwrapBody(await res.json());
    }
};
