
import { ApiConfig, Dataset, ExecutionResult, SessionMetadata, OperationNode, Command } from "../types";

// --- MOCK DATA GENERATORS ---

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
            hire_date: new Date(2020, i % 12, (i % 28) + 1).toISOString().split('T')[0]
        });
    }
    return rows;
};

const generateSales = (count: number) => {
    const rows = [];
    for (let i = 1; i <= count; i++) {
        rows.push({
            order_id: 1000 + i,
            uid: (i % 20) + 1, // Connects to employee ids 1-20
            amount: Math.round(Math.random() * 1000),
            date: new Date(2023, i % 12, (i % 28) + 1).toISOString()
        });
    }
    return rows;
};

const MOCK_DATASETS: Dataset[] = [
    { 
        id: "mock_employees", 
        name: "employees.csv", 
        totalCount: 50, 
        fields: ["id", "name", "dept", "salary", "active", "hire_date"], 
        rows: generateEmployees(50) 
    },
    { 
        id: "mock_sales", 
        name: "sales_data.csv", 
        totalCount: 150, 
        fields: ["order_id", "uid", "amount", "date"], 
        rows: generateSales(150) 
    }
];

const MOCK_SESSIONS: SessionMetadata[] = [
    { sessionId: "mock-session-demo", createdAt: Date.now() - 100000 },
    { sessionId: "mock-session-archive", createdAt: Date.now() - 86400000 }
];

// Memory store for mock session states
const MOCK_SESSION_STATES: Record<string, any> = {};

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

const executeMockLogic = (tree: OperationNode, targetNodeId: string): any => {
    const path = findPathToNode(tree, targetNodeId);
    if (!path) throw new Error("Target node not found in tree");

    // 1. Determine Initial Data Source
    // Look for the first command that specifies a mainTable, or default to the first dataset
    let currentData: any[] = [];
    
    // Find the first data source used in the path
    let sourceName = "";
    for (const node of path) {
        if (node.commands) {
            for (const cmd of node.commands) {
                if (cmd.config.mainTable) {
                    sourceName = cmd.config.mainTable;
                    break;
                }
            }
        }
        if (sourceName) break;
    }

    if (!sourceName && MOCK_DATASETS.length > 0) {
        sourceName = MOCK_DATASETS[0].name;
    }

    const sourceDataset = MOCK_DATASETS.find(d => d.name === sourceName);
    currentData = sourceDataset ? [...sourceDataset.rows] : [];

    // 2. Apply Commands
    for (const node of path) {
        if (!node.enabled) continue;

        for (const cmd of node.commands) {
            currentData = applyMockCommand(currentData, cmd);
        }
    }

    // 3. Format Result (Return full data here, allow caller to paginate)
    const columns = currentData.length > 0 ? Object.keys(currentData[0]) : (sourceDataset?.fields || []);
    return {
        rows: currentData,
        totalCount: currentData.length,
        columns: columns
    };
};

const applyMockCommand = (data: any[], cmd: Command): any[] => {
    const { config } = cmd;

    if (cmd.type === 'filter') {
        if (!config.field) return data;
        return data.filter(row => {
            const val = row[config.field!];
            const target = config.value;
            
            // Simple type coercion
            const numVal = Number(val);
            const rowNum = Number(val);

            switch (config.operator) {
                case '=': return String(val) == String(target); // loose equality for mock
                case '!=': return String(val) != String(target);
                case '>': return val > target;
                case '>=': return val >= target;
                case '<': return val < target;
                case '<=': return val <= target;
                case 'contains': return String(val).toLowerCase().includes(String(target).toLowerCase());
                case 'not_contains': return !String(val).toLowerCase().includes(String(target).toLowerCase());
                case 'starts_with': return String(val).startsWith(String(target));
                case 'ends_with': return String(val).endsWith(String(target));
                default: return true;
            }
        });
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

    if (cmd.type === 'transform') {
        // Simple mock transform for demo
        if (config.outputField) {
            return data.map(row => ({
                ...row,
                [config.outputField!]: "Calculated"
            }));
        }
    }

    // Join, Aggregate not fully implemented in mock
    return data;
};


// --- API EXPORT ---

export const api = {
    async get(config: ApiConfig, endpoint: string) {
        if (config.isMock) {
            await new Promise(r => setTimeout(r, 400)); // Simulate latency
            if (endpoint === '/sessions') return MOCK_SESSIONS;
            if (endpoint.includes('/datasets')) return MOCK_DATASETS;
            
            // Mock state endpoint
            if (endpoint.match(/\/sessions\/.*\/state/)) {
                const parts = endpoint.split('/');
                const sessId = parts[2];
                return MOCK_SESSION_STATES[sessId] || {};
            }

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
            
            if (endpoint === '/execute') {
                const { tree, targetNodeId, page = 1, pageSize = 50 } = body;
                const fullResult = executeMockLogic(tree, targetNodeId);
                
                const start = (page - 1) * pageSize;
                const end = start + pageSize;
                
                return {
                    rows: fullResult.rows.slice(start, end),
                    totalCount: fullResult.totalCount,
                    columns: fullResult.columns,
                    page,
                    pageSize
                };
            }

            // Mock state save
            if (endpoint.match(/\/sessions\/.*\/state/)) {
                const parts = endpoint.split('/');
                const sessId = parts[2];
                MOCK_SESSION_STATES[sessId] = body;
                return { status: "ok" };
            }

            if (endpoint === '/query') {
                // Simple mock SQL parser for "SELECT * FROM table"
                const query = body.query?.toLowerCase() || "";
                const { page = 1, pageSize = 50 } = body;
                
                if (query.includes("select") && query.includes("from")) {
                    const parts = query.split("from");
                    if (parts.length > 1) {
                        const tableName = parts[1].trim().split(" ")[0].replace(";", "");
                        const dataset = MOCK_DATASETS.find(d => d.name === tableName);
                        if (dataset) {
                            const start = (page - 1) * pageSize;
                            const end = start + pageSize;
                            return {
                                rows: dataset.rows.slice(start, end),
                                totalCount: dataset.totalCount,
                                columns: dataset.fields,
                                page,
                                pageSize
                            };
                        }
                    }
                }
                return { rows: [], totalCount: 0, columns: [], page, pageSize };
            }
            return {};
        }

        const res = await fetch(`${config.baseUrl}${endpoint}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body)
        });
        if (!res.ok) {
            const err = await res.json();
            throw new Error(err.detail || `API Error: ${res.statusText}`);
        }
        return res.json();
    },

    async delete(config: ApiConfig, endpoint: string) {
        if (config.isMock) {
            await new Promise(r => setTimeout(r, 300));
            return { status: "ok" };
        }
        const res = await fetch(`${config.baseUrl}${endpoint}`, { method: 'DELETE' });
        if (!res.ok) throw new Error(`API Error: ${res.statusText}`);
        return res.json();
    },

    async upload(config: ApiConfig, endpoint: string, formData: FormData) {
        if (config.isMock) {
            await new Promise(r => setTimeout(r, 1000));
            const file = formData.get('file') as File;
            const name = formData.get('name') as string;
            
            const newDs = {
                id: `mock_table_${Date.now()}`,
                name: name || file.name,
                fields: ["col1", "col2", "col3"],
                rows: [{ col1: "A", col2: 100, col3: true }, { col1: "B", col2: 200, col3: false }],
                totalCount: 2
            };
            MOCK_DATASETS.push(newDs);
            return newDs;
        }

        const res = await fetch(`${config.baseUrl}${endpoint}`, {
            method: 'POST',
            body: formData,
        });
        if (!res.ok) throw new Error(`Upload Error: ${res.statusText}`);
        return res.json();
    }
};
