
import { describe, it, expect } from 'vitest';
import { api } from '../utils/api';
import { OperationNode, Command, ExecutionResult, FilterGroup } from '../types';

const MOCK_CONFIG = { baseUrl: '', isMock: true };

// Helper to create a tree structure
const createTree = (commands: Command[], sourceTable = 'employees.csv'): OperationNode => {
    return {
        id: 'root',
        type: 'operation',
        operationType: 'process',
        name: 'Root',
        enabled: true,
        commands: [
            { id: 'src', type: 'source', config: { mainTable: sourceTable }, order: 0 },
            ...commands
        ],
        children: []
    };
};

// Helper to create filter command with filterRoot
const createFilterCommand = (filterRoot: FilterGroup, order = 1): Command => ({
    id: `f${order}`,
    type: 'filter',
    order,
    config: { filterRoot }
});

describe('Mock DataFlow Engine', () => {
    
    it('should load mock data source', async () => {
        const tree = createTree([], 'employees.csv');
        const res: ExecutionResult = await api.post(MOCK_CONFIG, '/execute', {
            sessionId: 'test',
            tree,
            targetNodeId: 'root'
        });

        expect(res.totalCount).toBe(50);
        expect(res.rows[0]).toHaveProperty('name');
        expect(res.rows[0]).toHaveProperty('salary');
    });

    it('should filter data (simple > condition)', async () => {
        const commands: Command[] = [
            {
                id: 'f1', type: 'filter', order: 1,
                config: {
                    filterRoot: {
                        id: 'g1', type: 'group', logicalOperator: 'AND',
                        conditions: [
                            { id: 'c1', type: 'condition', field: 'salary', operator: '>', value: 80000, dataType: 'number' }
                        ]
                    }
                }
            }
        ];
        const tree = createTree(commands);
        const res: ExecutionResult = await api.post(MOCK_CONFIG, '/execute', {
            sessionId: 'test', tree, targetNodeId: 'root'
        });

        expect(res.totalCount).toBeLessThan(50);
        expect(res.totalCount).toBeGreaterThan(0);
        // Verify constraint
        const invalidRow = res.rows.find(r => r.salary <= 80000);
        expect(invalidRow).toBeUndefined();
    });

    it('should filter data (group OR condition)', async () => {
        // (Dept = HR OR Dept = Engineering)
        const commands: Command[] = [
            {
                id: 'f1', type: 'filter', order: 1,
                config: {
                    filterRoot: {
                        id: 'g1', type: 'group', logicalOperator: 'OR',
                        conditions: [
                            { id: 'c1', type: 'condition', field: 'dept', operator: '=', value: 'HR' },
                            { id: 'c2', type: 'condition', field: 'dept', operator: '=', value: 'Engineering' }
                        ]
                    }
                }
            }
        ];
        const tree = createTree(commands);
        const res: ExecutionResult = await api.post(MOCK_CONFIG, '/execute', {
            sessionId: 'test', tree, targetNodeId: 'root'
        });

        // Verify
        const invalidRow = res.rows.find(r => r.dept !== 'HR' && r.dept !== 'Engineering');
        expect(invalidRow).toBeUndefined();
        // Should contain both
        expect(res.rows.some(r => r.dept === 'HR')).toBe(true);
        expect(res.rows.some(r => r.dept === 'Engineering')).toBe(true);
    });

    it('should sort data descending', async () => {
        const commands: Command[] = [
            { id: 's1', type: 'sort', order: 1, config: { field: 'salary', ascending: false } }
        ];
        const tree = createTree(commands);
        const res: ExecutionResult = await api.post(MOCK_CONFIG, '/execute', {
            sessionId: 'test', tree, targetNodeId: 'root'
        });

        const salaries = res.rows.map(r => r.salary);
        const sorted = [...salaries].sort((a, b) => b - a);
        expect(salaries).toEqual(sorted);
    });

    it('should group by and aggregate (mean)', async () => {
        const commands: Command[] = [
            {
                id: 'g1', type: 'group', order: 1,
                config: {
                    groupByFields: ['dept'],
                    aggregations: [
                        { field: 'salary', func: 'mean', alias: 'avg_salary' }
                    ]
                }
            }
        ];
        const tree = createTree(commands);
        const res: ExecutionResult = await api.post(MOCK_CONFIG, '/execute', {
            sessionId: 'test', tree, targetNodeId: 'root'
        });

        // Departments are unique
        const depts = new Set(res.rows.map(r => r.dept));
        expect(depts.size).toBe(res.rows.length);
        expect(res.rows[0]).toHaveProperty('avg_salary');
    });

    it('should join two tables (Left Join)', async () => {
        // mock_employees (50 rows) left join mock_sales (200 rows) on id = uid
        // This is 1:N join potentially
        const commands: Command[] = [
            {
                id: 'j1', type: 'join', order: 1,
                config: {
                    joinTable: 'sales_data.csv',
                    joinType: 'LEFT',
                    on: 'employees.id = sales.uid',
                    joinSuffix: '_joined'
                }
            }
        ];
        const tree = createTree(commands);
        const res: ExecutionResult = await api.post(MOCK_CONFIG, '/execute', {
            sessionId: 'test', tree, targetNodeId: 'root', pageSize: 500
        });

        // Because sales is generated with uid (i % 50) + 1, all employee IDs (1-50) should have matches
        expect(res.totalCount).toBeGreaterThan(50); // 1:N join increases rows
        expect(res.rows[0]).toHaveProperty('amount'); // Joined column
        expect(res.rows[0]).toHaveProperty('order_id');
    });

    it('should resolve defined variables', async () => {
        // 1. Define variable
        // 2. Use variable in filter
        const tree: OperationNode = {
            id: 'root', type: 'operation', operationType: 'process', name: 'Root', enabled: true,
            commands: [
                { id: 'src', type: 'source', config: { mainTable: 'employees.csv' }, order: 0 },
                { 
                    id: 'def', type: 'define_variable', order: 1,
                    config: { variableName: 'target_dept', variableValue: 'Sales' } 
                },
                {
                    id: 'filt', type: 'filter', order: 2,
                    config: {
                        filterRoot: {
                            id: 'g1', type: 'group', logicalOperator: 'AND',
                            conditions: [
                                { id: 'c1', type: 'condition', field: 'dept', operator: '=', value: '{target_dept}' }
                            ]
                        }
                    }
                }
            ],
            children: []
        };

        const res: ExecutionResult = await api.post(MOCK_CONFIG, '/execute', {
            sessionId: 'test', tree, targetNodeId: 'root'
        });

        expect(res.rows.length).toBeGreaterThan(0);
        const invalid = res.rows.find(r => r.dept !== 'Sales');
        expect(invalid).toBeUndefined();
    });

    it('should calculate new column with JS transform', async () => {
        const commands: Command[] = [
            {
                id: 't1', type: 'transform', order: 1,
                config: {
                    mappings: [
                        { id: 'm1', mode: 'simple', expression: 'salary * 2', outputField: 'double_salary' }
                    ]
                }
            }
        ];
        const tree = createTree(commands);
        const res: ExecutionResult = await api.post(MOCK_CONFIG, '/execute', {
            sessionId: 'test', tree, targetNodeId: 'root'
        });

        const row = res.rows[0];
        expect(row.double_salary).toBe(row.salary * 2);
    });
});

// === NEW OPERATOR TESTS ===

describe('Mock Engine - String Operators', () => {
    it('should filter with starts_with operator', async () => {
        const commands: Command[] = [
            createFilterCommand({
                id: 'g1', type: 'group', logicalOperator: 'AND',
                conditions: [
                    { id: 'c1', type: 'condition', field: 'name', operator: 'starts_with', value: 'Alice' }
                ]
            })
        ];
        const tree = createTree(commands);
        const res: ExecutionResult = await api.post(MOCK_CONFIG, '/execute', {
            sessionId: 'test', tree, targetNodeId: 'root'
        });

        expect(res.totalCount).toBeGreaterThan(0);
        for (const row of res.rows) {
            expect(row.name.startsWith('Alice')).toBe(true);
        }
    });

    it('should filter with ends_with operator', async () => {
        const commands: Command[] = [
            createFilterCommand({
                id: 'g1', type: 'group', logicalOperator: 'AND',
                conditions: [
                    { id: 'c1', type: 'condition', field: 'dept', operator: 'ends_with', value: 'ing' }
                ]
            })
        ];
        const tree = createTree(commands);
        const res: ExecutionResult = await api.post(MOCK_CONFIG, '/execute', {
            sessionId: 'test', tree, targetNodeId: 'root'
        });

        expect(res.totalCount).toBeGreaterThan(0);
        for (const row of res.rows) {
            expect(row.dept.endsWith('ing')).toBe(true);
        }
    });

    it('should filter with contains operator', async () => {
        const commands: Command[] = [
            createFilterCommand({
                id: 'g1', type: 'group', logicalOperator: 'AND',
                conditions: [
                    { id: 'c1', type: 'condition', field: 'dept', operator: 'contains', value: 'neer' }
                ]
            })
        ];
        const tree = createTree(commands);
        const res: ExecutionResult = await api.post(MOCK_CONFIG, '/execute', {
            sessionId: 'test', tree, targetNodeId: 'root'
        });

        expect(res.totalCount).toBeGreaterThan(0);
        for (const row of res.rows) {
            expect(row.dept.toLowerCase()).toContain('neer');
        }
    });

    it('should filter with not_contains operator', async () => {
        const commands: Command[] = [
            createFilterCommand({
                id: 'g1', type: 'group', logicalOperator: 'AND',
                conditions: [
                    { id: 'c1', type: 'condition', field: 'dept', operator: 'not_contains', value: 'Engineering' }
                ]
            })
        ];
        const tree = createTree(commands);
        const res: ExecutionResult = await api.post(MOCK_CONFIG, '/execute', {
            sessionId: 'test', tree, targetNodeId: 'root'
        });

        for (const row of res.rows) {
            expect(row.dept.toLowerCase()).not.toContain('engineering');
        }
    });

    it('should filter with is_empty operator', async () => {
        // Note: Mock data doesn't have empty string values, so expect none to pass
        const commands: Command[] = [
            createFilterCommand({
                id: 'g1', type: 'group', logicalOperator: 'AND',
                conditions: [
                    { id: 'c1', type: 'condition', field: 'name', operator: 'is_empty', value: '' }
                ]
            })
        ];
        const tree = createTree(commands);
        const res: ExecutionResult = await api.post(MOCK_CONFIG, '/execute', {
            sessionId: 'test', tree, targetNodeId: 'root'
        });

        // No empty names in mock data
        expect(res.totalCount).toBe(0);
    });
});

describe('Mock Engine - Nested Filter Groups', () => {
    it('should handle deeply nested AND/OR groups', async () => {
        // (dept = Engineering AND salary > 70000) OR (dept = HR AND salary < 60000)
        const commands: Command[] = [
            createFilterCommand({
                id: 'root', type: 'group', logicalOperator: 'OR',
                conditions: [
                    {
                        id: 'g1', type: 'group', logicalOperator: 'AND',
                        conditions: [
                            { id: 'c1', type: 'condition', field: 'dept', operator: '=', value: 'Engineering' },
                            { id: 'c2', type: 'condition', field: 'salary', operator: '>', value: 70000, dataType: 'number' }
                        ]
                    },
                    {
                        id: 'g2', type: 'group', logicalOperator: 'AND',
                        conditions: [
                            { id: 'c3', type: 'condition', field: 'dept', operator: '=', value: 'HR' },
                            { id: 'c4', type: 'condition', field: 'salary', operator: '<', value: 60000, dataType: 'number' }
                        ]
                    }
                ]
            })
        ];
        const tree = createTree(commands);
        const res: ExecutionResult = await api.post(MOCK_CONFIG, '/execute', {
            sessionId: 'test', tree, targetNodeId: 'root'
        });

        for (const row of res.rows) {
            const isGroupA = row.dept === 'Engineering' && row.salary > 70000;
            const isGroupB = row.dept === 'HR' && row.salary < 60000;
            expect(isGroupA || isGroupB).toBe(true);
        }
    });

    it('should handle triple nested filter groups', async () => {
        // ((A AND B) OR C) AND D
        const commands: Command[] = [
            createFilterCommand({
                id: 'root', type: 'group', logicalOperator: 'AND',
                conditions: [
                    {
                        id: 'level1', type: 'group', logicalOperator: 'OR',
                        conditions: [
                            {
                                id: 'level2', type: 'group', logicalOperator: 'AND',
                                conditions: [
                                    { id: 'a', type: 'condition', field: 'dept', operator: '=', value: 'Engineering' },
                                    { id: 'b', type: 'condition', field: 'salary', operator: '>=', value: 60000, dataType: 'number' }
                                ]
                            },
                            { id: 'c', type: 'condition', field: 'dept', operator: '=', value: 'Sales' }
                        ]
                    },
                    { id: 'd', type: 'condition', field: 'active', operator: 'is_true', value: null }
                ]
            })
        ];
        const tree = createTree(commands);
        const res: ExecutionResult = await api.post(MOCK_CONFIG, '/execute', {
            sessionId: 'test', tree, targetNodeId: 'root'
        });

        for (const row of res.rows) {
            const innerOr = (row.dept === 'Engineering' && row.salary >= 60000) || row.dept === 'Sales';
            expect(innerOr && row.active === true).toBe(true);
        }
    });
});

describe('Mock Engine - Variable in Filter', () => {
    it('should resolve in_variable operator with defined variable', async () => {
        // Define variable with list of departments, then filter by it
        const tree: OperationNode = {
            id: 'root', type: 'operation', operationType: 'process', name: 'Root', enabled: true,
            commands: [
                { id: 'src', type: 'source', config: { mainTable: 'employees.csv' }, order: 0 },
                {
                    id: 'def', type: 'define_variable', order: 1,
                    config: { variableName: 'target_depts', variableValue: ['Engineering', 'HR'] }
                },
                {
                    id: 'filt', type: 'filter', order: 2,
                    config: {
                        filterRoot: {
                            id: 'g1', type: 'group', logicalOperator: 'AND',
                            conditions: [
                                { id: 'c1', type: 'condition', field: 'dept', operator: 'in_variable', value: 'target_depts' }
                            ]
                        }
                    }
                }
            ],
            children: []
        };

        const res: ExecutionResult = await api.post(MOCK_CONFIG, '/execute', {
            sessionId: 'test', tree, targetNodeId: 'root'
        });

        expect(res.totalCount).toBeGreaterThan(0);
        for (const row of res.rows) {
            expect(['Engineering', 'HR']).toContain(row.dept);
        }
    });

    it('should resolve not_in_variable operator', async () => {
        const tree: OperationNode = {
            id: 'root', type: 'operation', operationType: 'process', name: 'Root', enabled: true,
            commands: [
                { id: 'src', type: 'source', config: { mainTable: 'employees.csv' }, order: 0 },
                {
                    id: 'def', type: 'define_variable', order: 1,
                    config: { variableName: 'exclude_depts', variableValue: ['Sales', 'Marketing'] }
                },
                {
                    id: 'filt', type: 'filter', order: 2,
                    config: {
                        filterRoot: {
                            id: 'g1', type: 'group', logicalOperator: 'AND',
                            conditions: [
                                { id: 'c1', type: 'condition', field: 'dept', operator: 'not_in_variable', value: 'exclude_depts' }
                            ]
                        }
                    }
                }
            ],
            children: []
        };

        const res: ExecutionResult = await api.post(MOCK_CONFIG, '/execute', {
            sessionId: 'test', tree, targetNodeId: 'root'
        });

        for (const row of res.rows) {
            expect(['Sales', 'Marketing']).not.toContain(row.dept);
        }
    });

    it('should resolve variable placeholder syntax {var_name}', async () => {
        const tree: OperationNode = {
            id: 'root', type: 'operation', operationType: 'process', name: 'Root', enabled: true,
            commands: [
                { id: 'src', type: 'source', config: { mainTable: 'employees.csv' }, order: 0 },
                {
                    id: 'def', type: 'define_variable', order: 1,
                    config: { variableName: 'min_salary', variableValue: '70000' }
                },
                {
                    id: 'filt', type: 'filter', order: 2,
                    config: {
                        filterRoot: {
                            id: 'g1', type: 'group', logicalOperator: 'AND',
                            conditions: [
                                { id: 'c1', type: 'condition', field: 'salary', operator: '>', value: '{min_salary}', dataType: 'number' }
                            ]
                        }
                    }
                }
            ],
            children: []
        };

        const res: ExecutionResult = await api.post(MOCK_CONFIG, '/execute', {
            sessionId: 'test', tree, targetNodeId: 'root'
        });

        expect(res.totalCount).toBeGreaterThan(0);
        for (const row of res.rows) {
            expect(row.salary).toBeGreaterThan(70000);
        }
    });
});

describe('Mock Engine - Boolean Operators', () => {
    it('should filter with is_true operator', async () => {
        const commands: Command[] = [
            createFilterCommand({
                id: 'g1', type: 'group', logicalOperator: 'AND',
                conditions: [
                    { id: 'c1', type: 'condition', field: 'active', operator: 'is_true', value: null }
                ]
            })
        ];
        const tree = createTree(commands);
        const res: ExecutionResult = await api.post(MOCK_CONFIG, '/execute', {
            sessionId: 'test', tree, targetNodeId: 'root'
        });

        for (const row of res.rows) {
            expect(row.active).toBe(true);
        }
    });

    it('should filter with is_false operator', async () => {
        const commands: Command[] = [
            createFilterCommand({
                id: 'g1', type: 'group', logicalOperator: 'AND',
                conditions: [
                    { id: 'c1', type: 'condition', field: 'active', operator: 'is_false', value: null }
                ]
            })
        ];
        const tree = createTree(commands);
        const res: ExecutionResult = await api.post(MOCK_CONFIG, '/execute', {
            sessionId: 'test', tree, targetNodeId: 'root'
        });

        for (const row of res.rows) {
            expect(row.active).toBe(false);
        }
    });
});

describe('Mock Engine - Aggregation', () => {
    it('should count rows in group', async () => {
        const commands: Command[] = [
            {
                id: 'g1', type: 'group', order: 1,
                config: {
                    groupByFields: ['dept'],
                    aggregations: [
                        { field: '*', func: 'count', alias: 'employee_count' }
                    ]
                }
            }
        ];
        const tree = createTree(commands);
        const res: ExecutionResult = await api.post(MOCK_CONFIG, '/execute', {
            sessionId: 'test', tree, targetNodeId: 'root'
        });

        // We have 5 departments in mock data
        expect(res.rows.length).toBe(5);
        for (const row of res.rows) {
            expect(row.employee_count).toBeDefined();
            expect(row.employee_count).toBeGreaterThan(0);
        }
    });

    it('should calculate min and max', async () => {
        const commands: Command[] = [
            {
                id: 'g1', type: 'group', order: 1,
                config: {
                    groupByFields: ['dept'],
                    aggregations: [
                        { field: 'salary', func: 'min', alias: 'min_salary' },
                        { field: 'salary', func: 'max', alias: 'max_salary' }
                    ]
                }
            }
        ];
        const tree = createTree(commands);
        const res: ExecutionResult = await api.post(MOCK_CONFIG, '/execute', {
            sessionId: 'test', tree, targetNodeId: 'root'
        });

        for (const row of res.rows) {
            expect(row.min_salary).toBeDefined();
            expect(row.max_salary).toBeDefined();
            expect(row.max_salary).toBeGreaterThanOrEqual(row.min_salary);
        }
    });

    it('should calculate sum', async () => {
        const commands: Command[] = [
            {
                id: 'g1', type: 'group', order: 1,
                config: {
                    groupByFields: ['dept'],
                    aggregations: [
                        { field: 'salary', func: 'sum', alias: 'total_salary' }
                    ]
                }
            }
        ];
        const tree = createTree(commands);
        const res: ExecutionResult = await api.post(MOCK_CONFIG, '/execute', {
            sessionId: 'test', tree, targetNodeId: 'root'
        });

        for (const row of res.rows) {
            expect(row.total_salary).toBeDefined();
            expect(row.total_salary).toBeGreaterThan(0);
        }
    });
});

describe('Mock Engine - Sequential Operations', () => {
    it('should execute filter then sort', async () => {
        const commands: Command[] = [
            createFilterCommand({
                id: 'g1', type: 'group', logicalOperator: 'AND',
                conditions: [
                    { id: 'c1', type: 'condition', field: 'dept', operator: '=', value: 'Engineering' }
                ]
            }),
            { id: 's1', type: 'sort', order: 2, config: { field: 'salary', ascending: false } }
        ];
        const tree = createTree(commands);
        const res: ExecutionResult = await api.post(MOCK_CONFIG, '/execute', {
            sessionId: 'test', tree, targetNodeId: 'root'
        });

        // All should be Engineering
        for (const row of res.rows) {
            expect(row.dept).toBe('Engineering');
        }
        // Should be sorted descending
        const salaries = res.rows.map(r => r.salary);
        const sorted = [...salaries].sort((a, b) => b - a);
        expect(salaries).toEqual(sorted);
    });

    it('should execute filter then group', async () => {
        const commands: Command[] = [
            createFilterCommand({
                id: 'g1', type: 'group', logicalOperator: 'AND',
                conditions: [
                    { id: 'c1', type: 'condition', field: 'active', operator: 'is_true', value: null }
                ]
            }),
            {
                id: 'agg1', type: 'group', order: 2,
                config: {
                    groupByFields: ['dept'],
                    aggregations: [
                        { field: '*', func: 'count', alias: 'active_count' }
                    ]
                }
            }
        ];
        const tree = createTree(commands);
        const res: ExecutionResult = await api.post(MOCK_CONFIG, '/execute', {
            sessionId: 'test', tree, targetNodeId: 'root'
        });

        // Should have grouped active employees by department
        expect(res.rows.length).toBeLessThanOrEqual(5);
        for (const row of res.rows) {
            expect(row.active_count).toBeGreaterThan(0);
        }
    });
});

describe('Mock Engine - Hierarchical Tree Execution', () => {
    it('should execute child node inheriting parent filters', async () => {
        const tree: OperationNode = {
            id: 'root', type: 'operation', operationType: 'root', name: 'Root', enabled: true,
            commands: [
                { id: 'src', type: 'source', config: { mainTable: 'employees.csv' }, order: 0 },
                createFilterCommand({
                    id: 'g1', type: 'group', logicalOperator: 'AND',
                    conditions: [
                        { id: 'c1', type: 'condition', field: 'active', operator: 'is_true', value: null }
                    ]
                })
            ],
            children: [
                {
                    id: 'child1', type: 'operation', operationType: 'process', name: 'Child', enabled: true,
                    commands: [
                        createFilterCommand({
                            id: 'g2', type: 'group', logicalOperator: 'AND',
                            conditions: [
                                { id: 'c2', type: 'condition', field: 'dept', operator: '=', value: 'Engineering' }
                            ]
                        }, 1)
                    ],
                    children: []
                }
            ]
        };

        // Execute at root - only active employees
        const resRoot: ExecutionResult = await api.post(MOCK_CONFIG, '/execute', {
            sessionId: 'test', tree, targetNodeId: 'root'
        });
        for (const row of resRoot.rows) {
            expect(row.active).toBe(true);
        }

        // Execute at child - active AND Engineering
        const resChild: ExecutionResult = await api.post(MOCK_CONFIG, '/execute', {
            sessionId: 'test', tree, targetNodeId: 'child1'
        });
        for (const row of resChild.rows) {
            expect(row.active).toBe(true);
            expect(row.dept).toBe('Engineering');
        }
        expect(resChild.totalCount).toBeLessThan(resRoot.totalCount);
    });
});
