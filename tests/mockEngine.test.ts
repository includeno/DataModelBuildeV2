
import { describe, it, expect } from 'vitest';
import { api } from '../utils/api';
import { OperationNode, Command, ExecutionResult } from '../types';

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
