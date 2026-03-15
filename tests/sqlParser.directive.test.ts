import { describe, it, expect } from 'vitest';
import { parseSqlToCommands } from '../components/command-editor/sqlParser';

const resolveDataSource = (name: string) => `link_${name}`;

describe('parseSqlToCommands (DMB_COMMAND metadata)', () => {
  it('parses single directive command and resolves dataSource', () => {
    const sql = `-- DMB_COMMAND: {"version":1,"type":"filter","config":{"dataSource":"orders","filterRoot":{"id":"g1","type":"group","logicalOperator":"AND","conditions":[{"id":"c1","type":"condition","field":"status","operator":"=","value":"PAID"}]}}}
SELECT * FROM orders WHERE status = 'PAID'`;

    const res = parseSqlToCommands(sql, resolveDataSource);
    expect(res.error).toBeNull();
    expect(res.commands).toHaveLength(1);
    expect(res.commands[0].type).toBe('filter');
    expect(res.commands[0].config.dataSource).toBe('link_orders');
    expect(res.commands[0].config.filterRoot?.conditions?.length).toBe(1);
  });

  it('parses multiple directives and keeps order', () => {
    const sql = [
      '-- DMB_COMMAND: {"version":1,"type":"join","config":{"dataSource":"orders","joinTargetType":"table","joinTable":"customers","joinType":"LEFT","on":"orders.customer_id = customers.customer_id"}}',
      "SELECT t1.*, t2.* FROM orders t1 LEFT JOIN customers t2 ON orders.customer_id = customers.customer_id",
      '-- DMB_COMMAND: {"version":1,"type":"group","config":{"dataSource":"orders","groupByFields":["customer_id"],"aggregations":[{"func":"sum","field":"amount","alias":"sum_amount"}],"outputTableName":"orders_by_customer"}}',
      'SELECT customer_id, SUM(amount) AS sum_amount FROM orders GROUP BY customer_id',
      '-- DMB_COMMAND: {"version":1,"type":"save","config":{"dataSource":"orders","field":"customer_id","distinct":true,"value":"customer_ids"}}'
    ].join('\n');

    const res = parseSqlToCommands(sql, resolveDataSource);
    expect(res.error).toBeNull();
    expect(res.commands.map(c => c.type)).toEqual(['join', 'group', 'save']);
    expect(res.commands[0].order).toBe(1);
    expect(res.commands[1].order).toBe(2);
    expect(res.commands[2].order).toBe(3);
    expect(res.commands[0].config.dataSource).toBe('link_orders');
    expect(res.commands[0].config.joinTable).toBe('link_customers');
  });

  it('supports non-sql commands via directive only', () => {
    const sql = [
      '-- DMB_COMMAND: {"version":1,"type":"define_variable","config":{"variableName":"status_filter","variableType":"text","variableValue":"PAID"}}',
      '-- DMB_COMMAND: {"version":1,"type":"multi_table","config":{"dataSource":"orders","subTables":[{"id":"sub_1","table":"order_items","on":"main.order_id = sub.order_id","label":"Items"}]}}'
    ].join('\n');

    const res = parseSqlToCommands(sql, resolveDataSource);
    expect(res.error).toBeNull();
    expect(res.commands.map(c => c.type)).toEqual(['define_variable', 'multi_table']);
    expect(res.commands[1].config.dataSource).toBe('link_orders');
  });
});

