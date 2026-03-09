import { describe, it, expect } from 'vitest';
import { parseSqlToCommands } from '../components/command-editor/sqlParser';
import { Command } from '../types';

const resolveDataSource = (name: string) => name;

const getCommand = (commands: Command[], type: Command['type']) => commands.find(c => c.type === type);

describe('parseSqlToCommands (pure SQL command inference)', () => {
  it('infers JOIN with alias rewrite and keeps projection fields', () => {
    const sql = 'select o.id, c.name from orders o left join customers c on o.customer_id = c.id';
    const res = parseSqlToCommands(sql, resolveDataSource);

    expect(res.error).toBeNull();
    expect(res.warnings).toEqual([]);

    const join = getCommand(res.commands, 'join');
    expect(join?.config).toMatchObject({
      dataSource: 'orders',
      joinTargetType: 'table',
      joinTable: 'customers',
      joinType: 'LEFT',
      on: 'orders.customer_id = customers.id'
    });

    const view = getCommand(res.commands, 'view');
    expect(view?.config.viewFields).toEqual([{ field: 'id' }, { field: 'name' }]);
  });

  it('infers GROUP BY and HAVING into group command', () => {
    const sql = 'select region, sum(amount) as total_amount from sales group by region having total_amount > 100';
    const res = parseSqlToCommands(sql, resolveDataSource);

    expect(res.error).toBeNull();
    expect(res.warnings).toEqual([]);

    const group = getCommand(res.commands, 'group');
    expect(group?.config.groupByFields).toEqual(['region']);
    expect(group?.config.aggregations).toEqual([
      { func: 'sum', field: 'amount', alias: 'total_amount' }
    ]);
    expect(group?.config.havingConditions).toEqual([
      expect.objectContaining({
        metricAlias: 'total_amount',
        operator: '>',
        value: 100
      })
    ]);

    const view = getCommand(res.commands, 'view');
    expect(view?.config.viewFields).toEqual([{ field: 'region' }, { field: 'total_amount' }]);
  });

  it('infers TRANSFORM from expression alias projection', () => {
    const sql = 'select id, amount * 1.1 as taxed_amount from sales';
    const res = parseSqlToCommands(sql, resolveDataSource);

    expect(res.error).toBeNull();
    expect(res.warnings).toEqual([]);

    const transform = getCommand(res.commands, 'transform');
    expect(transform?.config.mappings).toEqual([
      expect.objectContaining({
        mode: 'simple',
        expression: 'amount * 1.1',
        outputField: 'taxed_amount'
      })
    ]);

    const view = getCommand(res.commands, 'view');
    expect(view?.config.viewFields).toEqual([{ field: 'id' }, { field: 'taxed_amount' }]);
  });

  it('infers SAVE from save_ alias convention', () => {
    const sql = 'select distinct user_id as save_active_user from users';
    const res = parseSqlToCommands(sql, resolveDataSource);

    expect(res.error).toBeNull();
    expect(res.warnings).toEqual([]);

    const save = getCommand(res.commands, 'save');
    expect(save?.config).toMatchObject({
      dataSource: 'users',
      field: 'user_id',
      distinct: true,
      value: 'active_user'
    });
  });

  it('keeps inferred command order stable for mixed clauses', () => {
    const sql = `
      select c.region, sum(o.amount) as total_amount
      from orders o
      inner join customers c on o.customer_id = c.id
      where o.status = 'paid'
      group by c.region
      having total_amount >= 1000
      order by total_amount desc
      limit 10
    `;
    const res = parseSqlToCommands(sql, resolveDataSource);

    expect(res.error).toBeNull();
    expect(res.warnings).toEqual([]);
    expect(res.commands.map(c => c.type)).toEqual(['join', 'filter', 'group', 'sort', 'view']);

    const sort = getCommand(res.commands, 'sort');
    expect(sort?.config).toMatchObject({ field: 'total_amount', ascending: false });

    const view = getCommand(res.commands, 'view');
    expect(view?.config.viewLimit).toBe(10);
    expect(view?.config.viewFields).toEqual([{ field: 'region' }, { field: 'total_amount' }]);
  });
});
