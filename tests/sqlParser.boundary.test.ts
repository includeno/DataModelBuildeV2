import { describe, it, expect } from 'vitest';
import { parseSqlToCommands } from '../components/command-editor/sqlParser';
import { Command, FilterCondition } from '../types';

const resolveDataSource = (name: string) => name;

const getFilterConditions = (commands: Command[]) => {
  const filter = commands.find(c => c.type === 'filter');
  if (!filter) return [];
  return (filter.config.filterRoot?.conditions || []) as FilterCondition[];
};

const getView = (commands: Command[]) => commands.find(c => c.type === 'view');
const getSort = (commands: Command[]) => commands.find(c => c.type === 'sort');

describe('parseSqlToCommands (boundary cases)', () => {
  it('handles uppercase keywords and trailing semicolon', () => {
    const sql = 'SELECT * FROM T WHERE A = 1 ORDER BY A DESC LIMIT 2;';
    const res = parseSqlToCommands(sql, resolveDataSource);
    expect(res.error).toBeNull();
    expect(getSort(res.commands)?.config).toMatchObject({ field: 'A', ascending: false });
    expect(getView(res.commands)?.config.viewLimit).toBe(2);
  });

  it('parses multiline SQL with extra whitespace', () => {
    const sql = `
      select
        id, name
      from
        users
      where
        name like '%Chen%'
      order by id
      limit 1
    `;
    const res = parseSqlToCommands(sql, resolveDataSource);
    expect(res.error).toBeNull();
    expect(getView(res.commands)?.config.viewFields).toEqual([{ field: 'id' }, { field: 'name' }]);
  });

  it('parses string literals with escaped quotes', () => {
    const sql = "select * from t where name = 'A''B' and note = 'C''D'";
    const res = parseSqlToCommands(sql, resolveDataSource);
    expect(res.error).toBeNull();
    const [c1, c2] = getFilterConditions(res.commands);
    expect(c1).toMatchObject({ field: 'name', operator: '=', value: "A'B" });
    expect(c2).toMatchObject({ field: 'note', operator: '=', value: "C'D" });
  });

  it('parses aggregate projections into group metrics', () => {
    const sql = 'select id, sum(amount), count(*) from t';
    const res = parseSqlToCommands(sql, resolveDataSource);
    expect(res.error).toBeNull();
    expect(res.warnings).toEqual([]);
    const group = res.commands.find(c => c.type === 'group');
    expect(group?.config.groupByFields).toEqual([]);
    expect(group?.config.aggregations).toEqual([
      { func: 'sum', field: 'amount', alias: 'sum_amount' },
      { func: 'count', field: '*', alias: 'count_all' }
    ]);
    expect(getView(res.commands)?.config.viewFields).toEqual([
      { field: 'id' },
      { field: 'sum_amount' },
      { field: 'count_all' }
    ]);
  });

  it('handles select aliases by stripping AS alias', () => {
    const sql = 'select customer_id as cid, name as customer_name from customers';
    const res = parseSqlToCommands(sql, resolveDataSource);
    expect(res.error).toBeNull();
    expect(getView(res.commands)?.config.viewFields).toEqual([
      { field: 'customer_id' },
      { field: 'name' }
    ]);
  });

  it('parses ORDER BY with table-qualified field', () => {
    const sql = 'select * from t order by t.created_at desc';
    const res = parseSqlToCommands(sql, resolveDataSource);
    expect(res.error).toBeNull();
    expect(getSort(res.commands)?.config).toMatchObject({ field: 'created_at', ascending: false });
  });

  it('parses quoted identifiers in FROM and WHERE', () => {
    const sql = 'select * from "order-items" where "line-item" = 1';
    const res = parseSqlToCommands(sql, resolveDataSource);
    expect(res.error).toBeNull();
    const [cond] = getFilterConditions(res.commands);
    expect(cond).toMatchObject({ field: 'line-item', operator: '=', value: 1 });
    const filter = res.commands.find(c => c.type === 'filter');
    expect(filter?.config.dataSource).toBe('order-items');
  });

  it('parses quoted select fields', () => {
    const sql = 'select "order-id", name from "order"';
    const res = parseSqlToCommands(sql, resolveDataSource);
    expect(res.error).toBeNull();
    expect(getView(res.commands)?.config.viewFields).toEqual([
      { field: 'order-id' },
      { field: 'name' }
    ]);
    expect(getView(res.commands)?.config.dataSource).toBe('order');
  });

  it('supports limit 0 as a boundary value', () => {
    const sql = 'select * from t limit 0';
    const res = parseSqlToCommands(sql, resolveDataSource);
    expect(res.error).toBeNull();
    expect(getView(res.commands)?.config.viewLimit).toBe(0);
  });

  it('parses empty IN list into empty value array', () => {
    const sql = 'select * from t where id in ()';
    const res = parseSqlToCommands(sql, resolveDataSource);
    expect(res.error).toBeNull();
    const [cond] = getFilterConditions(res.commands);
    expect(cond).toMatchObject({ field: 'id', operator: 'in_list', value: [] });
  });

  it('parses NOT LIKE without wildcards as not_contains', () => {
    const sql = "select * from t where name not like 'foo'";
    const res = parseSqlToCommands(sql, resolveDataSource);
    expect(res.error).toBeNull();
    const [cond] = getFilterConditions(res.commands);
    expect(cond).toMatchObject({ field: 'name', operator: 'not_contains', value: 'foo' });
  });

  it('keeps warnings for unsupported trailing tokens in WHERE', () => {
    const sql = 'select * from t where a = 1 foo';
    const res = parseSqlToCommands(sql, resolveDataSource);
    expect(res.error).toBeNull();
    expect(res.warnings.length).toBeGreaterThan(0);
  });
});
