import { describe, it, expect } from 'vitest';
import { parseSqlToCommands } from '../components/command-editor/sqlParser';
import { Command, FilterCondition, FilterGroup } from '../types';

const resolveDataSource = (name: string) => name;

const getFilterRoot = (commands: Command[]): FilterGroup => {
  const filter = commands.find(c => c.type === 'filter');
  if (!filter) throw new Error('Filter command not found');
  return filter.config.filterRoot as FilterGroup;
};

const getViewCommand = (commands: Command[]) => commands.find(c => c.type === 'view');
const getSortCommand = (commands: Command[]) => commands.find(c => c.type === 'sort');

describe('parseSqlToCommands (SQL Builder)', () => {
  it('parses IS NULL and IS NOT NULL', () => {
    const sql = "select * from abcd where name is null and email is not null";
    const res = parseSqlToCommands(sql, resolveDataSource);
    expect(res.error).toBeNull();
    const root = getFilterRoot(res.commands);
    expect(root.logicalOperator).toBe('AND');
    const [c1, c2] = root.conditions as FilterCondition[];
    expect(c1).toMatchObject({ field: 'name', operator: 'is_null' });
    expect(c2).toMatchObject({ field: 'email', operator: 'is_not_null' });
  });

  it('parses IN / NOT IN lists including IS IN variant', () => {
    const sql = "select * from abcd where age in (18, 28) and id not in (1,2,3) and tier is in ('a','b')";
    const res = parseSqlToCommands(sql, resolveDataSource);
    expect(res.error).toBeNull();
    const root = getFilterRoot(res.commands);
    expect(root.logicalOperator).toBe('AND');
    const [c1, c2, c3] = root.conditions as FilterCondition[];
    expect(c1).toMatchObject({ field: 'age', operator: 'in_list', value: [18, 28] });
    expect(c2).toMatchObject({ field: 'id', operator: 'not_in_list', value: [1, 2, 3] });
    expect(c3).toMatchObject({ field: 'tier', operator: 'in_list', value: ['a', 'b'] });
  });

  it('parses LIKE patterns (contains/starts/ends) and NOT LIKE', () => {
    const sql = "select * from t where name like '%foo%' and tag like 'bar%' and note like '%baz' and code not like '%x%'";
    const res = parseSqlToCommands(sql, resolveDataSource);
    expect(res.error).toBeNull();
    const root = getFilterRoot(res.commands);
    const [c1, c2, c3, c4] = root.conditions as FilterCondition[];
    expect(c1).toMatchObject({ field: 'name', operator: 'contains', value: 'foo' });
    expect(c2).toMatchObject({ field: 'tag', operator: 'starts_with', value: 'bar' });
    expect(c3).toMatchObject({ field: 'note', operator: 'ends_with', value: 'baz' });
    expect(c4).toMatchObject({ field: 'code', operator: 'not_contains', value: 'x' });
  });

  it('parses comparison operators', () => {
    const sql = "select * from t where a = 1 and b != 2 and c <> 3 and d > 4 and e >= 5 and f < 6 and g <= 7";
    const res = parseSqlToCommands(sql, resolveDataSource);
    expect(res.error).toBeNull();
    const root = getFilterRoot(res.commands);
    const ops = (root.conditions as FilterCondition[]).map(c => [c.field, c.operator]);
    expect(ops).toEqual([
      ['a', '='],
      ['b', '!='],
      ['c', '!='],
      ['d', '>'],
      ['e', '>='],
      ['f', '<'],
      ['g', '<=']
    ]);
  });

  it('respects parentheses and AND/OR precedence', () => {
    const sql = "select * from t where a = 1 or b = 2 and c = 3";
    const res = parseSqlToCommands(sql, resolveDataSource);
    expect(res.error).toBeNull();
    const root = getFilterRoot(res.commands);
    expect(root.logicalOperator).toBe('OR');
    const [left, right] = root.conditions as (FilterCondition | FilterGroup)[];
    expect((left as FilterCondition).field).toBe('a');
    expect((right as FilterGroup).logicalOperator).toBe('AND');
    expect(((right as FilterGroup).conditions as FilterCondition[]).map(c => c.field)).toEqual(['b', 'c']);
  });

  it('parses nested groups with parentheses', () => {
    const sql = "select * from t where (a = 1 or b = 2) and (c = 3 or d = 4)";
    const res = parseSqlToCommands(sql, resolveDataSource);
    expect(res.error).toBeNull();
    const root = getFilterRoot(res.commands);
    expect(root.logicalOperator).toBe('AND');
    const [g1, g2] = root.conditions as FilterGroup[];
    expect(g1.logicalOperator).toBe('OR');
    expect(g2.logicalOperator).toBe('OR');
    expect((g1.conditions as FilterCondition[]).map(c => c.field)).toEqual(['a', 'b']);
    expect((g2.conditions as FilterCondition[]).map(c => c.field)).toEqual(['c', 'd']);
  });

  it('parses deep nested parentheses (4 levels)', () => {
    const sql = "select * from t where ((((a = 1)))) and (((b = 2 or (c = 3 and (d = 4 or (e = 5 and f = 6))))))";
    const res = parseSqlToCommands(sql, resolveDataSource);
    expect(res.error).toBeNull();
    const root = getFilterRoot(res.commands);
    expect(root.logicalOperator).toBe('AND');
    const [left, right] = root.conditions as (FilterCondition | FilterGroup)[];
    expect((left as FilterCondition).field).toBe('a');
    const rightGroup = right as FilterGroup;
    expect(rightGroup.logicalOperator).toBe('OR');
    const rightFields = rightGroup.conditions.map(c => (c as FilterCondition | FilterGroup).type === 'condition' ? (c as FilterCondition).field : 'group');
    expect(rightFields[0]).toBe('b');
    expect(rightFields[1]).toBe('group');
  });

  it('parses view fields, order by, and limit', () => {
    const sql = "select name, age from users order by age desc limit 10";
    const res = parseSqlToCommands(sql, resolveDataSource);
    expect(res.error).toBeNull();
    const sort = getSortCommand(res.commands);
    const view = getViewCommand(res.commands);
    expect(sort?.config).toMatchObject({ field: 'age', ascending: false });
    expect(view?.config.viewFields).toEqual([{ field: 'name' }, { field: 'age' }]);
    expect(view?.config.viewLimit).toBe(10);
  });

  it('parses simple SELECT * without filters', () => {
    const sql = "select * from simple_table";
    const res = parseSqlToCommands(sql, resolveDataSource);
    expect(res.error).toBeNull();
    const view = getViewCommand(res.commands);
    expect(view).toBeTruthy();
    expect(view?.config.dataSource).toBe('simple_table');
  });

  it('parses complex where with IS NULL and IN', () => {
    const sql = "select * from abcd where name is null and age is in (18,28)";
    const res = parseSqlToCommands(sql, resolveDataSource);
    expect(res.error).toBeNull();
    const root = getFilterRoot(res.commands);
    const [c1, c2] = root.conditions as FilterCondition[];
    expect(c1).toMatchObject({ field: 'name', operator: 'is_null' });
    expect(c2).toMatchObject({ field: 'age', operator: 'in_list', value: [18, 28] });
  });

  it('parses 6-level nested groups with mixed AND/OR and keywords', () => {
    const sql = "select * from t where ((((((a = 1)))) and (((b = 2 or c is null) and (d in (1,2) or e not in (3,4))))) and (f like '%z%' and g not like '%q%'))";
    const res = parseSqlToCommands(sql, resolveDataSource);
    expect(res.error).toBeNull();
    const root = getFilterRoot(res.commands);
    expect(root.logicalOperator).toBe('AND');
  });

  it('parses 7-level nested groups with mixed AND/OR', () => {
    const sql = "select * from t where (((((((a = 1) or (b = 2 and (c = 3 or (d = 4 and (e = 5 or (f = 6)))))))))))";
    const res = parseSqlToCommands(sql, resolveDataSource);
    expect(res.error).toBeNull();
    const root = getFilterRoot(res.commands);
    expect(root.logicalOperator).toBe('OR');
  });

  it('parses 8-level nested groups with OR root', () => {
    const sql = "select * from t where ((((((((a = 1)))))))) or ((((((((b = 2))))))))";
    const res = parseSqlToCommands(sql, resolveDataSource);
    expect(res.error).toBeNull();
    const root = getFilterRoot(res.commands);
    expect(root.logicalOperator).toBe('OR');
    expect((root.conditions as FilterCondition[]).map(c => c.field)).toEqual(['a', 'b']);
  });

  it('parses mix of comparisons, IN, IS NULL, LIKE with deep nesting', () => {
    const sql = "select * from t where ((((a >= 1 and b <= 2) or (c <> 3 and d != 4)) and (e is not null)) or ((f in (1,2,3) and g not in (4,5)) and (h like 'ab%' or i like '%yz')))";
    const res = parseSqlToCommands(sql, resolveDataSource);
    expect(res.error).toBeNull();
    const root = getFilterRoot(res.commands);
    expect(root.logicalOperator).toBe('OR');
  });

  it('parses not like with nested groups', () => {
    const sql = "select * from t where ((a not like '%x%') and ((b like '%y%') or (c like 'z%')))";
    const res = parseSqlToCommands(sql, resolveDataSource);
    expect(res.error).toBeNull();
    const root = getFilterRoot(res.commands);
    expect(root.logicalOperator).toBe('AND');
  });

  it('parses complex precedence with parentheses and IN lists', () => {
    const sql = "select * from t where (a in (1,2) or b in (3,4)) and (c = 5 or (d = 6 and (e in (7,8) or f in (9,10))))";
    const res = parseSqlToCommands(sql, resolveDataSource);
    expect(res.error).toBeNull();
    const root = getFilterRoot(res.commands);
    expect(root.logicalOperator).toBe('AND');
  });

  it('parses deep mixed keywords with is in / is null / not in', () => {
    const sql = "select * from t where (((a is in (1,2) and b is null) or (c is not null and d not in (3,4))) and (e like '%x%' or f not like '%y%'))";
    const res = parseSqlToCommands(sql, resolveDataSource);
    expect(res.error).toBeNull();
    const root = getFilterRoot(res.commands);
    expect(root.logicalOperator).toBe('AND');
  });

  it('parses nested groups with multiple OR chains', () => {
    const sql = "select * from t where (((a = 1 or b = 2 or c = 3) and (d = 4 or e = 5)) or (f = 6 and (g = 7 or (h = 8 and i = 9))))";
    const res = parseSqlToCommands(sql, resolveDataSource);
    expect(res.error).toBeNull();
    const root = getFilterRoot(res.commands);
    expect(root.logicalOperator).toBe('OR');
  });

  it('parses deep nested groups with numeric and string literals', () => {
    const sql = "select * from t where (((((a = 1 and b = 'x')) or (c = 2 and d = 'y')) and (e = 3 or f = 'z')) and (g >= 4 and h <= 5))";
    const res = parseSqlToCommands(sql, resolveDataSource);
    expect(res.error).toBeNull();
    const root = getFilterRoot(res.commands);
    expect(root.logicalOperator).toBe('AND');
  });

  it('parses complex query with select fields, order by, limit, and nested where', () => {
    const sql = "select id, name from t where ((a = 1 and (b = 2 or c = 3)) or (d is null and e in (4,5))) order by id asc limit 5";
    const res = parseSqlToCommands(sql, resolveDataSource);
    expect(res.error).toBeNull();
    const sort = getSortCommand(res.commands);
    const view = getViewCommand(res.commands);
    expect(sort?.config).toMatchObject({ field: 'id', ascending: true });
    expect(view?.config.viewFields).toEqual([{ field: 'id' }, { field: 'name' }]);
    expect(view?.config.viewLimit).toBe(5);
  });
});
