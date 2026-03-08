import { describe, it, expect } from 'vitest';
import { parseSqlToCommands } from '../components/command-editor/sqlParser';
import { Command, FilterCondition } from '../types';

const resolveDataSource = (name: string) => name;

const getSort = (commands: Command[]) => commands.find(c => c.type === 'sort');
const getView = (commands: Command[]) => commands.find(c => c.type === 'view');
const getFilterConditions = (commands: Command[]) => {
  const filter = commands.find(c => c.type === 'filter');
  if (!filter) return [];
  return (filter.config.filterRoot?.conditions || []) as FilterCondition[];
};

describe('parseSqlToCommands (additional coverage)', () => {
  it('parses FROM with schema-qualified table', () => {
    const res = parseSqlToCommands('select * from public.users', resolveDataSource);
    expect(res.error).toBeNull();
    const view = getView(res.commands);
    expect(view?.config.dataSource).toBe('public.users');
  });

  it('ignores FROM alias and uses base table name', () => {
    const res = parseSqlToCommands('select * from customers c', resolveDataSource);
    expect(res.error).toBeNull();
    const view = getView(res.commands);
    expect(view?.config.dataSource).toBe('customers');
  });

  it('parses SELECT DISTINCT and marks fields distinct', () => {
    const res = parseSqlToCommands('select distinct id, name from t', resolveDataSource);
    expect(res.error).toBeNull();
    const view = getView(res.commands);
    expect(view?.config.viewFields).toEqual([
      { field: 'id', distinct: true },
      { field: 'name', distinct: true }
    ]);
  });

  it('warns on select expressions with operators', () => {
    const res = parseSqlToCommands('select id, amount + 1 from t', resolveDataSource);
    expect(res.error).toBeNull();
    expect(res.warnings.length).toBeGreaterThan(0);
    const view = getView(res.commands);
    expect(view?.config.viewFields).toEqual([{ field: 'id', distinct: undefined }]);
  });

  it('warns on invalid LIMIT clause', () => {
    const res = parseSqlToCommands('select * from t limit -1', resolveDataSource);
    expect(res.error).toBeNull();
    expect(res.warnings.length).toBeGreaterThan(0);
    const view = getView(res.commands);
    expect(view?.config.viewLimit).toBeUndefined();
  });

  it('warns on unsupported trailing GROUP BY clause', () => {
    const res = parseSqlToCommands('select * from t where a = 1 group by b', resolveDataSource);
    expect(res.error).toBeNull();
    expect(res.warnings.length).toBeGreaterThan(0);
  });

  it('warns on BETWEEN in WHERE', () => {
    const res = parseSqlToCommands('select * from t where a between 1 and 2', resolveDataSource);
    expect(res.error).toBeNull();
    expect(res.warnings.length).toBeGreaterThan(0);
  });

  it('warns on IS TRUE / IS FALSE', () => {
    const res = parseSqlToCommands('select * from t where active is true', resolveDataSource);
    expect(res.error).toBeNull();
    expect(res.warnings.length).toBeGreaterThan(0);
  });

  it('parses mixed IN list values including NULL', () => {
    const res = parseSqlToCommands("select * from t where id in (1, '2', null)", resolveDataSource);
    expect(res.error).toBeNull();
    const [cond] = getFilterConditions(res.commands);
    expect(cond).toMatchObject({ field: 'id', operator: 'in_list', value: [1, '2', null] });
  });

  it('parses null comparison values', () => {
    const res = parseSqlToCommands('select * from t where deleted_at = null', resolveDataSource);
    expect(res.error).toBeNull();
    const [cond] = getFilterConditions(res.commands);
    expect(cond).toMatchObject({ field: 'deleted_at', operator: '=', value: null });
  });

  it('warns on NOT unary expressions', () => {
    const res = parseSqlToCommands('select * from t where not (a = 1)', resolveDataSource);
    expect(res.error).toBeNull();
    expect(res.warnings.length).toBeGreaterThan(0);
  });

  it('warns on invalid tokens like && or ===', () => {
    const res = parseSqlToCommands('select * from t where a && b === c', resolveDataSource);
    expect(res.error).toBeNull();
    expect(res.warnings.length).toBeGreaterThan(0);
  });

  it('drops constant predicate 1=1 inside nested groups', () => {
    const sql = "select order_id, status from orders where ((1=1) and (order_id != 'O1001'))";
    const res = parseSqlToCommands(sql, resolveDataSource);
    expect(res.error).toBeNull();
    const filter = res.commands.find(c => c.type === 'filter');
    expect(filter).toBeTruthy();
    const conditions = filter?.config.filterRoot?.conditions || [];
    expect(conditions.length).toBe(1);
  });

  it('reduces OR with constant TRUE to no filter', () => {
    const sql = "select * from t where (1=1) or (a = 1)";
    const res = parseSqlToCommands(sql, resolveDataSource);
    expect(res.error).toBeNull();
    expect(res.commands.some(c => c.type === 'filter')).toBe(false);
  });

  it('reduces constant FALSE to always-false filter', () => {
    const sql = "select * from t where 1=0";
    const res = parseSqlToCommands(sql, resolveDataSource);
    expect(res.error).toBeNull();
    const filter = res.commands.find(c => c.type === 'filter');
    expect(filter).toBeTruthy();
    const cond = filter?.config.filterRoot?.conditions?.[0] as any;
    expect(cond?.operator).toBe('always_false');
  });

  it('parses quoted unicode identifiers', () => {
    const res = parseSqlToCommands('select * from "客户" where "名" = \'a\'', resolveDataSource);
    expect(res.error).toBeNull();
    const [cond] = getFilterConditions(res.commands);
    expect(cond).toMatchObject({ field: '名', operator: '=', value: 'a' });
  });

  it('parses multiple trailing semicolons', () => {
    const res = parseSqlToCommands('select * from t;;', resolveDataSource);
    expect(res.error).toBeNull();
    const view = getView(res.commands);
    expect(view?.config.dataSource).toBe('t');
  });

  it('uses first ORDER BY field when multiple provided', () => {
    const res = parseSqlToCommands('select * from t order by a asc, b desc', resolveDataSource);
    expect(res.error).toBeNull();
    expect(res.warnings.length).toBeGreaterThan(0);
    const sort = getSort(res.commands);
    expect(sort?.config).toMatchObject({ field: 'a', ascending: true });
  });
});
