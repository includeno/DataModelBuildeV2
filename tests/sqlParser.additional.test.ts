import { describe, it, expect } from 'vitest';
import { parseSqlToCommands } from '../components/command-editor/sqlParser';
import { Command } from '../types';

const resolveDataSource = (name: string) => name;

const hasType = (commands: Command[], type: string) => commands.some(c => c.type === type);

const parse = (sql: string) => parseSqlToCommands(sql, resolveDataSource);

const okCases: Array<{ name: string; sql: string; filter?: boolean; sort?: boolean; view?: boolean }> = [
  { name: 'select star no where', sql: 'select * from t', filter: false, sort: false, view: true },
  { name: 'select fields creates view', sql: 'select a, b from t', view: true },
  { name: 'select field with limit', sql: 'select a from t limit 10', view: true },
  { name: 'select star with limit', sql: 'select * from t limit 5', view: true },
  { name: 'order by asc', sql: 'select * from t order by a', sort: true },
  { name: 'order by desc with limit', sql: 'select * from t order by a desc limit 5', sort: true, view: true },
  { name: 'where equals', sql: 'select * from t where a = 1', filter: true },
  { name: 'where and', sql: 'select * from t where a = 1 and b = 2', filter: true },
  { name: 'where or', sql: 'select * from t where a = 1 or b = 2', filter: true },
  { name: 'where mixed groups', sql: 'select * from t where (a = 1 or b = 2) and c = 3', filter: true },
  { name: 'like contains', sql: "select * from t where name like '%foo%'", filter: true },
  { name: 'not like', sql: "select * from t where name not like '%foo%'", filter: true },
  { name: 'is null', sql: 'select * from t where name is null', filter: true },
  { name: 'is not null', sql: 'select * from t where name is not null', filter: true },
  { name: 'in list', sql: 'select * from t where age in (1,2,3)', filter: true },
  { name: 'not in list', sql: 'select * from t where age not in (1,2,3)', filter: true },
  { name: 'is in list', sql: 'select * from t where age is in (1,2,3)', filter: true },
  { name: 'boolean equality', sql: 'select * from t where active = true', filter: true },
  { name: 'qualified field', sql: 'select * from t where t.amount >= 10', filter: true },
  { name: 'not equals', sql: 'select * from t where a != 1', filter: true }
];

const warningCases: Array<{ name: string; sql: string; expectFilter?: boolean }> = [
  { name: 'missing closing paren', sql: 'select * from t where (a = 1' },
  { name: 'extra closing paren', sql: 'select * from t where a = 1)' },
  { name: 'dangling and', sql: 'select * from t where a = 1 and' },
  { name: 'dangling or', sql: 'select * from t where a = 1 or' },
  { name: 'leading and', sql: 'select * from t where and a = 1' },
  { name: 'empty group', sql: 'select * from t where ()' },
  { name: 'invalid operator', sql: 'select * from t where a @@ 1' },
  { name: 'missing operator between', sql: 'select * from t where a = 1 b = 2' },
  { name: 'double or', sql: 'select * from t where (a = 1) or or (b = 2)' },
  { name: 'order by multiple fields', sql: 'select * from t order by a, b' }
];

const errorCases: Array<{ name: string; sql: string; error: string }> = [
  { name: 'empty', sql: '', error: 'SQL is empty.' },
  { name: 'whitespace only', sql: '   ;  ', error: 'SQL is empty.' },
  { name: 'update', sql: 'update t set a = 1', error: 'Only simple SELECT ... FROM ... queries are supported.' },
  { name: 'delete', sql: 'delete from t', error: 'Only simple SELECT ... FROM ... queries are supported.' },
  { name: 'insert', sql: "insert into t values (1)", error: 'Only simple SELECT ... FROM ... queries are supported.' },
  { name: 'select no from', sql: 'select a, b', error: 'Only simple SELECT ... FROM ... queries are supported.' },
  { name: 'select star missing table', sql: 'select * from', error: 'Only simple SELECT ... FROM ... queries are supported.' },
  { name: 'select missing from keyword', sql: 'select * t', error: 'Only simple SELECT ... FROM ... queries are supported.' },
  { name: 'select only keyword', sql: 'select', error: 'Only simple SELECT ... FROM ... queries are supported.' },
  { name: 'select where no from', sql: 'select a where a = 1', error: 'Only simple SELECT ... FROM ... queries are supported.' }
];

describe('parseSqlToCommands (SQL Builder) additional cases', () => {
  it.each(okCases)('ok: $name', ({ sql, filter, sort, view }) => {
    const res = parse(sql);
    expect(res.error).toBeNull();
    if (filter !== undefined) expect(hasType(res.commands, 'filter')).toBe(filter);
    if (sort !== undefined) expect(hasType(res.commands, 'sort')).toBe(sort);
    if (view !== undefined) expect(hasType(res.commands, 'view')).toBe(view);
  });

  it.each(warningCases)('warning: $name', ({ sql }) => {
    const res = parse(sql);
    expect(res.error).toBeNull();
    expect(res.warnings.length).toBeGreaterThan(0);
  });

  it.each(errorCases)('error: $name', ({ sql, error }) => {
    const res = parse(sql);
    expect(res.error).toBe(error);
    expect(res.commands).toHaveLength(0);
  });
});
