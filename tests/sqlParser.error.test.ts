import { describe, it, expect } from 'vitest';
import { parseSqlToCommands } from '../components/command-editor/sqlParser';
import { Command } from '../types';

const resolveDataSource = (name: string) => name;

const getFilterCommand = (commands: Command[]) => commands.find(c => c.type === 'filter');

const expectParseError = (sql: string, message: string) => {
  const res = parseSqlToCommands(sql, resolveDataSource);
  expect(res.error).toBe(message);
  expect(res.commands).toHaveLength(0);
};

const expectWhereWarning = (sql: string) => {
  const res = parseSqlToCommands(sql, resolveDataSource);
  expect(res.error).toBeNull();
  expect(res.warnings.length).toBeGreaterThan(0);
  expect(getFilterCommand(res.commands)).toBeUndefined();
};

describe('parseSqlToCommands (SQL Builder) invalid SQL', () => {
  it('returns error on empty SQL', () => {
    expectParseError('', 'SQL is empty.');
  });

  it('returns error on non-select SQL', () => {
    expectParseError('update t set a = 1', 'Only simple SELECT ... FROM ... queries are supported.');
  });

  it('returns error when FROM is missing', () => {
    expectParseError('select id, name', 'Only simple SELECT ... FROM ... queries are supported.');
  });

  it('returns error when table is missing after FROM', () => {
    expectParseError('select * from', 'Only simple SELECT ... FROM ... queries are supported.');
  });

  it('warns on unbalanced parentheses in WHERE', () => {
    expectWhereWarning('select * from t where (a = 1');
  });

  it('warns on dangling AND in WHERE', () => {
    expectWhereWarning('select * from t where a = 1 and');
  });

  it('warns on dangling OR in WHERE', () => {
    expectWhereWarning('select * from t where a = 1 or');
  });

  it('warns on leading logical operator in WHERE', () => {
    expectWhereWarning('select * from t where and a = 1');
  });

  it('warns on extra closing parenthesis', () => {
    expectWhereWarning('select * from t where (a = 1))');
  });

  it('warns on malformed operator tokens', () => {
    expectWhereWarning('select * from t where a @@ 1');
  });

  it('warns on empty parentheses group', () => {
    expectWhereWarning('select * from t where ()');
  });
});
