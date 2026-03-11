import { describe, it, expect } from 'vitest';
import { getSourceLabel, renderSqlCommandSummary } from '../components/command-editor/helpers';
import { Command } from '../types';

describe('renderSqlCommandSummary', () => {
  it('shows Limit 0 for view command', () => {
    const cmd: Command = {
      id: 'cmd_view',
      type: 'view',
      order: 1,
      config: {
        dataSource: 'users',
        viewFields: [{ field: 'id' }],
        viewLimit: 0
      }
    };

    expect(renderSqlCommandSummary(cmd)).toContain('Limit 0');
  });

  it('does not show limit when viewLimit is undefined', () => {
    const cmd: Command = {
      id: 'cmd_view_2',
      type: 'view',
      order: 1,
      config: {
        dataSource: 'users',
        viewFields: [{ field: 'id' }]
      }
    };

    expect(renderSqlCommandSummary(cmd)).toBe('View id');
  });
});

describe('getSourceLabel', () => {
  it('hides internal source ids when alias mapping is missing', () => {
    expect(getSourceLabel([], 'link_abc123')).toBe('');
  });

  it('returns alias when source is known', () => {
    expect(getSourceLabel([
      { alias: 'orders_alias', nodeName: 'setup', id: 'setup_1', sourceTable: 'orders', linkId: 'link_orders' }
    ], 'link_orders')).toBe('orders_alias');
  });
});
