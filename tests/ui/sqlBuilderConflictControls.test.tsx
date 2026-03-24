import React from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { act } from 'react';
import { createRoot, Root } from 'react-dom/client';

import { SqlBuilderModal } from '../../components/command-editor/SqlBuilderModal';
import type { Command } from '../../types';

const flush = () => new Promise((resolve) => setTimeout(resolve, 0));

const datasets = [
  { name: 'orders', fields: ['id', 'customer_id', 'amount', 'created_at', 'status', 'region'], totalCount: 3 },
  { name: 'customers', fields: ['id', 'region'], totalCount: 3 },
] as any[];

const availableSourceAliases = [
  { alias: 'orders', nodeName: 'setup', id: 'setup', sourceTable: 'orders', linkId: 'link_orders' },
  { alias: 'customers', nodeName: 'setup', id: 'setup', sourceTable: 'customers', linkId: 'link_customers' },
] as any[];

describe('SqlBuilderModal conflict controls', () => {
  let container: HTMLDivElement;
  let root: Root;

  beforeEach(() => {
    container = document.createElement('div');
    document.body.appendChild(container);
    root = createRoot(container);
  });

  afterEach(() => {
    root.unmount();
    container.remove();
    vi.restoreAllMocks();
  });

  it('prunes commands against existing ones and re-parses when compare mode is disabled', async () => {
    const onUpdateCommands = vi.fn();
    const onParse = vi.fn();

    const commands: Command[] = [
      {
        id: 'cmd_filter',
        type: 'filter',
        order: 1,
        config: {
          dataSource: 'link_orders',
          filterRoot: {
            id: 'group_1',
            type: 'group',
            logicalOperator: 'AND',
            conditions: [
              { id: 'cond_1', type: 'condition', field: 'status', operator: '=', value: 'active' },
              { id: 'cond_2', type: 'condition', field: 'region', operator: '=', value: 'APAC' },
            ],
          },
        },
      } as Command,
      {
        id: 'cmd_sort',
        type: 'sort',
        order: 2,
        config: { dataSource: 'link_orders', field: 'created_at', ascending: true },
      } as Command,
      {
        id: 'cmd_view',
        type: 'view',
        order: 3,
        config: { dataSource: 'link_orders', viewFields: [{ field: 'id' }], viewLimit: 10 },
      } as Command,
    ];

    const existingCommands: Command[] = [
      {
        id: 'existing_filter',
        type: 'filter',
        order: 1,
        config: {
          dataSource: 'link_orders',
          filterRoot: {
            id: 'group_existing',
            type: 'group',
            logicalOperator: 'AND',
            conditions: [
              { id: 'existing_1', type: 'condition', field: 'status', operator: '=', value: 'active' },
            ],
          },
        },
      } as Command,
      {
        id: 'existing_sort',
        type: 'sort',
        order: 2,
        config: { dataSource: 'link_orders', field: 'created_at', ascending: true },
      } as Command,
      {
        id: 'existing_view',
        type: 'view',
        order: 3,
        config: { dataSource: 'link_orders', viewFields: [{ field: 'id' }], viewLimit: 10 },
      } as Command,
    ];

    await act(async () => {
      root.render(
        <SqlBuilderModal
          isOpen={true}
          sqlInput="select * from orders"
          onSqlInputChange={vi.fn()}
          onParse={onParse}
          onApply={vi.fn()}
          onClose={vi.fn()}
          warnings={[]}
          error={null}
          commands={commands}
          datasets={datasets}
          availableSourceAliases={availableSourceAliases}
          onUpdateCommands={onUpdateCommands}
          existingCommands={existingCommands}
          renderSummary={(cmd) => `${cmd.type}:${cmd.id}`}
        />
      );
      await flush();
    });

    const considerExisting = container.querySelector('input[type="checkbox"]') as HTMLInputElement | null;
    await act(async () => {
      considerExisting?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });

    expect(onUpdateCommands).toHaveBeenCalled();
    const prunedCommands = onUpdateCommands.mock.calls[0][0] as Command[];
    expect(prunedCommands).toHaveLength(1);
    expect(prunedCommands[0].id).toBe('cmd_filter');
    expect((prunedCommands[0].config.filterRoot as any).conditions).toHaveLength(1);
    expect(container.textContent).toContain('Omitted 1 condition(s) and 2 command(s) already defined.');

    await act(async () => {
      considerExisting?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });

    expect(onParse).toHaveBeenCalledTimes(1);
  });

  it('renders warnings/errors, expands editors, applies ON builder and routes footer actions', async () => {
    const onClose = vi.fn();
    const onApply = vi.fn();
    const onParse = vi.fn();
    const onSqlInputChange = vi.fn();
    const onUpdateCommands = vi.fn();

    const commands: Command[] = [
      {
        id: 'cmd_join',
        type: 'join',
        order: 1,
        config: {
          dataSource: 'link_orders',
          joinTargetType: 'table',
          joinTable: 'link_customers',
          joinType: 'LEFT',
          joinLeftField: 'customer_id',
          joinOperator: '=',
          joinRightField: 'id',
          on: '',
        },
      } as Command,
      {
        id: 'cmd_group',
        type: 'group',
        order: 2,
        config: {
          dataSource: 'link_orders',
          groupByFields: ['region'],
          aggregations: [{ field: 'amount', func: 'sum', alias: 'total_amount' }],
          havingConditions: [{ id: 'having_1', metricAlias: 'total_amount', operator: '>', value: 100 }],
          outputTableName: 'agg_orders',
        },
      } as Command,
    ];

    await act(async () => {
      root.render(
        <SqlBuilderModal
          isOpen={true}
          sqlInput="select * from orders"
          onSqlInputChange={onSqlInputChange}
          onParse={onParse}
          onApply={onApply}
          onClose={onClose}
          warnings={['warning one']}
          error="parse failed"
          commands={commands}
          datasets={datasets}
          availableSourceAliases={availableSourceAliases}
          onUpdateCommands={onUpdateCommands}
          existingCommands={[]}
          renderSummary={(cmd) => cmd.type}
        />
      );
      await flush();
    });

    expect(container.textContent).toContain('parse failed');
    expect(container.textContent).toContain('warning one');

    const textarea = container.querySelector('[data-testid="sql-builder-input"]') as HTMLTextAreaElement | null;
    await act(async () => {
      if (textarea) {
        const setter = Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype, 'value')?.set;
        setter?.call(textarea, 'select id from orders');
        textarea.dispatchEvent(new Event('input', { bubbles: true }));
      }
      await flush();
    });
    expect(onSqlInputChange).toHaveBeenCalledWith('select id from orders');

    const expandButton = Array.from(container.querySelectorAll('button')).find((button) => button.textContent?.includes('Expand'));
    await act(async () => {
      expandButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });
    expect(container.textContent).toContain('ON Builder');
    expect(container.textContent).toContain('Output Table Name');

    const applyToOn = Array.from(container.querySelectorAll('button')).find((button) => button.textContent?.includes('Apply to ON'));
    await act(async () => {
      applyToOn?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });
    expect(onUpdateCommands).toHaveBeenCalledWith(
      expect.arrayContaining([
        expect.objectContaining({
          id: 'cmd_join',
          config: expect.objectContaining({ on: 'orders.customer_id = customers.id' }),
        }),
      ])
    );

    const collapseButton = Array.from(container.querySelectorAll('button')).find((button) => button.textContent?.includes('Collapse'));
    await act(async () => {
      collapseButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });
    expect(container.textContent).not.toContain('ON Builder');

    const parseButton = Array.from(container.querySelectorAll('button')).find((button) => button.textContent?.trim() === 'Parse');
    const cancelButton = Array.from(container.querySelectorAll('button')).find((button) => button.textContent?.trim() === 'Cancel');
    const applyButton = Array.from(container.querySelectorAll('button')).find((button) => button.textContent?.trim() === 'Apply');
    await act(async () => {
      parseButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      cancelButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      applyButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });
    expect(onParse).toHaveBeenCalledTimes(1);
    expect(onClose).toHaveBeenCalledTimes(1);
    expect(onApply).toHaveBeenCalledTimes(1);
  });
});
