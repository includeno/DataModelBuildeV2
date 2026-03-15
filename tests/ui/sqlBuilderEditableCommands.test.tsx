import React from 'react';
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { act } from 'react-dom/test-utils';
import { createRoot, Root } from 'react-dom/client';
import { SqlBuilderModal } from '../../components/command-editor/SqlBuilderModal';
import { Command } from '../../types';

const flush = () => new Promise(resolve => setTimeout(resolve, 0));

const datasets = [
  { name: 'orders', fields: ['id', 'customer_id', 'amount'], totalCount: 3 },
  { name: 'customers', fields: ['id', 'name', 'region'], totalCount: 3 },
  { name: 'users', fields: ['id', 'email'], totalCount: 2 }
] as any[];

const availableSourceAliases = [
  { alias: 'orders', nodeName: 'setup', id: 'setup', sourceTable: 'orders', linkId: 'link_orders' },
  { alias: 'customers', nodeName: 'setup', id: 'setup', sourceTable: 'customers', linkId: 'link_customers' },
  { alias: 'users', nodeName: 'setup', id: 'setup', sourceTable: 'users', linkId: 'link_users' }
] as any[];

describe('SqlBuilderModal editable command sections', () => {
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

  const renderModal = async (commands: Command[], onUpdateCommands = vi.fn()) => {
    await act(async () => {
      root.render(
        <SqlBuilderModal
          isOpen
          sqlInput="select * from t"
          onSqlInputChange={vi.fn()}
          onParse={vi.fn()}
          onApply={vi.fn()}
          onClose={vi.fn()}
          warnings={[]}
          error={null}
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
    return onUpdateCommands;
  };

  const expandCommandCard = async (typeLabel: string) => {
    const buttons = Array.from(container.querySelectorAll('button')) as HTMLButtonElement[];
    const target = buttons.find(b => (b.textContent || '').toLowerCase().includes(typeLabel.toLowerCase()));
    if (!target) throw new Error(`command card button not found: ${typeLabel}`);
    await act(async () => {
      target.click();
      await flush();
    });
  };

  it('renders editable join section', async () => {
    await renderModal([
      {
        id: 'cmd_join',
        type: 'join',
        order: 1,
        config: {
          dataSource: 'link_orders',
          joinTargetType: 'table',
          joinTable: 'link_customers',
          joinType: 'LEFT',
          on: 'orders.id = customers.id'
        }
      }
    ]);

    await expandCommandCard('join');
    expect(container.textContent).toContain('Join Type');
    expect(container.textContent).toContain('ON Condition');

    const onInput = container.querySelector('input[placeholder="left.id = right.user_id"]') as HTMLInputElement | null;
    expect(onInput).toBeTruthy();
    expect(onInput?.disabled).toBe(false);
  });

  it('renders editable group/having section', async () => {
    await renderModal([
      {
        id: 'cmd_group',
        type: 'group',
        order: 1,
        config: {
          dataSource: 'link_orders',
          groupByFields: ['customer_id'],
          aggregations: [{ func: 'sum', field: 'amount', alias: 'total_amount' }],
          havingConditions: [{ id: 'h1', metricAlias: 'total_amount', operator: '>', value: 100 }]
        }
      }
    ]);

    await expandCommandCard('group');
    expect(container.textContent).toContain('Group By');
    expect(container.textContent).toContain('Aggregations');
    expect(container.textContent).toContain('Having');
  });

  it('renders editable transform section', async () => {
    await renderModal([
      {
        id: 'cmd_transform',
        type: 'transform',
        order: 1,
        config: {
          dataSource: 'link_orders',
          mappings: [{ id: 'm1', mode: 'simple', expression: 'amount * 1.1', outputField: 'taxed_amount' }]
        }
      }
    ]);

    await expandCommandCard('transform');
    expect(container.textContent).toContain('Mappings');
    expect(container.textContent).toContain('Mode');
    expect(container.textContent).toContain('Expression');
  });

  it('renders editable save section', async () => {
    await renderModal([
      {
        id: 'cmd_save',
        type: 'save',
        order: 1,
        config: {
          dataSource: 'link_users',
          field: 'id',
          distinct: true,
          value: 'user_ids'
        }
      }
    ]);

    await expandCommandCard('save');
    expect(container.textContent).toContain('Distinct');
    expect(container.textContent).toContain('Variable');
  });
});
