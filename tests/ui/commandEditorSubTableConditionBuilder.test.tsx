import React from 'react';
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { act } from 'react-dom/test-utils';
import { createRoot, Root } from 'react-dom/client';
import { CommandEditor } from '../../components/CommandEditor';
import { Command, Dataset, OperationNode } from '../../types';

const flush = () => new Promise(resolve => setTimeout(resolve, 0));

const tree: OperationNode = {
  id: 'root',
  type: 'operation',
  operationType: 'root',
  name: 'Project',
  enabled: true,
  commands: [],
  children: [
    {
      id: 'setup_1',
      type: 'operation',
      operationType: 'setup',
      name: 'Setup',
      enabled: true,
      commands: [
        {
          id: 'src_orders',
          type: 'source',
          order: 0,
          config: { mainTable: 'orders', alias: 'orders', linkId: 'link_orders' }
        } as Command,
        {
          id: 'src_customers',
          type: 'source',
          order: 1,
          config: { mainTable: 'customers', alias: 'customers', linkId: 'link_customers' }
        } as Command
      ],
      children: []
    }
  ]
};

const datasets: Dataset[] = [
  {
    id: 'orders',
    name: 'orders',
    fields: ['customer_id', 'expected_status'],
    rows: [],
    totalCount: 0
  },
  {
    id: 'customers',
    name: 'customers',
    fields: ['customer_id', 'status', 'name'],
    rows: [],
    totalCount: 0
  }
];

const commands: Command[] = [
  {
    id: 'cmd_multi_1',
    type: 'multi_table',
    order: 1,
    config: {
      dataSource: 'link_orders',
      subTables: [
        {
          id: 'sub_1',
          table: 'link_customers',
          on: 'customers.customer_id = orders.customer_id',
          label: 'Customers',
          onConditionGroup: {
            id: 'g1',
            type: 'group',
            logicalOperator: 'AND',
            conditions: [
              {
                id: 'c1',
                type: 'condition',
                field: 'status',
                operator: '=',
                mainField: 'customer_id'
              }
            ]
          }
        }
      ]
    }
  } as Command
];

describe('CommandEditor sub-table condition builder', () => {
  let container: HTMLDivElement;
  let root: Root;
  const onUpdateCommands = vi.fn();

  beforeEach(async () => {
    container = document.createElement('div');
    document.body.appendChild(container);
    root = createRoot(container);

    await act(async () => {
      root.render(
        <CommandEditor
          operationId="op_1"
          operationName="Complex Op"
          operationType="process"
          commands={commands}
          datasets={datasets}
          inputSchema={{}}
          tree={tree}
          onUpdateCommands={onUpdateCommands}
          onUpdateName={() => {}}
          onUpdateType={() => {}}
          onViewPath={() => {}}
          canRun={true}
        />
      );
      await flush();
    });
  });

  afterEach(() => {
    onUpdateCommands.mockReset();
    root.unmount();
    container.remove();
  });

  it('renders main-field picker and writes condition updates', async () => {
    expect(container.textContent).toContain('ON Condition Builder');

    const allSelects = Array.from(container.querySelectorAll('select'));
    const subFieldSelect = allSelects.find((el) =>
      Array.from(el.options).some(opt => (opt.textContent || '').trim() === 'Sub Field...')
    ) as HTMLSelectElement | undefined;
    const mainFieldSelect = allSelects.find((el) =>
      Array.from(el.options).some(opt => (opt.textContent || '').trim() === 'Main Field...')
    ) as HTMLSelectElement | undefined;

    expect(subFieldSelect).toBeDefined();
    expect(mainFieldSelect).toBeDefined();
    const subOptions = Array.from(subFieldSelect!.options).map(o => o.textContent?.trim() || '');
    const mainOptions = Array.from(mainFieldSelect!.options).map(o => o.textContent?.trim() || '');
    expect(subOptions.some(text => text.endsWith('.status'))).toBe(true);
    expect(mainOptions.some(text => text.endsWith('.expected_status'))).toBe(true);

    await act(async () => {
      mainFieldSelect!.value = 'expected_status';
      mainFieldSelect!.dispatchEvent(new Event('change', { bubbles: true }));
      await flush();
    });

    expect(onUpdateCommands).toHaveBeenCalled();
    const lastCall = onUpdateCommands.mock.calls[onUpdateCommands.mock.calls.length - 1];
    const updatedCommands = lastCall[1] as Command[];
    const updatedMulti = updatedCommands.find(c => c.id === 'cmd_multi_1');
    const updatedSub = updatedMulti?.config.subTables?.[0] as any;
    expect(updatedSub.onConditionGroup.conditions[0].mainField).toBe('expected_status');
    expect(updatedSub.on).toContain('sub.status = main.expected_status');
  });

  it('shows explicit main/sub aliases in ON condition builder', () => {
    expect(container.textContent).toContain('Sub: customers');
    expect(container.textContent).toContain('Main: orders');
  });

  it('does not allow manual ON text editing input', () => {
    const onInput = container.querySelector('input[placeholder="main.id = sub.user_id"]');
    expect(onInput).toBeNull();
  });
});
