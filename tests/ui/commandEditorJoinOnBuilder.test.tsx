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
    fields: ['customer_id', 'amount'],
    rows: [],
    totalCount: 0
  },
  {
    id: 'customers',
    name: 'customers',
    fields: ['customer_id', 'name'],
    rows: [],
    totalCount: 0
  }
];

const commands: Command[] = [
  {
    id: 'cmd_join_1',
    type: 'join',
    order: 1,
    config: {
      dataSource: 'link_orders',
      joinTargetType: 'table',
      joinTable: 'link_customers',
      joinType: 'LEFT',
      joinLeftField: 'customer_id',
      joinOperator: '=',
      joinRightField: 'customer_id',
      on: 'orders.customer_id = customers.customer_id'
    }
  } as Command
];

describe('CommandEditor join ON builder', () => {
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
          operationId="op_join"
          operationName="Join Op"
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

  it('renders alias-aware ON builder and disables manual ON input', async () => {
    expect(container.textContent).toContain('ON Condition Builder');
    expect(container.textContent).toContain('Left: orders');
    expect(container.textContent).toContain('Right: customers');

    const manualOnInput = container.querySelector('input[placeholder^="ON Condition"]');
    expect(manualOnInput).toBeNull();

    const allSelects = Array.from(container.querySelectorAll('select'));
    const rightFieldSelect = allSelects.find((el) =>
      Array.from(el.options).some(opt => (opt.textContent || '').trim() === 'Right Field...')
    ) as HTMLSelectElement | undefined;
    expect(rightFieldSelect).toBeDefined();

    const rightOptions = Array.from(rightFieldSelect!.options).map(o => o.textContent?.trim() || '');
    expect(rightOptions).toContain('customers.customer_id');
    expect(rightOptions).toContain('customers.name');

    await act(async () => {
      rightFieldSelect!.value = 'name';
      rightFieldSelect!.dispatchEvent(new Event('change', { bubbles: true }));
      await flush();
    });

    expect(onUpdateCommands).toHaveBeenCalled();
    const lastCall = onUpdateCommands.mock.calls[onUpdateCommands.mock.calls.length - 1];
    const updatedCommands = lastCall[1] as Command[];
    const updatedJoin = updatedCommands.find(c => c.id === 'cmd_join_1');
    expect(updatedJoin?.config.joinRightField).toBe('name');
    expect(updatedJoin?.config.on).toBe('orders.customer_id = customers.name');
  });

  it('updates ON when operator changes and clears ON when condition is incomplete', async () => {
    const allSelects = Array.from(container.querySelectorAll('select'));
    const operatorSelect = allSelects.find((el) =>
      Array.from(el.options).some(opt => (opt.textContent || '').trim() === '!=')
    ) as HTMLSelectElement | undefined;
    const rightFieldSelect = allSelects.find((el) =>
      Array.from(el.options).some(opt => (opt.textContent || '').trim() === 'Right Field...')
    ) as HTMLSelectElement | undefined;

    expect(operatorSelect).toBeDefined();
    expect(rightFieldSelect).toBeDefined();

    await act(async () => {
      operatorSelect!.value = '!=';
      operatorSelect!.dispatchEvent(new Event('change', { bubbles: true }));
      await flush();
    });

    let lastCall = onUpdateCommands.mock.calls[onUpdateCommands.mock.calls.length - 1];
    let updatedCommands = lastCall[1] as Command[];
    let updatedJoin = updatedCommands.find(c => c.id === 'cmd_join_1');
    expect(updatedJoin?.config.joinOperator).toBe('!=');
    expect(updatedJoin?.config.on).toBe('orders.customer_id != customers.customer_id');

    await act(async () => {
      rightFieldSelect!.value = '';
      rightFieldSelect!.dispatchEvent(new Event('change', { bubbles: true }));
      await flush();
    });

    lastCall = onUpdateCommands.mock.calls[onUpdateCommands.mock.calls.length - 1];
    updatedCommands = lastCall[1] as Command[];
    updatedJoin = updatedCommands.find(c => c.id === 'cmd_join_1');
    expect(updatedJoin?.config.joinRightField).toBe('');
    expect(updatedJoin?.config.on).toBe('');
  });
});
