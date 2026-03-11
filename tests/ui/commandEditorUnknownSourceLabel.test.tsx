import React from 'react';
import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { act } from 'react-dom/test-utils';
import { createRoot, Root } from 'react-dom/client';
import { CommandEditor } from '../../components/CommandEditor';
import { Command, OperationNode } from '../../types';

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
          id: 'src_customers',
          type: 'source',
          order: 1,
          config: { mainTable: 'customers', alias: 'customers', linkId: 'link_customers' }
        } as Command,
        {
          id: 'src_orders',
          type: 'source',
          order: 2,
          config: { mainTable: 'orders', alias: 'orders', linkId: 'link_orders' }
        } as Command
      ],
      children: [
        {
          id: 'op_1',
          type: 'operation',
          operationType: 'process',
          name: 'Join Op',
          enabled: true,
          commands: [],
          children: []
        }
      ]
    }
  ]
};

const commands: Command[] = [
  {
    id: 'cmd_join_1',
    type: 'join',
    order: 1,
    config: {
      dataSource: 'link_customers',
      joinTargetType: 'table',
      joinTable: 'link_orders',
      on: 'orders.customer_id = customers.customer_id'
    }
  } as Command
];

describe('CommandEditor unknown source labels', () => {
  let container: HTMLDivElement;
  let root: Root;

  beforeEach(async () => {
    container = document.createElement('div');
    document.body.appendChild(container);
    root = createRoot(container);

    await act(async () => {
      root.render(
        <CommandEditor
          operationId="op_1"
          operationName="Join Op"
          operationType="process"
          commands={commands}
          datasets={[]}
          inputSchema={{}}
          tree={tree}
          onUpdateCommands={() => {}}
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
    root.unmount();
    container.remove();
  });

  it('shows ? for unresolved source tables in dropdown labels', () => {
    const optionTexts = Array.from(container.querySelectorAll('option')).map(opt => (opt.textContent || '').trim());

    expect(optionTexts).toContain('customers to ? · link_customers');
    expect(optionTexts).toContain('orders to ? · link_orders');

    expect(optionTexts).not.toContain('customers to customers · link_customers');
    expect(optionTexts).not.toContain('orders to orders · link_orders');

    const unresolvedCustomersOption = Array.from(container.querySelectorAll('option'))
      .find(opt => (opt.textContent || '').includes('customers to ?')) as HTMLOptionElement | undefined;
    expect(unresolvedCustomersOption).toBeDefined();
    expect(unresolvedCustomersOption!.style.color).not.toBe('');

    const datasetSelect = Array.from(container.querySelectorAll('select')).find((el) =>
      Array.from(el.options).some(opt => (opt.textContent || '').includes('customers to ? · link_customers'))
    ) as HTMLSelectElement | undefined;
    expect(datasetSelect).toBeDefined();
    expect(datasetSelect!.className).toContain('text-red-600');

    const stepRunButton = container.querySelector('button[title*="unavailable"]') as HTMLButtonElement | null;
    expect(stepRunButton).not.toBeNull();
    expect(stepRunButton!.disabled).toBe(true);

    const operationRunButton = container.querySelector('button[title=\"Select a valid data source for each step before running\"]') as HTMLButtonElement | null;
    expect(operationRunButton).not.toBeNull();
    expect(operationRunButton!.disabled).toBe(true);
  });
});
