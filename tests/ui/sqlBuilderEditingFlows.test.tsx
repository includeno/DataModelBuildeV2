import React, { useState } from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { act } from 'react-dom/test-utils';
import { createRoot, Root } from 'react-dom/client';

import { SqlBuilderModal } from '../../components/command-editor/SqlBuilderModal';
import type { Command } from '../../types';

const flush = () => new Promise((resolve) => setTimeout(resolve, 0));

const datasets = [
  { name: 'orders', fields: ['id', 'customer_id', 'amount'], totalCount: 3 },
  { name: 'customers', fields: ['id', 'name', 'region'], totalCount: 3 },
] as any[];

const availableSourceAliases = [
  { alias: 'orders', nodeName: 'setup', id: 'setup', sourceTable: 'orders', linkId: 'link_orders' },
  { alias: 'customers', nodeName: 'setup', id: 'setup', sourceTable: 'customers', linkId: 'link_customers' },
] as any[];

describe('SqlBuilderModal editing flows', () => {
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

  const Harness = ({
    initialCommands,
    onUpdateCommands,
    onParse,
    props,
  }: {
    initialCommands: Command[];
    onUpdateCommands: ReturnType<typeof vi.fn>;
    onParse: ReturnType<typeof vi.fn>;
    props: Partial<React.ComponentProps<typeof SqlBuilderModal>>;
  }) => {
    const [commands, setCommands] = useState(initialCommands);
    return (
      <SqlBuilderModal
        isOpen
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
        onUpdateCommands={(next) => {
          setCommands(next);
          onUpdateCommands(next);
        }}
        existingCommands={[]}
        renderSummary={(cmd) => cmd.type}
        {...props}
      />
    );
  };

  const renderModal = async (commands: Command[], onUpdateCommands = vi.fn(), props: Partial<React.ComponentProps<typeof SqlBuilderModal>> = {}) => {
    const onParse = vi.fn();
    await act(async () => {
      root.render(
        <Harness
          initialCommands={commands}
          onUpdateCommands={onUpdateCommands}
          onParse={onParse}
          props={props}
        />
      );
      await flush();
    });
    return { onUpdateCommands, onParse };
  };

  const expandCommand = async (summary: string) => {
    const button = Array.from(container.querySelectorAll('button')).find((node) => (node.textContent || '').includes(summary)) as HTMLButtonElement | undefined;
    if (!button) throw new Error(`Command summary not found: ${summary}`);
    await act(async () => {
      button.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });
  };

  const setSelectValue = async (select: HTMLSelectElement, value: string) => {
    await act(async () => {
      select.value = value;
      select.dispatchEvent(new Event('change', { bubbles: true }));
      await flush();
    });
  };

  const setInputValue = async (input: HTMLInputElement | HTMLTextAreaElement, value: string) => {
    await act(async () => {
      const proto = input instanceof HTMLTextAreaElement ? HTMLTextAreaElement.prototype : HTMLInputElement.prototype;
      const setter = Object.getOwnPropertyDescriptor(proto, 'value')?.set;
      setter?.call(input, value);
      input.dispatchEvent(new Event('input', { bubbles: true }));
      await flush();
    });
  };

  it('builds JOIN conditions and supports node targets', async () => {
    const { onUpdateCommands } = await renderModal([
      {
        id: 'cmd_join',
        type: 'join',
        order: 1,
        config: {
          dataSource: 'link_orders',
          joinTargetType: 'table',
          joinTable: 'link_customers',
          joinType: 'LEFT',
          on: '',
        },
      },
    ]);

    await expandCommand('join');

    const leftFieldSelect = Array.from(container.querySelectorAll('select')).find((node) =>
      Array.from(node.options).some((opt) => opt.textContent === 'Left Field...')
    ) as HTMLSelectElement | undefined;
    const operatorSelect = Array.from(container.querySelectorAll('select')).find((node) =>
      Array.from(node.options).some((opt) => opt.value === '!=')
    ) as HTMLSelectElement | undefined;
    const rightFieldSelect = Array.from(container.querySelectorAll('select')).find((node) =>
      Array.from(node.options).some((opt) => opt.textContent === 'Right Field...')
    ) as HTMLSelectElement | undefined;
    expect(leftFieldSelect).toBeDefined();
    expect(operatorSelect).toBeDefined();
    expect(rightFieldSelect).toBeDefined();

    await setSelectValue(leftFieldSelect!, 'id');
    await setSelectValue(operatorSelect!, '=');
    await setSelectValue(rightFieldSelect!, 'id');

    const applyOnButton = Array.from(container.querySelectorAll('button')).find((node) => (node.textContent || '').includes('Apply to ON')) as HTMLButtonElement | undefined;
    expect(applyOnButton?.disabled).toBe(false);
    await act(async () => {
      applyOnButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });

    const latestJoinUpdate = onUpdateCommands.mock.calls[onUpdateCommands.mock.calls.length - 1][0] as Command[];
    expect(latestJoinUpdate[0].config.on).toContain('orders.id = customers.id');

    const targetTypeSelect = Array.from(container.querySelectorAll('select')).find((node) =>
      Array.from(node.options).some((opt) => opt.value === 'node')
    ) as HTMLSelectElement | undefined;
    expect(targetTypeSelect).toBeDefined();
    await setSelectValue(targetTypeSelect!, 'node');
    const targetInput = container.querySelector('input[placeholder="Node ID"]') as HTMLInputElement | null;
    expect(targetInput).not.toBeNull();
    await setInputValue(targetInput!, 'node_customers');

    const nodeTargetUpdate = onUpdateCommands.mock.calls[onUpdateCommands.mock.calls.length - 1][0] as Command[];
    expect(nodeTargetUpdate[0].config.joinTargetType).toBe('node');
    expect(nodeTargetUpdate[0].config.joinTargetNodeId).toBe('node_customers');
  });

  it('adds and removes group fields, aggregations and having conditions', async () => {
    const { onUpdateCommands } = await renderModal([
      {
        id: 'cmd_group',
        type: 'group',
        order: 1,
        config: {
          dataSource: 'link_orders',
          groupByFields: [],
          aggregations: [],
          havingConditions: [],
        },
      },
    ]);

    await expandCommand('group');

    await act(async () => {
      Array.from(container.querySelectorAll('button')).find((node) => (node.textContent || '').includes('Add Field'))?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });
    await act(async () => {
      Array.from(container.querySelectorAll('button')).find((node) => (node.textContent || '').includes('Add Metric'))?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });
    await act(async () => {
      Array.from(container.querySelectorAll('button')).find((node) => (node.textContent || '').includes('Add Condition'))?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });

    const latestUpdate = onUpdateCommands.mock.calls[onUpdateCommands.mock.calls.length - 1][0] as Command[];
    expect(latestUpdate[0].config.groupByFields.length).toBe(1);
    expect(latestUpdate[0].config.aggregations.length).toBe(1);
    expect(latestUpdate[0].config.havingConditions.length).toBe(1);
  });

  it('edits transform mappings and toggles consider-existing footer behaviour', async () => {
    const { onUpdateCommands, onParse } = await renderModal([
      {
        id: 'cmd_transform',
        type: 'transform',
        order: 1,
        config: {
          dataSource: 'link_orders',
          mappings: [{ id: 'map_1', mode: 'simple', expression: 'amount * 1.1', outputField: 'gross_amount' }],
        },
      },
    ]);

    await expandCommand('transform');

    await act(async () => {
      Array.from(container.querySelectorAll('button')).find((node) => (node.textContent || '').includes('Add Mapping'))?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });

    const modeSelect = Array.from(container.querySelectorAll('select')).find((node) =>
      Array.from(node.options).some((opt) => opt.value === 'python')
    ) as HTMLSelectElement | undefined;
    expect(modeSelect).toBeDefined();
    await setSelectValue(modeSelect!, 'python');

    const outputInputs = Array.from(container.querySelectorAll('input')).filter((node) => node.getAttribute('placeholder') === 'new_field') as HTMLInputElement[];
    expect(outputInputs.length).toBeGreaterThan(0);
    await setInputValue(outputInputs[outputInputs.length - 1], 'python_amount');

    const pythonArea = container.querySelector('textarea[placeholder="def transform(row): ..."]') as HTMLTextAreaElement | null;
    expect(pythonArea).not.toBeNull();
    await setInputValue(pythonArea!, 'def transform(row):\n    return row["amount"]');

    const considerExisting = Array.from(container.querySelectorAll('input')).find((node) => node.getAttribute('type') === 'checkbox') as HTMLInputElement | undefined;
    expect(considerExisting).toBeDefined();

    await act(async () => {
      considerExisting?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });
    expect(container.textContent).toContain('No redundant steps found.');

    await act(async () => {
      considerExisting?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });
    expect(onParse).toHaveBeenCalled();

    const latestUpdate = onUpdateCommands.mock.calls[onUpdateCommands.mock.calls.length - 1][0] as Command[];
    expect(latestUpdate[0].config.mappings.length).toBeGreaterThan(1);
  });
});
