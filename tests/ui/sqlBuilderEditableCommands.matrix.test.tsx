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

const findSelectByAllOptionValues = (container: HTMLElement, optionValues: string[]): HTMLSelectElement | null => {
  const selects = Array.from(container.querySelectorAll('select')) as HTMLSelectElement[];
  return selects.find((sel) => {
    const values = Array.from(sel.options).map(o => o.value);
    return optionValues.every(v => values.includes(v));
  }) || null;
};

describe('SqlBuilderModal editable command matrix', () => {
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

  const renderSingleCommand = async (cmd: Command) => {
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
          commands={[cmd]}
          datasets={datasets}
          availableSourceAliases={availableSourceAliases}
          onUpdateCommands={vi.fn()}
          existingCommands={[]}
          renderSummary={(c) => c.type}
        />
      );
      await flush();
    });
  };

  const expandFirstCommandCard = async (typeLabel: string) => {
    const buttons = Array.from(container.querySelectorAll('button')) as HTMLButtonElement[];
    const cardBtn = buttons.find(b => (b.textContent || '').toLowerCase().includes(typeLabel.toLowerCase()));
    if (!cardBtn) throw new Error(`command card button not found: ${typeLabel}`);
    await act(async () => {
      cardBtn.click();
      await flush();
    });
  };

  const joinTypes = ['INNER', 'LEFT', 'RIGHT', 'FULL'] as const;
  const joinTargets = ['table', 'node'] as const;
  const joinOns = [
    'orders.id = customers.id',
    'orders.customer_id = customers.id',
    '1=1',
    'orders.amount > customers.id'
  ];
  const joinCases = joinTargets.flatMap((targetType) =>
    joinTypes.flatMap((joinType) =>
      joinOns.map((on, idx) => ({
        name: `${targetType}-${joinType}-on-${idx + 1}`,
        cmd: {
          id: `cmd_join_${targetType}_${joinType}_${idx}`,
          type: 'join' as const,
          order: 1,
          config: {
            dataSource: 'link_orders',
            joinTargetType: targetType,
            joinType,
            joinTable: targetType === 'table' ? 'link_customers' : undefined,
            joinTargetNodeId: targetType === 'node' ? `node_${idx}` : undefined,
            on
          }
        } as Command,
        targetType,
        joinType,
        on,
        expectedNodeId: targetType === 'node' ? `node_${idx}` : undefined
      }))
    )
  );

  it.each(joinCases)('join editable case: $name', async ({ cmd, targetType, joinType, on, expectedNodeId }) => {
    await renderSingleCommand(cmd);
    await expandFirstCommandCard('join');

    const targetTypeSelect = findSelectByAllOptionValues(container, ['table', 'node']);
    expect(targetTypeSelect).toBeTruthy();
    expect(targetTypeSelect?.value).toBe(targetType);

    const joinTypeSelect = findSelectByAllOptionValues(container, ['INNER', 'LEFT', 'RIGHT', 'FULL']);
    expect(joinTypeSelect).toBeTruthy();
    expect(joinTypeSelect?.value).toBe(joinType);

    const onInput = container.querySelector('input[placeholder="left.id = right.user_id"]') as HTMLInputElement | null;
    expect(onInput).toBeTruthy();
    expect(onInput?.value).toBe(on);

    const nodeIdInput = container.querySelector('input[placeholder="Node ID"]') as HTMLInputElement | null;
    if (targetType === 'node') {
      expect(nodeIdInput).toBeTruthy();
      expect(nodeIdInput?.value).toBe(expectedNodeId);
    } else {
      expect(nodeIdInput).toBeNull();
    }
  });

  const aggFuncs = ['count', 'sum', 'mean', 'min', 'max', 'first', 'last'] as const;
  const havingOps = ['=', '!=', '>', '>=', '<', '<='] as const;
  const groupCases = aggFuncs.flatMap((func, fi) =>
    havingOps.map((op, oi) => {
      const alias = `metric_${func}_${op.replace(/[^a-z0-9]/gi, '_')}_${fi}_${oi}`;
      return {
        name: `${func}-${op}-${fi}-${oi}`,
        cmd: {
          id: `cmd_group_${fi}_${oi}`,
          type: 'group' as const,
          order: 1,
          config: {
            dataSource: 'link_orders',
            groupByFields: (fi + oi) % 2 === 0 ? ['customer_id'] : ['id'],
            aggregations: [{ func, field: 'amount', alias }],
            havingConditions: [{ id: `h_${fi}_${oi}`, metricAlias: alias, operator: op, value: fi * 10 + oi }]
          }
        } as Command,
        expectedFunc: func,
        expectedOp: op,
        expectedAlias: alias,
        expectedValue: String(fi * 10 + oi)
      };
    })
  );

  it.each(groupCases)('group/having editable case: $name', async ({ cmd, expectedFunc, expectedOp, expectedAlias, expectedValue }) => {
    await renderSingleCommand(cmd);
    await expandFirstCommandCard('group');

    const aggFuncSelect = findSelectByAllOptionValues(container, ['count', 'sum', 'mean', 'min', 'max', 'first', 'last']);
    expect(aggFuncSelect).toBeTruthy();
    expect(aggFuncSelect?.value).toBe(expectedFunc);

    const aliasInput = container.querySelector('input[placeholder="Alias"]') as HTMLInputElement | null;
    expect(aliasInput).toBeTruthy();
    expect(aliasInput?.value).toBe(expectedAlias);

    const havingOpSelect = findSelectByAllOptionValues(container, ['=', '!=', '>', '>=', '<', '<=']);
    expect(havingOpSelect).toBeTruthy();
    expect(havingOpSelect?.value).toBe(expectedOp);

    const valueInput = container.querySelector('input[placeholder="Value"]') as HTMLInputElement | null;
    expect(valueInput).toBeTruthy();
    expect(valueInput?.value).toBe(expectedValue);
  });

  const transformCases = Array.from({ length: 20 }).map((_, idx) => {
    const mode = idx % 2 === 0 ? 'simple' : 'python';
    const expression = mode === 'simple'
      ? `amount * ${idx + 1}`
      : `def transform(row):\n    return row.get('amount', 0) * ${idx + 1}`;
    const outputField = `out_${idx}`;
    return {
      name: `${mode}-${idx}`,
      cmd: {
        id: `cmd_transform_${idx}`,
        type: 'transform' as const,
        order: 1,
        config: {
          dataSource: 'link_orders',
          mappings: [{ id: `m_${idx}`, mode, expression, outputField }]
        }
      } as Command,
      mode,
      expression,
      outputField
    };
  });

  it.each(transformCases)('transform editable case: $name', async ({ cmd, mode, expression, outputField }) => {
    await renderSingleCommand(cmd);
    await expandFirstCommandCard('transform');

    const modeSelect = findSelectByAllOptionValues(container, ['simple', 'python']);
    expect(modeSelect).toBeTruthy();
    expect(modeSelect?.value).toBe(mode);

    const outputInput = container.querySelector('input[placeholder="new_field"]') as HTMLInputElement | null;
    expect(outputInput).toBeTruthy();
    expect(outputInput?.value).toBe(outputField);

    if (mode === 'python') {
      const exprTextarea = container.querySelector('textarea[placeholder="def transform(row): ..."]') as HTMLTextAreaElement | null;
      expect(exprTextarea).toBeTruthy();
      expect(exprTextarea?.value).toBe(expression);
    } else {
      const exprInput = container.querySelector('input[placeholder="amount * 1.1"]') as HTMLInputElement | null;
      expect(exprInput).toBeTruthy();
      expect(exprInput?.value).toBe(expression);
    }
  });

  const saveCases = ['id', 'customer_id', 'amount', 'id'].flatMap((field, fi) =>
    [true, false].flatMap((distinct) =>
      ['var_alpha', 'var_beta'].map((value, vi) => ({
        name: `${field}-${distinct}-${value}-${fi}-${vi}`,
        cmd: {
          id: `cmd_save_${fi}_${vi}_${distinct ? 1 : 0}`,
          type: 'save' as const,
          order: 1,
          config: {
            dataSource: 'link_orders',
            field,
            distinct,
            value
          }
        } as Command,
        expectedDistinct: distinct ? 'true' : 'false',
        expectedVar: value,
        expectedField: field
      }))
    )
  );

  it.each(saveCases)('save editable case: $name', async ({ cmd, expectedDistinct, expectedVar, expectedField }) => {
    await renderSingleCommand(cmd);
    await expandFirstCommandCard('save');

    const distinctSelect = findSelectByAllOptionValues(container, ['true', 'false']);
    expect(distinctSelect).toBeTruthy();
    expect(distinctSelect?.value).toBe(expectedDistinct);

    const varInput = container.querySelector('input[placeholder="var_name"]') as HTMLInputElement | null;
    expect(varInput).toBeTruthy();
    expect(varInput?.value).toBe(expectedVar);

    const fieldSelect = Array.from(container.querySelectorAll('select')).find((sel) => {
      const values = Array.from(sel.options).map(o => o.value);
      return values.includes('id') && values.includes('customer_id') && values.includes('amount');
    }) as HTMLSelectElement | undefined;
    expect(fieldSelect).toBeTruthy();
    expect(fieldSelect?.value).toBe(expectedField);
  });
});
