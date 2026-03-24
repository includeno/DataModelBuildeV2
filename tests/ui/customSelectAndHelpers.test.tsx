import React from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { act } from 'react';
import { createRoot, Root } from 'react-dom/client';

import { CustomSelect } from '../../components/command-editor/CustomSelect';
import {
  formatSourceOptionLabel,
  getDatasetFieldNames,
  getSourceLabel,
  renderSqlCommandSummary,
  resolveDataSource,
  type SourceAlias,
} from '../../components/command-editor/helpers';
import type { Command, Dataset } from '../../types';

const flush = () => new Promise((resolve) => setTimeout(resolve, 0));

const DummyIcon = () => <span data-testid="dummy-icon">icon</span>;

describe('CustomSelect and command-editor helpers', () => {
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

  it('opens, selects options, prevents disabled picks, handles empty state and closes on outside click', async () => {
    const onChange = vi.fn();

    await act(async () => {
      root.render(
        <div>
          <CustomSelect
            value=""
            onChange={onChange}
            placeholder="Pick source"
            icon={DummyIcon}
            hasError={true}
            options={[
              { value: 'cust', label: 'Customers', subLabel: 'Primary source', icon: DummyIcon },
              { value: 'used', label: 'Already Used', disabled: true },
            ]}
          />
          <div id="outside">Outside</div>
        </div>
      );
      await flush();
    });

    expect(container.textContent).toContain('Pick source');

    const trigger = container.querySelector('.cursor-pointer') as HTMLDivElement | null;
    await act(async () => {
      trigger?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });

    expect(container.textContent).toContain('Customers');
    expect(container.textContent).toContain('Primary source');
    expect(container.textContent).toContain('Used');

    const disabledOption = Array.from(container.querySelectorAll('[role="option"]')).find((node) => node.textContent?.includes('Already Used'));
    await act(async () => {
      disabledOption?.dispatchEvent(new MouseEvent('mousedown', { bubbles: true }));
      disabledOption?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });
    expect(onChange).not.toHaveBeenCalled();

    const activeOption = Array.from(container.querySelectorAll('[role="option"]')).find((node) => node.textContent?.includes('Customers'));
    await act(async () => {
      activeOption?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });
    expect(onChange).toHaveBeenCalledWith('cust');

    await act(async () => {
      trigger?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });
    await act(async () => {
      document.dispatchEvent(new MouseEvent('mousedown', { bubbles: true }));
      await flush();
    });
    expect(container.querySelector('[role="option"]')).toBeNull();

    await act(async () => {
      root.render(
        <CustomSelect
          value=""
          onChange={onChange}
          options={[]}
        />
      );
      await flush();
    });
    const emptyTrigger = container.querySelector('.cursor-pointer') as HTMLDivElement | null;
    await act(async () => {
      emptyTrigger?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });
    expect(container.textContent).toContain('No options available');
  });

  it('formats helper outputs for sources, datasets and command summaries', () => {
    const datasets: Dataset[] = [
      {
        id: 'ds_1',
        name: 'customers',
        fields: ['id', 'name'],
        rows: [],
        fieldTypes: {
          id: { type: 'number' },
          name: { type: 'string' },
        },
      },
      {
        id: 'ds_2',
        name: 'orders',
        fields: ['amount'],
        rows: [],
      },
    ];

    const aliases: SourceAlias[] = [
      { alias: 'cust', nodeName: 'Customers', id: 'node_1', sourceTable: 'customers', linkId: 'link_1' },
      { alias: 'orders_alias', nodeName: 'Orders', id: 'node_2', sourceTable: 'orders', linkId: 'link_2' },
    ];

    expect(getDatasetFieldNames(datasets, 'customers')).toEqual(['id', 'name']);
    expect(getDatasetFieldNames(datasets, 'orders')).toEqual(['amount']);
    expect(getDatasetFieldNames(datasets, undefined)).toEqual([]);

    expect(getSourceLabel(aliases, 'link_1')).toBe('cust');
    expect(getSourceLabel(aliases, 'orders')).toBe('orders_alias');
    expect(getSourceLabel(aliases, 'link_private')).toBe('');
    expect(getSourceLabel(aliases, 'manual_table')).toBe('manual_table');

    expect(formatSourceOptionLabel('cust', 'customers', 'link_1')).toBe('cust to customers · link_1');
    expect(resolveDataSource(aliases, 'customers')).toBe('link_1');
    expect(resolveDataSource(aliases, 'raw_table')).toBe('raw_table');

    const summaries: Command[] = [
      { id: 'f', type: 'filter', order: 1, config: { filterRoot: { id: 'g', type: 'group', logicalOperator: 'AND', conditions: [{ id: 'c1', type: 'condition', field: 'status', operator: '=', value: 'active' }] } } },
      { id: 'j', type: 'join', order: 2, config: { joinType: 'left', joinTable: 'orders' } },
      { id: 'g', type: 'group', order: 3, config: { groupByFields: ['country'], aggregations: [{ field: 'amount', func: 'sum', alias: 'total' }] } },
      { id: 's', type: 'sort', order: 4, config: { field: 'created_at', ascending: false } },
      { id: 't', type: 'transform', order: 5, config: { mappings: [{ id: 'm1', mode: 'simple', expression: '1', outputField: 'score' }] } },
      { id: 'v', type: 'view', order: 6, config: { viewFields: [{ field: 'id' }, { field: 'name' }], viewLimit: 10 } },
      { id: 'save', type: 'save', order: 7, config: { distinct: true, field: 'status', value: 'status_out' } },
      { id: 'src', type: 'source', order: 8, config: { mainTable: 'customers' } },
      { id: 'var', type: 'define_variable', order: 9, config: { variableName: 'region' } },
      { id: 'multi', type: 'multi_table', order: 10, config: { subTables: [{ id: 'sub', table: 'orders', on: 'id=id', label: 'Orders' }] } },
      { id: 'custom', type: 'custom', order: 11, config: {} },
    ];

    expect(renderSqlCommandSummary(summaries[0])).toBe('Filter (1 conditions)');
    expect(renderSqlCommandSummary(summaries[1])).toBe('Join LEFT orders');
    expect(renderSqlCommandSummary(summaries[2])).toBe('Group by country (1 metrics)');
    expect(renderSqlCommandSummary(summaries[3])).toBe('Sort created_at DESC');
    expect(renderSqlCommandSummary(summaries[4])).toBe('Mapping (1)');
    expect(renderSqlCommandSummary(summaries[5])).toBe('View id, name Limit 10');
    expect(renderSqlCommandSummary(summaries[6])).toBe('Save Distinct status -> status_out');
    expect(renderSqlCommandSummary(summaries[7])).toBe('Source customers');
    expect(renderSqlCommandSummary(summaries[8])).toBe('Define Variable region');
    expect(renderSqlCommandSummary(summaries[9])).toBe('Complex View (1 sub-table)');
    expect(renderSqlCommandSummary(summaries[10])).toBe('custom');
  });
});
