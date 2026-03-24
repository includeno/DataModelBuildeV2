import React from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { act } from 'react-dom/test-utils';
import { createRoot, Root } from 'react-dom/client';

import { ComplexDataPreview } from '../../components/ComplexDataPreview';
import type { ExecutionResult, OperationNode } from '../../types';

const flush = () => new Promise((resolve) => setTimeout(resolve, 0));

const selectedNode: OperationNode = {
  id: 'op_complex',
  type: 'operation',
  operationType: 'process',
  name: 'Complex Orders',
  enabled: true,
  children: [],
  commands: [
    {
      id: 'cmd_multi',
      type: 'multi_table',
      order: 1,
      config: {
        subTables: [
          {
            id: 'sub_orders',
            table: 'orders',
            label: 'Orders',
            on: 'main.customer_id = sub.customer_id',
            conditionGroup: {
              logicalOperator: 'AND',
              conditions: [
                {
                  type: 'condition',
                  field: 'status',
                  operator: '=',
                  value: 'paid',
                  mainField: 'status_target',
                },
              ],
            },
          },
        ],
      },
    },
  ],
};

const initialResult: ExecutionResult = {
  rows: [
    { customer_id: 1, status_target: 'paid', customer_name: 'Alice' },
    { customer_id: 2, status_target: 'paid', customer_name: 'Bob' },
  ],
  columns: ['customer_id', 'status_target', 'customer_name'],
  totalCount: 4,
  page: 2,
  pageSize: 2,
};

describe('ComplexDataPreview', () => {
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

  it('renders loading and empty states', async () => {
    const onRefreshView = vi.fn().mockResolvedValue(initialResult);

    await act(async () => {
      root.render(
        <ComplexDataPreview
          initialResult={initialResult}
          selectedNode={selectedNode}
          loading={true}
          onRefreshView={onRefreshView}
          onExportFull={() => {}}
        />
      );
      await flush();
    });
    expect(container.textContent).toContain('Loading Complex View');

    await act(async () => {
      root.render(
        <ComplexDataPreview
          initialResult={{ ...initialResult, rows: [] }}
          selectedNode={selectedNode}
          loading={false}
          onRefreshView={onRefreshView}
          onExportFull={() => {}}
        />
      );
      await flush();
    });
    expect(container.textContent).toContain('No Main Stream Data');
  });

  it('loads sub-table previews and shows matched related rows when a row is expanded', async () => {
    const onRefreshView = vi.fn().mockImplementation(async (viewId: string) => {
      if (viewId === 'sub_orders') {
        return {
          rows: [
            { customer_id: 1, status: 'paid', order_id: 10 },
            { customer_id: 2, status: 'pending', order_id: 11 },
          ],
          columns: ['customer_id', 'status', 'order_id'],
          totalCount: 2,
          page: 1,
          pageSize: 200,
        };
      }
      return initialResult;
    });

    await act(async () => {
      root.render(
        <ComplexDataPreview
          initialResult={initialResult}
          selectedNode={selectedNode}
          loading={false}
          onRefreshView={onRefreshView}
          onExportFull={() => {}}
          mainSourceName="customers.csv"
        />
      );
      await flush();
      await flush();
    });

    expect(container.textContent).toContain('Complex Orders');
    expect(container.textContent).toContain('customers.csv');
    expect(container.textContent).toContain('4 Rows');
    expect(onRefreshView).toHaveBeenCalledWith('sub_orders', 1, 200);

    const firstDataRow = container.querySelector('tbody tr');
    await act(async () => {
      firstDataRow?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });

    expect(container.textContent).toContain('Orders');
    expect(container.textContent).toContain('Matched by ON + group conditions');
    expect(container.textContent).toContain('Showing 1 related record(s)');
    expect(container.textContent).toContain('10');
  });

  it('shows no-match fallback when sub-table rows do not satisfy join conditions', async () => {
    const onRefreshView = vi.fn().mockResolvedValue({
      rows: [{ customer_id: 99, status: 'pending', order_id: 12 }],
      columns: ['customer_id', 'status', 'order_id'],
      totalCount: 1,
      page: 1,
      pageSize: 200,
    });

    await act(async () => {
      root.render(
        <ComplexDataPreview
          initialResult={initialResult}
          selectedNode={selectedNode}
          loading={false}
          onRefreshView={onRefreshView}
          onExportFull={() => {}}
        />
      );
      await flush();
      await flush();
    });

    const firstDataRow = container.querySelector('tbody tr');
    await act(async () => {
      firstDataRow?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });

    expect(container.textContent).toContain('No related records found');
  });

  it('exports current page, exports full data and triggers pagination refreshes', async () => {
    const onRefreshView = vi.fn().mockResolvedValue(initialResult);
    const onExportFull = vi.fn();
    const createObjectURLStub = vi.fn().mockReturnValue('blob:mock-url');
    Object.defineProperty(URL, 'createObjectURL', {
      value: createObjectURLStub,
      configurable: true,
      writable: true,
    });
    const clickSpy = vi.spyOn(HTMLAnchorElement.prototype, 'click').mockImplementation(() => {});

    await act(async () => {
      root.render(
        <ComplexDataPreview
          initialResult={initialResult}
          selectedNode={selectedNode}
          loading={false}
          onRefreshView={onRefreshView}
          onExportFull={onExportFull}
        />
      );
      await flush();
      await flush();
    });

    const exportButton = Array.from(container.querySelectorAll('button')).find((button) => button.textContent?.includes('Export'));
    await act(async () => {
      exportButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });

    const currentPageButton = Array.from(container.querySelectorAll('button')).find((button) => button.textContent?.includes('Export Current Page'));
    await act(async () => {
      currentPageButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });
    expect(createObjectURLStub).toHaveBeenCalled();
    expect(clickSpy).toHaveBeenCalled();

    const iconButtons = Array.from(container.querySelectorAll('button')).filter((button) => !button.textContent);
    const refreshButton = iconButtons[0];
    const prevButton = iconButtons[1];
    const nextButton = iconButtons[2];

    await act(async () => {
      refreshButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      prevButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      nextButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });

    expect(onRefreshView).toHaveBeenCalledWith('main', 2, 50);
    expect(onRefreshView).toHaveBeenCalledWith('main', 1, 50);
    expect(onRefreshView).not.toHaveBeenCalledWith('main', 3, 50);
    expect(container.textContent).toContain('Page 2 of 2');
  });
});
