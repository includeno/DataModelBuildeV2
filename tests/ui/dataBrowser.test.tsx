import React from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { act } from 'react-dom/test-utils';
import { createRoot, Root } from 'react-dom/client';

import { DataBrowser } from '../../components/DataBrowser';
import { api } from '../../utils/api';
import type { Dataset, ImportHistoryItem } from '../../types';

const flush = () => new Promise((resolve) => setTimeout(resolve, 0));

const datasets: Dataset[] = [
  {
    id: 'ds_customers',
    name: 'customers.csv',
    fields: ['id', 'name', 'city'],
    fieldTypes: { id: { type: 'number' }, name: { type: 'string' }, city: { type: 'string' } },
    rows: [],
    totalCount: 2,
  },
  {
    id: 'ds_orders',
    name: 'orders.csv',
    fields: ['id', 'customer_id', 'status'],
    fieldTypes: { id: { type: 'number' }, customer_id: { type: 'number' }, status: { type: 'string' } },
    rows: [],
    totalCount: 1,
  },
];

const imports: ImportHistoryItem[] = [
  {
    timestamp: 10,
    originalFileName: 'orders.csv',
    datasetName: 'orders.csv',
    tableName: 'orders.csv',
    rows: 1,
  },
  {
    timestamp: 20,
    originalFileName: 'customers.csv',
    datasetName: 'customers.csv',
    tableName: 'customers.csv',
    rows: 2,
  },
];

const previewRows = {
  'customers.csv': [
    { id: 1, name: 'Alice', city: 'Shanghai' },
    { id: 2, name: 'bob', city: 'Shenzhen' },
  ],
  'orders.csv': [
    { id: 10, customer_id: 1, status: 'Paid' },
  ],
};

const setInputValue = (element: HTMLInputElement, value: string) => {
  const setter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')?.set;
  setter?.call(element, value);
  element.dispatchEvent(new Event('input', { bubbles: true }));
};

const setSelectValue = (element: HTMLSelectElement, value: string) => {
  const setter = Object.getOwnPropertyDescriptor(HTMLSelectElement.prototype, 'value')?.set;
  setter?.call(element, value);
  element.dispatchEvent(new Event('change', { bubbles: true }));
};

const setCheckboxValue = (element: HTMLInputElement, value: boolean) => {
  if (element.checked !== value) {
    element.click();
  }
};

describe('DataBrowser', () => {
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

  const renderBrowser = async (selectedTable?: string | null, onSelectTable = vi.fn()) => {
    const getSpy = vi.spyOn(api, 'get').mockImplementation(async (_config, endpoint: string) => {
      if (endpoint.endsWith('/imports')) return imports;
      if (endpoint.includes('/datasets/')) {
        const datasetName = decodeURIComponent(endpoint.split('/datasets/')[1].split('/preview')[0]);
        return { rows: previewRows[datasetName as keyof typeof previewRows] || [], totalCount: 0 };
      }
      return {};
    });

    await act(async () => {
      root.render(
        <DataBrowser
          projectId="prj_1"
          apiConfig={{ baseUrl: 'http://example.test', isMock: false }}
          datasets={datasets}
          selectedTable={selectedTable}
          onSelectTable={onSelectTable}
        />
      );
      await flush();
      await flush();
    });

    return { getSpy, onSelectTable };
  };

  it('loads imports and preview for the default dataset, then refreshes', async () => {
    const { getSpy } = await renderBrowser();

    expect(container.textContent).toContain('Raw Data Viewer');
    expect(container.textContent).toContain('Alice');
    expect(container.textContent).toContain('Shanghai');
    expect(container.textContent).toContain('orders.csv');
    expect(container.textContent).toContain('customers.csv');
    expect(container.textContent).toContain('Preview: 2');
    expect(container.textContent).toContain('Total: 2');
    expect(getSpy).toHaveBeenCalledWith(
      expect.anything(),
      '/projects/prj_1/datasets/customers.csv/preview?limit=200'
    );

    const refreshButton = Array.from(container.querySelectorAll('button')).find((button) => button.textContent?.includes('Refresh'));
    await act(async () => {
      refreshButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
      await flush();
    });

    const previewCalls = getSpy.mock.calls.filter(([, endpoint]) => String(endpoint).includes('/preview'));
    expect(previewCalls.length).toBeGreaterThanOrEqual(2);
  });

  it('applies filter modes, case sensitivity and clear action', async () => {
    await renderBrowser();

    const selects = Array.from(container.querySelectorAll('select')) as HTMLSelectElement[];
    const filterColumn = selects[2];
    const filterMode = selects[3];
    const filterInput = container.querySelector('input[placeholder="Type to filter rows..."]') as HTMLInputElement;
    const caseSensitive = container.querySelector('input[type="checkbox"]') as HTMLInputElement;
    const clearButton = Array.from(container.querySelectorAll('button')).find((button) => button.textContent?.includes('Clear'));

    await act(async () => {
      setInputValue(filterInput, 'AL');
      await flush();
    });
    expect(container.textContent).toContain('Alice');
    expect(container.textContent).not.toContain('No matching rows');

    await act(async () => {
      setCheckboxValue(caseSensitive, true);
      await flush();
    });
    expect(container.textContent).toContain('No matching rows');

    await act(async () => {
      setSelectValue(filterColumn, 'name');
      setSelectValue(filterMode, 'equals');
      setInputValue(filterInput, 'bob');
      setCheckboxValue(caseSensitive, false);
      await flush();
    });
    expect(container.textContent).toContain('bob');
    expect(container.textContent).not.toContain('AliceShanghai');

    await act(async () => {
      clearButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });
    expect(filterInput.value).toBe('');
  });

  it('switches datasets from import history click and reports selection', async () => {
    const onSelectTable = vi.fn();
    await renderBrowser(undefined, onSelectTable);

    const importButton = Array.from(container.querySelectorAll('button')).find((button) => button.textContent?.includes('#1 orders.csv'));
    await act(async () => {
      importButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
      await flush();
      await flush();
    });

    expect(onSelectTable).toHaveBeenCalledWith('orders.csv');
    expect(container.textContent).toContain('Paid');
    expect(container.textContent).toContain('Total: 1');
  });

  it('renders empty and error states', async () => {
    const getSpy = vi.spyOn(api, 'get')
      .mockRejectedValueOnce(new Error('imports failed'))
      .mockRejectedValueOnce(new Error('preview failed'));

    await act(async () => {
      root.render(
        <DataBrowser
          projectId="prj_2"
          apiConfig={{ baseUrl: 'http://example.test', isMock: false }}
          datasets={datasets}
        />
      );
      await flush();
      await flush();
    });

    expect(container.textContent).toContain('imports failed');
    expect(container.textContent).toContain('preview failed');
    expect(getSpy).toHaveBeenCalledTimes(2);

    getSpy.mockReset();
    getSpy.mockResolvedValue([]);

    await act(async () => {
      root.unmount();
      container.innerHTML = '';
      root = createRoot(container);
      root.render(
        <DataBrowser
          projectId="prj_3"
          apiConfig={{ baseUrl: 'http://example.test', isMock: false }}
          datasets={[]}
        />
      );
      await flush();
      await flush();
    });

    expect(container.textContent).toContain('Select a dataset to preview');
  });
});
