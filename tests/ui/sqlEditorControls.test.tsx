import React, { act } from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { createRoot, Root } from 'react-dom/client';

import { SqlEditor } from '../../components/SqlEditor';
import { api } from '../../utils/api';
import type { ApiConfig, Dataset, SqlHistoryItem } from '../../types';

const flush = () => new Promise((resolve) => setTimeout(resolve, 0));

const apiConfig: ApiConfig = { baseUrl: 'mockServer', isMock: true };
const datasets: Dataset[] = [
  { name: 'customers', fields: ['customer_id', 'name', 'email'], totalCount: 1 } as Dataset,
  { name: 'order-items', fields: ['order_id', 'sku'], totalCount: 1 } as Dataset,
];

describe('SqlEditor controls', () => {
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

  const renderEditor = async (props: Partial<React.ComponentProps<typeof SqlEditor>> = {}) => {
    await act(async () => {
      root.render(
        <SqlEditor
          sessionId="sess_test"
          apiConfig={apiConfig}
          datasets={datasets}
          {...props}
        />
      );
      await flush();
      await flush();
    });
  };

  const getTextarea = () => {
    const textarea = container.querySelector('textarea') as HTMLTextAreaElement | null;
    if (!textarea) throw new Error('SQL textarea not found');
    return textarea;
  };

  const clickButtonByText = async (text: string) => {
    const button = Array.from(container.querySelectorAll('button')).find((item) =>
      (item.textContent || '').includes(text) || item.getAttribute('title') === text
    ) as HTMLButtonElement | undefined;
    if (!button) throw new Error(`Button not found: ${text}`);
    await act(async () => {
      button.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
      await flush();
    });
    return button;
  };

  it('opens external target tables, quotes identifiers and reports run state', async () => {
    const onClearTarget = vi.fn();
    const onRunStateChange = vi.fn();
    const postSpy = vi.spyOn(api, 'post').mockResolvedValue({
      rows: [{ order_id: 'A-1', sku: 'SKU-1' }],
      totalCount: 1,
      page: 1,
      pageSize: 50,
      columns: ['order_id', 'sku'],
    });

    await renderEditor({
      targetTable: 'order-items',
      onClearTarget,
      onRunStateChange,
    });

    expect(onClearTarget).toHaveBeenCalledTimes(1);
    expect(getTextarea().value).toBe('SELECT * FROM "order-items"');
    expect(container.textContent).toContain('order-items');

    await renderEditor({
      targetTable: 'order-items',
      onClearTarget,
      runRequestId: 1,
      onRunStateChange,
    });

    expect(postSpy).toHaveBeenCalledWith(
      apiConfig,
      '/projects/sess_test/query',
      expect.objectContaining({
        projectId: 'sess_test',
        query: 'SELECT * FROM "order-items"',
        page: 1,
        pageSize: 50,
      })
    );
    expect(onRunStateChange).toHaveBeenCalledWith({ canRun: true, running: false });
    expect(container.textContent).toContain('A-1');
  });

  it('manages tabs and preserves the last remaining tab', async () => {
    await renderEditor();

    expect(container.textContent).toContain('Query 1');
    await clickButtonByText('New Query Tab');
    expect(container.textContent).toContain('Query 2');

    const closeButtons = Array.from(container.querySelectorAll('button[title="Close Tab"]')) as HTMLButtonElement[];
    expect(closeButtons).toHaveLength(2);

    await act(async () => {
      closeButtons[0]?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });

    expect(container.textContent).not.toContain('Query 1');
    expect(container.textContent).toContain('Query 2');
    expect(container.querySelectorAll('button[title="Close Tab"]').length).toBe(0);
  });

  it('restores and clears query history from the side panel', async () => {
    const history: SqlHistoryItem[] = [
      {
        id: 'hist_1',
        timestamp: Date.now(),
        query: 'SELECT customer_id FROM customers',
        status: 'success',
        durationMs: 12,
        rowCount: 1,
      },
      {
        id: 'hist_2',
        timestamp: Date.now() - 1000,
        query: 'SELECT missing FROM customers',
        status: 'error',
        durationMs: 9,
        errorMessage: 'missing column',
      },
    ];
    const onUpdateHistory = vi.fn();

    await renderEditor({
      history,
      onUpdateHistory,
      targetTable: 'customers',
      onClearTarget: vi.fn(),
    });

    await clickButtonByText('Log');
    expect(container.textContent).toContain('Execution Log');
    expect(container.textContent).toContain('SELECT customer_id FROM customers');
    expect(container.textContent).toContain('SELECT missing FROM customers');

    const restoreCard = Array.from(container.querySelectorAll('div')).find((node) =>
      String(node.className || '').includes('cursor-pointer') &&
      String(node.className || '').includes('rounded-lg') &&
      (node.textContent || '').includes('SELECT customer_id FROM customers')
    ) as HTMLDivElement | undefined;
    expect(restoreCard).toBeDefined();

    await act(async () => {
      restoreCard?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });

    expect(getTextarea().value).toBe('SELECT customer_id FROM customers');

    const clearButton = Array.from(container.querySelectorAll('button')).find((item) => (item.textContent || '').includes('Clear')) as HTMLButtonElement | undefined;
    expect(clearButton).toBeDefined();
    await act(async () => {
      clearButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });

    expect(onUpdateHistory).toHaveBeenCalledWith([]);
  });
});
