import React from 'react';
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { act } from 'react-dom/test-utils';
import { createRoot, Root } from 'react-dom/client';
import { SqlEditor } from '../../components/SqlEditor';
import { api } from '../../utils/api';
import { ApiConfig, Dataset } from '../../types';

const flush = () => new Promise(resolve => setTimeout(resolve, 0));

const datasets: Dataset[] = [
  { name: 'customers', fields: ['customer_id', 'name'], totalCount: 2 } as Dataset
];

const apiConfig: ApiConfig = { baseUrl: 'mockServer', isMock: true };

describe('SQL Studio results (UI)', () => {
  let container: HTMLDivElement;
  let root: Root;

  beforeEach(async () => {
    container = document.createElement('div');
    document.body.appendChild(container);
    root = createRoot(container);
  });

  afterEach(() => {
    root.unmount();
    container.remove();
    vi.restoreAllMocks();
  });

  const renderEditor = async (props?: Partial<React.ComponentProps<typeof SqlEditor>>) => {
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

  const setQuery = async (value: string) => {
    const textarea = container.querySelector('textarea') as HTMLTextAreaElement | null;
    if (!textarea) throw new Error('SQL textarea not found');
    textarea.focus();
    textarea.value = value;
    textarea.selectionStart = value.length;
    textarea.selectionEnd = value.length;
    textarea.dispatchEvent(new Event('input', { bubbles: true }));
    textarea.dispatchEvent(new Event('change', { bubbles: true }));
    textarea.dispatchEvent(new KeyboardEvent('keyup', { key: 'a', bubbles: true }));
    await act(async () => { await flush(); await flush(); });
  };

  const clickRun = async () => {
    const runBtn = Array.from(container.querySelectorAll('button'))
      .find(b => (b.textContent || '').includes('Run Query')) as HTMLButtonElement | undefined;
    if (!runBtn) throw new Error('Run Query button not found');
    await act(async () => {
      runBtn.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
      await flush();
    });
  };

  it('renders results after a successful query', async () => {
    vi.spyOn(api, 'post').mockResolvedValueOnce({
      rows: [{ customer_id: 'C001', name: 'Alice' }],
      totalCount: 1,
      page: 1,
      pageSize: 50,
      columns: ['customer_id', 'name']
    });

    await renderEditor({ targetTable: 'customers', onClearTarget: vi.fn() });
    await clickRun();

    expect(container.textContent).toContain('C001');
    expect(container.textContent).toContain('customer_id');
  });

  it('shows error banner on query failure', async () => {
    vi.spyOn(api, 'post').mockRejectedValueOnce(new Error('boom'));

    await renderEditor({ targetTable: 'customers', onClearTarget: vi.fn() });
    await clickRun();

    expect(container.textContent).toContain('boom');
  });

  it('records successful runs in history', async () => {
    const onUpdateHistory = vi.fn();
    vi.spyOn(api, 'post').mockResolvedValueOnce({
      rows: [{ customer_id: 'C001', name: 'Alice' }],
      totalCount: 1,
      page: 1,
      pageSize: 50,
      columns: ['customer_id', 'name']
    });

    await renderEditor({ history: [], onUpdateHistory, targetTable: 'customers', onClearTarget: vi.fn() });
    await clickRun();

    expect(onUpdateHistory).toHaveBeenCalled();
    const [historyArg] = onUpdateHistory.mock.calls[0];
    expect(historyArg[0].status).toBe('success');
    expect(historyArg[0].query.toLowerCase()).toContain('select * from customers');
  });

  it('disables Run Query when query is empty', async () => {
    await renderEditor();
    const runBtn = Array.from(container.querySelectorAll('button'))
      .find(b => (b.textContent || '').includes('Run Query')) as HTMLButtonElement | undefined;
    expect(runBtn).toBeTruthy();
    expect(runBtn!.disabled).toBe(true);
  });

  it('enables Run Query when query has content', async () => {
    await renderEditor({ targetTable: 'customers', onClearTarget: vi.fn() });
    const runBtn = Array.from(container.querySelectorAll('button'))
      .find(b => (b.textContent || '').includes('Run Query')) as HTMLButtonElement | undefined;
    expect(runBtn).toBeTruthy();
    expect(runBtn!.disabled).toBe(false);
  });

  it('requests next page when pagination next is clicked', async () => {
    const postSpy = vi.spyOn(api, 'post').mockImplementation(async (_cfg, _path, payload) => {
      const page = payload.page || 1;
      return {
        rows: [{ customer_id: page === 1 ? 'C001' : 'C002', name: 'Alice' }],
        totalCount: 120,
        page,
        pageSize: 50,
        columns: ['customer_id', 'name']
      };
    });

    await renderEditor({ targetTable: 'customers', onClearTarget: vi.fn() });
    await clickRun();

    const nextBtn = container.querySelector('button[data-testid="page-next"]') as HTMLButtonElement | null;
    if (!nextBtn) throw new Error('Next page button not found');
    nextBtn.dispatchEvent(new MouseEvent('click', { bubbles: true }));
    await act(async () => { await flush(); await flush(); });

    expect(postSpy).toHaveBeenCalled();
    const lastCall = postSpy.mock.calls[postSpy.mock.calls.length - 1][2];
    expect(lastCall.page).toBe(2);
  });
});
