import React from 'react';
import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { act } from 'react-dom/test-utils';
import { createRoot, Root } from 'react-dom/client';
import { SqlEditor } from '../../components/SqlEditor';
import { ApiConfig, Dataset } from '../../types';

const flush = () => new Promise(resolve => setTimeout(resolve, 0));

const datasets: Dataset[] = [
  { name: 'customers', fields: ['customer_id', 'name', 'email'], totalCount: 1 } as Dataset,
  { name: 'orders', fields: ['order_id', 'customer_id', 'amount'], totalCount: 1 } as Dataset
];

const apiConfig: ApiConfig = { baseUrl: 'mockServer', isMock: true };

describe('SQL Studio autocomplete (aliases)', () => {
  let container: HTMLDivElement;
  let root: Root;

  beforeEach(async () => {
    container = document.createElement('div');
    document.body.appendChild(container);
    root = createRoot(container);

    await act(async () => {
      root.render(
        <SqlEditor
          sessionId="sess_test"
          apiConfig={apiConfig}
          datasets={datasets}
        />
      );
      await flush();
    });
  });

  afterEach(() => {
    root.unmount();
    container.remove();
  });

  const setInput = async (value: string, cursor: number) => {
    const textarea = container.querySelector('textarea') as HTMLTextAreaElement | null;
    if (!textarea) throw new Error('SQL textarea not found');
    textarea.focus();
    textarea.value = value;
    textarea.selectionStart = cursor;
    textarea.selectionEnd = cursor;
    textarea.dispatchEvent(new Event('input', { bubbles: true }));
    textarea.dispatchEvent(new KeyboardEvent('keyup', { key: 'a', bubbles: true }));
    await act(async () => { await flush(); });
  };

  const getSuggestions = () => {
    const containerEl = container.querySelector('[data-testid="sql-suggestions"]');
    if (!containerEl) return [];
    return Array.from(containerEl.querySelectorAll('button')).map(btn => (btn.textContent || '').trim());
  };

  it('suggests fields for FROM alias', async () => {
    const sql = 'select c. from customers c';
    await setInput(sql, 'select c.'.length);
    expect(getSuggestions()).toContain('c.name');
  });

  it('suggests fields for JOIN alias', async () => {
    const sql = 'select o. from customers c join orders o on c.customer_id = o.customer_id';
    await setInput(sql, 'select o.'.length);
    expect(getSuggestions()).toContain('o.order_id');
  });

  it('includes alias in keyword/table suggestion list', async () => {
    const sql = 'select c from customers c';
    await setInput(sql, 'select c'.length);
    expect(getSuggestions()).toContain('c');
  });

  it('shows no alias suggestions for unknown alias', async () => {
    const sql = 'select x. from customers c';
    await setInput(sql, 'select x.'.length);
    expect(getSuggestions().some(s => s.startsWith('x.'))).toBe(false);
  });
});
