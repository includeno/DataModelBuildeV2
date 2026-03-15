import React from 'react';
import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { act } from 'react-dom/test-utils';
import { createRoot, Root } from 'react-dom/client';
import { SqlEditor } from '../../components/SqlEditor';
import { ApiConfig, Dataset } from '../../types';

const flush = () => new Promise(resolve => setTimeout(resolve, 0));

const datasets: Dataset[] = [
  {
    name: 'customers',
    fields: ['customer_id', 'name', 'email'],
    totalCount: 1
  } as Dataset,
  {
    name: 'orders',
    fields: ['order_id', 'customer_id', 'amount'],
    totalCount: 1
  } as Dataset
];

const apiConfig: ApiConfig = { baseUrl: 'mockServer', isMock: true };

describe('SQL Studio autocomplete (UI)', () => {
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

  const getTextarea = () => {
    const el = container.querySelector('textarea');
    if (!el) throw new Error('SQL textarea not found');
    return el as HTMLTextAreaElement;
  };

  const setInput = async (value: string) => {
    const textarea = getTextarea();
    textarea.focus();
    textarea.value = value;
    textarea.selectionStart = value.length;
    textarea.selectionEnd = value.length;
    textarea.dispatchEvent(new Event('input', { bubbles: true }));
    textarea.dispatchEvent(new KeyboardEvent('keyup', { key: 'a', bubbles: true }));
    await act(async () => { await flush(); });
  };

  it('suggests SQL keyword and applies suggestion on click', async () => {
    await setInput('SEL');

    const buttons = Array.from(container.querySelectorAll('button')) as HTMLButtonElement[];
    const selectBtn = buttons.find(b => (b.textContent || '').trim() === 'SELECT');
    expect(selectBtn).toBeTruthy();

    selectBtn!.dispatchEvent(new MouseEvent('mousedown', { bubbles: true }));
    await act(async () => { await flush(); });

    const textarea = getTextarea();
    expect(textarea.value).toBe('SELECT ');
  });

  it('suggests table-qualified fields for table prefix', async () => {
    await setInput('customers.n');
    const buttons = Array.from(container.querySelectorAll('button')) as HTMLButtonElement[];
    const suggestion = buttons.find(b => (b.textContent || '').trim() === 'customers.name');
    expect(suggestion).toBeTruthy();
  });
});
