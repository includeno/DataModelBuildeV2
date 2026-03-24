import React from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { act } from 'react';
import { createRoot, Root } from 'react-dom/client';

import { DatasetSchemaModal } from '../../components/DatasetSchemaModal';
import type { Dataset } from '../../types';

const flush = () => new Promise((resolve) => setTimeout(resolve, 0));

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

describe('DatasetSchemaModal', () => {
  let container: HTMLDivElement;
  let root: Root;

  const dataset: Dataset = {
    id: 'ds_orders',
    name: 'Orders',
    fields: ['created_at', 'amount', 'meta'],
    rows: [
      {
        created_at: '2026.03.19',
        amount: 42,
        meta: { channel: 'web' },
      },
    ],
    fieldTypes: {
      created_at: { type: 'date', format: 'YYYY.MM.DD' },
      amount: { type: 'string' },
    },
  };

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

  it('renders dataset fields, supports custom date formats and saves updated schema', async () => {
    const onClose = vi.fn();
    const onSave = vi.fn().mockResolvedValue(undefined);

    await act(async () => {
      root.render(
        <DatasetSchemaModal
          isOpen={true}
          onClose={onClose}
          dataset={dataset}
          onSave={onSave}
        />
      );
      await flush();
    });

    expect(container.textContent).toContain('Dataset Schema Configuration');
    expect(container.textContent).toContain('Orders');
    expect(container.textContent).toContain('created_at');
    expect(container.textContent).toContain('2026.03.19');
    expect(container.textContent).toContain('"channel": "web"');

    let customInput = container.querySelector('input[placeholder="e.g. YYYY/MM/DD"]') as HTMLInputElement | null;
    expect(customInput?.value).toBe('YYYY.MM.DD');

    const selects = Array.from(container.querySelectorAll('select')) as HTMLSelectElement[];
    const createdAtTypeSelect = selects[0];
    const createdAtFormatSelect = selects[1];
    const amountTypeSelect = selects[2];

    await act(async () => {
      setSelectValue(createdAtTypeSelect, 'timestamp');
      setSelectValue(createdAtFormatSelect, 'YYYY/MM/DD');
      setSelectValue(amountTypeSelect, 'number');
      await flush();
    });

    expect(container.querySelector('input[placeholder="e.g. YYYY/MM/DD"]')).toBeNull();

    await act(async () => {
      setSelectValue(createdAtFormatSelect, 'custom');
      await flush();
    });

    customInput = container.querySelector('input[placeholder="e.g. YYYY/MM/DD"]') as HTMLInputElement | null;
    expect(customInput?.value).toBe('YYYY/MM/DD');

    await act(async () => {
      if (customInput) {
        setInputValue(customInput, 'DD.MM.YYYY');
      }
      await flush();
    });

    const saveButton = Array.from(container.querySelectorAll('button')).find((button) => button.textContent?.includes('Save Schema'));
    await act(async () => {
      saveButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });

    expect(onSave).toHaveBeenCalledWith(
      'ds_orders',
      expect.objectContaining({
        created_at: expect.objectContaining({ type: 'timestamp', format: 'DD.MM.YYYY' }),
        amount: expect.objectContaining({ type: 'number' }),
        meta: expect.objectContaining({ type: 'string' }),
      })
    );
    expect(onClose).toHaveBeenCalledTimes(1);
  });

  it('handles escape close and save failures gracefully', async () => {
    const onClose = vi.fn();
    const onSave = vi.fn().mockRejectedValue(new Error('boom'));
    const alertSpy = vi.spyOn(window, 'alert').mockImplementation(() => {});

    await act(async () => {
      root.render(
        <DatasetSchemaModal
          isOpen={true}
          onClose={onClose}
          dataset={dataset}
          onSave={onSave}
        />
      );
      await flush();
    });

    await act(async () => {
      document.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape', bubbles: true }));
      await flush();
    });
    expect(onClose).toHaveBeenCalledTimes(1);

    const saveButton = Array.from(container.querySelectorAll('button')).find((button) => button.textContent?.includes('Save Schema'));
    await act(async () => {
      saveButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });

    expect(onSave).toHaveBeenCalled();
    expect(alertSpy).toHaveBeenCalledWith('Failed to save schema');
    expect(onClose).toHaveBeenCalledTimes(1);
  });
});
