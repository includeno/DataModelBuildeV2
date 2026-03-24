import React from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { act } from 'react';
import { createRoot, Root } from 'react-dom/client';

import { DataPreview } from '../../components/DataPreview';
import type { ExecutionResult } from '../../types';

const flush = () => new Promise((resolve) => setTimeout(resolve, 0));

const setSelectValue = (element: HTMLSelectElement, value: string) => {
  const setter = Object.getOwnPropertyDescriptor(HTMLSelectElement.prototype, 'value')?.set;
  setter?.call(element, value);
  element.dispatchEvent(new Event('change', { bubbles: true }));
};

describe('DataPreview', () => {
  let container: HTMLDivElement;
  let root: Root;

  const data: ExecutionResult = {
    rows: [
      { id: 1, active: true, meta: { score: 9 }, note: 'A,B' },
      { id: 2, active: false, meta: null, note: 'plain' },
    ],
    totalCount: 7,
    columns: ['id', 'active', 'meta', 'note'],
    page: 2,
    pageSize: 2,
  };

  beforeEach(() => {
    container = document.createElement('div');
    document.body.appendChild(container);
    root = createRoot(container);
    Object.defineProperty(URL, 'createObjectURL', {
      configurable: true,
      writable: true,
      value: vi.fn(() => 'blob:test'),
    });
    Object.defineProperty(URL, 'revokeObjectURL', {
      configurable: true,
      writable: true,
      value: vi.fn(),
    });
  });

  afterEach(() => {
    root.unmount();
    container.remove();
    vi.restoreAllMocks();
  });

  it('renders loading and empty states', async () => {
    await act(async () => {
      root.render(
        <DataPreview
          data={null}
          loading={true}
          onRefresh={vi.fn()}
        />
      );
      await flush();
    });

    expect(container.textContent).toContain('Processing Data...');

    await act(async () => {
      root.render(
        <DataPreview
          data={{ rows: [], totalCount: 0, page: 1, pageSize: 50 }}
          loading={false}
          onRefresh={vi.fn()}
        />
      );
      await flush();
    });

    expect(container.textContent).toContain('No Data Available');
    expect(container.textContent).toContain('Run the analysis or select a table.');
  });

  it('refreshes, paginates, toggles columns, exports data and formats cells', async () => {
    const onRefresh = vi.fn();
    const onPageChange = vi.fn();
    const onUpdatePageSize = vi.fn();
    const onExportFull = vi.fn();
    const appendSpy = vi.spyOn(document.body, 'appendChild');
    const removeSpy = vi.spyOn(document.body, 'removeChild');
    const anchorClick = vi.fn();
    vi.spyOn(document, 'createElement').mockImplementation(((tagName: string) => {
      const element = document.createElementNS('http://www.w3.org/1999/xhtml', tagName) as HTMLElement;
      if (tagName === 'a') {
        Object.defineProperty(element, 'click', { value: anchorClick });
      }
      return element;
    }) as typeof document.createElement);

    await act(async () => {
      root.render(
        <DataPreview
          data={data}
          loading={false}
          pageSize={2}
          onRefresh={onRefresh}
          onPageChange={onPageChange}
          onUpdatePageSize={onUpdatePageSize}
          onExportFull={onExportFull}
          sourceId="orders"
        />
      );
      await flush();
    });

    expect(container.textContent).toContain('Preview');
    expect(container.textContent).toContain('7 Rows');
    expect(container.textContent).toContain('true');
    expect(container.textContent).toContain('false');
    expect(container.textContent).toContain('{"score":9}');
    expect(container.textContent).toContain('3');
    expect(container.textContent).toContain('4');

    const refreshButton = container.querySelector('button[title="Refresh Data"]');
    await act(async () => {
      refreshButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });
    expect(onRefresh).toHaveBeenCalledTimes(1);

    const pageSizeSelect = container.querySelector('select') as HTMLSelectElement | null;
    await act(async () => {
      if (pageSizeSelect) {
        setSelectValue(pageSizeSelect, '100');
      }
      await flush();
    });
    expect(onUpdatePageSize).toHaveBeenCalledWith(100);

    const columnButton = Array.from(container.querySelectorAll('button')).find((button) => button.textContent?.includes('Columns'));
    await act(async () => {
      columnButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });
    expect(container.textContent).toContain('Visible Columns');

    const metaToggle = Array.from(container.querySelectorAll('label')).find((label) => label.textContent?.includes('meta'));
    await act(async () => {
      metaToggle?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });
    expect(container.textContent).not.toContain('{"score":9}');

    const resetAll = Array.from(container.querySelectorAll('button')).find((button) => button.textContent?.includes('Reset All'));
    await act(async () => {
      resetAll?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });
    expect(container.textContent).toContain('{"score":9}');

    const exportButton = Array.from(container.querySelectorAll('button')).find((button) => button.textContent?.includes('Export'));
    await act(async () => {
      exportButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });

    const exportPageButton = Array.from(container.querySelectorAll('button')).find((button) => button.textContent?.includes('Export Current Page'));
    await act(async () => {
      exportPageButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });
    expect(URL.createObjectURL).toHaveBeenCalled();
    expect(anchorClick).toHaveBeenCalledTimes(1);
    expect(appendSpy).toHaveBeenCalled();
    expect(removeSpy).toHaveBeenCalled();

    await act(async () => {
      exportButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });
    const exportFullButton = Array.from(container.querySelectorAll('button')).find((button) => button.textContent?.includes('Export All Rows'));
    await act(async () => {
      exportFullButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });
    expect(onExportFull).toHaveBeenCalledTimes(1);

    const prevButton = container.querySelector('[data-testid="page-prev"]');
    const nextButton = container.querySelector('[data-testid="page-next"]');
    await act(async () => {
      prevButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      nextButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });
    expect(onPageChange).toHaveBeenCalledWith(1);
    expect(onPageChange).toHaveBeenCalledWith(3);
  });
});
