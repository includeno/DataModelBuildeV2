import React from 'react';
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { act } from 'react-dom/test-utils';
import { createRoot, Root } from 'react-dom/client';
import { DataImportModal } from '../../components/DataImport';

const flush = () => new Promise(resolve => setTimeout(resolve, 0));

describe('DataImportModal', () => {
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

  it('handles file selection and fills default dataset name', async () => {
    await act(async () => {
      root.render(
        <DataImportModal
          isOpen={true}
          onClose={() => {}}
          onImport={() => {}}
          sessionId="sess_demo"
          apiConfig={{ baseUrl: 'mockServer', isMock: true }}
        />
      );
      await flush();
    });

    const dropzone = container.querySelector('[data-testid="data-import-dropzone"]') as HTMLDivElement | null;
    const fileInput = container.querySelector('[data-testid="data-import-file-input"]') as HTMLInputElement | null;

    expect(dropzone).toBeTruthy();
    expect(fileInput).toBeTruthy();

    const file = new File(['id,name\n1,Alice'], 'people.csv', { type: 'text/csv' });

    await act(async () => {
      const fileList = {
        0: file,
        length: 1,
        item: (index: number) => (index === 0 ? file : null)
      } as unknown as FileList;
      Object.defineProperty(fileInput!, 'files', { value: fileList, configurable: true });
      fileInput!.dispatchEvent(new Event('change', { bubbles: true }));
      await flush();
    });

    const nameInput = container.querySelector('input[placeholder=\"Enter a name for this dataset\"]') as HTMLInputElement | null;
    expect(nameInput).toBeTruthy();
    expect(nameInput!.value).toBe('people');
  });

  it('opens file picker when clicking dropzone', async () => {
    const inputClickSpy = vi.spyOn(HTMLInputElement.prototype, 'click');

    await act(async () => {
      root.render(
        <DataImportModal
          isOpen={true}
          onClose={() => {}}
          onImport={() => {}}
          sessionId="sess_demo"
          apiConfig={{ baseUrl: 'mockServer', isMock: true }}
        />
      );
      await flush();
    });

    const dropzone = container.querySelector('[data-testid="data-import-dropzone"]') as HTMLDivElement | null;
    expect(dropzone).toBeTruthy();

    await act(async () => {
      dropzone!.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });

    expect(inputClickSpy).toHaveBeenCalled();
  });
});
