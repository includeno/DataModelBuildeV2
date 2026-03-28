import React from 'react';
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { act } from 'react';
import { createRoot, Root } from 'react-dom/client';
import { DataImportModal } from '../../components/DataImport';
import { api } from '../../utils/api';

const flush = () => new Promise(resolve => setTimeout(resolve, 0));

const dispatchDrop = (target: Element, file: File) => {
  const dropEvent = new Event('drop', { bubbles: true, cancelable: true }) as Event & { dataTransfer?: { files: File[] } };
  Object.defineProperty(dropEvent, 'dataTransfer', {
    configurable: true,
    value: { files: [file] },
  });
  target.dispatchEvent(new Event('dragenter', { bubbles: true, cancelable: true }));
  target.dispatchEvent(new Event('dragover', { bubbles: true, cancelable: true }));
  target.dispatchEvent(dropEvent);
};

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

  it('supports drag-drop, escape close, reserved names and successful upload', async () => {
    const onClose = vi.fn();
    const onImport = vi.fn();
    const mockPreviewResult = {
      previewToken: 'tok_preview_abc',
      fields: ['id', 'name'],
      fieldTypes: { id: { type: 'number' as const }, name: { type: 'string' as const } },
      rows: [{ id: 1, name: 'Alice' }],
      totalCount: 1,
      cleanReport: { duplicateRowCount: 0, missingValueCounts: {}, outlierCounts: {}, whitespaceFieldCount: 0 },
    };
    vi.spyOn(api, 'uploadPreview').mockResolvedValue(mockPreviewResult);
    vi.spyOn(api, 'upload').mockResolvedValue({
      id: 'ds_people',
      name: 'people_dataset',
      fields: ['id', 'name'],
      rows: [{ id: 1, name: 'Alice' }],
      totalCount: 1,
    } as any);

    await act(async () => {
      root.render(
        <DataImportModal
          isOpen={true}
          onClose={onClose}
          onImport={onImport}
          projectId="prj_demo"
          apiConfig={{ baseUrl: 'http://localhost:8000', isMock: false }}
        />
      );
      await flush();
    });

    const dropzone = container.querySelector('[data-testid="data-import-dropzone"]') as HTMLDivElement | null;
    const file = new File(['id,name\n1,Alice'], 'people.csv', { type: 'text/csv' });
    await act(async () => {
      if (dropzone) {
        dispatchDrop(dropzone, file);
      }
      await flush();
    });

    const nameInput = container.querySelector('input[placeholder="Enter a name for this dataset"]') as HTMLInputElement | null;
    expect(nameInput?.value).toBe('people');

    // Reserved name validation
    await act(async () => {
      nameInput && Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')?.set?.call(nameInput, 'select');
      nameInput?.dispatchEvent(new Event('input', { bubbles: true }));
      await flush();
    });
    expect(container.textContent).toContain("reserved keyword");

    // Set valid name
    await act(async () => {
      nameInput && Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')?.set?.call(nameInput, 'people_dataset');
      nameInput?.dispatchEvent(new Event('input', { bubbles: true }));
      await flush();
    });

    // Step 1 → click "Next: Preview" to trigger preview
    const nextButton = Array.from(container.querySelectorAll('button')).find((b) => b.textContent?.includes('Next: Preview'));
    await act(async () => {
      nextButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });

    // Now in preview step — click "Import Dataset"
    const importButton = Array.from(container.querySelectorAll('button')).find((b) => b.textContent?.includes('Import Dataset'));
    await act(async () => {
      importButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });

    expect(api.upload).toHaveBeenCalledWith(expect.anything(), '/projects/prj_demo/upload', expect.any(FormData));
    expect(onImport).toHaveBeenCalledWith(expect.objectContaining({ id: 'ds_people', name: 'people_dataset' }));
    expect(onClose).toHaveBeenCalledTimes(1);
    onClose.mockClear();

    await act(async () => {
      document.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape', bubbles: true }));
      await flush();
    });
    expect(onClose).toHaveBeenCalledTimes(1);
  });

  it('shows upload errors and lets the user clear selected file', async () => {
    const onClose = vi.fn();
    const mockPreviewResult = {
      previewToken: 'tok_err_abc',
      fields: ['id', 'name'],
      fieldTypes: { id: { type: 'number' as const }, name: { type: 'string' as const } },
      rows: [{ id: 1, name: 'Alice' }],
      totalCount: 1,
      cleanReport: { duplicateRowCount: 0, missingValueCounts: {}, outlierCounts: {}, whitespaceFieldCount: 0 },
    };
    vi.spyOn(api, 'uploadPreview').mockResolvedValue(mockPreviewResult);
    vi.spyOn(api, 'upload').mockRejectedValue(new Error('upload failed'));
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});

    await act(async () => {
      root.render(
        <DataImportModal
          isOpen={true}
          onClose={onClose}
          onImport={() => {}}
          sessionId="sess_demo"
          apiConfig={{ baseUrl: 'http://localhost:8000', isMock: false }}
        />
      );
      await flush();
    });

    const fileInput = container.querySelector('[data-testid="data-import-file-input"]') as HTMLInputElement | null;
    const file = new File(['id,name\n1,Alice'], 'people.csv', { type: 'text/csv' });
    await act(async () => {
      const fileList = {
        0: file,
        length: 1,
        item: (index: number) => (index === 0 ? file : null),
      } as unknown as FileList;
      Object.defineProperty(fileInput!, 'files', { value: fileList, configurable: true });
      fileInput!.dispatchEvent(new Event('change', { bubbles: true }));
      await flush();
    });

    // Step 1 → "Next: Preview"
    const nextButton = Array.from(container.querySelectorAll('button')).find((b) => b.textContent?.includes('Next: Preview'));
    await act(async () => {
      nextButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });

    // Step 2 → "Import Dataset" → triggers upload error
    const importButton = Array.from(container.querySelectorAll('button')).find((b) => b.textContent?.includes('Import Dataset'));
    await act(async () => {
      importButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });

    expect(container.textContent).toContain('upload failed');
    expect(errorSpy).toHaveBeenCalled();

    // Go back to select step via the back (ChevronLeft) button
    const backButton = Array.from(container.querySelectorAll('button')).find(
      (b) => !b.textContent?.trim() && b.querySelector('svg')
        && b !== container.querySelector('[aria-label="Close Import Data Source"]')
    );
    await act(async () => {
      backButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });

    // Now in select step with file still loaded — click X to clear
    const clearButton = Array.from(container.querySelectorAll('button')).find(
      (b) => !b.textContent?.trim() && b.querySelector('svg')
    );
    await act(async () => {
      clearButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });
    expect(container.querySelector('[data-testid="data-import-dropzone"]')).not.toBeNull();
  });
});
