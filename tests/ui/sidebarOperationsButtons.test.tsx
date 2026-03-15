import React from 'react';
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { act } from 'react-dom/test-utils';
import { createRoot, Root } from 'react-dom/client';
import { Sidebar } from '../../components/Sidebar';
import { AppearanceConfig, OperationNode } from '../../types';

const flush = () => new Promise(resolve => setTimeout(resolve, 0));

const tree: OperationNode = {
  id: 'root',
  type: 'operation',
  operationType: 'root',
  name: 'Project',
  enabled: true,
  commands: [],
  children: [
    {
      id: 'setup_1',
      type: 'operation',
      operationType: 'setup',
      name: 'Data Setup',
      enabled: true,
      commands: [],
      children: []
    }
  ]
};

const appearance: AppearanceConfig = {
  textSize: 13,
  textColor: '#374151',
  guideLineColor: '#E5E7EB',
  showGuideLines: true,
  showNodeIds: false,
  showOperationIds: false,
  showCommandIds: false,
  showDatasetIds: false
};

describe('Sidebar operations import/export buttons', () => {
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
  });

  it('shows export button and calls handler on click', async () => {
    const onExportOperations = vi.fn();
    const onImportOperations = vi.fn();

    await act(async () => {
      root.render(
        <Sidebar
          width={260}
          currentView="workflow"
          sessionId="sess_1"
          tree={tree}
          datasets={[]}
          selectedNodeId="setup_1"
          onSelectNode={() => {}}
          onToggleEnabled={() => {}}
          onAddChild={() => {}}
          onDeleteNode={() => {}}
          onImportClick={() => {}}
          onOpenTableInSql={() => {}}
          onExportOperations={onExportOperations}
          onImportOperations={onImportOperations}
          appearance={appearance}
        />
      );
      await flush();
    });

    const exportBtn = container.querySelector('button[title="Export"]') as HTMLButtonElement | null;
    expect(exportBtn).not.toBeNull();

    await act(async () => {
      exportBtn!.click();
      await flush();
    });

    expect(onExportOperations).toHaveBeenCalledTimes(1);
  });

  it('calls import handler with selected json file', async () => {
    const onExportOperations = vi.fn();
    const onImportOperations = vi.fn();

    await act(async () => {
      root.render(
        <Sidebar
          width={260}
          currentView="workflow"
          sessionId="sess_1"
          tree={tree}
          datasets={[]}
          selectedNodeId="setup_1"
          onSelectNode={() => {}}
          onToggleEnabled={() => {}}
          onAddChild={() => {}}
          onDeleteNode={() => {}}
          onImportClick={() => {}}
          onOpenTableInSql={() => {}}
          onExportOperations={onExportOperations}
          onImportOperations={onImportOperations}
          appearance={appearance}
        />
      );
      await flush();
    });

    const fileInput = container.querySelector('input[type="file"]') as HTMLInputElement | null;
    expect(fileInput).not.toBeNull();
    const file = new File(['{"type":"dmb_operations"}'], 'ops.json', { type: 'application/json' });
    Object.defineProperty(fileInput!, 'files', { value: [file], configurable: true });

    await act(async () => {
      fileInput!.dispatchEvent(new Event('change', { bubbles: true }));
      await flush();
    });

    expect(onImportOperations).toHaveBeenCalledTimes(1);
    expect(onImportOperations).toHaveBeenCalledWith(file);
  });
});
