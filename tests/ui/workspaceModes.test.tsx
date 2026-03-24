import React, { act } from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { createRoot, Root } from 'react-dom/client';

import { Workspace } from '../../components/Workspace';
import type { Command, Dataset, ExecutionResult, OperationNode } from '../../types';

const flush = () => new Promise((resolve) => setTimeout(resolve, 0));

const sqlEditorSpy = vi.fn();
const dataBrowserSpy = vi.fn();
const dataPreviewSpy = vi.fn();
const complexPreviewSpy = vi.fn();
const commandEditorSpy = vi.fn();

vi.mock('../../components/SqlEditor', () => ({
  SqlEditor: (props: any) => {
    sqlEditorSpy(props);
    return <div data-testid="workspace-sql-editor">SQL_EDITOR_{props.projectId}</div>;
  },
}));

vi.mock('../../components/DataBrowser', () => ({
  DataBrowser: (props: any) => {
    dataBrowserSpy(props);
    return (
      <button data-testid="workspace-data-browser" onClick={() => props.onSelectTable?.('orders')}>
        DATA_BROWSER_{props.selectedTable || 'none'}
      </button>
    );
  },
}));

vi.mock('../../components/DataPreview', () => ({
  DataPreview: (props: any) => {
    dataPreviewSpy(props);
    return (
      <div data-testid="workspace-data-preview">
        <button data-testid="workspace-refresh" onClick={() => props.onRefresh?.()}>
          refresh
        </button>
        <button data-testid="workspace-page-2" onClick={() => props.onPageChange?.(2)}>
          page2
        </button>
      </div>
    );
  },
}));

vi.mock('../../components/ComplexDataPreview', () => ({
  ComplexDataPreview: (props: any) => {
    complexPreviewSpy(props);
    return <div data-testid="workspace-complex-preview">COMPLEX_PREVIEW</div>;
  },
}));

vi.mock('../../components/CommandEditor', () => ({
  CommandEditor: (props: any) => {
    commandEditorSpy(props);
    return <div data-testid="workspace-command-editor">COMMAND_EDITOR</div>;
  },
}));

const selectedNode: OperationNode = {
  id: 'node_1',
  type: 'operation',
  operationType: 'process',
  name: 'Preview Node',
  enabled: true,
  pageSize: 25,
  commands: [
    { id: 'cmd_1', type: 'filter', order: 1, config: {} } as Command,
  ],
  children: [],
};

const tree: OperationNode = {
  id: 'root',
  type: 'operation',
  operationType: 'root',
  name: 'Root',
  enabled: true,
  commands: [],
  children: [selectedNode],
};

const previewData: ExecutionResult = {
  rows: [{ id: 1 }],
  totalCount: 1,
  columns: ['id'],
  page: 1,
  pageSize: 25,
};

describe('Workspace view modes', () => {
  let container: HTMLDivElement;
  let root: Root;

  beforeEach(() => {
    sqlEditorSpy.mockReset();
    dataBrowserSpy.mockReset();
    dataPreviewSpy.mockReset();
    complexPreviewSpy.mockReset();
    commandEditorSpy.mockReset();

    container = document.createElement('div');
    document.body.appendChild(container);
    root = createRoot(container);
  });

  afterEach(() => {
    root.unmount();
    container.remove();
  });

  const renderWorkspace = async (props: Partial<React.ComponentProps<typeof Workspace>> = {}) => {
    await act(async () => {
      root.render(
        <Workspace
          currentView="workflow"
          projectId="prj_1"
          apiConfig={{ baseUrl: 'mockServer', isMock: true }}
          targetSqlTable={null}
          targetDataTable={null}
          onSelectDataTable={() => {}}
          onClearTargetSqlTable={() => {}}
          selectedNode={selectedNode}
          datasets={[] as Dataset[]}
          inputFields={[]}
          inputSchema={{}}
          onUpdateCommands={() => {}}
          onUpdateName={() => {}}
          onUpdateType={() => {}}
          onViewPath={() => {}}
          isRightPanelOpen={true}
          onCloseRightPanel={() => {}}
          rightPanelWidth={320}
          onRightPanelResizeStart={() => {}}
          previewData={null}
          loading={false}
          onRefreshPreview={() => {}}
          onUpdatePageSize={() => {}}
          onExportFull={() => {}}
          isMobile={false}
          tree={tree}
          {...props}
        />
      );
      await flush();
      await flush();
    });
  };

  it('routes sql and data views with project context', async () => {
    const onClearTargetSqlTable = vi.fn();
    const onSelectDataTable = vi.fn();

    await renderWorkspace({
      currentView: 'sql',
      targetSqlTable: 'customers',
      onClearTargetSqlTable,
    });

    expect(container.textContent).toContain('SQL_EDITOR_prj_1');
    expect(sqlEditorSpy).toHaveBeenCalledWith(expect.objectContaining({
      projectId: 'prj_1',
      targetTable: 'customers',
      onClearTarget: onClearTargetSqlTable,
    }));

    await renderWorkspace({
      currentView: 'data',
      targetDataTable: 'customers',
      onSelectDataTable,
    });

    const browserButton = container.querySelector('[data-testid="workspace-data-browser"]') as HTMLButtonElement | null;
    expect(browserButton).not.toBeNull();
    expect(browserButton?.textContent).toContain('customers');
    await act(async () => {
      browserButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });
    expect(onSelectDataTable).toHaveBeenCalledWith('orders');
    expect(dataBrowserSpy).toHaveBeenCalledWith(expect.objectContaining({
      projectId: 'prj_1',
      selectedTable: 'customers',
    }));
  });

  it('shows placeholder content when no editable node is selected', async () => {
    await renderWorkspace({
      selectedNode: {
        ...tree,
        id: 'root',
      },
      previewData: null,
    });

    expect(container.textContent).toContain('Configuration Panel');
    expect(container.textContent).toContain('No Result');
    expect(commandEditorSpy).not.toHaveBeenCalled();
  });

  it('renders normal result panel controls, refresh and resize callbacks', async () => {
    const onCloseRightPanel = vi.fn();
    const onRightPanelResizeStart = vi.fn();
    const onClearPreview = vi.fn();
    const onRefreshPreview = vi.fn();

    await renderWorkspace({
      previewData,
      onCloseRightPanel,
      onRightPanelResizeStart,
      onClearPreview,
      onRefreshPreview,
      canRunOperation: true,
    });

    expect(container.querySelector('[data-testid="workspace-data-preview"]')).not.toBeNull();

    const closeResult = container.querySelector('button[title="Close Result"]') as HTMLButtonElement | null;
    expect(closeResult).not.toBeNull();
    await act(async () => {
      closeResult?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });
    expect(onClearPreview).toHaveBeenCalled();

    const closePanel = container.querySelector('button[title="Close Panel"]') as HTMLButtonElement | null;
    expect(closePanel).not.toBeNull();
    await act(async () => {
      closePanel?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });
    expect(onCloseRightPanel).toHaveBeenCalled();

    const refreshButton = container.querySelector('[data-testid="workspace-refresh"]') as HTMLButtonElement | null;
    expect(refreshButton).not.toBeNull();
    await act(async () => {
      refreshButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });
    expect(onRefreshPreview).toHaveBeenCalledWith(1, undefined);

    const resizer = Array.from(container.querySelectorAll('div')).find((node) =>
      (node.className || '').toString().includes('cursor-col-resize')
    ) as HTMLDivElement | undefined;
    expect(resizer).toBeDefined();
    await act(async () => {
      resizer?.dispatchEvent(new MouseEvent('mousedown', { bubbles: true }));
      await flush();
    });
    expect(onRightPanelResizeStart).toHaveBeenCalled();
  });
});
