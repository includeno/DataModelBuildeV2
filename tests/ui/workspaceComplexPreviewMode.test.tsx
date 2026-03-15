import React, { useState } from 'react';
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { act } from 'react-dom/test-utils';
import { createRoot, Root } from 'react-dom/client';
import { Workspace } from '../../components/Workspace';
import { Command, ExecutionResult, OperationNode } from '../../types';

const flush = () => new Promise(resolve => setTimeout(resolve, 0));

vi.mock('../../components/CommandEditor', () => ({
  CommandEditor: (props: any) => (
    <div>
      <button data-testid="run-step-before" onClick={() => props.onRun?.('cmd_filter_1')}>
        Run Step Before
      </button>
      <button data-testid="run-full" onClick={() => props.onRun?.()}>
        Run Full
      </button>
    </div>
  )
}));

vi.mock('../../components/DataPreview', () => ({
  DataPreview: (props: any) => (
    <div data-testid="data-preview">
      DATA_PREVIEW
      <button data-testid="data-refresh" onClick={() => props.onRefresh?.()}>
        Refresh
      </button>
    </div>
  )
}));

vi.mock('../../components/ComplexDataPreview', () => ({
  ComplexDataPreview: () => <div data-testid="complex-preview">COMPLEX_PREVIEW</div>
}));

vi.mock('../../components/SqlEditor', () => ({
  SqlEditor: () => <div>SQL_EDITOR</div>
}));

vi.mock('../../components/DataBrowser', () => ({
  DataBrowser: () => <div>DATA_BROWSER</div>
}));

const selectedNode: OperationNode = {
  id: 'op_1',
  type: 'operation',
  operationType: 'process',
  name: 'Op 1',
  enabled: true,
  commands: [
    {
      id: 'cmd_filter_1',
      type: 'filter',
      order: 1,
      config: { dataSource: 'link_users', filterRoot: { id: 'g1', type: 'group', logicalOperator: 'AND', conditions: [] } }
    } as Command,
    {
      id: 'cmd_complex_1',
      type: 'multi_table',
      order: 2,
      config: { subTables: [] }
    } as Command
  ],
  children: []
};

const tree: OperationNode = {
  id: 'root',
  type: 'operation',
  operationType: 'root',
  name: 'Root',
  enabled: true,
  commands: [],
  children: [selectedNode]
};

const Harness = () => {
  const [previewData, setPreviewData] = useState<ExecutionResult | null>(null);
  return (
    <Workspace
      currentView="workflow"
      sessionId="sess_1"
      apiConfig={{ baseUrl: 'mockServer', isMock: true }}
      targetSqlTable={null}
      onClearTargetSqlTable={() => {}}
      selectedNode={selectedNode}
      datasets={[]}
      inputFields={[]}
      inputSchema={{}}
      onUpdateCommands={() => {}}
      onUpdateName={() => {}}
      onUpdateType={() => {}}
      onViewPath={() => {}}
      isRightPanelOpen={true}
      onCloseRightPanel={() => {}}
      rightPanelWidth={400}
      onRightPanelResizeStart={() => {}}
      previewData={previewData}
      loading={false}
      onRefreshPreview={(page, _commandId) => {
        setPreviewData({
          rows: [{ id: 1 }],
          totalCount: 1,
          columns: ['id'],
          page: page || 1,
          pageSize: 50,
          activeViewId: 'main'
        });
      }}
      canRunOperation={true}
      onUpdatePageSize={() => {}}
      onExportFull={() => {}}
      isMobile={false}
      tree={tree}
    />
  );
};

describe('Workspace complex preview mode', () => {
  let container: HTMLDivElement;
  let root: Root;

  beforeEach(async () => {
    container = document.createElement('div');
    document.body.appendChild(container);
    root = createRoot(container);
    await act(async () => {
      root.render(<Harness />);
      await flush();
    });
  });

  afterEach(() => {
    root.unmount();
    container.remove();
  });

  it('uses normal data preview when running to a step before complex view', async () => {
    const runStepBtn = container.querySelector('[data-testid="run-step-before"]') as HTMLButtonElement | null;
    expect(runStepBtn).not.toBeNull();

    await act(async () => {
      runStepBtn!.click();
      await flush();
    });

    expect(container.querySelector('[data-testid="data-preview"]')).not.toBeNull();
    expect(container.querySelector('[data-testid="complex-preview"]')).toBeNull();
  });

  it('uses complex preview when running full operation', async () => {
    const runFullBtn = container.querySelector('[data-testid="run-full"]') as HTMLButtonElement | null;
    expect(runFullBtn).not.toBeNull();

    await act(async () => {
      runFullBtn!.click();
      await flush();
    });

    expect(container.querySelector('[data-testid="complex-preview"]')).not.toBeNull();
  });

  it('keeps normal preview on refresh after running to a step before complex view', async () => {
    const runStepBtn = container.querySelector('[data-testid="run-step-before"]') as HTMLButtonElement | null;
    expect(runStepBtn).not.toBeNull();

    await act(async () => {
      runStepBtn!.click();
      await flush();
    });

    expect(container.querySelector('[data-testid="data-preview"]')).not.toBeNull();
    expect(container.querySelector('[data-testid="complex-preview"]')).toBeNull();

    const refreshBtn = container.querySelector('[data-testid="data-refresh"]') as HTMLButtonElement | null;
    expect(refreshBtn).not.toBeNull();

    await act(async () => {
      refreshBtn!.click();
      await flush();
    });

    expect(container.querySelector('[data-testid="data-preview"]')).not.toBeNull();
    expect(container.querySelector('[data-testid="complex-preview"]')).toBeNull();
  });
});
