import React from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { act } from 'react';
import { createRoot, Root } from 'react-dom/client';

import { CommandEditor } from '../../components/CommandEditor';
import type { Command, Dataset, OperationNode } from '../../types';

const flush = () => new Promise((resolve) => setTimeout(resolve, 0));

const datasets: Dataset[] = [
  { id: 'ds_users', name: 'users', fields: ['id', 'name'], rows: [], totalCount: 2 } as Dataset,
  { id: 'ds_orders', name: 'orders', fields: ['id', 'user_id', 'amount'], rows: [], totalCount: 3 } as Dataset,
];

const tree: OperationNode = {
  id: 'root',
  type: 'operation',
  operationType: 'root',
  name: 'Root',
  enabled: true,
  commands: [],
  children: [
    {
      id: 'setup_1',
      type: 'operation',
      operationType: 'setup',
      name: 'Setup',
      enabled: true,
      commands: [
        { id: 'cmd_src_users', type: 'source', order: 1, config: { mainTable: 'users', alias: 'users', linkId: 'link_users', note: 'primary' } } as Command,
        { id: 'cmd_src_orders', type: 'source', order: 2, config: { mainTable: 'orders', alias: 'orders', linkId: 'link_orders', note: '' } } as Command,
      ],
      children: [
        {
          id: 'process_1',
          type: 'operation',
          operationType: 'process',
          name: 'Process 1',
          enabled: true,
          commands: [],
        },
      ],
    },
  ],
};

describe('CommandEditor setup and controls', () => {
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

  it('handles setup-mode editing, add/remove actions and project path navigation', async () => {
    const onUpdateCommands = vi.fn();
    const onUpdateName = vi.fn();
    const onViewPath = vi.fn();

    const setupCommands: Command[] = [
      { id: 'cmd_source_1', type: 'source', order: 1, config: { mainTable: '', alias: '', note: '', linkId: 'link_empty' } } as Command,
      { id: 'cmd_variable_1', type: 'define_variable', order: 2, config: { variableName: 'region', variableType: 'text', variableValue: 'APAC', note: '' } } as Command,
    ];

    await act(async () => {
      root.render(
        <CommandEditor
          operationId="setup_1"
          operationName="Setup Operation"
          operationType="setup"
          commands={setupCommands}
          datasets={datasets}
          inputSchema={{}}
          onUpdateCommands={onUpdateCommands}
          onUpdateName={onUpdateName}
          onUpdateType={vi.fn()}
          onViewPath={onViewPath}
          tree={tree}
          appearance={{ textSize: 13, textColor: '#333333', guideLineColor: '#dddddd', showGuideLines: true, showNodeIds: false, showOperationIds: true, showCommandIds: false, showDatasetIds: false }}
        />
      );
      await flush();
    });

    expect(container.textContent).toContain('Configured Sources');
    expect(container.textContent).toContain('Custom Variables');

    const nameInput = container.querySelector('input[placeholder="Operation Name"]') as HTMLInputElement | null;
    await act(async () => {
      if (nameInput) {
        const setter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')?.set;
        setter?.call(nameInput, 'Renamed Setup');
        nameInput.dispatchEvent(new Event('input', { bubbles: true }));
      }
      await flush();
    });
    expect(onUpdateName).toHaveBeenCalledWith('Renamed Setup');

    const viewPathButton = container.querySelector('button[title="View Logic Path"]');
    await act(async () => {
      viewPathButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });
    expect(onViewPath).toHaveBeenCalledTimes(1);

    const aliasInput = container.querySelector('input[placeholder="e.g. Users"]') as HTMLInputElement | null;
    await act(async () => {
      if (aliasInput) {
        const setter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')?.set;
        setter?.call(aliasInput, 'users_alias');
        aliasInput.dispatchEvent(new Event('input', { bubbles: true }));
      }
      await flush();
    });
    expect(onUpdateCommands).toHaveBeenCalled();
    expect(onUpdateCommands.mock.calls[0][1]).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          id: 'cmd_source_1',
          config: expect.objectContaining({ alias: 'users_alias' }),
        }),
      ])
    );

    const variableValueInput = container.querySelector('input[placeholder="Enter value"]') as HTMLInputElement | null;
    await act(async () => {
      if (variableValueInput) {
        const setter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')?.set;
        setter?.call(variableValueInput, 'EMEA');
        variableValueInput.dispatchEvent(new Event('input', { bubbles: true }));
      }
      await flush();
    });
    expect(onUpdateCommands).toHaveBeenCalled();

    const addSourceButton = Array.from(container.querySelectorAll('button')).find((button) => button.textContent?.includes('Add Data Source'));
    const addVariableButton = Array.from(container.querySelectorAll('button')).find((button) => button.textContent?.includes('Add Variable'));
    const removeSourceButton = container.querySelector('button[title="Remove Source"]');
    const removeVariableButton = container.querySelector('button[title="Remove Variable"]');
    await act(async () => {
      addSourceButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      addVariableButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      removeSourceButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      removeVariableButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });
    expect(onUpdateCommands).toHaveBeenCalled();
  });

  it('handles regular operation controls, SQL builder entry, step-level SQL generation and run guards', async () => {
    const onUpdateCommands = vi.fn();
    const onRun = vi.fn();
    const onViewPath = vi.fn();
    const onGenerateSql = vi.fn().mockResolvedValue('select * from users');

    const commands: Command[] = [
      {
        id: 'cmd_1',
        type: 'filter',
        order: 1,
        config: {
          dataSource: '',
          filterRoot: { id: 'group_1', type: 'group', logicalOperator: 'AND', conditions: [] },
        },
      } as Command,
      { id: 'cmd_2', type: 'sort', order: 2, config: { dataSource: 'link_users', field: 'id', ascending: true } } as Command,
      { id: 'cmd_3', type: 'save', order: 3, config: { dataSource: 'link_users', field: 'id', value: 'user_ids', distinct: true } } as Command,
      { id: 'cmd_4', type: 'view', order: 4, config: { dataSource: 'link_users', viewFields: [{ field: 'id' }], viewLimit: 10 } } as Command,
    ];

    await act(async () => {
      root.render(
        <CommandEditor
          operationId="process_1"
          operationName="Process Operation"
          operationType="process"
          commands={commands}
          datasets={datasets}
          inputSchema={{ id: 'number' }}
          onUpdateCommands={onUpdateCommands}
          onUpdateName={vi.fn()}
          onUpdateType={vi.fn()}
          onViewPath={onViewPath}
          onRun={onRun}
          onGenerateSql={onGenerateSql}
          tree={tree}
          canRun={true}
          appearance={{ textSize: 13, textColor: '#333333', guideLineColor: '#dddddd', showGuideLines: true, showNodeIds: false, showOperationIds: false, showCommandIds: true, showDatasetIds: false }}
        />
      );
      await flush();
    });

    const headerInput = container.querySelector('input[placeholder="Operation Name"]') as HTMLInputElement | null;
    expect(headerInput?.value).toBe('Process Operation');
    expect(container.textContent).toContain('Save Variable');
    expect(container.textContent).toContain('View / Select Table');

    const pinButton = container.querySelector('button[title="Pin step outline"]');
    await act(async () => {
      pinButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });
    expect(container.querySelector('button[title="Unpin step outline"]')).not.toBeNull();

    const operationRunButton = container.querySelector('button[title="Select a valid data source for each step before running"]');
    expect(operationRunButton).not.toBeNull();

    const stepPathButtons = Array.from(container.querySelectorAll('button[title="View Path Logic Synthesis"]'));
    const stepSqlButtons = Array.from(container.querySelectorAll('button[title="Generate SQL"]'));
    const stepRunButtons = Array.from(container.querySelectorAll('button[title="Run logic up to this step"], button[title="Select a data source for this step before running"]'));
    const collapseButton = container.querySelector('button[title="Collapse step"]');
    const deleteButton = Array.from(container.querySelectorAll('button')).find((button) => button.querySelector('svg') && !button.getAttribute('title') && !button.textContent?.trim());

    await act(async () => {
      stepPathButtons[0]?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      stepSqlButtons[1]?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      stepRunButtons[1]?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      collapseButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });

    expect(onViewPath).toHaveBeenCalledWith('cmd_1');
    expect(onGenerateSql).toHaveBeenCalledWith('cmd_2');
    expect(onRun).toHaveBeenCalledWith('cmd_2');
    expect(container.textContent).toContain('select * from users');

    await act(async () => {
      deleteButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });
    expect(onUpdateCommands).toHaveBeenCalled();

    await act(async () => {
      root.render(
        <CommandEditor
          operationId="process_2"
          operationName="Empty Process"
          operationType="process"
          commands={[]}
          datasets={datasets}
          inputSchema={{}}
          onUpdateCommands={onUpdateCommands}
          onUpdateName={vi.fn()}
          onUpdateType={vi.fn()}
          onViewPath={onViewPath}
          onRun={onRun}
          tree={tree}
          canRun={true}
        />
      );
      await flush();
    });

    const addFirstCommand = Array.from(container.querySelectorAll('div')).find((node) => node.textContent?.includes('Add your first command'));
    const buildFromSqlButton = Array.from(container.querySelectorAll('button')).find((button) => button.textContent?.includes('Build from SQL'));
    await act(async () => {
      addFirstCommand?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      buildFromSqlButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });
    expect(onUpdateCommands).toHaveBeenCalled();
    expect(container.textContent).toContain('Build Commands from SQL');
  });
});
