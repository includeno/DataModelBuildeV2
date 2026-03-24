import React from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { act } from 'react';
import { createRoot, Root } from 'react-dom/client';

import { FilterGroupEditor } from '../../components/command-editor/FilterGroupEditor';
import { OperationTree } from '../../components/OperationTree';
import { Sidebar } from '../../components/Sidebar';
import type { AppearanceConfig, FilterGroup, OperationNode } from '../../types';

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

const appearance: AppearanceConfig = {
  textSize: 13,
  textColor: '#374151',
  guideLineColor: '#E5E7EB',
  showGuideLines: true,
  showNodeIds: true,
  showOperationIds: false,
  showCommandIds: false,
  showDatasetIds: true,
};

describe('FilterGroupEditor, OperationTree and Sidebar', () => {
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

  it('updates filter groups, variable mode, unary operators and removal actions', async () => {
    const onUpdate = vi.fn();
    const onRemove = vi.fn();
    const baseGroup: FilterGroup = {
      id: 'group_root',
      type: 'group',
      logicalOperator: 'AND',
      conditions: [
        {
          id: 'cond_main',
          type: 'condition',
          field: 'missing_field',
          operator: '=',
          value: '42',
          valueType: 'raw',
        },
        {
          id: 'group_child',
          type: 'group',
          logicalOperator: 'OR',
          conditions: [
            {
              id: 'cond_unary',
              type: 'condition',
              field: 'status',
              operator: 'is_null',
              value: '',
            },
          ],
        },
      ],
    };

    await act(async () => {
      root.render(
        <FilterGroupEditor
          group={baseGroup}
          activeSchema={{ status: 'string', amount: 'number' }}
          onUpdate={onUpdate}
          onRemove={onRemove}
          isRoot={false}
          availableVariables={['region_var', 'status_var']}
          getConditionIssue={(condition) => condition.field === 'missing_field' ? 'Unknown field' : undefined}
        />
      );
      await flush();
    });

    expect(container.textContent).toContain('Unknown field');
    expect(container.textContent).toContain('missing_field (Missing)');

    const buttons = Array.from(container.querySelectorAll('button'));
    const orButton = buttons.find((button) => button.textContent?.trim() === 'OR');
    await act(async () => {
      orButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });
    expect(onUpdate).toHaveBeenCalledWith(expect.objectContaining({ logicalOperator: 'OR' }));

    const selects = Array.from(container.querySelectorAll('select')) as HTMLSelectElement[];
    await act(async () => {
      setSelectValue(selects[0], 'status');
      setSelectValue(selects[1], 'contains');
      setSelectValue(selects[2], 'variable');
      await flush();
    });

    const variableInput = container.querySelector('input[placeholder*="输入或选择变量名"]') as HTMLInputElement | null;
    await act(async () => {
      if (variableInput) {
        setInputValue(variableInput, 'region_var');
      }
      await flush();
    });
    expect(onUpdate).toHaveBeenCalled();

    const addRuleButton = buttons.find((button) => button.textContent?.includes('Add Rule'));
    const addGroupButton = buttons.find((button) => button.textContent?.includes('Add Group'));
    await act(async () => {
      addRuleButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      addGroupButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });
    expect(onUpdate).toHaveBeenCalledTimes(6);

    const trashButtons = Array.from(container.querySelectorAll('button')).filter((button) => !button.textContent?.trim());
    await act(async () => {
      trashButtons[0]?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      trashButtons[1]?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });
    expect(onRemove).toHaveBeenCalledWith('group_root');
    expect(onUpdate).toHaveBeenCalled();
  });

  it('supports tree actions, nested expansion, move buttons and active editor badges', async () => {
    const onSelect = vi.fn();
    const onToggleEnabled = vi.fn();
    const onAddChild = vi.fn();
    const onDelete = vi.fn();
    const onMoveNode = vi.fn();
    const onAnalyzeOverlap = vi.fn();

    const node: OperationNode = {
      id: 'node_parent',
      type: 'operation',
      operationType: 'setup',
      name: 'Setup Node',
      enabled: false,
      commands: [],
      children: [
        {
          id: 'node_child',
          type: 'operation',
          operationType: 'process',
          name: 'Child Node',
          enabled: true,
          commands: [],
        },
      ],
    };

    await act(async () => {
      root.render(
        <OperationTree
          node={node}
          selectedId="node_parent"
          onSelect={onSelect}
          onToggleEnabled={onToggleEnabled}
          onAddChild={onAddChild}
          onDelete={onDelete}
          onMoveNode={onMoveNode}
          onAnalyzeOverlap={onAnalyzeOverlap}
          appearance={appearance}
          level={1}
          parentId="root"
          index={1}
          siblingCount={3}
          activeEditorsByNode={{ node_parent: ['Alice'], node_child: ['Bob', 'Carol'] }}
        />
      );
      await flush();
    });

    expect(container.textContent).toContain('Setup Node');
    expect(container.textContent).toContain('node_parent');
    expect(container.textContent).toContain('Alice 编辑中');
    expect(container.querySelector('[title="Click to Enable this operation"]')).not.toBeNull();

    const row = container.querySelector('[title="Setup Node"]') as HTMLDivElement | null;
    await act(async () => {
      row?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });
    expect(onSelect).toHaveBeenCalledWith('node_parent');

    const enableButton = container.querySelector('button[title="Click to Enable this operation"]');
    const addChildButton = container.querySelector('button[title="Add Child"]');
    const moveUpButton = container.querySelector('button[title="Move Up"]');
    const moveDownButton = container.querySelector('button[title="Move Down"]');
    const deleteButton = container.querySelector('button[title="Delete"]');
    await act(async () => {
      enableButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      addChildButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      moveUpButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      moveDownButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      deleteButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });
    expect(onToggleEnabled).toHaveBeenCalledWith('node_parent');
    expect(onAddChild).toHaveBeenCalledWith('node_parent');
    expect(onMoveNode).toHaveBeenCalledWith('node_parent', 'up');
    expect(onMoveNode).toHaveBeenCalledWith('node_parent', 'down');
    expect(onDelete).toHaveBeenCalledWith('node_parent');

    await act(async () => {
      root.render(
        <OperationTree
          node={{ ...node, enabled: true }}
          selectedId="node_child"
          onSelect={onSelect}
          onToggleEnabled={onToggleEnabled}
          onAddChild={onAddChild}
          onDelete={onDelete}
          onMoveNode={onMoveNode}
          onAnalyzeOverlap={onAnalyzeOverlap}
          appearance={appearance}
          globalAction="collapse"
          collapseTrigger={1}
          level={0}
          parentId={null}
          index={0}
          siblingCount={1}
          activeEditorsByNode={{ node_child: ['Bob', 'Carol'] }}
        />
      );
      await flush();
    });

    expect(container.textContent).not.toContain('Child Node');

    await act(async () => {
      root.render(
        <OperationTree
          node={{ ...node, enabled: true }}
          selectedId="node_child"
          onSelect={onSelect}
          onToggleEnabled={onToggleEnabled}
          onAddChild={onAddChild}
          onDelete={onDelete}
          onMoveNode={onMoveNode}
          onAnalyzeOverlap={onAnalyzeOverlap}
          appearance={appearance}
          globalAction="expand"
          expandTrigger={1}
          level={0}
          parentId={null}
          index={0}
          siblingCount={1}
          activeEditorsByNode={{ node_child: ['Bob', 'Carol'] }}
        />
      );
      await flush();
    });

    expect(container.textContent).toContain('2 人编辑中');
    const overlapButton = container.querySelector('button[title="Overlap"]');
    const disableButton = container.querySelector('button[title="Disable"]');
    await act(async () => {
      overlapButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      disableButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });
    expect(onAnalyzeOverlap).toHaveBeenCalledWith('node_parent');
    expect(onToggleEnabled).toHaveBeenCalledWith('node_parent');
  });

  it('handles sidebar workflow and dataset actions, collapsed mode and import restrictions', async () => {
    const onSelectNode = vi.fn();
    const onToggleEnabled = vi.fn();
    const onAddChild = vi.fn();
    const onDeleteNode = vi.fn();
    const onMoveNode = vi.fn();
    const onImportClick = vi.fn();
    const onOpenTableInSql = vi.fn();
    const onOpenTableInData = vi.fn();
    const onExportOperations = vi.fn();
    const onImportOperations = vi.fn();
    const onAnalyzeOverlap = vi.fn();
    const onOpenSchema = vi.fn();
    const onDeleteDataset = vi.fn();

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
          name: 'Setup One',
          enabled: true,
          commands: [],
          children: [],
        },
      ],
    };

    await act(async () => {
      root.render(
        <Sidebar
          width={260}
          currentView="workflow"
          projectId="prj_sidebar"
          tree={tree}
          datasets={[{ id: 'ds_1', name: 'customers', rows: [], fields: ['id'] }]}
          selectedNodeId="setup_1"
          onSelectNode={onSelectNode}
          onToggleEnabled={onToggleEnabled}
          onAddChild={onAddChild}
          onDeleteNode={onDeleteNode}
          onMoveNode={onMoveNode}
          onImportClick={onImportClick}
          onOpenTableInSql={onOpenTableInSql}
          onOpenTableInData={onOpenTableInData}
          onExportOperations={onExportOperations}
          onImportOperations={onImportOperations}
          onAnalyzeOverlap={onAnalyzeOverlap}
          onOpenSchema={onOpenSchema}
          onDeleteDataset={onDeleteDataset}
          remoteEditorsByNode={{ setup_1: ['Alice'] }}
          appearance={appearance}
        />
      );
      await flush();
    });

    const collapseAll = container.querySelector('button[title="Collapse All"]');
    const expandAll = container.querySelector('button[title="Expand All"]');
    const exportButton = container.querySelector('button[title="Export"]');
    const importButton = container.querySelector('button[title="Import"]');
    const addSetupButton = container.querySelector('button[title="Add Data Setup"]');
    await act(async () => {
      collapseAll?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      expandAll?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      exportButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      importButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      addSetupButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });
    expect(onExportOperations).toHaveBeenCalledTimes(1);
    expect(onAddChild).toHaveBeenCalledWith('root');

    const fileInput = container.querySelector('input[type="file"]') as HTMLInputElement | null;
    const file = new File(['{}'], 'ops.json', { type: 'application/json' });
    Object.defineProperty(fileInput!, 'files', { value: [file], configurable: true });
    await act(async () => {
      fileInput?.dispatchEvent(new Event('change', { bubbles: true }));
      await flush();
    });
    expect(onImportOperations).toHaveBeenCalledWith(file);

    const datasetLabel = Array.from(container.querySelectorAll('div')).find((node) => node.textContent === 'customers');
    const datasetRow = datasetLabel?.closest('.group') as HTMLDivElement | null;
    const queryButton = datasetRow?.querySelector('button[title="Query"]') ?? null;
    const schemaButton = datasetRow?.querySelector('button[title="Settings"]') ?? null;
    const deleteButton = datasetRow?.querySelector('button[title="Delete"]') ?? null;
    await act(async () => {
      datasetLabel?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      queryButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      schemaButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      deleteButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });
    expect(onOpenTableInSql).toHaveBeenCalledWith('customers');
    expect(onOpenSchema).toHaveBeenCalledWith('customers');
    expect(onDeleteDataset).toHaveBeenCalledWith('customers');

    await act(async () => {
      root.render(
        <Sidebar
          width={260}
          currentView="data"
          projectId="prj_sidebar"
          tree={tree}
          datasets={[{ id: 'ds_1', name: 'customers', rows: [], fields: ['id'] }]}
          selectedNodeId="setup_1"
          onSelectNode={onSelectNode}
          onToggleEnabled={onToggleEnabled}
          onAddChild={onAddChild}
          onDeleteNode={onDeleteNode}
          onMoveNode={onMoveNode}
          onImportClick={onImportClick}
          onOpenTableInSql={onOpenTableInSql}
          onOpenTableInData={onOpenTableInData}
          onExportOperations={onExportOperations}
          onImportOperations={onImportOperations}
          onAnalyzeOverlap={onAnalyzeOverlap}
          onOpenSchema={onOpenSchema}
          onDeleteDataset={onDeleteDataset}
          appearance={appearance}
        />
      );
      await flush();
    });

    const datasetRowInDataView = Array.from(container.querySelectorAll('div')).find((node) => node.textContent === 'customers');
    await act(async () => {
      datasetRowInDataView?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });
    expect(onOpenTableInData).toHaveBeenCalledWith('customers');

    await act(async () => {
      root.render(
        <Sidebar
          width={260}
          currentView="data"
          tree={tree}
          datasets={[{ id: 'ds_1', name: 'customers', rows: [], fields: ['id'] }]}
          selectedNodeId="setup_1"
          onSelectNode={onSelectNode}
          onToggleEnabled={onToggleEnabled}
          onAddChild={onAddChild}
          onDeleteNode={onDeleteNode}
          onMoveNode={onMoveNode}
          onImportClick={onImportClick}
          onOpenTableInSql={onOpenTableInSql}
          onOpenTableInData={onOpenTableInData}
          onExportOperations={onExportOperations}
          onImportOperations={onImportOperations}
          onAnalyzeOverlap={onAnalyzeOverlap}
          onOpenSchema={onOpenSchema}
          onDeleteDataset={onDeleteDataset}
          appearance={appearance}
        />
      );
      await flush();
    });
    expect(container.textContent).toContain('Create a project to manage data.');
  });
});
