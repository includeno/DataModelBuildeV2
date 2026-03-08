import React from 'react';
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { act } from 'react-dom/test-utils';
import { createRoot, Root } from 'react-dom/client';
import { SqlBuilderModal } from '../../components/command-editor/SqlBuilderModal';
import { Command } from '../../types';

const flush = () => new Promise(resolve => setTimeout(resolve, 0));

const baseProps = {
  isOpen: true,
  sqlInput: 'select * from t',
  onSqlInputChange: vi.fn(),
  onParse: vi.fn(),
  onApply: vi.fn(),
  onClose: vi.fn(),
  warnings: [],
  error: null as string | null,
  availableSourceAliases: [] as any[],
  existingCommands: [] as Command[],
  renderSummary: (cmd: Command) => cmd.type
};

describe('SqlBuilderModal validation (UI)', () => {
  let container: HTMLDivElement;
  let root: Root;

  beforeEach(async () => {
    container = document.createElement('div');
    document.body.appendChild(container);
    root = createRoot(container);
  });

  afterEach(() => {
    root.unmount();
    container.remove();
    vi.restoreAllMocks();
  });

  const renderModal = async (commands: Command[], datasets: any[]) => {
    await act(async () => {
      root.render(
        <SqlBuilderModal
          {...baseProps}
          commands={commands}
          datasets={datasets}
          onUpdateCommands={vi.fn()}
        />
      );
      await flush();
    });
  };

  const getApplyButton = () => {
    const buttons = Array.from(container.querySelectorAll('button')) as HTMLButtonElement[];
    const apply = buttons.find(b => (b.textContent || '').trim() === 'Apply');
    if (!apply) throw new Error('Apply button not found');
    return apply;
  };

  it('disables Apply when dataset is missing', async () => {
    const commands: Command[] = [
      {
        id: 'cmd_filter',
        type: 'filter',
        order: 0,
        config: {
          dataSource: 'missing_dataset',
          filterRoot: {
            id: 'g1',
            type: 'group',
            logicalOperator: 'AND',
            conditions: [
              { id: 'c1', type: 'condition', field: 'name', operator: '=', value: 'Alice' }
            ]
          }
        }
      }
    ];

    await renderModal(commands, []);
    const apply = getApplyButton();
    expect(apply.disabled).toBe(true);
  });

  it('disables Apply when a field is missing', async () => {
    const commands: Command[] = [
      {
        id: 'cmd_filter',
        type: 'filter',
        order: 0,
        config: {
          dataSource: 'customers',
          filterRoot: {
            id: 'g1',
            type: 'group',
            logicalOperator: 'AND',
            conditions: [
              { id: 'c1', type: 'condition', field: 'missing_field', operator: '=', value: 'x' }
            ]
          }
        }
      }
    ];

    await renderModal(commands, [
      { name: 'customers', fields: ['id', 'name'], totalCount: 1 }
    ]);
    const apply = getApplyButton();
    expect(apply.disabled).toBe(true);
  });

  it('enables Apply when dataset and fields are valid', async () => {
    const commands: Command[] = [
      {
        id: 'cmd_filter',
        type: 'filter',
        order: 0,
        config: {
          dataSource: 'customers',
          filterRoot: {
            id: 'g1',
            type: 'group',
            logicalOperator: 'AND',
            conditions: [
              { id: 'c1', type: 'condition', field: 'name', operator: '=', value: 'Alice' }
            ]
          }
        }
      }
    ];

    await renderModal(commands, [
      { name: 'customers', fields: ['id', 'name'], totalCount: 1 }
    ]);
    const apply = getApplyButton();
    expect(apply.disabled).toBe(false);
  });
});
