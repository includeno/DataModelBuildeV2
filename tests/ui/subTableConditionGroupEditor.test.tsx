import React, { act, useState } from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { createRoot, Root } from 'react-dom/client';

import { SubTableConditionGroupEditor } from '../../components/command-editor/SubTableConditionGroupEditor';
import type { SubTableConditionGroup } from '../../types';

const flush = () => new Promise((resolve) => setTimeout(resolve, 0));

const initialGroup: SubTableConditionGroup = {
  id: 'group_root',
  type: 'group',
  logicalOperator: 'AND',
  conditions: [
    {
      id: 'cond_missing',
      type: 'condition',
      field: 'missing_sub',
      operator: '=',
      mainField: 'missing_main',
    },
    {
      id: 'cond_unary',
      type: 'condition',
      field: 'status',
      operator: 'is_null',
      mainField: '',
    },
  ],
};

const Harness = ({ onRemove = () => {}, ...props }: Partial<React.ComponentProps<typeof SubTableConditionGroupEditor>> & {
  onRemove?: (id: string) => void;
}) => {
  const [group, setGroup] = useState<SubTableConditionGroup>(props.group || initialGroup);
  return (
    <SubTableConditionGroupEditor
      group={group}
      subFields={props.subFields || ['status', 'customer_id']}
      mainFields={props.mainFields || ['customer_id', 'expected_status']}
      onUpdate={(next) => {
        setGroup(next);
        props.onUpdate?.(next);
      }}
      onRemove={onRemove}
      isRoot={props.isRoot}
      subAliasLabel={props.subAliasLabel}
      mainAliasLabel={props.mainAliasLabel}
    />
  );
};

describe('SubTableConditionGroupEditor', () => {
  let container: HTMLDivElement;
  let root: Root;
  let renderVersion: number;

  beforeEach(() => {
    renderVersion = 0;
    container = document.createElement('div');
    document.body.appendChild(container);
    root = createRoot(container);
  });

  afterEach(() => {
    root.unmount();
    container.remove();
    vi.restoreAllMocks();
  });

  const renderEditor = async (props: React.ComponentProps<typeof Harness> = {}) => {
    renderVersion += 1;
    await act(async () => {
      root.render(<Harness key={renderVersion} {...props} />);
      await flush();
      await flush();
    });
  };

  it('shows missing field fallbacks and unary operators without main-field selectors', async () => {
    await renderEditor({
      subAliasLabel: ' ',
      mainAliasLabel: '',
      group: {
        id: 'group_missing',
        type: 'group',
        logicalOperator: 'AND',
        conditions: [
          {
            id: 'cond_missing',
            type: 'condition',
            field: 'missing_sub',
            operator: '=',
            mainField: 'missing_main',
          },
        ],
      },
    });

    const selects = Array.from(container.querySelectorAll('select')) as HTMLSelectElement[];
    const missingSubSelect = selects.find((select) => Array.from(select.options).some((opt) => (opt.textContent || '').includes('missing_sub (Missing)')));
    const missingMainSelect = selects.find((select) => Array.from(select.options).some((opt) => (opt.textContent || '').includes('missing_main (Missing)')));

    expect(missingSubSelect).toBeDefined();
    expect(missingMainSelect).toBeDefined();
    expect(Array.from(missingSubSelect!.options).some((opt) => (opt.textContent || '').includes('sub.status'))).toBe(true);
    expect(Array.from(missingMainSelect!.options).some((opt) => (opt.textContent || '').includes('main.expected_status'))).toBe(true);

    await renderEditor({
      subAliasLabel: ' ',
      mainAliasLabel: '',
      group: {
        id: 'group_unary',
        type: 'group',
        logicalOperator: 'AND',
        conditions: [
          {
            id: 'cond_unary_only',
            type: 'condition',
            field: 'status',
            operator: 'is_null',
            mainField: '',
          },
        ],
      },
    });

    const unarySelects = Array.from(container.querySelectorAll('select')) as HTMLSelectElement[];
    expect(unarySelects).toHaveLength(2);
    expect(Array.from(unarySelects[0].options).some((opt) => (opt.textContent || '').includes('sub.status'))).toBe(true);
    expect(Array.from(unarySelects).some((select) => Array.from(select.options).some((opt) => (opt.textContent || '').includes('Main Field...')))).toBe(false);
  });

  it('adds nested groups and rules, toggles operators and removes nested children', async () => {
    const onUpdate = vi.fn();
    await renderEditor({ onUpdate });

    const addRuleButton = Array.from(container.querySelectorAll('button')).find((button) => (button.textContent || '').includes('Add Rule')) as HTMLButtonElement | undefined;
    const addGroupButton = Array.from(container.querySelectorAll('button')).find((button) => (button.textContent || '').includes('Add Group')) as HTMLButtonElement | undefined;
    expect(addRuleButton).toBeDefined();
    expect(addGroupButton).toBeDefined();

    await act(async () => {
      addRuleButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      addGroupButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });

    expect(onUpdate).toHaveBeenCalled();
    expect(container.textContent).toContain('AND');

    const orButton = Array.from(container.querySelectorAll('button')).find((button) => (button.textContent || '').trim() === 'OR') as HTMLButtonElement | undefined;
    expect(orButton).toBeDefined();
    await act(async () => {
      orButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });

    const trashButtons = Array.from(container.querySelectorAll('button')).filter((button) => button.querySelector('svg'));
    const removableButtons = trashButtons.filter((button) => (button.textContent || '').trim() === '');
    expect(removableButtons.length).toBeGreaterThan(0);

    await act(async () => {
      removableButtons[removableButtons.length - 1]?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });

    const latestGroup = onUpdate.mock.calls[onUpdate.mock.calls.length - 1]?.[0] as SubTableConditionGroup;
    expect(latestGroup.logicalOperator).toBe('OR');
    expect(latestGroup.conditions.length).toBeGreaterThanOrEqual(2);
  });

  it('calls onRemove for non-root groups', async () => {
    const onRemove = vi.fn();
    await renderEditor({
      isRoot: false,
      onRemove,
      group: {
        id: 'child_group',
        type: 'group',
        logicalOperator: 'AND',
        conditions: [],
      },
    });

    const removeButton = Array.from(container.querySelectorAll('button')).find((button) =>
      button.querySelector('svg')
    ) as HTMLButtonElement | undefined;
    expect(removeButton).toBeDefined();

    await act(async () => {
      removeButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });

    expect(onRemove).toHaveBeenCalledWith('child_group');
  });
});
