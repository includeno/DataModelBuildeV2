import React from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { act } from 'react-dom/test-utils';
import { createRoot, Root } from 'react-dom/client';

import { VariableInserter } from '../../components/command-editor/VariableInserter';
import { VariableSuggestionInput } from '../../components/command-editor/VariableSuggestionInput';
import { StepOutline } from '../../components/command-editor/StepOutline';

const flush = () => new Promise((resolve) => setTimeout(resolve, 0));

const setInputValue = (element: HTMLInputElement, value: string) => {
  const setter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')?.set;
  setter?.call(element, value);
  element.dispatchEvent(new Event('input', { bubbles: true }));
};

describe('command editor widgets', () => {
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

  it('opens variable inserter, handles empty state and inserts a variable', async () => {
    const onInsert = vi.fn();

    await act(async () => {
      root.render(<VariableInserter variables={[]} onInsert={onInsert} />);
      await flush();
    });

    const trigger = container.querySelector('button');
    await act(async () => {
      trigger?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });
    expect(container.textContent).toContain('Available Variables');
    expect(container.textContent).toContain('No variables found');

    await act(async () => {
      root.unmount();
      container.innerHTML = '';
      root = createRoot(container);
      root.render(<VariableInserter variables={['customer_ids', 'order_ids']} onInsert={onInsert} />);
      await flush();
    });
    await act(async () => {
      const reopenTrigger = container.querySelector('button');
      reopenTrigger?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });

    await act(async () => {
      const variableButton = Array.from(container.querySelectorAll('button')).find((button) => button.textContent?.includes('order_ids'));
      variableButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });

    expect(onInsert).toHaveBeenCalledWith('order_ids');
  });

  it('shows variable suggestions, updates value and closes on outside click', async () => {
    const onChange = vi.fn();

    await act(async () => {
      root.render(<VariableSuggestionInput value="seed" onChange={onChange} variables={['alpha', 'beta']} />);
      await flush();
    });

    const input = container.querySelector('input') as HTMLInputElement;
    await act(async () => {
      const chevronTrigger = container.querySelector('.cursor-pointer.text-gray-400');
      chevronTrigger?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });
    expect(container.textContent).toContain('Select Variable');
    expect(container.textContent).toContain('alpha');

    await act(async () => {
      setInputValue(input, 'manual');
      await flush();
    });
    expect(onChange).toHaveBeenCalledWith('manual');

    const option = Array.from(container.querySelectorAll('span')).find((node) => node.textContent === 'beta');
    await act(async () => {
      option?.parentElement?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });
    expect(onChange).toHaveBeenLastCalledWith('beta');

    await act(async () => {
      const chevronTrigger = container.querySelector('.cursor-pointer.text-gray-400');
      chevronTrigger?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });
    expect(container.textContent).toContain('Select Variable');

    await act(async () => {
      document.body.dispatchEvent(new MouseEvent('mousedown', { bubbles: true }));
      await flush();
    });
    expect(container.textContent).not.toContain('Select Variable');
  });

  it('renders outline actions and jumps to steps', async () => {
    const onJump = vi.fn();
    const onCollapseAll = vi.fn();
    const onExpandAll = vi.fn();

    await act(async () => {
      root.render(
        <StepOutline
          commands={[
            { id: 'cmd_1', type: 'filter', order: 0, config: {} },
            { id: 'cmd_2', type: 'sort', order: 1, config: { field: 'name', ascending: true } },
          ]}
          onJump={onJump}
          onCollapseAll={onCollapseAll}
          onExpandAll={onExpandAll}
          isPinned={true}
        />
      );
      await flush();
    });

    expect(container.innerHTML).toContain('sticky');
    expect(container.textContent).toContain('#1');
    expect(container.textContent).toContain('#2');
    expect(container.textContent).toContain('Collapse All');
    expect(container.textContent).toContain('Expand All');

    const buttons = Array.from(container.querySelectorAll('button'));
    await act(async () => {
      buttons[0].dispatchEvent(new MouseEvent('click', { bubbles: true }));
      buttons[2].dispatchEvent(new MouseEvent('click', { bubbles: true }));
      buttons[3].dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });

    expect(onJump).toHaveBeenCalledWith('cmd_1');
    expect(onCollapseAll).toHaveBeenCalledTimes(1);
    expect(onExpandAll).toHaveBeenCalledTimes(1);
  });
});
