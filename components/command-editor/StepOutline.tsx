import React from 'react';
import { Command } from '../../types';
import { COMMAND_LABELS } from './constants';

interface StepOutlineProps {
    commands: Command[];
    onJump: (id: string) => void;
    onCollapseAll: () => void;
    onExpandAll: () => void;
    isPinned?: boolean;
}

export const StepOutline: React.FC<StepOutlineProps> = ({ commands, onJump, onCollapseAll, onExpandAll, isPinned = false }) => {
    const containerClass = [
        'mb-4 p-3 bg-white border border-gray-200 rounded-lg shadow-sm flex items-center justify-between',
        isPinned ? 'sticky top-0 z-10' : ''
    ].join(' ');

    return (
        <div className={containerClass}>
            <div className="flex items-center space-x-2 overflow-x-auto no-scrollbar">
                {commands.map((c, idx) => (
                    <button
                        key={c.id}
                        onClick={() => onJump(c.id)}
                        className="px-2.5 py-1 rounded-md text-xs font-medium border border-gray-200 bg-gray-50 hover:bg-blue-50 hover:border-blue-300 text-gray-700 shrink-0"
                        title={`Jump to step #${idx + 1}`}
                    >
                        <span className="font-mono text-[10px] text-gray-500 mr-1">#{idx + 1}</span>
                        {COMMAND_LABELS[c.type] || c.type}
                    </button>
                ))}
            </div>
            <div className="flex items-center space-x-2 shrink-0 ml-3">
                <button
                    onClick={onCollapseAll}
                    className="text-xs font-medium text-gray-600 hover:text-gray-900 px-2 py-1 rounded-md hover:bg-gray-100 border border-gray-200"
                >
                    Collapse All
                </button>
                <button
                    onClick={onExpandAll}
                    className="text-xs font-medium text-gray-600 hover:text-gray-900 px-2 py-1 rounded-md hover:bg-gray-100 border border-gray-200"
                >
                    Expand All
                </button>
            </div>
        </div>
    );
};
