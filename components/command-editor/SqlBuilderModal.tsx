import React from 'react';
import { X } from 'lucide-react';
import { Button } from '../Button';
import { Command } from '../../types';

interface SqlBuilderModalProps {
    isOpen: boolean;
    sqlInput: string;
    onSqlInputChange: (value: string) => void;
    onParse: () => void;
    onApply: () => void;
    onClose: () => void;
    warnings: string[];
    error?: string | null;
    commands: Command[];
    renderSummary: (cmd: Command) => string;
}

export const SqlBuilderModal: React.FC<SqlBuilderModalProps> = ({
    isOpen,
    sqlInput,
    onSqlInputChange,
    onParse,
    onApply,
    onClose,
    warnings,
    error,
    commands,
    renderSummary
}) => {
    if (!isOpen) return null;

    return (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm p-4">
            <div className="bg-white rounded-xl shadow-2xl w-full max-w-3xl max-h-[85vh] overflow-hidden flex flex-col">
                <div className="flex items-center justify-between px-6 py-4 border-b border-gray-100 bg-gray-50">
                    <div>
                        <h3 className="text-lg font-bold text-gray-900">Build Commands from SQL</h3>
                        <p className="text-xs text-gray-500">Paste SQL and generate a command list you can edit before applying.</p>
                    </div>
                    <button onClick={onClose} className="text-gray-400 hover:text-gray-600 transition-colors">
                        <X className="w-5 h-5" />
                    </button>
                </div>

                <div className="p-6 space-y-4 overflow-y-auto">
                    <div>
                        <label className="block text-xs font-bold text-gray-500 uppercase mb-2">SQL</label>
                        <textarea
                            value={sqlInput}
                            onChange={(e) => onSqlInputChange(e.target.value)}
                            placeholder="SELECT * FROM my_table WHERE status = 'active' ORDER BY created_at DESC LIMIT 50"
                            className="w-full min-h-[140px] border border-gray-200 rounded-lg p-3 font-mono text-sm focus:ring-2 focus:ring-blue-100 focus:border-blue-400"
                        />
                    </div>

                    {error && (
                        <div className="text-sm text-red-600 bg-red-50 border border-red-100 rounded-md px-3 py-2">
                            {error}
                        </div>
                    )}

                    {warnings.length > 0 && (
                        <div className="text-xs text-yellow-700 bg-yellow-50 border border-yellow-100 rounded-md px-3 py-2">
                            {warnings.map((w, i) => (
                                <div key={i}>{w}</div>
                            ))}
                        </div>
                    )}

                    <div className="border-t border-gray-100 pt-3">
                        <label className="block text-xs font-bold text-gray-500 uppercase mb-2">Generated Commands</label>
                        {commands.length === 0 ? (
                            <div className="text-xs text-gray-400 italic">No commands parsed yet.</div>
                        ) : (
                            <div className="space-y-2">
                                {commands.map((cmd, idx) => (
                                    <div key={cmd.id} className="flex items-center justify-between px-3 py-2 border border-gray-100 rounded-md bg-gray-50 text-xs">
                                        <span className="font-mono text-gray-400">#{idx + 1}</span>
                                        <span className="flex-1 ml-3 text-gray-700">{renderSummary(cmd)}</span>
                                        <span className="text-[10px] text-gray-400">{cmd.type}</span>
                                    </div>
                                ))}
                            </div>
                        )}
                    </div>
                </div>

                <div className="p-4 bg-gray-50 border-t border-gray-200 flex justify-between items-center">
                    <Button variant="secondary" onClick={onParse}>Parse</Button>
                    <div className="flex items-center space-x-2">
                        <Button variant="secondary" onClick={onClose}>Cancel</Button>
                        <Button variant="primary" onClick={onApply} disabled={commands.length === 0}>Apply</Button>
                    </div>
                </div>
            </div>
        </div>
    );
};
