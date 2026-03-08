import React, { useState } from 'react';
import { Braces } from 'lucide-react';

interface VariableInserterProps {
    variables: string[];
    onInsert: (v: string) => void;
}

export const VariableInserter: React.FC<VariableInserterProps> = ({ variables, onInsert }) => {
    const [isOpen, setIsOpen] = useState(false);
    return (
        <div className="absolute right-1 top-1 z-10">
            <button 
                onClick={() => setIsOpen(!isOpen)}
                className="p-1 text-gray-400 hover:text-blue-600 rounded bg-transparent hover:bg-blue-50 transition-colors"
                title="Insert Variable"
            >
                <Braces className="w-3.5 h-3.5" />
            </button>
            {isOpen && (
                <>
                    <div className="fixed inset-0 z-20" onClick={() => setIsOpen(false)} />
                    <div className="absolute right-0 mt-1 w-48 bg-white border border-gray-200 rounded-lg shadow-xl z-30 py-1 max-h-48 overflow-y-auto animate-in fade-in zoom-in-95 duration-100">
                        <div className="px-3 py-1.5 text-[10px] font-bold text-gray-400 uppercase tracking-wider bg-gray-50 border-b border-gray-100 mb-1">
                            Available Variables
                        </div>
                        {variables.length === 0 ? (
                            <div className="px-3 py-2 text-xs text-gray-400 italic">No variables found</div>
                        ) : (
                            variables.map(v => (
                                <button
                                    key={v}
                                    onClick={() => { onInsert(v); setIsOpen(false); }}
                                    className="w-full text-left px-3 py-1.5 text-xs text-gray-700 hover:bg-blue-50 hover:text-blue-700 flex items-center"
                                >
                                    <span className="font-mono bg-gray-100 px-1 rounded mr-2 text-[10px] border border-gray-200">{v}</span>
                                </button>
                            ))
                        )}
                    </div>
                </>
            )}
        </div>
    );
};
