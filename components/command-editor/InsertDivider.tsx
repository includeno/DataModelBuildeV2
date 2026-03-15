import React from 'react';
import { Plus, Wand2 } from 'lucide-react';

export const InsertDivider = ({
    onInsert,
    onOpenBuilder,
    index
}: {
    onInsert: (i: number) => void;
    onOpenBuilder?: (i: number) => void;
    index: number;
}) => {
    return (
        <div className="relative h-5 group flex items-center justify-center my-1">
            <div className="absolute inset-x-8 top-1/2 -translate-y-1/2 h-px bg-blue-200 opacity-0 group-hover:opacity-100 transition-opacity duration-200"></div>
            <div className="relative z-10 flex items-center space-x-1 opacity-0 group-hover:opacity-100 transition-all duration-200">
                <button
                    onClick={() => onInsert(index)}
                    className="bg-white border border-blue-200 text-blue-600 rounded-full p-0.5 shadow-sm transition-all duration-200 hover:bg-blue-50 hover:scale-110"
                    title="Insert Step Here"
                >
                    <Plus className="w-3.5 h-3.5" />
                </button>
                {onOpenBuilder && (
                    <button
                        onClick={() => onOpenBuilder(index)}
                        className="bg-white border border-blue-200 text-blue-600 rounded-full p-0.5 shadow-sm transition-all duration-200 hover:bg-blue-50 hover:scale-110"
                        title="Insert from SQL Builder"
                    >
                        <Wand2 className="w-3.5 h-3.5" />
                    </button>
                )}
            </div>
        </div>
    );
};
