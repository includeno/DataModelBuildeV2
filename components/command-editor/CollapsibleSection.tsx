import React, { useState } from 'react';
import { ChevronDown } from 'lucide-react';

interface CollapsibleSectionProps {
    title: string;
    icon: any;
    count: number;
    children: React.ReactNode;
    color?: string;
}

export const CollapsibleSection: React.FC<CollapsibleSectionProps> = ({ title, icon: Icon, count, children, color = "text-blue-500" }) => {
    const [isOpen, setIsOpen] = useState(true);

    return (
        <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden flex flex-col transition-all duration-200 group">
            <div 
                className="px-6 py-4 flex justify-between items-center cursor-pointer hover:bg-gray-50 transition-colors select-none"
                onClick={() => setIsOpen(!isOpen)}
            >
                <div className="flex items-center space-x-3">
                    <div className={`p-1.5 rounded-lg transition-colors ${isOpen ? 'bg-gray-100' : 'bg-transparent'}`}>
                        <Icon className={`w-4 h-4 ${color}`} />
                    </div>
                    <div className="flex items-center space-x-2">
                        <h3 className="text-sm font-bold text-gray-800 uppercase tracking-wider">{title}</h3>
                        {count > 0 && (
                            <span className="text-[10px] font-bold bg-gray-100 text-gray-500 px-2 py-0.5 rounded-full border border-gray-200">
                                {count}
                            </span>
                        )}
                    </div>
                </div>
                <div className={`transform transition-transform duration-200 text-gray-400 group-hover:text-gray-600 ${isOpen ? 'rotate-180' : ''}`}>
                    <ChevronDown className="w-4 h-4" />
                </div>
            </div>
            <div 
                className={`transition-all duration-300 ease-in-out overflow-hidden bg-white ${isOpen ? 'max-h-[800px] opacity-100 border-t border-gray-100' : 'max-h-0 opacity-0'}`}
            >
                <div className="p-6 overflow-y-auto max-h-[60vh] space-y-4 custom-scrollbar">
                    {children}
                </div>
            </div>
        </div>
    );
};
