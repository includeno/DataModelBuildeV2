import React, { useEffect, useRef, useState } from 'react';
import { Braces, ChevronDown } from 'lucide-react';
import { baseInputStyles } from './constants';

interface VariableSuggestionInputProps {
    value: string;
    onChange: (val: string) => void;
    variables: string[];
}

export const VariableSuggestionInput: React.FC<VariableSuggestionInputProps> = ({ value, onChange, variables }) => {
    const [showSuggestions, setShowSuggestions] = useState(false);
    const containerRef = useRef<HTMLDivElement>(null);
    const inputRef = useRef<HTMLInputElement>(null);

    useEffect(() => {
        const handleClickOutside = (event: MouseEvent) => {
            if (containerRef.current && !containerRef.current.contains(event.target as Node)) {
                setShowSuggestions(false);
            }
        };
        document.addEventListener('mousedown', handleClickOutside);
        return () => document.removeEventListener('mousedown', handleClickOutside);
    }, []);
    
    return (
        <div className="relative w-full" ref={containerRef}>
            <input 
                ref={inputRef}
                className={`${baseInputStyles} py-1 px-2 pr-6`} 
                placeholder="Variable Name" 
                value={value} 
                onChange={(e) => onChange(e.target.value)}
                onFocus={() => setShowSuggestions(true)}
            />
            <div 
                className="absolute right-1 top-1/2 -translate-y-1/2 cursor-pointer text-gray-400 hover:text-blue-500 p-1"
                onClick={() => {
                    setShowSuggestions(!showSuggestions);
                    if (!showSuggestions) {
                        inputRef.current?.focus();
                    }
                }}
            >
                <ChevronDown className="w-3 h-3" />
            </div>
            
            {showSuggestions && (
                <div className="absolute left-0 right-0 z-[100] mt-1 bg-white border border-gray-200 rounded-md shadow-lg max-h-48 overflow-y-auto animate-in fade-in zoom-in-95 duration-100 min-w-[120px]">
                    <div className="px-2 py-1.5 text-[10px] font-bold text-gray-400 uppercase bg-gray-50 border-b border-gray-100 sticky top-0">Select Variable</div>
                    {variables.length === 0 ? (
                        <div className="px-3 py-2 text-xs text-gray-400 italic">No variables available</div>
                    ) : (
                        variables.map(v => (
                            <div 
                                key={v}
                                className="px-3 py-2 text-xs text-gray-700 hover:bg-blue-50 hover:text-blue-700 cursor-pointer flex items-center transition-colors"
                                onClick={() => { onChange(v); setShowSuggestions(false); }}
                            >
                                <Braces className="w-3 h-3 mr-2 text-blue-400 shrink-0" />
                                <span className="truncate font-medium">{v}</span>
                            </div>
                        ))
                    )}
                </div>
            )}
        </div>
    );
};
