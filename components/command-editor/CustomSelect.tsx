import React, { useEffect, useRef, useState } from 'react';
import { ChevronDown, Check } from 'lucide-react';

export interface SelectOption {
    value: string;
    label: string;
    subLabel?: string;
    disabled?: boolean;
    icon?: React.ElementType;
}

interface CustomSelectProps {
    value: string;
    onChange: (val: string) => void;
    options: SelectOption[];
    placeholder?: string;
    icon?: React.ElementType;
    hasError?: boolean;
    className?: string;
}

export const CustomSelect: React.FC<CustomSelectProps> = ({
    value,
    onChange,
    options,
    placeholder = "Select...",
    icon: DefaultIcon,
    hasError,
    className = ""
}) => {
    const [isOpen, setIsOpen] = useState(false);
    const containerRef = useRef<HTMLDivElement>(null);

    useEffect(() => {
        const handleClickOutside = (event: MouseEvent) => {
            if (containerRef.current && !containerRef.current.contains(event.target as Node)) {
                setIsOpen(false);
            }
        };
        document.addEventListener('mousedown', handleClickOutside);
        return () => document.removeEventListener('mousedown', handleClickOutside);
    }, []);

    const selectedOption = options.find(o => o.value === value);
    const isPlaceholder = !value;
    const IconToUse = selectedOption?.icon || DefaultIcon;

    return (
        <div className={`relative w-full ${className}`} ref={containerRef}>
            <div
                onClick={() => setIsOpen(!isOpen)}
                className={`
                    w-full px-3 py-2.5 rounded-lg border flex items-center justify-between cursor-pointer transition-all bg-white
                    ${hasError ? 'border-red-300 focus:ring-2 focus:ring-red-100' : 'border-gray-200 hover:border-blue-400 focus:ring-2 focus:ring-blue-50'}
                    ${isOpen ? 'ring-2 ring-blue-100 border-blue-400' : 'shadow-sm'}
                `}
            >
                <div className="flex items-center overflow-hidden">
                    {IconToUse && <IconToUse className={`w-4 h-4 mr-2.5 shrink-0 ${selectedOption ? 'text-blue-600' : 'text-gray-400'}`} />}
                    <span className={`text-sm truncate font-medium ${isPlaceholder ? 'text-gray-400 italic' : 'text-gray-900'}`}>
                        {selectedOption ? selectedOption.label : placeholder}
                    </span>
                </div>
                <ChevronDown className={`w-4 h-4 text-gray-400 transition-transform duration-200 ${isOpen ? 'rotate-180' : ''}`} />
            </div>

            {isOpen && (
                <div className="absolute z-50 left-0 right-0 mt-1.5 bg-white border border-gray-100 rounded-xl shadow-xl max-h-60 overflow-y-auto animate-in fade-in zoom-in-95 duration-100 p-1">
                    {options.length === 0 ? (
                        <div className="px-4 py-3 text-xs text-gray-400 text-center italic">No options available</div>
                    ) : (
                        options.map((opt) => (
                            <div
                                key={opt.value}
                                role="option"
                                aria-disabled={opt.disabled}
                                title={opt.disabled ? 'Already selected in another source' : opt.label}
                                onMouseDown={(e) => {
                                    if (opt.disabled) e.preventDefault();
                                }}
                                onClick={() => {
                                    if (!opt.disabled) {
                                        onChange(opt.value);
                                        setIsOpen(false);
                                    }
                                }}
                                className={`
                                    flex items-center justify-between px-3 py-2.5 rounded-lg mb-0.5 transition-colors
                                    ${opt.disabled 
                                        ? 'opacity-50 cursor-not-allowed bg-gray-50' 
                                        : 'cursor-pointer hover:bg-blue-50 group'
                                    }
                                    ${opt.value === value ? 'bg-blue-50/80' : ''}
                                `}
                            >
                                <div className="flex flex-col min-w-0">
                                    <div className="flex items-center">
                                        {opt.icon && <opt.icon className={`w-3.5 h-3.5 mr-2 ${opt.value === value ? 'text-blue-700' : 'text-gray-400 group-hover:text-blue-600'}`} />}
                                        <span className={`text-sm font-medium ${opt.value === value ? 'text-blue-700' : 'text-gray-700 group-hover:text-blue-700'}`}>
                                            {opt.label}
                                        </span>
                                    </div>
                                    {opt.subLabel && (
                                        <span className="text-[10px] text-gray-400 mt-0.5 ml-5.5">
                                            {opt.subLabel}
                                        </span>
                                    )}
                                </div>
                                {opt.value === value && <Check className="w-4 h-4 text-blue-600" />}
                                {opt.disabled && opt.value !== value && (
                                    <span className="text-[10px] text-gray-400 bg-gray-100 px-1.5 py-0.5 rounded ml-2">Used</span>
                                )}
                            </div>
                        ))
                    )}
                </div>
            )}
        </div>
    );
};
