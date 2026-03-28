import { DataType } from '../../types';

export const PYTHON_TEMPLATE = `def transform(row):
    # Available: np, pd, math, datetime, re
    # Name must be 'transform', return the calculated value
    val = row.get('id', 0)
    return val * 1.1`;

export const COMMAND_LABELS: Record<string, string> = {
    filter: 'Filter',
    join: 'Join',
    sort: 'Sort',
    transform: 'Mapping',
    group: 'Group',
    save: 'Save Var',
    view: 'View',
    multi_table: 'Complex View',
    validate: 'Validate'
};

export const OPERATORS: Record<string, { value: string; label: string }[]> = {
    string: [
        { value: '=', label: 'Equals' },
        { value: '!=', label: 'Not Equals' },
        { value: 'contains', label: 'Contains (Substring)' },
        { value: 'not_contains', label: 'Does Not Contain (Substring)' },
        { value: 'starts_with', label: 'Starts With' },
        { value: 'ends_with', label: 'Ends With' },
        { value: 'in_list', label: 'In List' },
        { value: 'not_in_list', label: 'Not In List' },
        { value: 'is_null', label: 'Is Null' },
        { value: 'is_not_null', label: 'Is Not Null' },
        { value: 'is_empty', label: 'Is Empty String' },
        { value: 'is_not_empty', label: 'Is Not Empty String' },
    ],
    number: [
        { value: '=', label: 'Equals' },
        { value: '!=', label: 'Not Equals' },
        { value: '>', label: 'Greater Than' },
        { value: '>=', label: 'Greater/Equal' },
        { value: '<', label: 'Less Than' },
        { value: '<=', label: 'Less/Equal' },
        { value: 'in_list', label: 'In List' },
        { value: 'not_in_list', label: 'Not In List' },
        { value: 'is_null', label: 'Is Null' },
        { value: 'is_not_null', label: 'Is Not Null' },
    ],
    boolean: [
        { value: 'is_true', label: 'Is True' },
        { value: 'is_false', label: 'Is False' },
        { value: 'is_null', label: 'Is Null' },
        { value: 'is_not_null', label: 'Is Not Null' },
    ],
    date: [
        { value: '=', label: 'Is On' },
        { value: '!=', label: 'Is Not On' },
        { value: 'before', label: 'Before' },
        { value: 'after', label: 'After' },
        { value: 'is_null', label: 'Is Null' },
        { value: 'is_not_null', label: 'Is Not Null' },
    ],
    json: [
        { value: 'has_key', label: 'Has Key' },
        { value: 'contains', label: 'Contains Value' },
    ]
};

OPERATORS['timestamp'] = OPERATORS['date'];

export const baseInputStyles = "w-full text-sm border border-gray-200 rounded-md focus:ring-2 focus:ring-blue-100 focus:border-blue-500 bg-white text-gray-900 shadow-sm transition-all hover:border-gray-300 py-1.5";
export const errorInputStyles = "w-full text-sm border border-red-300 rounded-md focus:ring-2 focus:ring-red-100 focus:border-red-500 bg-red-50 text-red-900 shadow-sm transition-all py-1.5";
export const codeAreaStyles = "w-full text-[11px] border border-gray-700 rounded-md focus:ring-2 focus:ring-blue-900 focus:border-blue-700 bg-[#1e1e1e] text-[#d4d4d4] font-mono shadow-sm transition-all py-2 px-3 leading-relaxed resize-y selection:bg-[#264f78]";

export const DEFAULT_FIELD_OPERATORS: Record<DataType, { value: string; label: string }[]> = OPERATORS as any;
