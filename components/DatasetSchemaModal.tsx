
import React, { useState, useEffect } from 'react';
import { X, Save, Type, Hash, Calendar, Clock, CheckCircle, Code, Database, Table, Loader2, ChevronDown, Info } from 'lucide-react';
import { Button } from './Button';
import { Dataset, DataType, FieldInfo } from '../types';

interface DatasetSchemaModalProps {
  isOpen: boolean;
  onClose: () => void;
  dataset: Dataset | null;
  onSave: (datasetId: string, fieldTypes: Record<string, FieldInfo>) => Promise<void>;
}

const DATA_TYPES: { value: DataType; label: string; icon: any }[] = [
    { value: 'string', label: 'String', icon: Type },
    { value: 'number', label: 'Number', icon: Hash },
    { value: 'boolean', label: 'Boolean', icon: CheckCircle },
    { value: 'date', label: 'Date', icon: Calendar },
    { value: 'timestamp', label: 'Timestamp', icon: Clock },
    { value: 'json', label: 'JSON', icon: Code },
];

const COMMON_DATE_FORMATS = [
    'YYYY-MM-DD',
    'YYYY/MM/DD',
    'DD-MM-YYYY',
    'DD/MM/YYYY',
    'MM/DD/YYYY',
    'YYYY-MM-DD HH:mm:ss',
    'YYYY/MM/DD HH:mm:ss',
    'ISO8601',
    'Timestamp (ms)',
];

export const DatasetSchemaModal: React.FC<DatasetSchemaModalProps> = ({ isOpen, onClose, dataset, onSave }) => {
  const [fieldTypes, setFieldTypes] = useState<Record<string, FieldInfo>>({});
  const [saving, setSaving] = useState(false);
  // 用于追踪哪些字段处于“自定义模式”，解决下拉框无法切回自定义输入的问题
  const [customModeFields, setCustomModeFields] = useState<Set<string>>(new Set());

  useEffect(() => {
    if (dataset) {
        const types: Record<string, FieldInfo> = {};
        const initialCustomFields = new Set<string>();
        
        dataset.fields.forEach(f => {
            const existing = dataset.fieldTypes?.[f];
            types[f] = existing ? { ...existing } : { type: 'string' };
            
            // 如果已有格式不在常用列表中，自动进入自定义模式
            if (existing?.format && !COMMON_DATE_FORMATS.includes(existing.format)) {
                initialCustomFields.add(f);
            }
        });
        setFieldTypes(types);
        setCustomModeFields(initialCustomFields);
    }
  }, [dataset, isOpen]);

  if (!isOpen || !dataset) return null;

  const handleSave = async () => {
      setSaving(true);
      try {
          await onSave(dataset.id, fieldTypes);
          onClose();
      } catch (e) {
          alert("Failed to save schema");
      } finally {
          setSaving(false);
      }
  };

  const handleTypeChange = (field: string, type: DataType) => {
      setFieldTypes(prev => ({ 
          ...prev, 
          [field]: { 
              ...prev[field], 
              type,
              format: (type === 'date' || type === 'timestamp') ? (prev[field].format || 'YYYY-MM-DD') : undefined
          } 
      }));
  };

  const handleFormatChange = (field: string, format: string) => {
      setFieldTypes(prev => ({
          ...prev,
          [field]: { ...prev[field], format }
      }));
      
      // 如果输入的内容在常用列表中，退出自定义模式
      if (COMMON_DATE_FORMATS.includes(format)) {
          const next = new Set(customModeFields);
          next.delete(field);
          setCustomModeFields(next);
      }
  };

  const toggleCustomMode = (field: string, isCustom: boolean) => {
      const next = new Set(customModeFields);
      if (isCustom) {
          next.add(field);
          // 进入自定义模式时，如果当前是常用格式，可以清空或保留让用户修改
      } else {
          next.delete(field);
      }
      setCustomModeFields(next);
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm p-4">
      <div className="bg-white rounded-xl shadow-2xl w-full max-w-4xl flex flex-col max-h-[85vh] animate-in fade-in zoom-in duration-200">
        <div className="flex items-center justify-between px-6 py-4 border-b border-gray-100 bg-gray-50">
          <div className="flex items-center space-x-2">
            <div className="p-2 bg-blue-100 rounded-lg">
                <Database className="w-5 h-5 text-blue-600" />
            </div>
            <div>
                <h3 className="text-lg font-bold text-gray-900 leading-tight">Dataset Schema Configuration</h3>
                <p className="text-xs text-gray-500 font-mono flex items-center mt-0.5">
                    <Table className="w-3 h-3 mr-1" /> {dataset.name}
                </p>
            </div>
          </div>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-600 p-2 rounded-full hover:bg-gray-200 transition-all">
            <X className="w-5 h-5" />
          </button>
        </div>

        <div className="p-0 overflow-hidden flex-1 flex flex-col bg-white">
            <div className="overflow-y-auto flex-1">
                <table className="min-w-full divide-y divide-gray-200 table-fixed">
                    <thead className="bg-gray-50 sticky top-0 z-10 shadow-sm">
                        <tr>
                            <th className="w-1/4 px-6 py-3 text-left text-[10px] font-bold text-gray-500 uppercase tracking-wider">Field Name</th>
                            <th className="w-1/5 px-6 py-3 text-left text-[10px] font-bold text-gray-500 uppercase tracking-wider">Data Type</th>
                            <th className="w-1/4 px-6 py-3 text-left text-[10px] font-bold text-gray-500 uppercase tracking-wider">Format / Options</th>
                            <th className="w-1/3 px-6 py-3 text-left text-[10px] font-bold text-gray-500 uppercase tracking-wider">Data Preview (Row 1)</th>
                        </tr>
                    </thead>
                    <tbody className="bg-white divide-y divide-gray-100">
                        {dataset.fields.map(field => {
                            const fieldInfo = fieldTypes[field] || { type: 'string' };
                            const currentType = fieldInfo.type;
                            const TypeIcon = DATA_TYPES.find(t => t.value === currentType)?.icon || Type;
                            const previewVal = dataset.rows.length > 0 ? dataset.rows[0][field] : null;
                            const isDateType = currentType === 'date' || currentType === 'timestamp';
                            const isCustomMode = customModeFields.has(field);

                            return (
                                <tr key={field} className="hover:bg-blue-50/30 transition-colors">
                                    <td className="px-6 py-4 whitespace-nowrap">
                                        <span className="text-sm font-semibold text-gray-900">{field}</span>
                                    </td>
                                    <td className="px-6 py-4">
                                        <div className="relative group">
                                            <div className="absolute inset-y-0 left-0 flex items-center pl-2 pointer-events-none">
                                                <TypeIcon className="w-3.5 h-3.5 text-blue-500" />
                                            </div>
                                            <select
                                                value={currentType}
                                                onChange={(e) => handleTypeChange(field, e.target.value as DataType)}
                                                className="block w-full pl-8 pr-8 py-1.5 text-xs border border-gray-200 focus:outline-none focus:ring-2 focus:ring-blue-100 focus:border-blue-400 rounded-md bg-white cursor-pointer shadow-sm appearance-none"
                                            >
                                                {DATA_TYPES.map(t => (
                                                    <option key={t.value} value={t.value}>{t.label}</option>
                                                ))}
                                            </select>
                                            <div className="absolute inset-y-0 right-0 flex items-center pr-2 pointer-events-none text-gray-400">
                                                <ChevronDown className="w-3 h-3" />
                                            </div>
                                        </div>
                                    </td>
                                    <td className="px-6 py-4">
                                        {isDateType ? (
                                            <div className="space-y-1.5 animate-in fade-in slide-in-from-left-2 duration-200">
                                                <div className="relative">
                                                    <select
                                                        value={isCustomMode ? 'custom' : (fieldInfo.format || '')}
                                                        onChange={(e) => {
                                                            const val = e.target.value;
                                                            if (val === 'custom') {
                                                                toggleCustomMode(field, true);
                                                            } else {
                                                                toggleCustomMode(field, false);
                                                                handleFormatChange(field, val);
                                                            }
                                                        }}
                                                        className="block w-full px-2 py-1.5 text-[11px] border border-gray-200 focus:outline-none focus:ring-2 focus:ring-blue-100 rounded-md bg-white shadow-sm"
                                                    >
                                                        <option value="" disabled>Select Format...</option>
                                                        {COMMON_DATE_FORMATS.map(f => <option key={f} value={f}>{f}</option>)}
                                                        <option value="custom">-- Custom Pattern --</option>
                                                    </select>
                                                </div>
                                                {isCustomMode && (
                                                    <input 
                                                        type="text"
                                                        value={fieldInfo.format || ''}
                                                        onChange={(e) => handleFormatChange(field, e.target.value)}
                                                        placeholder="e.g. YYYY/MM/DD"
                                                        autoFocus
                                                        className="block w-full px-2 py-1.5 text-[11px] border border-blue-200 focus:outline-none focus:ring-2 focus:ring-blue-100 rounded-md bg-white shadow-inner font-mono animate-in zoom-in-95 duration-150"
                                                    />
                                                )}
                                            </div>
                                        ) : (
                                            <span className="text-[10px] text-gray-400 italic">No specific options</span>
                                        )}
                                    </td>
                                    <td className="px-6 py-4">
                                        <div className="text-[11px] text-gray-600 font-mono bg-gray-50 p-2 rounded border border-gray-100 break-all whitespace-pre-wrap max-h-24 overflow-y-auto leading-relaxed shadow-inner">
                                            {previewVal === null || previewVal === undefined ? (
                                                <span className="text-gray-300 italic">NULL</span>
                                            ) : typeof previewVal === 'object' ? (
                                                JSON.stringify(previewVal, null, 2)
                                            ) : (
                                                String(previewVal)
                                            )}
                                        </div>
                                    </td>
                                </tr>
                            );
                        })}
                    </tbody>
                </table>
            </div>
            
            <div className="px-6 py-3 bg-blue-50/50 border-t border-blue-100 flex items-start space-x-2">
                <Info className="w-4 h-4 text-blue-500 mt-0.5 shrink-0" />
                <p className="text-xs text-blue-700 leading-relaxed">
                    <b>Pro Tip:</b> Correct data types ensure the Command Editor provides relevant filter operators. Custom date patterns follow standard datetime formatting (e.g. YYYY, MM, DD, HH, mm, ss).
                </p>
            </div>
        </div>
        
        <div className="p-4 bg-gray-50 border-t border-gray-200 flex justify-end space-x-3 rounded-b-xl">
             <Button variant="secondary" onClick={onClose} disabled={saving}>
                Cancel
            </Button>
            <Button 
                variant="primary" 
                onClick={handleSave} 
                disabled={saving} 
                className="min-w-[120px]"
                icon={saving ? <Loader2 className="w-4 h-4 animate-spin"/> : <Save className="w-4 h-4" />}
            >
                {saving ? 'Saving...' : 'Save Schema'}
            </Button>
        </div>
      </div>
    </div>
  );
};
