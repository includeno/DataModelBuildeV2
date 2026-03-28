import React, { useState, useRef, useEffect } from 'react';
import { Upload, CheckCircle, X, AlertTriangle, FileSpreadsheet, ChevronLeft, Settings2 } from 'lucide-react';
import { ApiConfig, Dataset, ImportCleanConfig, CleanPreviewReport, FieldInfo } from '../types';
import { api } from '../utils/api';
import { buildDefaultCleanConfig } from '../utils/importDefaults';
import { ImportCleanPanel } from './ImportCleanPanel';
import { Button } from './Button';

interface DataImportModalProps {
  isOpen: boolean;
  onClose: () => void;
  onImport: (dataset: Dataset) => void;
  projectId?: string;
  sessionId?: string;
  apiConfig: ApiConfig;
}

type ImportStep = 'select' | 'preview' | 'uploading';

export const DataImportModal: React.FC<DataImportModalProps> = ({ isOpen, onClose, onImport, projectId, sessionId, apiConfig }) => {
  const [dragActive, setDragActive] = useState(false);
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [customName, setCustomName] = useState('');
  const [uploading, setUploading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const activeProjectId = projectId || sessionId || '';

  // Preview & clean state
  const [step, setStep] = useState<ImportStep>('select');
  const [previewing, setPreviewing] = useState(false);
  const [previewToken, setPreviewToken] = useState<string | null>(null);
  const [previewFields, setPreviewFields] = useState<string[]>([]);
  const [previewFieldTypes, setPreviewFieldTypes] = useState<Record<string, FieldInfo>>({});
  const [previewRows, setPreviewRows] = useState<any[]>([]);
  const [previewTotalCount, setPreviewTotalCount] = useState(0);
  const [cleanReport, setCleanReport] = useState<CleanPreviewReport | null>(null);
  const [cleanConfig, setCleanConfig] = useState<ImportCleanConfig | null>(null);
  const [showCleanPanel, setShowCleanPanel] = useState(false);

  const inputRef = useRef<HTMLInputElement>(null);

  const RESERVED_WORDS = new Set([
    'select', 'from', 'where', 'order', 'group', 'by', 'join', 'left', 'right',
    'inner', 'outer', 'full', 'on', 'limit', 'offset', 'union', 'distinct',
    'having', 'as', 'and', 'or', 'not', 'null', 'is', 'like', 'in', 'table', 'view'
  ]);

  const trimmedName = customName.trim();
  const nameError = trimmedName && RESERVED_WORDS.has(trimmedName.toLowerCase())
    ? `Dataset name '${trimmedName}' is a reserved keyword. Please choose another name.`
    : null;

  const resetState = () => {
    setSelectedFile(null);
    setCustomName('');
    setError(null);
    setUploading(false);
    setStep('select');
    setPreviewing(false);
    setPreviewToken(null);
    setPreviewFields([]);
    setPreviewFieldTypes({});
    setPreviewRows([]);
    setPreviewTotalCount(0);
    setCleanReport(null);
    setCleanConfig(null);
    setShowCleanPanel(false);
  };

  const handleClose = () => {
      resetState();
      onClose();
  };

  useEffect(() => {
      if (!isOpen) return;
      const handleKeyDown = (e: KeyboardEvent) => {
          if (e.key === 'Escape') handleClose();
      };
      document.addEventListener('keydown', handleKeyDown);
      return () => document.removeEventListener('keydown', handleKeyDown);
  }, [isOpen]);

  if (!isOpen) return null;

  const handleDrag = (e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    if (e.type === "dragenter" || e.type === "dragover") {
      setDragActive(true);
    } else if (e.type === "dragleave") {
      setDragActive(false);
    }
  };

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    setDragActive(false);
    if (e.dataTransfer.files && e.dataTransfer.files[0]) {
      processFileSelection(e.dataTransfer.files[0]);
    }
  };

  const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    e.preventDefault();
    if (e.target.files && e.target.files[0]) {
      processFileSelection(e.target.files[0]);
    }
  };

  const openFilePicker = () => {
    if (!inputRef.current) return;
    const input = inputRef.current as HTMLInputElement & { showPicker?: () => void };
    if (typeof input.showPicker === 'function') {
      try {
        input.showPicker();
        return;
      } catch {
        // Fall back to click for environments where showPicker exists but is restricted.
      }
    }
    input.click();
  };

  const processFileSelection = (file: File) => {
      setSelectedFile(file);
      const nameWithoutExt = file.name.replace(/\.[^/.]+$/, "");
      setCustomName(nameWithoutExt);
      setError(null);
  };

  // Step 2: Preview the file & get clean report
  const handlePreview = async () => {
    if (!selectedFile) return;
    if (nameError) { setError(nameError); return; }

    setPreviewing(true);
    setError(null);
    try {
      const result = await api.uploadPreview(apiConfig, activeProjectId, selectedFile);
      setPreviewToken(result.previewToken);
      setPreviewFields(result.fields);
      setPreviewFieldTypes(result.fieldTypes);
      setPreviewRows(result.rows);
      setPreviewTotalCount(result.totalCount);
      setCleanReport(result.cleanReport);
      setCleanConfig(buildDefaultCleanConfig(result.fields, result.fieldTypes));
      setStep('preview');
    } catch (err: any) {
      console.error("Preview error:", err);
      setError(err.message || "Failed to preview file");
    } finally {
      setPreviewing(false);
    }
  };

  // Step 3: Upload with clean config
  const handleUpload = async () => {
    if (!selectedFile) return;
    if (nameError) { setError(nameError); return; }

    setUploading(true);
    setStep('uploading');
    setError(null);

    const formData = new FormData();
    formData.append('file', selectedFile);
    formData.append('name', customName);
    if (previewToken) {
      formData.append('previewToken', previewToken);
    }
    if (cleanConfig) {
      formData.append('cleanConfig', JSON.stringify(cleanConfig));
    }

    try {
        const data = await api.upload(apiConfig, `/projects/${activeProjectId}/upload`, formData);

        if (data.error) {
            throw new Error(data.error);
        }

        const newDataset: Dataset = {
            id: data.id,
            name: data.name,
            fields: data.fields,
            rows: data.rows,
            totalCount: data.totalCount
        };

        onImport(newDataset);
        handleClose();
    } catch (err: any) {
        console.error("Upload error:", err);
        setError(err.message || "Failed to upload file");
        setStep('preview');
    } finally {
        setUploading(false);
    }
  };

  const handleResetClean = () => {
    if (previewFields.length > 0) {
      setCleanConfig(buildDefaultCleanConfig(previewFields, previewFieldTypes));
    }
  };

  const handleSkipClean = () => {
    setCleanConfig(null);
  };

  const enabledCleanCount = cleanConfig
    ? [cleanConfig.dedup.enabled, cleanConfig.trimWhitespace.enabled, cleanConfig.fillMissing.enabled, cleanConfig.outlier.enabled].filter(Boolean).length
    : 0;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm">
      <div className={`bg-white rounded-xl shadow-2xl w-full overflow-hidden animate-in fade-in zoom-in duration-200 ${step === 'preview' ? 'max-w-2xl' : 'max-w-lg'}`}>
        <div className="flex items-center justify-between px-6 py-4 border-b border-gray-100">
            <h3 className="text-lg font-semibold text-gray-900 flex items-center">
                {step === 'preview' && (
                  <button onClick={() => { setStep('select'); setError(null); }} className="mr-2 text-gray-400 hover:text-gray-600">
                    <ChevronLeft className="w-5 h-5" />
                  </button>
                )}
                <Upload className="w-5 h-5 mr-2 text-blue-600" />
                {step === 'select' ? 'Import Data Source' : step === 'preview' ? 'Preview & Configure' : 'Importing...'}
            </h3>
            <button
                onClick={handleClose}
                className="text-gray-400 hover:text-gray-600 transition-colors"
                aria-label="Close Import Data Source"
                title="Close"
            >
                <X className="w-5 h-5" />
            </button>
        </div>

        <div className="p-6">
            <div className="mb-4 text-xs text-center text-gray-400">
                Server: {apiConfig.isMock ? 'Mock Mode' : apiConfig.baseUrl}
            </div>

            {/* Step 1: File Selection */}
            {step === 'select' && (
              <>
                {!selectedFile ? (
                    <div
                        data-testid="data-import-dropzone"
                        className={`relative flex flex-col items-center justify-center w-full h-48 border-2 border-dashed rounded-xl transition-all duration-200 cursor-pointer ${
                            dragActive
                            ? "border-blue-500 bg-blue-50 scale-[1.02]"
                            : "border-gray-300 bg-gray-50 hover:bg-gray-100 hover:border-gray-400"
                        }`}
                        role="button"
                        tabIndex={0}
                        onClick={openFilePicker}
                        onKeyDown={(e) => {
                          if (e.key === 'Enter' || e.key === ' ') {
                            e.preventDefault();
                            openFilePicker();
                          }
                        }}
                        onDragEnter={handleDrag}
                        onDragLeave={handleDrag}
                        onDragOver={handleDrag}
                        onDrop={handleDrop}
                    >
                        <input
                            data-testid="data-import-file-input"
                            ref={inputRef}
                            type="file"
                            className="sr-only"
                            onChange={handleChange}
                            accept=".csv,.xlsx,.xls,.parquet,.pq"
                            aria-label="Upload file"
                        />

                        <div className="flex flex-col items-center text-gray-500">
                            <div className="bg-white p-3 rounded-full shadow-sm mb-3">
                                <FileSpreadsheet className="w-8 h-8 text-blue-600" />
                            </div>
                            <span className="text-sm font-medium text-gray-900">Click to upload</span>
                            <span className="text-sm text-gray-500 mt-1">Supports CSV, Excel (.xlsx), and Parquet (.parquet)</span>
                        </div>
                    </div>
                ) : (
                    <div className="space-y-4">
                        <div className="flex items-start p-4 bg-blue-50 border border-blue-100 rounded-lg">
                            <div className="bg-white p-2 rounded-md shadow-sm mr-3">
                                <FileSpreadsheet className="w-6 h-6 text-green-600" />
                            </div>
                            <div className="flex-1 min-w-0">
                                <p className="text-sm font-medium text-blue-900 truncate">{selectedFile.name}</p>
                                <p className="text-xs text-blue-700 mt-0.5">{(selectedFile.size / 1024).toFixed(1)} KB</p>
                            </div>
                            <button
                                onClick={resetState}
                                className="text-blue-400 hover:text-blue-600 p-1"
                                disabled={previewing}
                            >
                                <X className="w-4 h-4" />
                            </button>
                        </div>

                        <div>
                            <label className="block text-sm font-medium text-gray-700 mb-1">Dataset Name</label>
                            <input
                                type="text"
                                className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 text-sm"
                                value={customName}
                                onChange={(e) => setCustomName(e.target.value)}
                                placeholder="Enter a name for this dataset"
                                disabled={previewing}
                            />
                            <p className="mt-1 text-xs text-gray-500">This name will be used in SQL queries and operations.</p>
                            {nameError && (
                                <p className="mt-1 text-xs text-red-600">{nameError}</p>
                            )}
                        </div>
                    </div>
                )}
              </>
            )}

            {/* Step 2: Preview & Clean Config */}
            {step === 'preview' && (
              <div className="space-y-4">
                {/* File summary */}
                <div className="flex items-center gap-3 p-3 bg-gray-50 rounded-lg text-sm">
                  <FileSpreadsheet className="w-5 h-5 text-green-600 shrink-0" />
                  <div className="flex-1 min-w-0">
                    <span className="font-medium text-gray-900">{customName}</span>
                    <span className="text-gray-500 ml-2">{previewTotalCount} rows, {previewFields.length} fields</span>
                  </div>
                </div>

                {/* Data preview table */}
                <div className="border border-gray-200 rounded-lg overflow-hidden">
                  <div className="max-h-40 overflow-auto">
                    <table className="w-full text-xs">
                      <thead className="bg-gray-50 sticky top-0">
                        <tr>
                          {previewFields.map((f) => (
                            <th key={f} className="px-3 py-1.5 text-left font-medium text-gray-600 whitespace-nowrap border-b border-gray-200">{f}</th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {previewRows.slice(0, 10).map((row, i) => (
                          <tr key={i} className="border-b border-gray-100 last:border-0">
                            {previewFields.map((f) => (
                              <td key={f} className="px-3 py-1 text-gray-700 whitespace-nowrap max-w-[200px] truncate">
                                {row[f] == null ? <span className="text-gray-300 italic">null</span> : String(row[f])}
                              </td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                  {previewRows.length > 10 && (
                    <div className="px-3 py-1 text-xs text-gray-400 bg-gray-50 border-t border-gray-200">
                      Showing first 10 of {previewRows.length} preview rows
                    </div>
                  )}
                </div>

                {/* Clean config toggle */}
                <div>
                  <button
                    onClick={() => setShowCleanPanel(!showCleanPanel)}
                    className="flex items-center gap-2 text-sm font-medium text-gray-700 hover:text-gray-900 w-full py-2"
                  >
                    <Settings2 className="w-4 h-4" />
                    Data Cleaning
                    <span className="text-xs text-gray-500 font-normal">
                      {cleanConfig ? `${enabledCleanCount} enabled` : 'disabled'}
                    </span>
                    <span className="ml-auto text-xs text-gray-400">{showCleanPanel ? 'Hide' : 'Show'}</span>
                  </button>
                  {showCleanPanel && cleanConfig && (
                    <ImportCleanPanel
                      config={cleanConfig}
                      onChange={setCleanConfig}
                      previewReport={cleanReport}
                      fields={previewFields}
                      fieldTypes={previewFieldTypes}
                      onReset={handleResetClean}
                      onSkip={handleSkipClean}
                    />
                  )}
                </div>
              </div>
            )}

            {/* Step 3: Uploading indicator */}
            {step === 'uploading' && (
              <div className="flex flex-col items-center py-8 text-gray-500">
                <Upload className="w-8 h-8 animate-bounce text-blue-600 mb-3" />
                <span className="text-sm font-medium">Importing and cleaning data...</span>
              </div>
            )}

            {error && (
                <div className="mt-4 p-3 bg-red-50 text-red-700 text-sm rounded-lg flex items-center">
                    <AlertTriangle className="w-4 h-4 mr-2 shrink-0" />
                    {error}
                </div>
            )}

            <div className="mt-6 flex justify-end space-x-3">
                <Button variant="secondary" onClick={handleClose} disabled={uploading || previewing}>
                    Cancel
                </Button>
                {step === 'select' && selectedFile && (
                    <Button
                        variant="primary"
                        onClick={handlePreview}
                        disabled={previewing || !customName.trim() || !!nameError}
                        icon={previewing ? <Upload className="w-4 h-4 animate-bounce" /> : <CheckCircle className="w-4 h-4" />}
                    >
                        {previewing ? 'Analyzing...' : 'Next: Preview'}
                    </Button>
                )}
                {step === 'preview' && (
                    <Button
                        variant="primary"
                        onClick={handleUpload}
                        disabled={uploading}
                        icon={<CheckCircle className="w-4 h-4" />}
                    >
                        Import Dataset
                    </Button>
                )}
            </div>
        </div>
      </div>
    </div>
  );
};
