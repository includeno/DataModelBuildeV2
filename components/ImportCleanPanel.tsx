import React from 'react';
import { Trash2, RotateCcw, AlertTriangle, CheckCircle } from 'lucide-react';
import type { ImportCleanConfig, ImportFillRule, CleanPreviewReport, FieldInfo } from '../types';

interface ImportCleanPanelProps {
  config: ImportCleanConfig;
  onChange: (config: ImportCleanConfig) => void;
  previewReport: CleanPreviewReport | null;
  fields: string[];
  fieldTypes: Record<string, FieldInfo>;
  onReset: () => void;
  onSkip: () => void;
}

const SectionToggle: React.FC<{
  label: string;
  enabled: boolean;
  onToggle: (v: boolean) => void;
  badge?: string;
  badgeColor?: string;
  children: React.ReactNode;
}> = ({ label, enabled, onToggle, badge, badgeColor = 'text-gray-500', children }) => (
  <div className="border border-gray-200 rounded-lg overflow-hidden">
    <div
      className="flex items-center justify-between px-4 py-2.5 bg-gray-50 cursor-pointer select-none"
      onClick={() => onToggle(!enabled)}
    >
      <div className="flex items-center gap-2">
        <input
          type="checkbox"
          checked={enabled}
          onChange={(e) => { e.stopPropagation(); onToggle(e.target.checked); }}
          className="rounded border-gray-300 text-blue-600 focus:ring-blue-500"
        />
        <span className="text-sm font-medium text-gray-700">{label}</span>
        {badge && <span className={`text-xs ${badgeColor}`}>{badge}</span>}
      </div>
    </div>
    {enabled && <div className="px-4 py-3 space-y-3 bg-white">{children}</div>}
  </div>
);

const FILL_STRATEGIES = [
  { value: 'mean', label: 'Mean' },
  { value: 'median', label: 'Median' },
  { value: 'mode', label: 'Mode' },
  { value: 'constant', label: 'Constant' },
  { value: 'forward', label: 'Forward Fill' },
  { value: 'drop_row', label: 'Drop Row' },
] as const;

export const ImportCleanPanel: React.FC<ImportCleanPanelProps> = ({
  config,
  onChange,
  previewReport,
  fields,
  fieldTypes,
  onReset,
  onSkip,
}) => {
  const updateDedup = (patch: Partial<ImportCleanConfig['dedup']>) =>
    onChange({ ...config, dedup: { ...config.dedup, ...patch } });

  const updateFill = (patch: Partial<ImportCleanConfig['fillMissing']>) =>
    onChange({ ...config, fillMissing: { ...config.fillMissing, ...patch } });

  const updateOutlier = (patch: Partial<ImportCleanConfig['outlier']>) =>
    onChange({ ...config, outlier: { ...config.outlier, ...patch } });

  const updateTrim = (patch: Partial<ImportCleanConfig['trimWhitespace']>) =>
    onChange({ ...config, trimWhitespace: { ...config.trimWhitespace, ...patch } });

  const updateFillRule = (index: number, patch: Partial<ImportFillRule>) => {
    const newRules = [...config.fillMissing.rules];
    newRules[index] = { ...newRules[index], ...patch };
    updateFill({ rules: newRules });
  };

  const removeFillRule = (index: number) => {
    updateFill({ rules: config.fillMissing.rules.filter((_, i) => i !== index) });
  };

  const addFillRule = () => {
    updateFill({
      rules: [...config.fillMissing.rules, { field: fields[0] || '', strategy: 'constant', constantValue: '' }],
    });
  };

  const totalMissing = previewReport
    ? Object.values(previewReport.missingValueCounts).reduce((a, b) => a + b, 0)
    : 0;
  const missingFieldCount = previewReport ? Object.keys(previewReport.missingValueCounts).length : 0;

  const numericFields = fields.filter((f) => fieldTypes[f]?.type === 'number');

  return (
    <div className="space-y-3" data-testid="import-clean-panel">
      {/* Dedup */}
      <SectionToggle
        label="Deduplication"
        enabled={config.dedup.enabled}
        onToggle={(v) => updateDedup({ enabled: v })}
        badge={previewReport ? `${previewReport.duplicateRowCount} duplicate rows detected` : undefined}
        badgeColor={previewReport && previewReport.duplicateRowCount > 0 ? 'text-amber-600' : 'text-green-600'}
      >
        <div className="flex items-center gap-4 text-sm">
          <label className="flex items-center gap-2">
            <span className="text-gray-600">Fields:</span>
            <select
              value={config.dedup.fields === 'all' ? 'all' : 'custom'}
              onChange={(e) => updateDedup({ fields: e.target.value === 'all' ? 'all' : [] })}
              className="border border-gray-300 rounded px-2 py-1 text-sm"
            >
              <option value="all">All Fields</option>
              <option value="custom">Custom</option>
            </select>
          </label>
          <label className="flex items-center gap-2">
            <span className="text-gray-600">Keep:</span>
            <select
              value={config.dedup.keep}
              onChange={(e) => updateDedup({ keep: e.target.value as 'first' | 'last' })}
              className="border border-gray-300 rounded px-2 py-1 text-sm"
            >
              <option value="first">First</option>
              <option value="last">Last</option>
            </select>
          </label>
        </div>
        {Array.isArray(config.dedup.fields) && (
          <div className="flex flex-wrap gap-1">
            {fields.map((f) => (
              <label key={f} className="flex items-center gap-1 text-xs px-2 py-1 bg-gray-100 rounded">
                <input
                  type="checkbox"
                  checked={(config.dedup.fields as string[]).includes(f)}
                  onChange={(e) => {
                    const current = config.dedup.fields as string[];
                    updateDedup({ fields: e.target.checked ? [...current, f] : current.filter((x) => x !== f) });
                  }}
                  className="rounded border-gray-300 text-blue-600"
                />
                {f}
              </label>
            ))}
          </div>
        )}
      </SectionToggle>

      {/* Trim Whitespace */}
      <SectionToggle
        label="Trim Whitespace"
        enabled={config.trimWhitespace.enabled}
        onToggle={(v) => updateTrim({ enabled: v })}
        badge={previewReport ? `${previewReport.whitespaceFieldCount} fields with whitespace` : undefined}
        badgeColor={previewReport && previewReport.whitespaceFieldCount > 0 ? 'text-amber-600' : 'text-green-600'}
      >
        <div className="text-sm text-gray-600">
          Strips leading/trailing whitespace from string fields. Pure-whitespace values become empty (treated as missing).
        </div>
      </SectionToggle>

      {/* Fill Missing */}
      <SectionToggle
        label="Fill Missing Values"
        enabled={config.fillMissing.enabled}
        onToggle={(v) => updateFill({ enabled: v })}
        badge={previewReport ? `${missingFieldCount} fields, ${totalMissing} missing values` : undefined}
        badgeColor={totalMissing > 0 ? 'text-amber-600' : 'text-green-600'}
      >
        <div className="space-y-2">
          {config.fillMissing.rules.map((rule, idx) => (
            <div key={idx} className="flex items-center gap-2 text-sm">
              <select
                value={rule.field}
                onChange={(e) => updateFillRule(idx, { field: e.target.value })}
                className="border border-gray-300 rounded px-2 py-1 text-sm flex-1 min-w-0"
              >
                <option value="*number">All Number Fields</option>
                <option value="*string">All String Fields</option>
                <option value="*date">All Date Fields</option>
                {fields.map((f) => (
                  <option key={f} value={f}>{f}</option>
                ))}
              </select>
              <select
                value={rule.strategy}
                onChange={(e) => updateFillRule(idx, { strategy: e.target.value as ImportFillRule['strategy'] })}
                className="border border-gray-300 rounded px-2 py-1 text-sm"
              >
                {FILL_STRATEGIES.map((s) => (
                  <option key={s.value} value={s.value}>{s.label}</option>
                ))}
              </select>
              {rule.strategy === 'constant' && (
                <input
                  type="text"
                  value={rule.constantValue || ''}
                  onChange={(e) => updateFillRule(idx, { constantValue: e.target.value })}
                  placeholder="value"
                  className="border border-gray-300 rounded px-2 py-1 text-sm w-24"
                />
              )}
              <button onClick={() => removeFillRule(idx)} className="text-gray-400 hover:text-red-500 p-1">
                <Trash2 className="w-3.5 h-3.5" />
              </button>
            </div>
          ))}
          <button onClick={addFillRule} className="text-xs text-blue-600 hover:text-blue-800">
            + Add Rule
          </button>
        </div>
      </SectionToggle>

      {/* Outlier Detection */}
      <SectionToggle
        label="Outlier Detection"
        enabled={config.outlier.enabled}
        onToggle={(v) => updateOutlier({ enabled: v })}
        badge={
          previewReport && Object.keys(previewReport.outlierCounts).length > 0
            ? `${Object.values(previewReport.outlierCounts).reduce((a, b) => a + b, 0)} potential outliers`
            : undefined
        }
        badgeColor="text-amber-600"
      >
        <div className="flex flex-wrap items-center gap-4 text-sm">
          <label className="flex items-center gap-2">
            <span className="text-gray-600">Method:</span>
            <select
              value={config.outlier.method}
              onChange={(e) => updateOutlier({ method: e.target.value as 'iqr' | 'zscore' })}
              className="border border-gray-300 rounded px-2 py-1 text-sm"
            >
              <option value="iqr">IQR</option>
              <option value="zscore">Z-Score</option>
            </select>
          </label>
          <label className="flex items-center gap-2">
            <span className="text-gray-600">Threshold:</span>
            <input
              type="number"
              step="0.1"
              min="0.1"
              value={config.outlier.threshold}
              onChange={(e) => updateOutlier({ threshold: parseFloat(e.target.value) || 1.5 })}
              className="border border-gray-300 rounded px-2 py-1 text-sm w-20"
            />
          </label>
          <label className="flex items-center gap-2">
            <span className="text-gray-600">Action:</span>
            <select
              value={config.outlier.action}
              onChange={(e) => updateOutlier({ action: e.target.value as 'flag' | 'remove' })}
              className="border border-gray-300 rounded px-2 py-1 text-sm"
            >
              <option value="flag">Flag (add column)</option>
              <option value="remove">Remove rows</option>
            </select>
          </label>
        </div>
        {config.outlier.action === 'remove' && (
          <div className="flex items-center gap-1 text-xs text-amber-600">
            <AlertTriangle className="w-3.5 h-3.5" />
            Outlier rows will be permanently removed from the imported dataset.
          </div>
        )}
        <div className="text-xs text-gray-500">
          Applies to {numericFields.length} numeric field{numericFields.length !== 1 ? 's' : ''}:
          {numericFields.length > 0 ? ` ${numericFields.slice(0, 5).join(', ')}${numericFields.length > 5 ? '...' : ''}` : ' none'}
        </div>
      </SectionToggle>

      {/* Summary & Actions */}
      <div className="flex items-center justify-between pt-2">
        <div className="flex items-center gap-2 text-xs text-gray-500">
          <CheckCircle className="w-3.5 h-3.5 text-green-500" />
          {[
            config.dedup.enabled && 'Dedup',
            config.trimWhitespace.enabled && 'Trim',
            config.fillMissing.enabled && 'Fill',
            config.outlier.enabled && 'Outlier',
          ]
            .filter(Boolean)
            .join(' + ') || 'No cleaning enabled'}
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={onReset}
            className="flex items-center gap-1 text-xs text-gray-500 hover:text-gray-700 px-2 py-1 rounded hover:bg-gray-100"
          >
            <RotateCcw className="w-3 h-3" /> Reset Defaults
          </button>
          <button
            onClick={onSkip}
            className="text-xs text-gray-500 hover:text-gray-700 px-2 py-1 rounded hover:bg-gray-100"
          >
            Skip Cleaning
          </button>
        </div>
      </div>
    </div>
  );
};
