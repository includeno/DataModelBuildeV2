import type { FieldInfo, ImportCleanConfig, ImportFillRule } from '../types';

/**
 * Build a default ImportCleanConfig based on the dataset schema.
 * Design principle: don't lose data by default.
 * - Dedup: enabled, all fields, keep first
 * - Fill missing: enabled, median for numbers, empty string for strings, drop row for dates
 * - Outlier: disabled (user must opt in)
 * - Trim whitespace: enabled for all string columns
 */
export function buildDefaultCleanConfig(
  _fields: string[],
  _fieldTypes: Record<string, FieldInfo>
): ImportCleanConfig {
  const defaultRules: ImportFillRule[] = [
    { field: '*number', strategy: 'median' },
    { field: '*string', strategy: 'constant', constantValue: '' },
    { field: '*date', strategy: 'drop_row' },
  ];

  return {
    dedup: { enabled: true, fields: 'all', keep: 'first' },
    fillMissing: {
      enabled: true,
      rules: defaultRules,
    },
    outlier: {
      enabled: false,
      method: 'iqr',
      threshold: 1.5,
      action: 'flag',
      targetFields: 'numeric',
    },
    trimWhitespace: { enabled: true, fields: 'string' },
  };
}
