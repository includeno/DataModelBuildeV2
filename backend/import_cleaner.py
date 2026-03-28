"""Import-time data cleaning module.

Applies cleaning steps (dedup, trim whitespace, fill missing, outlier detection)
to a DataFrame during the import phase, before it is stored in DuckDB.
"""

import pandas as pd
import numpy as np
from typing import Any, Dict, List, Optional, Tuple, Union
from models import (
    ImportCleanConfig,
    DedupConfig,
    FillMissingConfig,
    ImportFillRule,
    OutlierConfig,
    TrimWhitespaceConfig,
)


class CleanPreviewReport:
    """Analysis report generated before cleaning (no data modification)."""
    def __init__(self):
        self.duplicate_row_count: int = 0
        self.missing_value_counts: Dict[str, int] = {}
        self.outlier_counts: Dict[str, int] = {}
        self.whitespace_field_count: int = 0

    def to_dict(self) -> dict:
        return {
            "duplicateRowCount": self.duplicate_row_count,
            "missingValueCounts": self.missing_value_counts,
            "outlierCounts": self.outlier_counts,
            "whitespaceFieldCount": self.whitespace_field_count,
        }


class CleanReport:
    """Report of what was actually cleaned."""
    def __init__(self, original_row_count: int):
        self.original_row_count = original_row_count
        self.final_row_count: int = original_row_count
        self.dedup_removed: int = 0
        self.fill_applied: Dict[str, int] = {}
        self.outlier_flagged: Dict[str, int] = {}
        self.outlier_removed: int = 0
        self.trim_applied: int = 0

    def to_dict(self) -> dict:
        return {
            "dedupRemoved": self.dedup_removed,
            "fillApplied": self.fill_applied,
            "outlierFlagged": self.outlier_flagged,
            "outlierRemoved": self.outlier_removed,
            "trimApplied": self.trim_applied,
            "originalRowCount": self.original_row_count,
            "finalRowCount": self.final_row_count,
        }


def _resolve_fields_for_type(
    df: pd.DataFrame, schema: Dict[str, Any], type_wildcard: str
) -> List[str]:
    """Resolve a type wildcard like '*number' to actual column names."""
    type_map = {
        "*number": "number",
        "*string": "string",
        "*date": "date",
    }
    target_type = type_map.get(type_wildcard)
    if not target_type:
        return []
    result = []
    for col in df.columns:
        col_info = schema.get(str(col), {})
        col_type = col_info.get("type", "string") if isinstance(col_info, dict) else "string"
        if col_type == target_type:
            result.append(str(col))
    return result


def _is_numeric_column(df: pd.DataFrame, col: str) -> bool:
    """Check if a column has numeric dtype (excluding booleans)."""
    series = df[col]
    return pd.api.types.is_numeric_dtype(series) and not pd.api.types.is_bool_dtype(series)


class ImportCleaner:
    """Applies cleaning operations to a DataFrame during import."""

    def preview(self, df: pd.DataFrame, schema: Dict[str, Any]) -> CleanPreviewReport:
        """Analyze the DataFrame without modifying it. Returns a preview report."""
        report = CleanPreviewReport()

        # Duplicate count
        report.duplicate_row_count = int(df.duplicated().sum())

        # Missing value counts per field
        for col in df.columns:
            null_count = int(df[col].isna().sum())
            if null_count > 0:
                report.missing_value_counts[str(col)] = null_count

        # Outlier counts for numeric columns (using IQR with default threshold 1.5)
        for col in df.columns:
            if _is_numeric_column(df, col):
                count = self._count_outliers_iqr(df, str(col), 1.5)
                if count > 0:
                    report.outlier_counts[str(col)] = count

        # Whitespace field count
        ws_count = 0
        for col in df.columns:
            if df[col].dtype == object:
                has_ws = df[col].dropna().apply(
                    lambda x: isinstance(x, str) and (x != x.strip() or (len(x) > 0 and x.strip() == ""))
                ).any()
                if has_ws:
                    ws_count += 1
        report.whitespace_field_count = ws_count

        return report

    def clean(
        self, df: pd.DataFrame, config: ImportCleanConfig, schema: Dict[str, Any]
    ) -> Tuple[pd.DataFrame, CleanReport]:
        """Apply cleaning steps and return (cleaned_df, report).

        Execution order: dedup -> trim whitespace -> fill missing -> outlier
        """
        report = CleanReport(original_row_count=len(df))
        df = df.copy()

        # 1. Dedup
        if config.dedup.enabled:
            df, report = self._apply_dedup(df, config.dedup, report)

        # 2. Trim whitespace
        if config.trimWhitespace.enabled:
            df, report = self._apply_trim(df, config.trimWhitespace, schema, report)

        # 3. Fill missing
        if config.fillMissing.enabled:
            df, report = self._apply_fill(df, config.fillMissing, schema, report)

        # 4. Outlier detection
        if config.outlier.enabled:
            df, report = self._apply_outlier(df, config.outlier, schema, report)

        report.final_row_count = len(df)
        return df, report

    def _apply_dedup(
        self, df: pd.DataFrame, config: DedupConfig, report: CleanReport
    ) -> Tuple[pd.DataFrame, CleanReport]:
        before = len(df)
        if config.fields == "all":
            df = df.drop_duplicates(keep=config.keep)
        else:
            # Only use fields that actually exist in the dataframe
            valid_fields = [f for f in config.fields if f in df.columns]
            if valid_fields:
                df = df.drop_duplicates(subset=valid_fields, keep=config.keep)
        df = df.reset_index(drop=True)
        report.dedup_removed = before - len(df)
        return df, report

    def _apply_trim(
        self,
        df: pd.DataFrame,
        config: TrimWhitespaceConfig,
        schema: Dict[str, Any],
        report: CleanReport,
    ) -> Tuple[pd.DataFrame, CleanReport]:
        if config.fields == "string":
            target_cols = _resolve_fields_for_type(df, schema, "*string")
            # Also include object-dtype columns not in schema
            for col in df.columns:
                if df[col].dtype == object and str(col) not in target_cols:
                    target_cols.append(str(col))
        else:
            target_cols = [f for f in config.fields if f in df.columns]

        total_trimmed = 0
        for col in target_cols:
            if col not in df.columns:
                continue
            if df[col].dtype != object:
                continue
            mask_not_null = df[col].notna()
            original = df.loc[mask_not_null, col]
            trimmed = original.apply(lambda x: x.strip() if isinstance(x, str) else x)
            changed = (original != trimmed) & mask_not_null[mask_not_null].index.isin(original.index)
            total_trimmed += int(changed.sum())
            df.loc[mask_not_null, col] = trimmed
            # Convert pure-whitespace (now empty string) to NaN
            df.loc[df[col].apply(lambda x: isinstance(x, str) and x == ""), col] = np.nan

        report.trim_applied = total_trimmed
        return df, report

    def _apply_fill(
        self,
        df: pd.DataFrame,
        config: FillMissingConfig,
        schema: Dict[str, Any],
        report: CleanReport,
    ) -> Tuple[pd.DataFrame, CleanReport]:
        # Build field -> rule mapping. Specific fields override wildcards.
        wildcard_rules: List[ImportFillRule] = []
        specific_rules: Dict[str, ImportFillRule] = {}

        for rule in config.rules:
            if rule.field.startswith("*"):
                wildcard_rules.append(rule)
            else:
                specific_rules[rule.field] = rule

        # Resolve wildcards to actual columns
        resolved: Dict[str, ImportFillRule] = {}
        for rule in wildcard_rules:
            cols = _resolve_fields_for_type(df, schema, rule.field)
            for col in cols:
                if col not in resolved:
                    resolved[col] = rule

        # Specific rules override wildcard
        resolved.update(specific_rules)

        rows_to_drop = set()
        for col, rule in resolved.items():
            if col not in df.columns:
                continue
            null_mask = df[col].isna()
            null_count = int(null_mask.sum())
            if null_count == 0:
                continue

            if rule.strategy == "mean":
                if _is_numeric_column(df, col):
                    fill_val = df[col].mean()
                    df[col] = df[col].fillna(fill_val)
                    report.fill_applied[col] = null_count
            elif rule.strategy == "median":
                if _is_numeric_column(df, col):
                    fill_val = df[col].median()
                    df[col] = df[col].fillna(fill_val)
                    report.fill_applied[col] = null_count
            elif rule.strategy == "mode":
                mode_vals = df[col].mode()
                if len(mode_vals) > 0:
                    df[col] = df[col].fillna(mode_vals.iloc[0])
                    report.fill_applied[col] = null_count
            elif rule.strategy == "constant":
                const_val = rule.constantValue if rule.constantValue is not None else ""
                df[col] = df[col].fillna(const_val)
                report.fill_applied[col] = null_count
            elif rule.strategy == "forward":
                df[col] = df[col].ffill()
                # ffill may not fill if first row is NaN
                remaining = int(df[col].isna().sum())
                filled = null_count - remaining
                if filled > 0:
                    report.fill_applied[col] = filled
            elif rule.strategy == "drop_row":
                rows_to_drop.update(df.index[null_mask].tolist())
                report.fill_applied[col] = null_count

        if rows_to_drop:
            df = df.drop(index=list(rows_to_drop)).reset_index(drop=True)

        return df, report

    def _apply_outlier(
        self,
        df: pd.DataFrame,
        config: OutlierConfig,
        schema: Dict[str, Any],
        report: CleanReport,
    ) -> Tuple[pd.DataFrame, CleanReport]:
        if config.targetFields == "numeric":
            target_cols = [str(c) for c in df.columns if _is_numeric_column(df, c)]
        else:
            target_cols = [f for f in config.targetFields if f in df.columns]

        all_outlier_rows = set()
        for col in target_cols:
            if not _is_numeric_column(df, col):
                continue
            if config.method == "iqr":
                outlier_mask = self._get_outlier_mask_iqr(df, col, config.threshold)
            else:  # zscore
                outlier_mask = self._get_outlier_mask_zscore(df, col, config.threshold)

            count = int(outlier_mask.sum())
            if count == 0:
                continue

            if config.action == "flag":
                df[f"_{col}_outlier"] = outlier_mask
                report.outlier_flagged[col] = count
            else:  # remove
                all_outlier_rows.update(df.index[outlier_mask].tolist())
                report.outlier_flagged[col] = count

        if config.action == "remove" and all_outlier_rows:
            report.outlier_removed = len(all_outlier_rows)
            df = df.drop(index=list(all_outlier_rows)).reset_index(drop=True)

        return df, report

    @staticmethod
    def _count_outliers_iqr(df: pd.DataFrame, col: str, threshold: float) -> int:
        series = df[col].dropna()
        if len(series) < 4:
            return 0
        q1 = series.quantile(0.25)
        q3 = series.quantile(0.75)
        iqr = q3 - q1
        if iqr == 0:
            return 0
        lower = q1 - threshold * iqr
        upper = q3 + threshold * iqr
        return int(((series < lower) | (series > upper)).sum())

    @staticmethod
    def _get_outlier_mask_iqr(df: pd.DataFrame, col: str, threshold: float) -> pd.Series:
        series = df[col]
        clean = series.dropna()
        if len(clean) < 4:
            return pd.Series(False, index=df.index)
        q1 = clean.quantile(0.25)
        q3 = clean.quantile(0.75)
        iqr = q3 - q1
        if iqr == 0:
            return pd.Series(False, index=df.index)
        lower = q1 - threshold * iqr
        upper = q3 + threshold * iqr
        return (series < lower) | (series > upper)

    @staticmethod
    def _get_outlier_mask_zscore(df: pd.DataFrame, col: str, threshold: float) -> pd.Series:
        series = df[col]
        clean = series.dropna()
        if len(clean) < 2:
            return pd.Series(False, index=df.index)
        mean = clean.mean()
        std = clean.std()
        if std == 0:
            return pd.Series(False, index=df.index)
        z = ((series - mean) / std).abs()
        return z > threshold
