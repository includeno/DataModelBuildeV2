"""Unit tests for import_cleaner module."""

import pandas as pd
import numpy as np
import pytest
from models import (
    ImportCleanConfig,
    DedupConfig,
    FillMissingConfig,
    ImportFillRule,
    OutlierConfig,
    TrimWhitespaceConfig,
)
from import_cleaner import ImportCleaner


@pytest.fixture
def cleaner():
    return ImportCleaner()


def _schema(df: pd.DataFrame) -> dict:
    """Quick schema builder mimicking _build_dataframe_schema."""
    result = {}
    for col, dtype in df.dtypes.items():
        raw = str(dtype).lower()
        if "int" in raw or "float" in raw:
            result[str(col)] = {"type": "number"}
        elif "bool" in raw:
            result[str(col)] = {"type": "boolean"}
        elif "date" in raw or "time" in raw:
            result[str(col)] = {"type": "date"}
        else:
            result[str(col)] = {"type": "string"}
    return result


# ── Preview ──────────────────────────────────────────────────────────────

class TestPreview:
    def test_preview_duplicates(self, cleaner):
        df = pd.DataFrame({"a": [1, 2, 2, 3, 3, 3]})
        report = cleaner.preview(df, _schema(df))
        assert report.to_dict()["duplicateRowCount"] == 3

    def test_preview_missing(self, cleaner):
        df = pd.DataFrame({"a": [1, np.nan, 3], "b": ["x", None, "z"]})
        report = cleaner.preview(df, _schema(df))
        assert report.to_dict()["missingValueCounts"] == {"a": 1, "b": 1}

    def test_preview_outliers(self, cleaner):
        df = pd.DataFrame({"val": [10, 11, 12, 13, 14, 100]})
        report = cleaner.preview(df, _schema(df))
        assert report.to_dict()["outlierCounts"]["val"] > 0

    def test_preview_outliers_ignores_boolean_columns(self, cleaner):
        df = pd.DataFrame({
            "flag": [True, False, True, False, True, False],
            "val": [10, 11, 12, 13, 14, 100],
        })
        report = cleaner.preview(df, _schema(df))
        outliers = report.to_dict()["outlierCounts"]
        assert "flag" not in outliers
        assert outliers.get("val", 0) > 0

    def test_preview_whitespace(self, cleaner):
        df = pd.DataFrame({"name": ["  hello  ", "world", "   "]})
        report = cleaner.preview(df, _schema(df))
        assert report.to_dict()["whitespaceFieldCount"] == 1

    def test_preview_clean_data(self, cleaner):
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        report = cleaner.preview(df, _schema(df))
        d = report.to_dict()
        assert d["duplicateRowCount"] == 0
        assert d["missingValueCounts"] == {}
        assert d["whitespaceFieldCount"] == 0


# ── Dedup ────────────────────────────────────────────────────────────────

class TestDedup:
    def test_dedup_all_fields(self, cleaner):
        df = pd.DataFrame({"a": [1, 2, 2, 3, 3, 3, 4, 5, 5, 5], "b": ["x"] * 10})
        config = ImportCleanConfig(
            dedup=DedupConfig(enabled=True, fields="all", keep="first"),
            fillMissing=FillMissingConfig(enabled=False),
            outlier=OutlierConfig(enabled=False),
            trimWhitespace=TrimWhitespaceConfig(enabled=False),
        )
        result, report = cleaner.clean(df, config, _schema(df))
        assert len(result) == 5  # unique (a, b) pairs
        assert report.dedup_removed == 5

    def test_dedup_specific_fields(self, cleaner):
        df = pd.DataFrame({"a": [1, 1, 2], "b": ["x", "y", "z"]})
        config = ImportCleanConfig(
            dedup=DedupConfig(enabled=True, fields=["a"], keep="first"),
            fillMissing=FillMissingConfig(enabled=False),
            outlier=OutlierConfig(enabled=False),
            trimWhitespace=TrimWhitespaceConfig(enabled=False),
        )
        result, report = cleaner.clean(df, config, _schema(df))
        assert len(result) == 2
        assert report.dedup_removed == 1

    def test_dedup_keep_last(self, cleaner):
        df = pd.DataFrame({"a": [1, 1], "b": ["first", "last"]})
        config = ImportCleanConfig(
            dedup=DedupConfig(enabled=True, fields=["a"], keep="last"),
            fillMissing=FillMissingConfig(enabled=False),
            outlier=OutlierConfig(enabled=False),
            trimWhitespace=TrimWhitespaceConfig(enabled=False),
        )
        result, _ = cleaner.clean(df, config, _schema(df))
        assert result.iloc[0]["b"] == "last"

    def test_dedup_disabled(self, cleaner):
        df = pd.DataFrame({"a": [1, 1, 1]})
        config = ImportCleanConfig(
            dedup=DedupConfig(enabled=False),
            fillMissing=FillMissingConfig(enabled=False),
            outlier=OutlierConfig(enabled=False),
            trimWhitespace=TrimWhitespaceConfig(enabled=False),
        )
        result, report = cleaner.clean(df, config, _schema(df))
        assert len(result) == 3
        assert report.dedup_removed == 0

    def test_dedup_invalid_fields_ignored(self, cleaner):
        df = pd.DataFrame({"a": [1, 1, 2]})
        config = ImportCleanConfig(
            dedup=DedupConfig(enabled=True, fields=["nonexistent"], keep="first"),
            fillMissing=FillMissingConfig(enabled=False),
            outlier=OutlierConfig(enabled=False),
            trimWhitespace=TrimWhitespaceConfig(enabled=False),
        )
        result, report = cleaner.clean(df, config, _schema(df))
        # No valid fields to dedup on, so no rows removed
        assert len(result) == 3
        assert report.dedup_removed == 0


# ── Trim Whitespace ──────────────────────────────────────────────────────

class TestTrimWhitespace:
    def test_trim_strips_whitespace(self, cleaner):
        df = pd.DataFrame({"name": ["  hello  ", "world"], "val": [1, 2]})
        schema = {"name": {"type": "string"}, "val": {"type": "number"}}
        config = ImportCleanConfig(
            dedup=DedupConfig(enabled=False),
            fillMissing=FillMissingConfig(enabled=False),
            outlier=OutlierConfig(enabled=False),
            trimWhitespace=TrimWhitespaceConfig(enabled=True, fields="string"),
        )
        result, report = cleaner.clean(df, config, schema)
        assert result.iloc[0]["name"] == "hello"
        assert report.trim_applied == 1

    def test_trim_pure_whitespace_becomes_nan(self, cleaner):
        df = pd.DataFrame({"name": ["ok", "   ", "fine"]})
        schema = {"name": {"type": "string"}}
        config = ImportCleanConfig(
            dedup=DedupConfig(enabled=False),
            fillMissing=FillMissingConfig(enabled=False),
            outlier=OutlierConfig(enabled=False),
            trimWhitespace=TrimWhitespaceConfig(enabled=True, fields="string"),
        )
        result, _ = cleaner.clean(df, config, schema)
        assert pd.isna(result.iloc[1]["name"])

    def test_trim_specific_fields(self, cleaner):
        df = pd.DataFrame({"a": ["  x  "], "b": ["  y  "]})
        schema = {"a": {"type": "string"}, "b": {"type": "string"}}
        config = ImportCleanConfig(
            dedup=DedupConfig(enabled=False),
            fillMissing=FillMissingConfig(enabled=False),
            outlier=OutlierConfig(enabled=False),
            trimWhitespace=TrimWhitespaceConfig(enabled=True, fields=["a"]),
        )
        result, _ = cleaner.clean(df, config, schema)
        assert result.iloc[0]["a"] == "x"
        assert result.iloc[0]["b"] == "  y  "  # untouched


# ── Fill Missing ─────────────────────────────────────────────────────────

class TestFillMissing:
    def test_fill_median(self, cleaner):
        df = pd.DataFrame({"salary": [100.0, 200.0, np.nan, 400.0]})
        config = ImportCleanConfig(
            dedup=DedupConfig(enabled=False),
            fillMissing=FillMissingConfig(
                enabled=True,
                rules=[ImportFillRule(field="salary", strategy="median")],
            ),
            outlier=OutlierConfig(enabled=False),
            trimWhitespace=TrimWhitespaceConfig(enabled=False),
        )
        result, report = cleaner.clean(df, config, _schema(df))
        assert not result["salary"].isna().any()
        assert result.iloc[2]["salary"] == 200.0  # median of [100, 200, 400]
        assert report.fill_applied["salary"] == 1

    def test_fill_mean(self, cleaner):
        df = pd.DataFrame({"val": [10.0, 20.0, np.nan]})
        config = ImportCleanConfig(
            dedup=DedupConfig(enabled=False),
            fillMissing=FillMissingConfig(
                enabled=True,
                rules=[ImportFillRule(field="val", strategy="mean")],
            ),
            outlier=OutlierConfig(enabled=False),
            trimWhitespace=TrimWhitespaceConfig(enabled=False),
        )
        result, report = cleaner.clean(df, config, _schema(df))
        assert abs(result.iloc[2]["val"] - 15.0) < 0.01
        assert report.fill_applied["val"] == 1

    def test_fill_mode(self, cleaner):
        df = pd.DataFrame({"cat": ["a", "a", "b", None]})
        schema = {"cat": {"type": "string"}}
        config = ImportCleanConfig(
            dedup=DedupConfig(enabled=False),
            fillMissing=FillMissingConfig(
                enabled=True,
                rules=[ImportFillRule(field="cat", strategy="mode")],
            ),
            outlier=OutlierConfig(enabled=False),
            trimWhitespace=TrimWhitespaceConfig(enabled=False),
        )
        result, report = cleaner.clean(df, config, schema)
        assert result.iloc[3]["cat"] == "a"
        assert report.fill_applied["cat"] == 1

    def test_fill_constant(self, cleaner):
        df = pd.DataFrame({"name": ["Alice", None, "Charlie"]})
        schema = {"name": {"type": "string"}}
        config = ImportCleanConfig(
            dedup=DedupConfig(enabled=False),
            fillMissing=FillMissingConfig(
                enabled=True,
                rules=[ImportFillRule(field="name", strategy="constant", constantValue="UNKNOWN")],
            ),
            outlier=OutlierConfig(enabled=False),
            trimWhitespace=TrimWhitespaceConfig(enabled=False),
        )
        result, report = cleaner.clean(df, config, schema)
        assert result.iloc[1]["name"] == "UNKNOWN"
        assert report.fill_applied["name"] == 1

    def test_fill_forward(self, cleaner):
        df = pd.DataFrame({"val": [1.0, np.nan, np.nan, 4.0]})
        config = ImportCleanConfig(
            dedup=DedupConfig(enabled=False),
            fillMissing=FillMissingConfig(
                enabled=True,
                rules=[ImportFillRule(field="val", strategy="forward")],
            ),
            outlier=OutlierConfig(enabled=False),
            trimWhitespace=TrimWhitespaceConfig(enabled=False),
        )
        result, report = cleaner.clean(df, config, _schema(df))
        assert result.iloc[1]["val"] == 1.0
        assert result.iloc[2]["val"] == 1.0
        assert report.fill_applied["val"] == 2

    def test_fill_drop_row(self, cleaner):
        df = pd.DataFrame({"date_col": ["2024-01-01", None, "2024-03-01"]})
        schema = {"date_col": {"type": "date"}}
        config = ImportCleanConfig(
            dedup=DedupConfig(enabled=False),
            fillMissing=FillMissingConfig(
                enabled=True,
                rules=[ImportFillRule(field="date_col", strategy="drop_row")],
            ),
            outlier=OutlierConfig(enabled=False),
            trimWhitespace=TrimWhitespaceConfig(enabled=False),
        )
        result, report = cleaner.clean(df, config, schema)
        assert len(result) == 2
        assert report.fill_applied["date_col"] == 1

    def test_fill_wildcard_number(self, cleaner):
        df = pd.DataFrame({"a": [1.0, np.nan], "b": [np.nan, 2.0], "c": ["x", None]})
        schema = {"a": {"type": "number"}, "b": {"type": "number"}, "c": {"type": "string"}}
        config = ImportCleanConfig(
            dedup=DedupConfig(enabled=False),
            fillMissing=FillMissingConfig(
                enabled=True,
                rules=[ImportFillRule(field="*number", strategy="median")],
            ),
            outlier=OutlierConfig(enabled=False),
            trimWhitespace=TrimWhitespaceConfig(enabled=False),
        )
        result, report = cleaner.clean(df, config, schema)
        assert not result["a"].isna().any()
        assert not result["b"].isna().any()
        assert result["c"].isna().any()  # string col not affected by *number
        assert "a" in report.fill_applied
        assert "b" in report.fill_applied
        assert "c" not in report.fill_applied

    def test_fill_specific_overrides_wildcard(self, cleaner):
        df = pd.DataFrame({"a": [np.nan], "b": [np.nan]})
        schema = {"a": {"type": "number"}, "b": {"type": "number"}}
        config = ImportCleanConfig(
            dedup=DedupConfig(enabled=False),
            fillMissing=FillMissingConfig(
                enabled=True,
                rules=[
                    ImportFillRule(field="*number", strategy="median"),
                    ImportFillRule(field="a", strategy="constant", constantValue="99"),
                ],
            ),
            outlier=OutlierConfig(enabled=False),
            trimWhitespace=TrimWhitespaceConfig(enabled=False),
        )
        result, report = cleaner.clean(df, config, schema)
        # "a" should use constant (specific rule), not median
        assert result.iloc[0]["a"] == "99"
        assert "a" in report.fill_applied


# ── Outlier Detection ────────────────────────────────────────────────────

class TestOutlier:
    def test_outlier_iqr_flag(self, cleaner):
        data = list(range(1, 21)) + [1000]
        df = pd.DataFrame({"val": data})
        config = ImportCleanConfig(
            dedup=DedupConfig(enabled=False),
            fillMissing=FillMissingConfig(enabled=False),
            outlier=OutlierConfig(enabled=True, method="iqr", threshold=1.5, action="flag", targetFields="numeric"),
            trimWhitespace=TrimWhitespaceConfig(enabled=False),
        )
        result, report = cleaner.clean(df, config, _schema(df))
        assert "_val_outlier" in result.columns
        assert result["_val_outlier"].sum() > 0
        assert report.outlier_flagged["val"] > 0
        assert len(result) == len(df)  # no rows removed

    def test_outlier_iqr_remove(self, cleaner):
        data = list(range(1, 21)) + [1000]
        df = pd.DataFrame({"val": data})
        config = ImportCleanConfig(
            dedup=DedupConfig(enabled=False),
            fillMissing=FillMissingConfig(enabled=False),
            outlier=OutlierConfig(enabled=True, method="iqr", threshold=1.5, action="remove", targetFields="numeric"),
            trimWhitespace=TrimWhitespaceConfig(enabled=False),
        )
        result, report = cleaner.clean(df, config, _schema(df))
        assert "_val_outlier" not in result.columns
        assert len(result) < len(df)
        assert report.outlier_removed > 0

    def test_outlier_zscore_flag(self, cleaner):
        data = list(range(1, 21)) + [1000]
        df = pd.DataFrame({"val": data})
        config = ImportCleanConfig(
            dedup=DedupConfig(enabled=False),
            fillMissing=FillMissingConfig(enabled=False),
            outlier=OutlierConfig(enabled=True, method="zscore", threshold=3.0, action="flag", targetFields="numeric"),
            trimWhitespace=TrimWhitespaceConfig(enabled=False),
        )
        result, report = cleaner.clean(df, config, _schema(df))
        assert "_val_outlier" in result.columns
        assert report.outlier_flagged.get("val", 0) > 0

    def test_outlier_numeric_target_ignores_boolean_columns(self, cleaner):
        df = pd.DataFrame({
            "flag": [True, False, True, False, True, False, True, False, True, False, True, False],
            "val": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 1000],
        })
        config = ImportCleanConfig(
            dedup=DedupConfig(enabled=False),
            fillMissing=FillMissingConfig(enabled=False),
            outlier=OutlierConfig(enabled=True, method="iqr", threshold=1.5, action="flag", targetFields="numeric"),
            trimWhitespace=TrimWhitespaceConfig(enabled=False),
        )
        result, report = cleaner.clean(df, config, _schema(df))
        assert "_val_outlier" in result.columns
        assert "_flag_outlier" not in result.columns
        assert "val" in report.outlier_flagged

    def test_outlier_specific_fields(self, cleaner):
        df = pd.DataFrame({"a": list(range(20)) + [1000], "b": list(range(20)) + [999]})
        config = ImportCleanConfig(
            dedup=DedupConfig(enabled=False),
            fillMissing=FillMissingConfig(enabled=False),
            outlier=OutlierConfig(enabled=True, method="iqr", threshold=1.5, action="flag", targetFields=["a"]),
            trimWhitespace=TrimWhitespaceConfig(enabled=False),
        )
        result, report = cleaner.clean(df, config, _schema(df))
        assert "_a_outlier" in result.columns
        assert "_b_outlier" not in result.columns

    def test_outlier_disabled_by_default(self, cleaner):
        df = pd.DataFrame({"val": list(range(20)) + [1000]})
        config = ImportCleanConfig()  # default: outlier disabled
        result, report = cleaner.clean(df, config, _schema(df))
        assert "_val_outlier" not in result.columns
        assert report.outlier_flagged == {}


# ── Execution Order ──────────────────────────────────────────────────────

class TestExecutionOrder:
    def test_dedup_before_fill(self, cleaner):
        """Dedup runs first, then fill. Duplicated NaN rows should be deduped first."""
        df = pd.DataFrame({"a": [1.0, np.nan, np.nan, 2.0]})
        config = ImportCleanConfig(
            dedup=DedupConfig(enabled=True, fields="all", keep="first"),
            fillMissing=FillMissingConfig(
                enabled=True,
                rules=[ImportFillRule(field="a", strategy="constant", constantValue="0")],
            ),
            outlier=OutlierConfig(enabled=False),
            trimWhitespace=TrimWhitespaceConfig(enabled=False),
        )
        result, report = cleaner.clean(df, config, _schema(df))
        # Two NaN rows -> deduped to one -> filled to 0
        assert report.dedup_removed == 1
        assert len(result) == 3
        assert not result["a"].isna().any()

    def test_trim_before_fill(self, cleaner):
        """Trim runs before fill. Pure whitespace becomes NaN, then fill replaces it."""
        df = pd.DataFrame({"name": ["  Alice  ", "   ", "Charlie"]})
        schema = {"name": {"type": "string"}}
        config = ImportCleanConfig(
            dedup=DedupConfig(enabled=False),
            fillMissing=FillMissingConfig(
                enabled=True,
                rules=[ImportFillRule(field="name", strategy="constant", constantValue="BLANK")],
            ),
            outlier=OutlierConfig(enabled=False),
            trimWhitespace=TrimWhitespaceConfig(enabled=True, fields="string"),
        )
        result, report = cleaner.clean(df, config, schema)
        assert result.iloc[0]["name"] == "Alice"
        assert result.iloc[1]["name"] == "BLANK"  # was "   " -> NaN -> "BLANK"
        assert report.trim_applied >= 1

    def test_full_pipeline(self, cleaner):
        """All steps enabled: dedup -> trim -> fill -> outlier."""
        df = pd.DataFrame({
            "id": [1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
            "name": ["  Alice  ", "  Alice  ", None, "Charlie", "  ", "Eve", "Frank", "Grace", "Heidi", "Ivan", "Judy", "Karl"],
            "salary": [100.0, 100.0, 200.0, np.nan, 300.0, 400.0, 500.0, 600.0, 700.0, 800.0, 900.0, 50000.0],
        })
        schema = {
            "id": {"type": "number"},
            "name": {"type": "string"},
            "salary": {"type": "number"},
        }
        config = ImportCleanConfig(
            dedup=DedupConfig(enabled=True, fields="all", keep="first"),
            fillMissing=FillMissingConfig(
                enabled=True,
                rules=[
                    ImportFillRule(field="*number", strategy="median"),
                    ImportFillRule(field="*string", strategy="constant", constantValue="UNKNOWN"),
                ],
            ),
            outlier=OutlierConfig(enabled=True, method="iqr", threshold=1.5, action="flag", targetFields="numeric"),
            trimWhitespace=TrimWhitespaceConfig(enabled=True, fields="string"),
        )
        result, report = cleaner.clean(df, config, schema)
        assert report.dedup_removed == 1
        assert not result["salary"].isna().any()
        assert not result["name"].isna().any()
        assert "_salary_outlier" in result.columns
        assert report.final_row_count == len(result)


# ── Default Config ───────────────────────────────────────────────────────

class TestDefaultConfig:
    def test_default_config_no_data_loss(self, cleaner):
        """Default config should not remove non-anomalous data."""
        df = pd.DataFrame({
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "Dave", "Eve"],
            "salary": [100.0, 200.0, 300.0, 400.0, 500.0],
        })
        schema = {
            "id": {"type": "number"},
            "name": {"type": "string"},
            "salary": {"type": "number"},
        }
        config = ImportCleanConfig()  # all defaults
        result, report = cleaner.clean(df, config, schema)
        assert len(result) == 5
        assert report.dedup_removed == 0
        assert report.outlier_removed == 0

    def test_default_config_dedup_and_fill(self, cleaner):
        """Default config should dedup and fill missing values."""
        df = pd.DataFrame({
            "a": [1.0, 1.0, np.nan],
            "b": ["x", "x", None],
        })
        schema = {"a": {"type": "number"}, "b": {"type": "string"}}
        config = ImportCleanConfig()
        result, report = cleaner.clean(df, config, schema)
        assert report.dedup_removed == 1
        assert not result["a"].isna().any()
        assert not result["b"].isna().any()


# ── Skip Cleaning ────────────────────────────────────────────────────────

class TestSkipCleaning:
    def test_all_disabled(self, cleaner):
        """When all steps disabled, data is returned as-is."""
        df = pd.DataFrame({"a": [1, 1, np.nan], "b": ["  x  ", None, "z"]})
        config = ImportCleanConfig(
            dedup=DedupConfig(enabled=False),
            fillMissing=FillMissingConfig(enabled=False),
            outlier=OutlierConfig(enabled=False),
            trimWhitespace=TrimWhitespaceConfig(enabled=False),
        )
        result, report = cleaner.clean(df, config, _schema(df))
        assert len(result) == 3
        assert result["a"].isna().sum() == 1
        assert result.iloc[0]["b"] == "  x  "
        assert report.dedup_removed == 0
        assert report.trim_applied == 0
        assert report.fill_applied == {}
        assert report.outlier_flagged == {}


# ── Report ───────────────────────────────────────────────────────────────

class TestReport:
    def test_report_row_counts(self, cleaner):
        df = pd.DataFrame({"a": [1, 1, 2, 3]})
        config = ImportCleanConfig(
            dedup=DedupConfig(enabled=True, fields="all", keep="first"),
            fillMissing=FillMissingConfig(enabled=False),
            outlier=OutlierConfig(enabled=False),
            trimWhitespace=TrimWhitespaceConfig(enabled=False),
        )
        _, report = cleaner.clean(df, config, _schema(df))
        d = report.to_dict()
        assert d["originalRowCount"] == 4
        assert d["finalRowCount"] == 3
        assert d["dedupRemoved"] == 1

    def test_report_to_dict_complete(self, cleaner):
        df = pd.DataFrame({"a": [1]})
        _, report = cleaner.clean(df, ImportCleanConfig(), _schema(df))
        d = report.to_dict()
        assert set(d.keys()) == {
            "dedupRemoved", "fillApplied", "outlierFlagged",
            "outlierRemoved", "trimApplied", "originalRowCount", "finalRowCount",
        }
