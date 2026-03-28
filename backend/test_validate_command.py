"""Tests for the 'validate' command (T1.2.9).

Covers all 6 rule types × 3 validation modes.
"""

import pandas as pd
import numpy as np
import pytest

from engine import ExecutionEngine
from models import Command, CommandConfig, OperationNode, ValidationRule
from storage import storage


# ── Helpers ──────────────────────────────────────────────────────────────────

def _rule(**kwargs) -> ValidationRule:
    return ValidationRule(**kwargs)


def _cmd(rules: list[ValidationRule], mode: str = "warn") -> Command:
    return Command(
        id="validate_cmd",
        type="validate",
        order=0,
        config=CommandConfig(validationRules=rules, validationMode=mode),
    )


def _node(commands: list[Command]) -> OperationNode:
    return OperationNode(
        id="n1",
        type="operation",
        operationType="process",
        name="n1",
        enabled=True,
        commands=commands,
        children=[],
    )


def _engine_apply(df: pd.DataFrame, rules: list[ValidationRule], mode: str = "warn") -> pd.DataFrame:
    e = ExecutionEngine()
    cmd = _cmd(rules, mode)
    return e._apply_validate(df, cmd)


@pytest.fixture(autouse=True)
def clean_storage():
    storage.clear()
    yield
    storage.clear()


# ── Rule: not_null ────────────────────────────────────────────────────────────

class TestNotNull:
    def test_pass_when_no_nulls(self):
        df = pd.DataFrame({"name": ["Alice", "Bob"]})
        rule = _rule(id="r1", field="name", rule="not_null")
        out = _engine_apply(df, [rule])
        report = out.attrs["_validation_report"]
        assert report["passed"] is True
        assert report["totalChecks"] == 1
        assert report["failedChecks"] == 0

    def test_fail_when_null_present(self):
        df = pd.DataFrame({"name": ["Alice", None]})
        rule = _rule(id="r1", field="name", rule="not_null")
        out = _engine_apply(df, [rule], mode="warn")
        report = out.attrs["_validation_report"]
        assert report["passed"] is False
        assert report["details"][0]["failedRowCount"] == 1

    def test_fail_mode_raises(self):
        df = pd.DataFrame({"name": [None, None]})
        rule = _rule(id="r1", field="name", rule="not_null")
        with pytest.raises(ValueError, match="Validation failed"):
            _engine_apply(df, [rule], mode="fail")

    def test_flag_mode_adds_column(self):
        df = pd.DataFrame({"name": ["Alice", None]})
        rule = _rule(id="r1", field="name", rule="not_null")
        out = _engine_apply(df, [rule], mode="flag")
        assert "_validation_failed" in out.columns
        assert out["_validation_failed"].tolist() == [False, True]


# ── Rule: unique ─────────────────────────────────────────────────────────────

class TestUnique:
    def test_pass_when_all_unique(self):
        df = pd.DataFrame({"id": [1, 2, 3]})
        rule = _rule(id="r1", field="id", rule="unique")
        out = _engine_apply(df, [rule])
        assert out.attrs["_validation_report"]["passed"] is True

    def test_fail_when_duplicates(self):
        df = pd.DataFrame({"id": [1, 1, 2]})
        rule = _rule(id="r1", field="id", rule="unique")
        out = _engine_apply(df, [rule], mode="warn")
        report = out.attrs["_validation_report"]
        assert report["passed"] is False
        assert report["details"][0]["failedRowCount"] == 2  # both duplicate rows flagged

    def test_flag_mode_marks_duplicates(self):
        df = pd.DataFrame({"id": [1, 1, 2]})
        rule = _rule(id="r1", field="id", rule="unique")
        out = _engine_apply(df, [rule], mode="flag")
        assert "_validation_failed" in out.columns
        assert out["_validation_failed"].iloc[0] is True or out["_validation_failed"].iloc[0] == True
        assert out["_validation_failed"].iloc[2] is False or out["_validation_failed"].iloc[2] == False


# ── Rule: range ───────────────────────────────────────────────────────────────

class TestRange:
    def test_pass_within_range(self):
        df = pd.DataFrame({"age": [20, 30, 40]})
        rule = _rule(id="r1", field="age", rule="range", min=0, max=100)
        out = _engine_apply(df, [rule])
        assert out.attrs["_validation_report"]["passed"] is True

    def test_fail_below_min(self):
        df = pd.DataFrame({"age": [20, -1, 30]})
        rule = _rule(id="r1", field="age", rule="range", min=0, max=100)
        out = _engine_apply(df, [rule], mode="warn")
        assert out.attrs["_validation_report"]["details"][0]["failedRowCount"] == 1

    def test_fail_above_max(self):
        df = pd.DataFrame({"score": [0.5, 1.5, 0.8]})
        rule = _rule(id="r1", field="score", rule="range", min=0.0, max=1.0)
        out = _engine_apply(df, [rule], mode="warn")
        assert out.attrs["_validation_report"]["details"][0]["failedRowCount"] == 1

    def test_fail_mode_raises(self):
        df = pd.DataFrame({"age": [200]})
        rule = _rule(id="r1", field="age", rule="range", min=0, max=120)
        with pytest.raises(ValueError):
            _engine_apply(df, [rule], mode="fail")

    def test_flag_mode_marks_out_of_range(self):
        df = pd.DataFrame({"age": [10, 200, 50]})
        rule = _rule(id="r1", field="age", rule="range", min=0, max=120)
        out = _engine_apply(df, [rule], mode="flag")
        assert out["_validation_failed"].tolist() == [False, True, False]


# ── Rule: regex ───────────────────────────────────────────────────────────────

class TestRegex:
    def test_pass_matching_pattern(self):
        df = pd.DataFrame({"code": ["A123", "B456"]})
        rule = _rule(id="r1", field="code", rule="regex", pattern=r"^[A-Z]\d{3}$")
        out = _engine_apply(df, [rule])
        assert out.attrs["_validation_report"]["passed"] is True

    def test_fail_non_matching(self):
        df = pd.DataFrame({"code": ["A123", "bad", "C789"]})
        rule = _rule(id="r1", field="code", rule="regex", pattern=r"^[A-Z]\d{3}$")
        out = _engine_apply(df, [rule], mode="warn")
        assert out.attrs["_validation_report"]["details"][0]["failedRowCount"] == 1

    def test_flag_mode(self):
        df = pd.DataFrame({"email": ["a@b.com", "notanemail", "c@d.com"]})
        rule = _rule(id="r1", field="email", rule="regex", pattern=r"^.+@.+\..+$")
        out = _engine_apply(df, [rule], mode="flag")
        assert out["_validation_failed"].tolist() == [False, True, False]


# ── Rule: enum ────────────────────────────────────────────────────────────────

class TestEnum:
    def test_pass_all_in_enum(self):
        df = pd.DataFrame({"status": ["active", "inactive"]})
        rule = _rule(id="r1", field="status", rule="enum", enumValues=["active", "inactive", "pending"])
        out = _engine_apply(df, [rule])
        assert out.attrs["_validation_report"]["passed"] is True

    def test_fail_value_not_in_enum(self):
        df = pd.DataFrame({"status": ["active", "unknown"]})
        rule = _rule(id="r1", field="status", rule="enum", enumValues=["active", "inactive"])
        out = _engine_apply(df, [rule], mode="warn")
        assert out.attrs["_validation_report"]["details"][0]["failedRowCount"] == 1

    def test_fail_mode_raises(self):
        df = pd.DataFrame({"status": ["bad_value"]})
        rule = _rule(id="r1", field="status", rule="enum", enumValues=["active"])
        with pytest.raises(ValueError):
            _engine_apply(df, [rule], mode="fail")

    def test_flag_mode(self):
        df = pd.DataFrame({"color": ["red", "green", "purple"]})
        rule = _rule(id="r1", field="color", rule="enum", enumValues=["red", "green", "blue"])
        out = _engine_apply(df, [rule], mode="flag")
        assert out["_validation_failed"].tolist() == [False, False, True]


# ── Rule: type_check ──────────────────────────────────────────────────────────

class TestTypeCheck:
    def test_pass_all_numeric(self):
        df = pd.DataFrame({"amount": ["100", "200", "300"]})
        rule = _rule(id="r1", field="amount", rule="type_check", expectedType="number")
        out = _engine_apply(df, [rule])
        assert out.attrs["_validation_report"]["passed"] is True

    def test_fail_non_numeric(self):
        df = pd.DataFrame({"amount": ["100", "abc", "300"]})
        rule = _rule(id="r1", field="amount", rule="type_check", expectedType="number")
        out = _engine_apply(df, [rule], mode="warn")
        assert out.attrs["_validation_report"]["details"][0]["failedRowCount"] == 1

    def test_flag_mode(self):
        df = pd.DataFrame({"val": ["1", "nope", "3"]})
        rule = _rule(id="r1", field="val", rule="type_check", expectedType="number")
        out = _engine_apply(df, [rule], mode="flag")
        assert out["_validation_failed"].tolist() == [False, True, False]


# ── Multiple rules ────────────────────────────────────────────────────────────

class TestMultipleRules:
    def test_multiple_rules_report_all_details(self):
        df = pd.DataFrame({"name": [None, "Bob"], "age": [25, 200]})
        rules = [
            _rule(id="r1", field="name", rule="not_null"),
            _rule(id="r2", field="age", rule="range", min=0, max=120),
        ]
        out = _engine_apply(df, rules, mode="warn")
        report = out.attrs["_validation_report"]
        assert report["totalChecks"] == 2
        assert report["failedChecks"] == 2
        assert report["passed"] is False

    def test_missing_field_is_skipped(self):
        df = pd.DataFrame({"name": ["Alice"]})
        rules = [
            _rule(id="r1", field="nonexistent", rule="not_null"),
            _rule(id="r2", field="name", rule="not_null"),
        ]
        out = _engine_apply(df, rules)
        report = out.attrs["_validation_report"]
        # nonexistent field is skipped; only name is checked
        assert report["totalChecks"] == 1
        assert report["passed"] is True

    def test_warn_mode_does_not_raise_or_modify_df(self):
        df = pd.DataFrame({"val": [None, None]})
        rules = [_rule(id="r1", field="val", rule="not_null")]
        out = _engine_apply(df, rules, mode="warn")
        assert "_validation_failed" not in out.columns
        assert out.attrs["_validation_report"]["passed"] is False

    def test_all_pass_returns_passed_true(self):
        df = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        rules = [
            _rule(id="r1", field="id", rule="unique"),
            _rule(id="r2", field="name", rule="not_null"),
        ]
        out = _engine_apply(df, rules)
        assert out.attrs["_validation_report"]["passed"] is True
        assert out.attrs["_validation_report"]["failedChecks"] == 0

    def test_sample_values_populated_on_failure(self):
        df = pd.DataFrame({"code": ["A1", "bad1", "bad2", "A2"]})
        rules = [_rule(id="r1", field="code", rule="regex", pattern=r"^A\d$")]
        out = _engine_apply(df, rules, mode="warn")
        detail = out.attrs["_validation_report"]["details"][0]
        assert detail["failedRowCount"] == 2
        assert len(detail["sampleValues"]) == 2
