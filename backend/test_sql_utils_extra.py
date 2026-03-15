import pytest

from sql_utils import (
    is_quoted_identifier,
    is_reserved_identifier,
    needs_quoting,
    quote_identifier,
    quote_table_ref,
    unquote_identifier,
)


def test_is_quoted_identifier_variants():
    assert is_quoted_identifier('"abc"') is True
    assert is_quoted_identifier("`abc`") is True
    assert is_quoted_identifier("[abc]") is True
    assert is_quoted_identifier("abc") is False
    assert is_quoted_identifier("") is False


def test_unquote_identifier_variants():
    assert unquote_identifier('"a""b"') == 'a"b'
    assert unquote_identifier("`abc`") == "abc"
    assert unquote_identifier("[abc]") == "abc"
    assert unquote_identifier(" abc ") == "abc"
    assert unquote_identifier("") == ""


@pytest.mark.parametrize(
    "name,expected",
    [
        ("", True),
        ("abc", False),
        ("select", True),
        ("line-item", True),
        ('"quoted"', False),
    ],
)
def test_needs_quoting(name, expected):
    assert needs_quoting(name) is expected


def test_is_reserved_identifier_handles_empty_and_quoted():
    assert is_reserved_identifier("") is False
    assert is_reserved_identifier("select") is True
    assert is_reserved_identifier('"select"') is True
    assert is_reserved_identifier("normal_name") is False


def test_quote_identifier_branches():
    assert quote_identifier("") == ""
    assert quote_identifier("*") == "*"
    assert quote_identifier('"already"') == '"already"'
    assert quote_identifier("schema.table") == 'schema."table"'
    assert quote_identifier("schema.select") == 'schema."select"'
    assert quote_identifier("line-item") == '"line-item"'


def test_quote_table_ref_branches():
    assert quote_table_ref(None) is None
    assert quote_table_ref("") == ""
    assert quote_table_ref("orders") == "orders"
    assert quote_table_ref("select") == '"select"'
    assert quote_table_ref("(SELECT * FROM t)") == "(SELECT * FROM t)"
    assert quote_table_ref("orders o") == "orders o"
