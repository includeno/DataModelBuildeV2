import pandas as pd
import pytest
from fastapi.testclient import TestClient

from main import app
from storage import storage

client = TestClient(app)


def _build_orders_df() -> pd.DataFrame:
    rows = []
    categories = ["A", "B", "C", "D"]
    statuses = ["new", "paid", "closed", "pending"]
    for i in range(1, 61):
        category = categories[(i - 1) % len(categories)]
        status = statuses[(i - 1) % len(statuses)]
        note = f"{category.lower()}-order-{i}"
        if i % 5 == 0:
            note = f"{note}-vip"
        if i % 11 == 0:
            note = ""
        if i % 13 == 0:
            note = None
        rows.append(
            {
                "id": i,
                "amount": ((i * 7) % 120) + 1,
                "quantity": ((i * 3) % 15) + 1,
                "category": category,
                "status": status,
                "note": note,
                "customer_id": (i % 20) + 1,
            }
        )
    return pd.DataFrame(rows)


def _build_customers_df() -> pd.DataFrame:
    rows = []
    tiers = ["gold", "silver", "bronze", "vip"]
    regions = ["north", "south", "east", "west"]
    for cid in range(1, 26):
        rows.append(
            {
                "customer_id": cid,
                "customer_name": f"customer_{cid}",
                "tier": tiers[(cid - 1) % len(tiers)],
                "region": regions[(cid - 1) % len(regions)],
            }
        )
    return pd.DataFrame(rows)


ORDERS_DF = _build_orders_df()
CUSTOMERS_DF = _build_customers_df()


@pytest.fixture(scope="module")
def api_context():
    storage.clear()
    res = client.post("/sessions")
    assert res.status_code == 200
    session_id = res.json()["sessionId"]
    storage.add_dataset(session_id, "orders", ORDERS_DF.copy())
    storage.add_dataset(session_id, "customers", CUSTOMERS_DF.copy())
    yield {"session_id": session_id}
    storage.clear()


def _build_setup_node():
    return {
        "id": "setup",
        "type": "operation",
        "operationType": "setup",
        "name": "Data Setup",
        "enabled": True,
        "commands": [
            {
                "id": "setup_orders",
                "type": "source",
                "order": 0,
                "config": {"mainTable": "orders", "alias": "orders", "linkId": "orders_link"},
            },
            {
                "id": "setup_customers",
                "type": "source",
                "order": 1,
                "config": {"mainTable": "customers", "alias": "customers", "linkId": "customers_link"},
            },
        ],
        "children": [],
    }


def _build_tree(commands):
    return {
        "id": "root",
        "type": "operation",
        "name": "Root",
        "enabled": True,
        "commands": [{"id": "src", "type": "source", "order": 0, "config": {"mainTable": "orders"}}] + commands,
        "children": [_build_setup_node()],
    }


def _build_sql_tree(target_command):
    if target_command["type"] == "source":
        root_commands = [target_command]
    else:
        root_commands = [{"id": "src", "type": "source", "order": 0, "config": {"mainTable": "orders"}}, target_command]
    return {
        "id": "root",
        "type": "operation",
        "name": "Root",
        "enabled": True,
        "commands": root_commands,
        "children": [_build_setup_node()],
    }


def _numeric_mask(series: pd.Series, op: str, value: float) -> pd.Series:
    if op == ">":
        return series > value
    if op == ">=":
        return series >= value
    if op == "<":
        return series < value
    if op == "<=":
        return series <= value
    if op == "=":
        return series == value
    if op == "!=":
        return series != value
    raise ValueError(f"Unsupported numeric operator: {op}")


def _resolve_values(raw_value):
    if isinstance(raw_value, list):
        return [str(x) for x in raw_value]
    return [x.strip() for x in str(raw_value).split(",")]


def _text_mask(series: pd.Series, op: str, raw_value) -> pd.Series:
    s_str = series.astype(str)
    val = str(raw_value)
    if op == "=":
        return s_str == val
    if op == "!=":
        return s_str != val
    if op == "contains":
        values = _resolve_values(raw_value)
        mask = pd.Series([False] * len(series), index=series.index)
        for part in values:
            mask = mask | s_str.str.contains(part, case=False, na=False)
        return mask
    if op == "not_contains":
        values = _resolve_values(raw_value)
        mask = pd.Series([False] * len(series), index=series.index)
        for part in values:
            mask = mask | s_str.str.contains(part, case=False, na=False)
        return ~mask
    if op == "starts_with":
        return s_str.str.startswith(val, na=False)
    if op == "ends_with":
        return s_str.str.endswith(val, na=False)
    if op == "is_empty":
        return series.isna() | (s_str == "")
    if op == "is_not_empty":
        return (~series.isna()) & (s_str != "")
    raise ValueError(f"Unsupported text operator: {op}")


def _list_mask(series: pd.Series, op: str, raw_value, numeric: bool = False) -> pd.Series:
    values = _resolve_values(raw_value)
    if numeric:
        try:
            list_vals = [float(x) for x in values if x.strip()]
            mask = series.isin(list_vals)
        except Exception:
            mask = pd.Series([False] * len(series), index=series.index)
    else:
        mask = series.astype(str).isin(values)
    if op in ("not_in_list", "not_in_variable"):
        return ~mask
    return mask


NUMERIC_OPS = [">", ">=", "<", "<=", "=", "!="]
NUMERIC_THRESHOLDS = list(range(0, 121, 10))
TEXT_OPS = ["=", "!=", "contains", "not_contains", "starts_with", "ends_with"]
TEXT_VALUES = ["A", "B", "new", "pay", "ing", "vip", "x", ""]
LIST_OPS = ["in_list", "not_in_list"]
LIST_VALUES_TEXT = ["A,B", "A,C", "B,D", "new,paid", "pending", "", "A,new"]
LIST_VALUES_NUM = ["1,2", "3,4,5", "6,7,8,9", "10,11,12", "13,14,15", "16,17,18,19", "20"]


EXECUTE_NUMERIC_CASES = []
for field in ["amount", "quantity"]:
    for op in NUMERIC_OPS:
        for threshold in NUMERIC_THRESHOLDS:
            for data_source in ["stream", "orders"]:
                for with_data_type in [False, True]:
                    expected = int(_numeric_mask(ORDERS_DF[field], op, threshold).sum())
                    case_id = f"{field}-{op}-{threshold}-{data_source}-dtype-{int(with_data_type)}"
                    EXECUTE_NUMERIC_CASES.append(
                        pytest.param(field, op, threshold, data_source, with_data_type, expected, id=case_id)
                    )


EXECUTE_TEXT_CASES = []
for field in ["category", "status"]:
    for op in TEXT_OPS:
        for value in TEXT_VALUES:
            for data_source in ["stream", "orders"]:
                expected = int(_text_mask(ORDERS_DF[field], op, value).sum())
                safe_value = str(value).replace(",", "_").replace(" ", "_")
                case_id = f"{field}-{op}-{safe_value}-{data_source}"
                EXECUTE_TEXT_CASES.append(pytest.param(field, op, value, data_source, expected, id=case_id))


EXECUTE_LIST_TEXT_CASES = []
for field in ["category", "status"]:
    for op in LIST_OPS:
        for values in LIST_VALUES_TEXT:
            for data_source in ["stream", "orders"]:
                expected = int(_list_mask(ORDERS_DF[field], op, values, numeric=False).sum())
                safe_values = values.replace(",", "_")
                case_id = f"{field}-{op}-{safe_values}-{data_source}"
                EXECUTE_LIST_TEXT_CASES.append(pytest.param(field, op, values, data_source, expected, id=case_id))


EXECUTE_LIST_NUM_CASES = []
for op in LIST_OPS:
    for values in LIST_VALUES_NUM:
        for data_source in ["stream", "orders"]:
            # Keep parity with current engine behavior: list ops on numeric fields
            # are evaluated through string matching unless explicit numeric-path logic is added.
            expected = int(_list_mask(ORDERS_DF["customer_id"], op, values, numeric=False).sum())
            safe_values = values.replace(",", "_")
            case_id = f"customer_id-{op}-{safe_values}-{data_source}"
            EXECUTE_LIST_NUM_CASES.append(pytest.param(op, values, data_source, expected, id=case_id))


EXECUTE_EMPTY_CASES = []
for op in ["is_empty", "is_not_empty", "is_null", "is_not_null"]:
    for data_source in ["stream", "orders"]:
        if op == "is_null":
            expected = int(ORDERS_DF["note"].isna().sum())
        elif op == "is_not_null":
            expected = int((~ORDERS_DF["note"].isna()).sum())
        else:
            expected = int(_text_mask(ORDERS_DF["note"], op, "").sum())
        case_id = f"note-{op}-{data_source}"
        EXECUTE_EMPTY_CASES.append(pytest.param(op, data_source, expected, id=case_id))


def _build_generate_sql_cases():
    cases = []
    # Numeric filter SQL
    for op in NUMERIC_OPS:
        for threshold in range(0, 81, 10):
            for include_meta in [False, True]:
                cmd = {
                    "id": "target",
                    "type": "filter",
                    "order": 1,
                    "config": {"field": "amount", "operator": op, "value": threshold, "dataType": "number"},
                }
                case_id = f"filter-num-{op}-{threshold}-meta-{int(include_meta)}"
                tokens = ["SELECT * FROM orders", f"amount {op} {threshold}"]
                cases.append(pytest.param(cmd, include_meta, tokens, id=case_id))

    # Text filter SQL
    for op in ["contains", "not_contains", "starts_with", "ends_with", "=", "!="]:
        for value in ["A", "vip", "new", "pending", "x", "order"]:
            for include_meta in [False, True]:
                cmd = {
                    "id": "target",
                    "type": "filter",
                    "order": 1,
                    "config": {"field": "note", "operator": op, "value": value},
                }
                if op == "contains":
                    token = f"note LIKE '%{value}%'"
                elif op == "not_contains":
                    token = f"note NOT LIKE '%{value}%'"
                elif op == "starts_with":
                    token = f"note LIKE '{value}%'"
                elif op == "ends_with":
                    token = f"note LIKE '%{value}'"
                else:
                    token = f"note {op} '{value}'"
                case_id = f"filter-text-{op}-{value}-meta-{int(include_meta)}"
                tokens = ["SELECT * FROM orders", token]
                cases.append(pytest.param(cmd, include_meta, tokens, id=case_id))

    # Join SQL
    for join_type in ["LEFT", "RIGHT", "INNER", "FULL"]:
        for include_meta in [False, True]:
            cmd = {
                "id": "target",
                "type": "join",
                "order": 1,
                "config": {"joinTargetType": "table", "joinTable": "customers", "joinType": join_type, "on": "customer_id = customer_id"},
            }
            case_id = f"join-{join_type}-meta-{int(include_meta)}"
            tokens = [f"{join_type} JOIN customers", "ON", "SELECT t1.*, t2.*"]
            cases.append(pytest.param(cmd, include_meta, tokens, id=case_id))

    # Group/Having SQL
    for func in ["sum", "avg", "count", "min", "max", "first", "last"]:
        for include_meta in [False, True]:
            cmd = {
                "id": "target",
                "type": "group",
                "order": 1,
                "config": {
                    "groupByFields": ["category"],
                    "aggregations": [{"func": func, "field": "amount", "alias": "metric_amount"}],
                    "havingConditions": [{"metricAlias": "metric_amount", "operator": ">", "value": 50}],
                },
            }
            case_id = f"group-{func}-meta-{int(include_meta)}"
            tokens = ["GROUP BY category", "HAVING metric_amount > 50", "metric_amount"]
            cases.append(pytest.param(cmd, include_meta, tokens, id=case_id))

    # View SQL
    for limit in [0, 1, 5, 10, 20]:
        for ascending in [True, False]:
            for include_meta in [False, True]:
                cmd = {
                    "id": "target",
                    "type": "view",
                    "order": 1,
                    "config": {
                        "viewFields": [{"field": "id", "distinct": False}, {"field": "category", "distinct": False}],
                        "viewSortField": "id",
                        "viewSortAscending": ascending,
                        "viewLimit": limit,
                    },
                }
                direction = "ASC" if ascending else "DESC"
                case_id = f"view-limit-{limit}-sort-{direction}-meta-{int(include_meta)}"
                tokens = [f"ORDER BY id {direction}", f"LIMIT {limit}", "SELECT id, category FROM orders"]
                cases.append(pytest.param(cmd, include_meta, tokens, id=case_id))

    # Save SQL
    for field in ["category", "status", "customer_id"]:
        for distinct in [True, False]:
            for include_meta in [False, True]:
                cmd = {
                    "id": "target",
                    "type": "save",
                    "order": 1,
                    "config": {"field": field, "distinct": distinct},
                }
                prefix = "SELECT DISTINCT" if distinct else "SELECT"
                case_id = f"save-{field}-distinct-{int(distinct)}-meta-{int(include_meta)}"
                tokens = [prefix, field, "FROM orders"]
                cases.append(pytest.param(cmd, include_meta, tokens, id=case_id))

    # Transform SQL
    transform_exprs = [
        ("amount + 1", "amount_plus_1"),
        ("amount * 2", "amount_x2"),
        ("quantity + amount", "qty_amount"),
        ("amount - quantity", "amount_minus_qty"),
        ("amount / 2", "amount_half"),
        ("quantity * 10", "qty_x10"),
    ]
    for expression, output_field in transform_exprs:
        for include_meta in [False, True]:
            cmd = {
                "id": "target",
                "type": "transform",
                "order": 1,
                "config": {"mappings": [{"id": "m1", "expression": expression, "outputField": output_field, "mode": "simple"}]},
            }
            case_id = f"transform-{output_field}-meta-{int(include_meta)}"
            tokens = [f"{expression} AS {output_field}", "SELECT *,", "FROM orders"]
            cases.append(pytest.param(cmd, include_meta, tokens, id=case_id))

    # Sort SQL
    for field in ["amount", "quantity"]:
        for ascending in [True, False]:
            for include_meta in [False, True]:
                cmd = {
                    "id": "target",
                    "type": "sort",
                    "order": 1,
                    "config": {"field": field, "ascending": ascending},
                }
                direction = "ASC" if ascending else "DESC"
                case_id = f"sort-{field}-{direction}-meta-{int(include_meta)}"
                tokens = [f"ORDER BY {field} {direction}", "SELECT * FROM"]
                cases.append(pytest.param(cmd, include_meta, tokens, id=case_id))

    # Source SQL
    for alias in ["o1", "src_orders", "main_orders", "orders_alias"]:
        for include_meta in [False, True]:
            cmd = {"id": "target", "type": "source", "order": 0, "config": {"mainTable": "orders", "alias": alias}}
            case_id = f"source-alias-{alias}-meta-{int(include_meta)}"
            tokens = [f"SELECT * FROM orders AS {alias}"]
            cases.append(pytest.param(cmd, include_meta, tokens, id=case_id))

    return cases


GENERATE_SQL_CASES = _build_generate_sql_cases()


def _build_query_cases():
    cases = []
    # Numeric where matrix
    for field in ["amount", "quantity"]:
        for op in NUMERIC_OPS:
            for threshold in range(0, 101, 10):
                query = f"SELECT * FROM orders WHERE {field} {op} {threshold} ORDER BY id"
                expected_total = int(_numeric_mask(ORDERS_DF[field], op, threshold).sum())
                case_id = f"query-num-{field}-{op}-{threshold}"
                cases.append(pytest.param(query, 1, 200, expected_total, min(expected_total, 200), id=case_id))

    # Category equals / not equals
    for op in ["=", "!="]:
        for category in ["A", "B", "C", "D"]:
            query = f"SELECT * FROM orders WHERE category {op} '{category}' ORDER BY id"
            if op == "=":
                expected_total = int((ORDERS_DF["category"] == category).sum())
            else:
                expected_total = int((ORDERS_DF["category"] != category).sum())
            case_id = f"query-cat-{op}-{category}"
            cases.append(pytest.param(query, 1, 200, expected_total, min(expected_total, 200), id=case_id))

    # Note LIKE / NOT LIKE
    for op in ["LIKE", "NOT LIKE"]:
        for token in ["vip", "order", "a-order", "b-order", "pending", "z"]:
            if op == "LIKE":
                query = f"SELECT * FROM orders WHERE COALESCE(note, '') LIKE '%{token}%' ORDER BY id"
                expected_total = int(ORDERS_DF["note"].fillna("").str.contains(token, regex=False).sum())
            else:
                query = f"SELECT * FROM orders WHERE COALESCE(note, '') NOT LIKE '%{token}%' ORDER BY id"
                expected_total = int((~ORDERS_DF["note"].fillna("").str.contains(token, regex=False)).sum())
            case_id = f"query-note-{op}-{token}"
            cases.append(pytest.param(query, 1, 200, expected_total, min(expected_total, 200), id=case_id))

    # Group/Having
    category_counts = ORDERS_DF.groupby("category").size()
    for min_cnt in [1, 5, 10, 12, 15, 16, 20, 25, 30, 40, 50, 60]:
        query = (
            "SELECT category, COUNT(*) AS cnt "
            "FROM orders "
            f"GROUP BY category HAVING COUNT(*) >= {min_cnt} "
            "ORDER BY category"
        )
        expected_total = int((category_counts >= min_cnt).sum())
        case_id = f"query-group-having-{min_cnt}"
        cases.append(pytest.param(query, 1, 100, expected_total, min(expected_total, 100), id=case_id))

    # Pagination on full table
    total_rows = len(ORDERS_DF)
    for page in [1, 2, 3, 4, 5, 6, 7, 8]:
        for page_size in [1, 5, 7, 10]:
            start = (page - 1) * page_size
            expected_len = 0 if start >= total_rows else min(page_size, total_rows - start)
            query = "SELECT * FROM orders ORDER BY id"
            case_id = f"query-page-{page}-size-{page_size}"
            cases.append(pytest.param(query, page, page_size, total_rows, expected_len, id=case_id))

    return cases


QUERY_CASES = _build_query_cases()


INVALID_SQL_CASES = [
    "SELECT FROM orders",
    "SELEC * FROM orders",
    "SELECT * FRM orders",
    "SELECT * FROM missing_table",
    "SELECT * FROM orders WHERE",
    "SELECT * FROM orders ORDER id",
    "SELECT * FROM orders GROUP category",
    "SELECT * FROM orders HAVING amount > 10",
    "SELECT * FROM orders WHERE amount >>> 10",
    "SELECT * FROM orders WHERE category = ",
    "SELECT * FROM orders WHERE unknown_col = 1",
    "SELECT COUNT( FROM orders",
    "SELECT * FROM orders WHERE (amount > 10",
    "SELECT * FROM orders WHERE amount > 'abc'::INT",
    "SELECT * FROM orders WHERE id IN )",
    "SELECT * FROM orders LIMIT -",
    "SELECT * FROM orders OFFSET -",
    "WITH t AS (SELECT * FROM orders) SELECT * FROM",
    "SELECT * FROM",
    "DROP TABLE orders",
]


@pytest.mark.parametrize(
    "field,op,threshold,data_source,with_data_type,expected_total",
    EXECUTE_NUMERIC_CASES,
)
def test_execute_numeric_filter_matrix(api_context, field, op, threshold, data_source, with_data_type, expected_total):
    cfg = {"field": field, "operator": op, "value": threshold}
    if data_source != "stream":
        cfg["dataSource"] = data_source
    if with_data_type:
        cfg["dataType"] = "number"
    tree = _build_tree([{"id": "flt", "type": "filter", "order": 1, "config": cfg}])
    res = client.post(
        "/execute",
        json={"sessionId": api_context["session_id"], "tree": tree, "targetNodeId": "root"},
    )
    assert res.status_code == 200
    data = res.json()
    assert data["totalCount"] == expected_total


@pytest.mark.parametrize("field,op,value,data_source,expected_total", EXECUTE_TEXT_CASES)
def test_execute_text_filter_matrix(api_context, field, op, value, data_source, expected_total):
    cfg = {"field": field, "operator": op, "value": value}
    if data_source != "stream":
        cfg["dataSource"] = data_source
    tree = _build_tree([{"id": "flt", "type": "filter", "order": 1, "config": cfg}])
    res = client.post(
        "/execute",
        json={"sessionId": api_context["session_id"], "tree": tree, "targetNodeId": "root"},
    )
    assert res.status_code == 200
    data = res.json()
    assert data["totalCount"] == expected_total


@pytest.mark.parametrize("field,op,values,data_source,expected_total", EXECUTE_LIST_TEXT_CASES)
def test_execute_list_text_filter_matrix(api_context, field, op, values, data_source, expected_total):
    cfg = {"field": field, "operator": op, "value": values}
    if data_source != "stream":
        cfg["dataSource"] = data_source
    tree = _build_tree([{"id": "flt", "type": "filter", "order": 1, "config": cfg}])
    res = client.post(
        "/execute",
        json={"sessionId": api_context["session_id"], "tree": tree, "targetNodeId": "root"},
    )
    assert res.status_code == 200
    data = res.json()
    assert data["totalCount"] == expected_total


@pytest.mark.parametrize("op,values,data_source,expected_total", EXECUTE_LIST_NUM_CASES)
def test_execute_list_numeric_filter_matrix(api_context, op, values, data_source, expected_total):
    cfg = {"field": "customer_id", "operator": op, "value": values}
    if data_source != "stream":
        cfg["dataSource"] = data_source
    tree = _build_tree([{"id": "flt", "type": "filter", "order": 1, "config": cfg}])
    res = client.post(
        "/execute",
        json={"sessionId": api_context["session_id"], "tree": tree, "targetNodeId": "root"},
    )
    assert res.status_code == 200
    data = res.json()
    assert data["totalCount"] == expected_total


@pytest.mark.parametrize("op,data_source,expected_total", EXECUTE_EMPTY_CASES)
def test_execute_empty_filter_matrix(api_context, op, data_source, expected_total):
    cfg = {"field": "note", "operator": op, "value": ""}
    if data_source != "stream":
        cfg["dataSource"] = data_source
    tree = _build_tree([{"id": "flt", "type": "filter", "order": 1, "config": cfg}])
    res = client.post(
        "/execute",
        json={"sessionId": api_context["session_id"], "tree": tree, "targetNodeId": "root"},
    )
    assert res.status_code == 200
    data = res.json()
    assert data["totalCount"] == expected_total


@pytest.mark.parametrize("command,include_meta,must_contain_tokens", GENERATE_SQL_CASES)
def test_generate_sql_matrix(api_context, command, include_meta, must_contain_tokens):
    tree = _build_sql_tree(command)
    res = client.post(
        "/generate_sql",
        json={
            "sessionId": api_context["session_id"],
            "tree": tree,
            "targetNodeId": "root",
            "targetCommandId": "target",
            "includeCommandMeta": include_meta,
        },
    )
    assert res.status_code == 200
    payload = res.json()
    sql_text = payload["sql"]
    if include_meta:
        assert payload["dmb"] is not None
    else:
        assert payload["dmb"] is None
    assert not sql_text.lstrip().startswith("-- DMB_COMMAND:")
    for token in must_contain_tokens:
        assert token in sql_text


@pytest.mark.parametrize("query,page,page_size,expected_total,expected_page_len", QUERY_CASES)
def test_query_matrix(api_context, query, page, page_size, expected_total, expected_page_len):
    res = client.post(
        "/query",
        json={
            "sessionId": api_context["session_id"],
            "query": query,
            "page": page,
            "pageSize": page_size,
        },
    )
    assert res.status_code == 200
    data = res.json()
    assert data["totalCount"] == expected_total
    assert len(data["rows"]) == expected_page_len


@pytest.mark.parametrize("bad_sql", INVALID_SQL_CASES)
def test_query_invalid_sql_matrix(api_context, bad_sql):
    res = client.post(
        "/query",
        json={
            "sessionId": api_context["session_id"],
            "query": bad_sql,
            "page": 1,
            "pageSize": 50,
        },
    )
    assert res.status_code == 400


@pytest.mark.parametrize("idx", list(range(10)))
def test_generate_sql_invalid_target_matrix(api_context, idx):
    tree = _build_tree([{"id": "flt", "type": "filter", "order": 1, "config": {"field": "amount", "operator": ">", "value": idx}}])
    res = client.post(
        "/generate_sql",
        json={
            "sessionId": api_context["session_id"],
            "tree": tree,
            "targetNodeId": "root",
            "targetCommandId": f"missing_{idx}",
        },
    )
    assert res.status_code == 500
    assert "Target command not found" in res.json()["detail"]


@pytest.mark.parametrize("idx", list(range(10)))
def test_execute_invalid_node_matrix(api_context, idx):
    tree = _build_tree([{"id": "flt", "type": "filter", "order": 1, "config": {"field": "amount", "operator": ">", "value": idx}}])
    res = client.post(
        "/execute",
        json={
            "sessionId": api_context["session_id"],
            "tree": tree,
            "targetNodeId": f"missing_node_{idx}",
        },
    )
    assert res.status_code == 500
    assert "Target node not found" in res.json()["detail"]


def test_generate_sql_missing_target_command_id(api_context):
    tree = _build_tree([{"id": "flt", "type": "filter", "order": 1, "config": {"field": "amount", "operator": ">", "value": 10}}])
    res = client.post(
        "/generate_sql",
        json={
            "sessionId": api_context["session_id"],
            "tree": tree,
            "targetNodeId": "root",
        },
    )
    assert res.status_code == 400
    assert "targetCommandId is required" in res.json()["detail"]


def test_generate_sql_null_target_command_id(api_context):
    tree = _build_tree([{"id": "flt", "type": "filter", "order": 1, "config": {"field": "amount", "operator": ">", "value": 10}}])
    res = client.post(
        "/generate_sql",
        json={
            "sessionId": api_context["session_id"],
            "tree": tree,
            "targetNodeId": "root",
            "targetCommandId": None,
        },
    )
    assert res.status_code == 400
    assert "targetCommandId is required" in res.json()["detail"]
