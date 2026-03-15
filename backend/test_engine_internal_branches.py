import pandas as pd
import pytest

import engine as engine_module
import sql_generator as sql_generator_module
from engine import ExecutionEngine
from models import Command, CommandConfig, OperationNode
from storage import storage


def _cmd(cmd_type: str, config: dict | None = None, cmd_id: str = "c", order: int = 0) -> Command:
    return Command(id=cmd_id, type=cmd_type, order=order, config=CommandConfig(**(config or {})))


def _node(
    node_id: str,
    commands: list[Command] | None = None,
    children: list[OperationNode] | None = None,
    enabled: bool = True,
    operation_type: str = "process",
) -> OperationNode:
    return OperationNode(
        id=node_id,
        type="operation",
        operationType=operation_type,
        name=node_id,
        enabled=enabled,
        commands=commands or [],
        children=children or [],
    )


@pytest.fixture(autouse=True)
def clean_storage():
    storage.clear()
    yield
    storage.clear()


def test_sub_table_condition_sql_builders_cover_all_ops():
    e = ExecutionEngine()
    cases = [
        ({"field": "sub.id", "operator": "always_true"}, "1=1"),
        ({"field": "sub.id", "operator": "always_false"}, "1=0"),
        ({"field": "sub.id", "operator": "is_null"}, "IS NULL"),
        ({"field": "sub.id", "operator": "is_not_null"}, "IS NOT NULL"),
        ({"field": "sub.id", "operator": "is_empty"}, "CAST"),
        ({"field": "sub.id", "operator": "is_not_empty"}, "CAST"),
        ({"field": "sub.id", "operator": "=", "mainField": "main.id"}, "="),
        ({"field": "sub.id", "operator": "!=", "mainField": "main.id"}, "!="),
        ({"field": "sub.id", "operator": ">", "mainField": "main.id"}, ">"),
        ({"field": "sub.id", "operator": ">=", "mainField": "main.id"}, ">="),
        ({"field": "sub.id", "operator": "<", "mainField": "main.id"}, "<"),
        ({"field": "sub.id", "operator": "<=", "mainField": "main.id"}, "<="),
        ({"field": "sub.name", "operator": "contains", "mainField": "main.name"}, "ILIKE"),
        ({"field": "sub.name", "operator": "not_contains", "mainField": "main.name"}, "NOT ILIKE"),
        ({"field": "sub.name", "operator": "starts_with", "mainField": "main.name"}, "ILIKE"),
        ({"field": "sub.name", "operator": "ends_with", "mainField": "main.name"}, "ILIKE"),
        ({"field": "sub.id", "operator": "unknown", "mainField": "main.id"}, "="),
    ]
    for cond, token in cases:
        sql = e._build_sub_table_link_condition_sql(cond)
        assert token in sql

    assert e._build_sub_table_link_condition_sql({"operator": "=", "mainField": "main.id"}) == ""
    assert e._normalize_field_name(None) == ""
    assert e._normalize_field_name("  ") == ""
    assert e._normalize_field_name("t.amount") == "amount"


def test_sub_table_group_sql_and_rewrite_helpers():
    e = ExecutionEngine()
    assert e._build_sub_table_condition_group_sql(None) == ""
    assert e._build_sub_table_condition_group_sql({"conditions": []}) == ""

    group = {
        "logicalOperator": "XOR",
        "conditions": [
            {"field": "sub.id", "operator": "=", "mainField": "main.id"},
            {
                "type": "group",
                "logicalOperator": "OR",
                "conditions": [
                    {"field": "sub.name", "operator": "contains", "mainField": "main.name"},
                ],
            },
            "invalid_item",
        ],
    }
    sql = e._build_sub_table_condition_group_sql(group)
    assert "AND" in sql  # invalid logical op should fallback to AND
    assert "ILIKE" in sql

    clause = "orders.id = customers.id AND o.name = c.name"
    table_to_ids = {"orders": {"orders", "o"}, "customers": {"customers", "c"}}
    rewritten = e._rewrite_sub_table_on(clause, "customers", table_to_ids)
    assert "main." in rewritten
    assert "sub." in rewritten
    assert e._rewrite_sub_table_on("", "customers", table_to_ids) == ""
    assert e._replace_ident_prefix("a.id = b.id", "", "x.") == "a.id = b.id"


def test_overlap_and_lookup_helpers():
    e = ExecutionEngine()
    root = _node("root", children=[_node("a"), _node("b")], commands=[])
    assert e._find_path_to_node(root, "b")[-1].id == "b"
    assert e._find_node_recursive(root, "a").id == "a"
    assert e._find_node_recursive(root, "missing") is None

    with pytest.raises(ValueError):
        e.calculate_overlap("sess", root, "missing")

    one_child = _node("parent", children=[_node("c1")], commands=[])
    assert e.calculate_overlap("sess", _node("root", children=[one_child]), "parent") == [
        "Not enough branches to compare."
    ]

    parent = _node("parent", children=[_node("c1"), _node("c2")], commands=[])
    tree = _node("root", children=[parent], commands=[])

    def _execute_overlap(_sid, _tree, node_id, *_args, **_kwargs):
        if node_id == "c1":
            return pd.DataFrame({"id": [1, 2]})
        return pd.DataFrame({"id": [2, 3]})

    e.execute = _execute_overlap  # type: ignore[assignment]
    overlap_report = e.calculate_overlap("sess", tree, "parent")
    assert any("Overlap" in line for line in overlap_report)

    def _execute_no_overlap(_sid, _tree, node_id, *_args, **_kwargs):
        if node_id == "c1":
            return pd.DataFrame({"id": [1]})
        return pd.DataFrame({"id": [2]})

    e.execute = _execute_no_overlap  # type: ignore[assignment]
    no_overlap_report = e.calculate_overlap("sess", tree, "parent")
    assert any("No overlap" in line for line in no_overlap_report)

    def _execute_error(_sid, _tree, node_id, *_args, **_kwargs):
        if node_id == "c1":
            raise RuntimeError("boom")
        return pd.DataFrame({"id": [1]})

    e.execute = _execute_error  # type: ignore[assignment]
    err_report = e.calculate_overlap("sess", tree, "parent")
    assert "Error executing branch" in err_report[0]


def test_setup_table_and_select_helpers():
    e = ExecutionEngine()
    setup = _node(
        "setup",
        operation_type="setup",
        commands=[
            _cmd("source", {"mainTable": "orders", "linkId": "lk_orders", "alias": "o"}, "src_orders"),
            _cmd("source", {"mainTable": "customers", "linkId": "lk_customers", "alias": "c"}, "src_customers"),
        ],
    )
    assert e._resolve_table_from_link_id(setup, "lk_orders") == "orders"
    assert e._resolve_table_from_link_id(setup, "missing") is None

    allowed, source_map, table_to_ids = e._collect_setup_sources(setup)
    assert "orders" in allowed and "customers" in allowed
    assert source_map["lk_orders"] == "orders"
    assert "o" in table_to_ids["orders"]

    with pytest.raises(ValueError):
        e._resolve_setup_table("select", {"orders"}, {"select": "select"})
    with pytest.raises(ValueError):
        e._resolve_setup_table("not_in_setup", {"orders"}, {"orders": "orders"})

    assert e._extract_simple_select("SELECT * FROM orders") == ("orders", None)
    assert e._extract_simple_select("SELECT * FROM orders WHERE id > 1") == ("orders", "id > 1")
    assert e._extract_simple_select("SELECT id FROM orders") == (None, None)
    assert e._extract_where_clause("SELECT * FROM orders WHERE id > 2") == "id > 2"
    assert e._extract_where_clause("SELECT id FROM orders") is None
    assert e._select_input_table("SELECT * FROM orders WHERE id > 1", "orders") == (
        "(SELECT * FROM orders WHERE id > 1)"
    )
    assert "input_subq" in e._select_input_table("SELECT id FROM orders", "orders")


def test_build_view_sql_and_copy_command_with_overrides():
    e = ExecutionEngine()
    view_cmd = _cmd(
        "view",
        {
            "viewFields": [
                {"field": "name", "distinct": True},
                {"field": "name", "distinct": True},
                {"field": "age", "distinct": False},
            ],
            "viewSorts": [
                {"field": "name", "ascending": True},
                {"field": "name", "ascending": False},
            ],
            "viewLimit": 10,
        },
        "v",
    )
    sql = e._build_view_sql(view_cmd, "users", "age > 1")
    assert "SELECT DISTINCT name FROM users WHERE age > 1 ORDER BY name ASC LIMIT 10" in sql

    view_cmd2 = _cmd("view", {"viewSortField": "age", "viewSortAscending": False, "viewLimit": 0}, "v2")
    sql2 = e._build_view_sql(view_cmd2, "users", None)
    assert "SELECT * FROM users ORDER BY age DESC LIMIT 0" in sql2

    class _FakeCmd:
        def dict(self):
            return {"id": "j1", "type": "join", "order": 1, "config": {"joinTable": "a", "on": "id=id"}}

    copied = e._copy_command_with_overrides(_FakeCmd(), joinTable="b")
    assert isinstance(copied, Command)
    assert copied.config.joinTable == "b"


def test_execute_multi_table_sub_branches(monkeypatch):
    e = ExecutionEngine()
    root = _node("root")
    op = _node("op", commands=[_cmd("multi_table", {"subTables": [{"id": "sub1", "table": "sub", "on": "", "label": "x"}]}, "mt")])
    path = [root, op]
    multi_cmd = op.commands[0]

    # Empty input branch
    monkeypatch.setattr(e, "_apply_node_commands", lambda *_a, **_k: pd.DataFrame())
    out = e._execute_multi_table_sub("sess", path, multi_cmd, "sub1")
    assert out.empty

    # Missing sub-config branch
    monkeypatch.setattr(e, "_apply_node_commands", lambda *_a, **_k: pd.DataFrame({"id": [1]}))
    with pytest.raises(ValueError):
        e._execute_multi_table_sub("sess", path, multi_cmd, "missing_sub")

    # Missing sub dataset branch
    monkeypatch.setattr(engine_module.storage, "get_full_dataset", lambda *_a, **_k: None)
    with pytest.raises(ValueError):
        e._execute_multi_table_sub("sess", path, multi_cmd, "sub1")

    # Success branch (no condition => 1=1)
    monkeypatch.setattr(
        engine_module.storage,
        "get_full_dataset",
        lambda *_a, **_k: pd.DataFrame({"id": [10, 20], "name": ["a", "b"]}),
    )
    out2 = e._execute_multi_table_sub("sess", path, multi_cmd, "sub1")
    assert len(out2) == 2


def test_apply_filter_condition_join_group_transform_sort_and_node_commands(monkeypatch):
    e = ExecutionEngine()
    df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["alice", "bob", ""],
            "amount": [10.0, 20.0, 30.0],
            "group": ["g1", "g1", "g2"],
        }
    )

    # _get_condition_mask edge ops
    assert e._get_condition_mask(df, {"operator": "always_true"}, {}).all()
    assert (~e._get_condition_mask(df, {"operator": "always_false"}, {})).all()
    assert e._get_condition_mask(df, {"field": "missing", "operator": "="}, {}).all()
    assert e._get_condition_mask(df, {"field": "name", "operator": "is_empty"}, {}).sum() == 1
    assert e._get_condition_mask(df, {"field": "name", "operator": "is_not_empty"}, {}).sum() == 2
    assert e._get_condition_mask(
        df,
        {"field": "id", "operator": "in_variable", "value": "{ids}", "valueType": "variable"},
        {"ids": [1, 3]},
    ).sum() == 2
    assert e._get_condition_mask(
        df,
        {"field": "id", "operator": "not_in_variable", "value": "{ids}", "valueType": "variable"},
        {"ids": [1, 3]},
    ).sum() == 1
    assert e._get_condition_mask(
        df,
        {"field": "id", "operator": "in_list", "value": "1,2,3", "dataType": "number"},
        {},
    ).sum() == 3
    assert e._get_condition_mask(
        df,
        {"field": "id", "operator": "not_in_list", "value": "1,2", "dataType": "number"},
        {},
    ).sum() == 3

    # _apply_filter legacy fallback (no field)
    no_field_cmd = _cmd("filter", {"operator": "=", "value": 1}, "f0")
    assert e._apply_filter(df, no_field_cmd, {}).equals(df)

    # _apply_join branches
    base = pd.DataFrame({"id": [1, 2], "x": [10, 20]})
    right = pd.DataFrame({"id": [2], "y": [99]})
    original_get_full_dataset = engine_module.storage.get_full_dataset
    monkeypatch.setattr(engine_module.storage, "get_full_dataset", lambda *_a, **_k: right)
    join_cmd = _cmd("join", {"joinTable": "right_tbl", "joinType": "left", "on": "id=id"}, "j1")
    joined = e._apply_join(base, join_cmd, "sess", _node("r"))
    assert "y" in joined.columns

    join_cmd2 = _cmd("join", {"joinTable": "right_tbl", "joinType": "left", "on": "id"}, "j2")
    joined2 = e._apply_join(base, join_cmd2, "sess", _node("r"))
    assert "y" in joined2.columns

    join_cmd_err = _cmd("join", {"joinTable": "right_tbl", "joinType": "left", "on": "bad=missing"}, "j3")
    # error branch should fallback to original df
    assert e._apply_join(base, join_cmd_err, "sess", _node("r")).equals(base)
    monkeypatch.setattr(engine_module.storage, "get_full_dataset", original_get_full_dataset)

    # _apply_group having numeric and string paths
    group_cmd = _cmd(
        "group",
        {
            "groupByFields": ["group"],
            "aggregations": [{"field": "amount", "func": "sum", "alias": "sum_amount"}],
            "havingConditions": [
                {"metricAlias": "sum_amount", "operator": ">=", "value": 30},
                {"metricAlias": "sum_amount", "operator": "<=", "value": 30},
                {"metricAlias": "sum_amount", "operator": "!=", "value": 0},
            ],
        },
        "g1",
    )
    grouped = e._apply_group(df, group_cmd, "sess")
    assert "sum_amount" in grouped.columns

    group_cmd2 = _cmd(
        "group",
        {
            "aggregations": [{"field": "name", "func": "count", "alias": "name_count"}],
            "havingConditions": [{"metricAlias": "name_count", "operator": "contains", "value": "3"}],
        },
        "g2",
    )
    grouped2 = e._apply_group(df, group_cmd2, "sess")
    assert isinstance(grouped2, pd.DataFrame)

    # _apply_transform python/non-python branches
    transform_cmd = _cmd(
        "transform",
        {
            "mappings": [
                {"id": "m1", "mode": "python", "expression": "def transform(row):\n    return row['id'] * 2", "outputField": "id2"},
                {"id": "m2", "mode": "python", "expression": "def custom(row):\n    raise Exception('x')", "outputField": "bad_py"},
                {"id": "m3", "mode": "python", "expression": "def oops(:", "outputField": "compile_fail"},
                {"id": "m4", "mode": "expr", "expression": "id + amount", "outputField": "sum_expr"},
                {"id": "m5", "mode": "expr", "expression": "unknown_var + 1", "outputField": "bad_expr"},
            ]
        },
        "t1",
    )
    transformed = e._apply_transform(df, transform_cmd)
    assert "id2" in transformed.columns
    assert "sum_expr" in transformed.columns
    assert transformed["bad_expr"].isna().all()

    # _apply_sort fallback branch
    sort_cmd = _cmd("sort", {"field": "missing_field", "ascending": True}, "s1")
    assert e._apply_sort(df, sort_cmd).equals(df)

    # _apply_node_commands: legacy mainTable load + save(list) + define_variable + exception continue
    sid = "sess_engine_cmd"
    storage.create_session(sid)
    storage.add_dataset(sid, "users", pd.DataFrame({"id": [1, 1, 2], "name": ["a", "a", "b"]}))
    variables = {}
    commands = [
        _cmd("filter", {"mainTable": "users", "field": "id", "operator": ">=", "value": 1, "dataType": "number"}, "c1", 1),
        _cmd("save", {"field": "id", "value": "ids_all", "distinct": False}, "c2", 2),
        _cmd("define_variable", {"variableName": "x", "variableValue": "y"}, "c3", 3),
        _cmd(
            "view",
            {
                "viewFields": [{"field": "id", "distinct": True}, {"field": "id", "distinct": True}],
                "viewSorts": [{"field": "id", "ascending": False}, {"field": "id", "ascending": True}],
                "viewLimit": 2,
            },
            "c4",
            4,
        ),
    ]
    tree = _node("root", commands=[_cmd("source", {"mainTable": "users", "linkId": "lk_users"}, "src")])
    result = e._apply_node_commands(None, commands, sid, variables, tree)
    assert result is not None
    assert variables["ids_all"] == [1, 1, 2]
    assert variables["x"] == "y"

    # command-level exception should be swallowed and continue
    monkeypatch.setattr(e, "_apply_filter", lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("boom")))
    commands_err = [
        _cmd("source", {"mainTable": "users"}, "s0", 0),
        _cmd("filter", {"field": "id", "operator": "=", "value": 1}, "s1", 1),
        _cmd("sort", {"field": "id", "ascending": True}, "s2", 2),
    ]
    out = e._apply_node_commands(None, commands_err, sid, {}, tree, limit_command_id="s2")
    assert out is not None


def test_execute_view_fallback_and_empty_df_branch(monkeypatch):
    e = ExecutionEngine()
    tree = _node(
        "root",
        commands=[],
        children=[_node("target", commands=[_cmd("filter", {"field": "id", "operator": "=", "value": 1}, "f1")])],
    )
    monkeypatch.setattr(e, "_apply_node_commands", lambda *_a, **_k: None)
    out = e.execute("sess", tree, "target", view_id="sub_not_found")
    assert out.empty


def test_generate_sql_error_and_early_return_branches(monkeypatch):
    e = ExecutionEngine()

    # No setup tables
    with pytest.raises(ValueError):
        e.generate_sql("sess", _node("root", commands=[]), "root", "c1")

    setup = _node(
        "setup",
        operation_type="setup",
        commands=[_cmd("source", {"mainTable": "orders", "linkId": "lk_orders"}, "src_setup")],
    )

    # Disabled node in path should be skipped
    disabled_root = _node("root", commands=[_cmd("source", {"mainTable": "orders"}, "src1")], enabled=False)
    target = _node("target", commands=[_cmd("source", {"mainTable": "orders"}, "src2")])
    disabled_root.children = [target]
    disabled_root.children.append(setup)
    monkeypatch.setattr(e, "_apply_node_commands", lambda df, *_a, **_k: df)
    sql = e.generate_sql("sess", disabled_root, "target", "src2")
    assert "SELECT * FROM orders" in sql

    # define_variable target branch (line 105)
    tree_define = _node("root", commands=[_cmd("define_variable", {"variableName": "x", "variableValue": "1"}, "dv1")], children=[setup])
    sql_define = e.generate_sql("sess", tree_define, "root", "dv1")
    assert sql_define.startswith("-- SQL generation not supported for define_variable")

    # current_sql is None for non-source command
    tree_no_source = _node("root", commands=[_cmd("filter", {"field": "id", "operator": "=", "value": 1}, "f1")], children=[setup])
    with pytest.raises(ValueError):
        e.generate_sql("sess", tree_no_source, "root", "f1")

    # target not found branch
    tree_source = _node("root", commands=[_cmd("source", {"mainTable": "orders"}, "src1")], children=[setup])
    with pytest.raises(ValueError):
        e.generate_sql("sess", tree_source, "root", "missing_cmd")


def test_generate_sql_view_fallback_filter_no_where_and_empty_sql(monkeypatch):
    e = ExecutionEngine()
    setup = _node(
        "setup",
        operation_type="setup",
        commands=[
            _cmd("source", {"mainTable": "orders", "linkId": "lk_orders"}, "src_orders"),
            _cmd("source", {"mainTable": "customers", "linkId": "lk_customers"}, "src_customers"),
        ],
    )

    # View fallback path (134-135): current_sql becomes non-simple after join
    tree_view = _node(
        "root",
        commands=[
            _cmd("source", {"mainTable": "orders"}, "src1", 0),
            _cmd("join", {"joinTable": "customers", "joinType": "left", "on": "orders.customer_id=customers.customer_id"}, "j1", 1),
            _cmd("view", {"viewFields": [{"field": "order_id"}]}, "v1", 2),
        ],
        children=[setup],
    )
    monkeypatch.setattr(e, "_apply_node_commands", lambda df, *_a, **_k: df)
    sql_view = e.generate_sql("sess", tree_view, "root", "v1")
    assert "input_subq" in sql_view or "FROM (" in sql_view

    # Filter with mocked sql generator returning no WHERE branch (147)
    tree_filter = _node(
        "root",
        commands=[
            _cmd("source", {"mainTable": "orders"}, "src2", 0),
            _cmd("filter", {"field": "order_id", "operator": "=", "value": 1}, "f2", 1),
        ],
        children=[setup],
    )
    original_generate = sql_generator_module.generate_sql_for_command
    monkeypatch.setattr(
        sql_generator_module,
        "generate_sql_for_command",
        lambda cmd, _vars, _input: "SELECT * FROM orders" if cmd.type == "filter" else original_generate(cmd, _vars, _input),
    )
    sql_filter = e.generate_sql("sess", tree_filter, "root", "f2")
    assert sql_filter == "SELECT * FROM orders"

    # Empty sql_text target branch (160)
    monkeypatch.setattr(sql_generator_module, "generate_sql_for_command", lambda *_a, **_k: "   ")
    tree_sort = _node(
        "root",
        commands=[
            _cmd("source", {"mainTable": "orders"}, "src3", 0),
            _cmd("sort", {"field": "order_id", "ascending": True}, "s3", 1),
        ],
        children=[setup],
    )
    sql_empty = e.generate_sql("sess", tree_sort, "root", "s3")
    assert sql_empty.startswith("-- No SQL generated")


def test_command_meta_serialization_and_decoration_exception_paths(monkeypatch):
    e = ExecutionEngine()

    class _BadConfig:
        def model_dump(self, **_kwargs):
            raise RuntimeError("bad model dump")

    class _BadCmd:
        type = "filter"
        config = _BadConfig()

    meta = e._serialize_command_meta(_BadCmd())
    assert meta["config"] == {}

    # json.dumps fail path in _decorate_sql_with_command_meta
    monkeypatch.setattr(e, "_serialize_command_meta", lambda _cmd: {"version": 1, "type": "x", "config": {"bad": object()}})
    decorated = e._decorate_sql_with_command_meta(_BadCmd(), "SELECT 1", True)
    assert decorated.startswith("-- DMB_COMMAND:")
    assert "\nSELECT 1" in decorated

    decorated_only_prefix = e._decorate_sql_with_command_meta(_BadCmd(), "   ", True)
    assert decorated_only_prefix.startswith("-- DMB_COMMAND:")


def test_additional_helper_branches_and_condition_mask_edges():
    e = ExecutionEngine()
    df = pd.DataFrame({"id": [1, 2], "txt": ["a", "b"]})

    # helper branches
    assert e._build_sub_table_condition_group_sql({"conditions": ["invalid", 1]}) == ""
    assert e._build_sub_table_link_condition_sql({"field": "sub.id", "operator": "="}) == ""
    assert e._resolve_table_from_link_id(None, "x") is None
    assert e._resolve_setup_table(None, {"orders"}, {"orders": "orders"}) is None
    assert e._rewrite_join_on("", "orders", "customers", {"orders": {"orders"}}) == ""
    assert e._extract_simple_select("") == (None, None)
    assert e._extract_where_clause("") is None

    # _build_view_sql line 582: entry without field should be skipped
    class _SortNoField:
        pass

    class _Cfg:
        viewFields = []
        viewSorts = [_SortNoField()]
        viewSortField = None
        viewSortAscending = True
        viewLimit = None

    class _FakeViewCmd:
        config = _Cfg()

    sql = e._build_view_sql(_FakeViewCmd(), "orders", None)
    assert "SELECT * FROM orders" in sql

    # save distinct True branch (736)
    sid = "sess_var_distinct"
    storage.create_session(sid)
    storage.add_dataset(sid, "users", pd.DataFrame({"id": [1, 1, 2]}))
    vars_map = {}
    tree = _node("root", commands=[_cmd("source", {"mainTable": "users"}, "src")])
    e._apply_node_commands(
        None,
        [
            _cmd("source", {"mainTable": "users"}, "s0", 0),
            _cmd("save", {"field": "id", "value": "ids_distinct", "distinct": True}, "s1", 1),
        ],
        sid,
        vars_map,
        tree,
    )
    assert sorted(vars_map["ids_distinct"]) == [1, 2]

    # in_variable/not_in_variable fallback get (848/856)
    assert e._get_condition_mask(df, {"field": "id", "operator": "in_variable", "value": "ids_ref"}, {"ids_ref": [1]}).sum() == 1
    assert e._get_condition_mask(df, {"field": "id", "operator": "not_in_variable", "value": "ids_ref"}, {"ids_ref": [1]}).sum() == 1

    # contains with list branch + empty list branch
    assert e._get_condition_mask(df, {"field": "txt", "operator": "contains", "value": ["a", "x"]}, {}).sum() == 1
    assert e._get_condition_mask(df, {"field": "txt", "operator": "contains", "value": []}, {}).sum() == 0

    # not_contains with list branch + empty list branch
    assert e._get_condition_mask(df, {"field": "txt", "operator": "not_contains", "value": ["a"]}, {}).sum() == 1
    assert e._get_condition_mask(df, {"field": "txt", "operator": "not_contains", "value": []}, {}).sum() == len(df)

    # numeric coercion failure currently falls back to all-True mask
    assert e._get_condition_mask(df, {"field": "id", "operator": "in_list", "value": ["x"], "dataType": "number"}, {}).sum() == len(df)
    assert e._get_condition_mask(df, {"field": "id", "operator": "not_in_list", "value": ["x"], "dataType": "number"}, {}).sum() == len(df)


def test_group_having_string_fallback_and_transform_no_mapping_branch():
    e = ExecutionEngine()
    df = pd.DataFrame({"g": ["a", "a", "b"], "name": ["aa", "ab", "bb"]})

    cmd = _cmd(
        "group",
        {
            "groupByFields": ["g"],
            "aggregations": [{"field": "name", "func": "count", "alias": "cnt"}],
            "havingConditions": [
                {"metricAlias": "cnt", "operator": "=", "value": "2"},
                {"metricAlias": "cnt", "operator": "!=", "value": "x"},
                {"metricAlias": "cnt", "operator": "contains", "value": "2"},
            ],
        },
    )
    out = e._apply_group(df, cmd, "sess")
    assert not out.empty

    # _apply_transform with no mappings should return original df (1063)
    tcmd = _cmd("transform", {"expression": None})
    out2 = e._apply_transform(df, tcmd)
    assert out2.equals(df)
