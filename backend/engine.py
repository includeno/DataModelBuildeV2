
import pandas as pd
import numpy as np
import math
import datetime
import re
import json
import duckdb
from typing import List, Optional, Dict, Set, Any, Union
from models import Command, OperationNode
import runtime_config as runtime_config_module
from security import compile_python_transform
from storage import storage
from simpleeval import simple_eval
from sql_utils import (
    quote_identifier,
    unquote_identifier,
    is_reserved_identifier,
    SIMPLE_SELECT_RE,
    SIMPLE_SELECT_WHERE_RE,
    WHERE_EXTRACT_RE,
)

class ExecutionEngine:
    def execute(self, session_id: str, tree: OperationNode, target_node_id: str, view_id: str = "main", target_command_id: str = None) -> pd.DataFrame:
        path = self._find_path_to_node(tree, target_node_id)
        if not path:
            raise ValueError("Target node not found in operation tree")

        # If requesting a specific sub-view (not main), find the command responsible for it
        if view_id != "main":
            target_node = path[-1]
            multi_cmd = None
            
            # Look for the multi_table command that contains this view_id
            for cmd in target_node.commands:
                if cmd.type == 'multi_table' and cmd.config.subTables:
                    for sub in cmd.config.subTables:
                        if sub.id == view_id:
                            multi_cmd = cmd
                            break
                if multi_cmd: break
            
            if multi_cmd:
                return self._execute_multi_table_sub(session_id, path, multi_cmd, view_id)
            else:
                # Fallback
                pass

        df = None
        variables: Dict[str, Any] = {} 

        for node in path:
            if node.enabled:
                # Only limit commands if we are at the target node AND a specific command ID was requested
                limit_cmd_id = target_command_id if node.id == target_node_id else None
                df = self._apply_node_commands(df, node.commands, session_id, variables, tree, limit_cmd_id)
        
        if df is None:
            return pd.DataFrame()

        return df

    def generate_sql(
        self,
        session_id: str,
        tree: OperationNode,
        target_node_id: str,
        target_command_id: str,
        include_command_meta: bool = False,
    ) -> str:
        from sql_generator import generate_sql_for_command
        
        path = self._find_path_to_node(tree, target_node_id)
        if not path: raise ValueError("Target node not found")
        
        df = None
        variables: Dict[str, Any] = {}

        allowed_tables, source_map, table_to_ids = self._collect_setup_sources(tree)
        if not allowed_tables:
            raise ValueError("No tables defined in Data Setup")

        current_sql: Optional[str] = None
        current_base_table: Optional[str] = None

        for node in path:
            if not node.enabled: 
                continue

            for cmd in sorted(node.commands, key=lambda x: x.order):
                # Execute command to update variables/state (needed for variable resolution in SQL)
                df = self._apply_node_commands(df, [cmd], session_id, variables, tree)

                # Handle explicit data source override
                data_source = cmd.config.dataSource
                if data_source and data_source != 'stream':
                    resolved = self._resolve_setup_table(data_source, allowed_tables, source_map)
                    if current_sql is None or current_base_table != resolved:
                        current_sql = f"SELECT * FROM {quote_identifier(resolved)}"
                        current_base_table = resolved

                cmd_sql = ""
                if cmd.type == 'define_variable':
                    cmd_sql = "-- SQL generation not supported for define_variable"
                    if node.id == target_node_id and cmd.id == target_command_id:
                        return self._decorate_sql_with_command_meta(cmd, cmd_sql, include_command_meta)
                    continue

                if cmd.type == 'source':
                    resolved = self._resolve_setup_table(cmd.config.mainTable, allowed_tables, source_map)
                    source_alias = (cmd.config.alias or "").strip() if cmd.config.alias else ""
                    if source_alias:
                        current_sql = f"SELECT * FROM {quote_identifier(resolved)} AS {quote_identifier(source_alias)}"
                    else:
                        current_sql = f"SELECT * FROM {quote_identifier(resolved)}"
                    current_base_table = resolved
                    cmd_sql = current_sql
                else:
                    if current_sql is None:
                        raise ValueError("No source table available for SQL generation")

                    if cmd.type == 'join' and cmd.config.joinTargetType != 'node':
                        join_ref = cmd.config.joinTable
                        resolved_join = self._resolve_setup_table(join_ref, allowed_tables, source_map)
                        on_clause = cmd.config.on or "1=1"
                        on_clause = self._rewrite_join_on(on_clause, current_base_table, resolved_join, table_to_ids)
                        cmd_for_sql = self._copy_command_with_overrides(cmd, joinTable=resolved_join, on=on_clause)
                        input_table = f"({current_sql})"
                        cmd_sql = generate_sql_for_command(cmd_for_sql, variables, input_table)
                    elif cmd.type == 'view':
                        base_table, existing_where = self._extract_simple_select(current_sql)
                        if base_table and current_base_table and base_table == current_base_table:
                            cmd_sql = self._build_view_sql(cmd, base_table, existing_where)
                        else:
                            input_table = self._select_input_table(current_sql, current_base_table)
                            cmd_sql = generate_sql_for_command(cmd, variables, input_table)
                    elif cmd.type == 'filter':
                        base_table, existing_where = self._extract_simple_select(current_sql)
                        if base_table and current_base_table and base_table == current_base_table:
                            filter_sql = generate_sql_for_command(cmd, variables, base_table)
                            new_where = self._extract_where_clause(filter_sql)
                            if new_where:
                                if existing_where:
                                    cmd_sql = f"SELECT * FROM {quote_identifier(base_table)} WHERE ({existing_where}) AND ({new_where})"
                                else:
                                    cmd_sql = f"SELECT * FROM {quote_identifier(base_table)} WHERE {new_where}"
                            else:
                                cmd_sql = filter_sql
                        else:
                            input_table = self._select_input_table(current_sql, current_base_table)
                            cmd_sql = generate_sql_for_command(cmd, variables, input_table)
                    else:
                        input_table = self._select_input_table(current_sql, current_base_table)
                        cmd_sql = generate_sql_for_command(cmd, variables, input_table)

                if node.id == target_node_id and cmd.id == target_command_id:
                    if cmd_sql.strip().startswith("--"):
                        return self._decorate_sql_with_command_meta(cmd, cmd_sql, include_command_meta)
                    if cmd_sql.strip():
                        return self._decorate_sql_with_command_meta(cmd, cmd_sql, include_command_meta)
                    return self._decorate_sql_with_command_meta(
                        cmd,
                        "-- No SQL generated (or all commands were unsupported)",
                        include_command_meta,
                    )

                if cmd_sql and not cmd_sql.strip().startswith("--"):
                    current_sql = cmd_sql

        raise ValueError("Target command not found")

    def _serialize_command_meta(self, cmd: Command) -> Dict[str, Any]:
        config_dict: Dict[str, Any] = {}
        try:
            if hasattr(cmd.config, "model_dump"):
                config_dict = cmd.config.model_dump(exclude_none=True)  # pydantic v2
            elif hasattr(cmd.config, "dict"):
                config_dict = cmd.config.dict(exclude_none=True)  # pydantic v1
        except Exception:
            config_dict = {}
        return {
            "version": 1,
            "type": cmd.type,
            "config": config_dict,
        }

    def _decorate_sql_with_command_meta(self, cmd: Command, sql_text: str, include_command_meta: bool) -> str:
        if not include_command_meta:
            return sql_text

        try:
            payload = json.dumps(
                self._serialize_command_meta(cmd),
                ensure_ascii=False,
                separators=(",", ":"),
            )
        except Exception:
            payload = json.dumps({"version": 1, "type": cmd.type, "config": {}}, separators=(",", ":"))

        prefix = f"-- DMB_COMMAND: {payload}"
        if sql_text and sql_text.strip():
            return f"{prefix}\n{sql_text}"
        return prefix

    def _execute_multi_table_sub(self, session_id: str, path: List[OperationNode], multi_cmd: Command, view_id: str) -> pd.DataFrame:
        # 1. Execute everything UP TO the multi_table command to get the "Main" context
        df = None
        variables: Dict[str, Any] = {}
        target_node = path[-1]
        
        for node in path:
            if not node.enabled: continue
            
            limit_cmd_id = None
            # If this is the target node, we stop AT the multi_table command (inclusive or exclusive depending on logic, exclusive for input context)
            if node.id == target_node.id:
                # We need to run commands UP TO the multi_table command to establish context
                # So we can reuse _apply_node_commands but we need to know where to stop manually if not using limit_cmd_id
                # Actually, simpler: construct a list of commands up to (but not including) multi_cmd
                trunc_cmds = []
                for cmd in node.commands:
                    if cmd.id == multi_cmd.id:
                        break
                    trunc_cmds.append(cmd)
                df = self._apply_node_commands(df, trunc_cmds, session_id, variables, path[0])
            else:
                df = self._apply_node_commands(df, node.commands, session_id, variables, path[0])
        
        if df is None or df.empty:
            return pd.DataFrame()

        # 2. Find the sub-table config
        sub_config = next((s for s in (multi_cmd.config.subTables or []) if s.id == view_id), None)
        if not sub_config:
            raise ValueError(f"Sub-table view '{view_id}' not found")

        # Resolve sub-table reference (supports linkId / alias / table name)
        root = path[0] if path else None
        allowed_tables, source_map, table_to_ids = self._collect_setup_sources(root) if root else (set(), {}, {})
        resolved_sub_table = sub_config.table
        if root and resolved_sub_table:
            resolved_sub_table = self._resolve_table_from_link_id(root, resolved_sub_table) or source_map.get(resolved_sub_table, resolved_sub_table)

        # 3. Perform the filter join using DuckDB
        con = duckdb.connect(":memory:")
        try:
            con.register('main_table', df)
            
            # Get the sub table
            sub_df = storage.get_full_dataset(session_id, resolved_sub_table)
            if sub_df is None:
                raise ValueError(f"Dataset {sub_config.table} not found")
            
            con.register('sub_table', sub_df)
            
            # Construct Query: SELECT * FROM sub_table sub WHERE EXISTS (SELECT 1 FROM main_table main WHERE condition)
            condition = sub_config.on
            if condition:
                condition = self._rewrite_sub_table_on(condition, resolved_sub_table, table_to_ids)
            on_group = getattr(sub_config, "onConditionGroup", None) or getattr(sub_config, "conditionGroup", None)
            group_condition = self._build_sub_table_condition_group_sql(on_group)
            where_parts = []
            if condition and str(condition).strip():
                where_parts.append(f"({condition})")
            if group_condition:
                where_parts.append(f"({group_condition})")
            where_clause = " AND ".join(where_parts) if where_parts else "1=1"
            
            query = f"""
                SELECT sub.* 
                FROM sub_table sub
                WHERE EXISTS (
                    SELECT 1 
                    FROM main_table main 
                    WHERE {where_clause}
                )
            """
            result = con.execute(query).df()
            return result
        except Exception as e:
            raise ValueError(f"Failed to execute sub-table query: {str(e)}")
        finally:
            con.close()

    def _rewrite_sub_table_on(self, on_clause: str, sub_table: Optional[str], table_to_ids: Dict[str, Set[str]]) -> str:
        if not on_clause:
            return on_clause
        rewritten = on_clause
        # Map identifiers for the sub table to "sub."
        if sub_table and sub_table in table_to_ids:
            for ident in sorted(table_to_ids.get(sub_table, set()), key=len, reverse=True):
                rewritten = self._replace_ident_prefix(rewritten, ident, "sub.")
        # Map any other known identifiers to "main."
        for table, idents in table_to_ids.items():
            if table == sub_table:
                continue
            for ident in sorted(idents, key=len, reverse=True):
                rewritten = self._replace_ident_prefix(rewritten, ident, "main.")
        return rewritten

    def _normalize_field_name(self, field: Any) -> str:
        if field is None:
            return ""
        name = str(field).strip()
        if not name:
            return ""
        if "." in name:
            name = name.split(".")[-1]
        return name

    def _build_sub_table_condition_group_sql(self, group: Optional[Dict[str, Any]]) -> str:
        if not group or not isinstance(group, dict):
            return ""
        conditions = group.get("conditions") or []
        if not isinstance(conditions, list) or not conditions:
            return ""

        parts: List[str] = []
        for item in conditions:
            if not isinstance(item, dict):
                continue
            if item.get("type") == "group":
                sub_sql = self._build_sub_table_condition_group_sql(item)
            else:
                sub_sql = self._build_sub_table_link_condition_sql(item)
            if sub_sql:
                parts.append(f"({sub_sql})")

        if not parts:
            return ""

        logical_op = str(group.get("logicalOperator", "AND")).upper()
        if logical_op not in ("AND", "OR"):
            logical_op = "AND"
        return f" {logical_op} ".join(parts)

    def _build_sub_table_link_condition_sql(self, cond: Dict[str, Any]) -> str:
        op = str(cond.get("operator") or "=").strip().lower()
        if op == "always_true":
            return "1=1"
        if op == "always_false":
            return "1=0"

        left_field = self._normalize_field_name(cond.get("field"))
        if not left_field:
            return ""
        left_expr = f"sub.{quote_identifier(left_field)}"

        if op == "is_null":
            return f"({left_expr} IS NULL)"
        if op == "is_not_null":
            return f"({left_expr} IS NOT NULL)"
        if op == "is_empty":
            return f"({left_expr} IS NULL OR CAST({left_expr} AS VARCHAR) = '')"
        if op == "is_not_empty":
            return f"({left_expr} IS NOT NULL AND CAST({left_expr} AS VARCHAR) != '')"

        right_field = self._normalize_field_name(cond.get("mainField") or cond.get("value"))
        if not right_field:
            return ""
        right_expr = f"main.{quote_identifier(right_field)}"

        if op == "=":
            return f"{left_expr} = {right_expr}"
        if op == "!=":
            return f"{left_expr} != {right_expr}"
        if op == ">":
            return f"{left_expr} > {right_expr}"
        if op == ">=":
            return f"{left_expr} >= {right_expr}"
        if op == "<":
            return f"{left_expr} < {right_expr}"
        if op == "<=":
            return f"{left_expr} <= {right_expr}"
        if op == "contains":
            return f"CAST({left_expr} AS VARCHAR) ILIKE '%' || CAST({right_expr} AS VARCHAR) || '%'"
        if op == "not_contains":
            return f"CAST({left_expr} AS VARCHAR) NOT ILIKE '%' || CAST({right_expr} AS VARCHAR) || '%'"
        if op == "starts_with":
            return f"CAST({left_expr} AS VARCHAR) ILIKE CAST({right_expr} AS VARCHAR) || '%'"
        if op == "ends_with":
            return f"CAST({left_expr} AS VARCHAR) ILIKE '%' || CAST({right_expr} AS VARCHAR)"

        return f"{left_expr} = {right_expr}"

    def calculate_overlap(self, session_id: str, tree: OperationNode, parent_node_id: str) -> List[str]:
        parent_node = self._find_node_recursive(tree, parent_node_id)
        if not parent_node: raise ValueError("Parent node not found")
        if not parent_node.children or len(parent_node.children) < 2: return ["Not enough branches to compare."]

        branch_hashes: Dict[str, Set[int]] = {}
        branch_names: Dict[str, str] = {}
        
        for child in parent_node.children:
            if not child.enabled: continue
            try:
                df_child = self.execute(session_id, tree, child.id)
                branch_names[child.id] = child.name
                if df_child.empty: branch_hashes[child.id] = set()
                else: branch_hashes[child.id] = set(pd.util.hash_pandas_object(df_child, index=False))
            except Exception as e: return [f"Error executing branch '{child.name}': {str(e)}"]

        report = []
        child_ids = list(branch_hashes.keys())
        for i in range(len(child_ids)):
            for j in range(i + 1, len(child_ids)):
                id1, id2 = child_ids[i], child_ids[j]
                intersection = len(branch_hashes[id1].intersection(branch_hashes[id2]))
                if intersection > 0:
                    report.append(f"⚠️ Overlap: '{branch_names[id1]}' and '{branch_names[id2]}' share {intersection} records.")
                else:
                    report.append(f"✅ No overlap: '{branch_names[id1]}' and '{branch_names[id2]}'.")
        return report

    def _find_path_to_node(self, root: OperationNode, target_id: str) -> Optional[List[OperationNode]]:
        if root.id == target_id: return [root]
        if root.children:
            for child in root.children:
                path = self._find_path_to_node(child, target_id)
                if path: return [root] + path
        return None

    def _find_node_recursive(self, root: OperationNode, target_id: str) -> Optional[OperationNode]:
        if root.id == target_id: return root
        if root.children:
            for child in root.children:
                found = self._find_node_recursive(child, target_id)
                if found: return found
        return None

    def _resolve_table_from_link_id(self, tree: OperationNode, link_id: str) -> Optional[str]:
        """Resolves a linkId to a table name by traversing the tree."""
        if not tree:
            return None
        
        # Check current node commands
        for cmd in tree.commands:
            if cmd.type == 'source' and cmd.config.linkId == link_id:
                return cmd.config.mainTable
        
        # Check children
        if tree.children:
            for child in tree.children:
                res = self._resolve_table_from_link_id(child, link_id)
                if res:
                    return res
        return None

    def _collect_setup_sources(self, tree: OperationNode):
        allowed_tables: Set[str] = set()
        source_map: Dict[str, str] = {}
        table_to_ids: Dict[str, Set[str]] = {}

        def add_mapping(table: str, identifier: Optional[str]):
            if not identifier:
                return
            source_map[identifier] = table
            table_to_ids.setdefault(table, set()).add(identifier)

        def visit(node: OperationNode):
            # Collect all source commands. Some older session payloads may omit operationType='setup'.
            for cmd in node.commands:
                if cmd.type == 'source' and cmd.config.mainTable:
                    table = cmd.config.mainTable
                    allowed_tables.add(table)
                    add_mapping(table, table)
                    add_mapping(table, cmd.id)
                    add_mapping(table, cmd.config.linkId)
                    add_mapping(table, cmd.config.alias)
            if node.children:
                for child in node.children:
                    visit(child)

        visit(tree)
        return allowed_tables, source_map, table_to_ids

    def _resolve_setup_table(self, ref: Optional[str], allowed_tables: Set[str], source_map: Dict[str, str]) -> Optional[str]:
        if not ref:
            return None
        resolved = source_map.get(ref, ref)
        if is_reserved_identifier(resolved):
            raise ValueError(f"Dataset name '{resolved}' is a reserved keyword. Please rename or re-import.")
        if resolved not in allowed_tables:
            raise ValueError(f"Table '{ref}' is not defined in Data Setup")
        return resolved

    def _rewrite_join_on(self, on_clause: str, input_table: Optional[str], join_table: Optional[str], table_to_ids: Dict[str, Set[str]]) -> str:
        if not on_clause:
            return on_clause
        rewritten = on_clause
        if input_table:
            for ident in sorted(table_to_ids.get(input_table, set()), key=len, reverse=True):
                rewritten = self._replace_ident_prefix(rewritten, ident, "t1.")
        if join_table:
            for ident in sorted(table_to_ids.get(join_table, set()), key=len, reverse=True):
                rewritten = self._replace_ident_prefix(rewritten, ident, "t2.")
        return rewritten

    def _replace_ident_prefix(self, clause: str, ident: str, replacement: str) -> str:
        if not ident:
            return clause
        forms = {ident, quote_identifier(ident)}
        rewritten = clause
        for form in forms:
            pattern = rf"(?<![A-Za-z0-9_]){re.escape(form)}\."
            rewritten = re.sub(pattern, replacement, rewritten)
        return rewritten

    def _extract_simple_select(self, sql: str) -> tuple[Optional[str], Optional[str]]:
        if not sql:
            return None, None
        m = SIMPLE_SELECT_RE.match(sql)
        if m:
            return unquote_identifier(m.group(1)), None
        m = SIMPLE_SELECT_WHERE_RE.match(sql)
        if m:
            return unquote_identifier(m.group(1)), m.group(2)
        return None, None

    def _extract_where_clause(self, sql: str) -> Optional[str]:
        if not sql:
            return None
        m = WHERE_EXTRACT_RE.match(sql)
        if not m:
            return None
        return m.group(1)

    def _select_input_table(self, current_sql: str, current_base_table: Optional[str]) -> str:
        base_table, existing_where = self._extract_simple_select(current_sql)
        if base_table and current_base_table and base_table == current_base_table:
            base_sql = quote_identifier(base_table)
            if existing_where:
                return f"(SELECT * FROM {base_sql} WHERE {existing_where})"
            return base_sql
        return f"({current_sql}) AS input_subq"

    def _build_view_sql(self, cmd: Command, base_table: str, existing_where: Optional[str]) -> str:
        c = cmd.config
        view_fields = c.viewFields or []
        fields = [vf.field for vf in view_fields if getattr(vf, 'field', None)]
        distinct_fields = [vf.field for vf in view_fields if getattr(vf, 'field', None) and getattr(vf, 'distinct', False)]

        if fields:
            seen = set()
            deduped = []
            for f in fields:
                if f in seen:
                    continue
                seen.add(f)
                deduped.append(f)
            fields = deduped

        if distinct_fields:
            seen = set()
            deduped = []
            for f in distinct_fields:
                if f in seen:
                    continue
                seen.add(f)
                deduped.append(f)
            distinct_fields = deduped

        if distinct_fields:
            select_fields = distinct_fields
            distinct = "DISTINCT "
        else:
            select_fields = fields
            distinct = ""

        if not select_fields:
            select_fields = ["*"]

        quoted_fields = ["*" if f == "*" else quote_identifier(f) for f in select_fields]
        sql = f"SELECT {distinct}{', '.join(quoted_fields)} FROM {quote_identifier(base_table)}"
        if existing_where:
            sql = f"{sql} WHERE {existing_where}"

        sort_parts = []
        if c.viewSorts:
            seen = set()
            for s in c.viewSorts:
                if not getattr(s, 'field', None):
                    continue
                if s.field in seen:
                    continue
                seen.add(s.field)
                sort_dir = "ASC" if getattr(s, 'ascending', True) is not False else "DESC"
                if select_fields == ["*"] or s.field in select_fields:
                    sort_parts.append(f"{quote_identifier(s.field)} {sort_dir}")
        elif c.viewSortField:
            sort_dir = "ASC" if c.viewSortAscending is not False else "DESC"
            if select_fields == ["*"] or c.viewSortField in select_fields:
                sort_parts.append(f"{quote_identifier(c.viewSortField)} {sort_dir}")

        if sort_parts:
            sql = f"{sql} ORDER BY {', '.join(sort_parts)}"

        limit = c.viewLimit
        if isinstance(limit, int) and limit >= 0:
            sql = f"{sql} LIMIT {limit}"

        return sql

    def _copy_command_with_overrides(self, cmd: Command, **config_updates) -> Command:
        if hasattr(cmd, "model_dump"):
            data = cmd.model_dump()
        else:
            data = cmd.dict()
        config = data.get("config", {})
        config.update(config_updates)
        data["config"] = config
        return Command(**data)

    def _apply_node_commands(self, df: Optional[pd.DataFrame], commands: List[Command], session_id: str, variables: Dict[str, Any], tree: OperationNode, limit_command_id: str = None) -> Optional[pd.DataFrame]:
        sorted_cmds = sorted(commands, key=lambda x: x.order)
        current_source_name = None
        source_stream_reusable = True
        if df is not None and hasattr(df, "attrs"):
            current_source_name = df.attrs.get("_source_name")
            source_stream_reusable = df.attrs.get("_source_stream_reusable", True)
        
        for idx, cmd in enumerate(sorted_cmds):
            try:
                # 1. Handle Context/Source Loading (Common to all commands)
                source = cmd.config.dataSource
                # Only load source if we are at the start of the stream (df is None)
                # OR if it is a View command explicitly asking to view a table
                if source and source != 'stream':
                     # Try to resolve source as a linkId first
                     resolved_table = self._resolve_table_from_link_id(tree, source)
                     if resolved_table:
                         source = resolved_table
                     
                     # Reuse stream only when it is still source-compatible (e.g. chained filters).
                     # After group/aggregate the stream is derived and must be reloaded even if source name matches.
                     if (
                         df is not None
                         and current_source_name
                         and source == current_source_name
                         and source_stream_reusable
                     ):
                         source = None
                     else:
                         df = storage.get_full_dataset(session_id, source)
                         if df is not None:
                             current_source_name = source
                             source_stream_reusable = True
                
                # Legacy fallback for Source Type
                if cmd.type == 'source':
                    table_name = cmd.config.mainTable
                    if table_name:
                        df = storage.get_full_dataset(session_id, table_name)
                        if df is not None:
                            current_source_name = table_name
                            source_stream_reusable = True
                elif df is None and (cmd.type not in ['join', 'group', 'multi_table', 'view', 'define_variable'] and cmd.config.mainTable):
                    table_name = cmd.config.mainTable
                    if table_name:
                        df = storage.get_full_dataset(session_id, table_name)
                        if df is not None:
                            current_source_name = table_name
                            source_stream_reusable = True
                    # If it's purely a source command, we are done with this step logic, but check loop exit below
                    if cmd.type == 'source':
                        pass # Continue to check exit condition

                # 2. View Command Logic
                if cmd.type == 'view':
                    # View command explicitly sets the dataframe to the dataSource, done above by generic handler.
                    # If dataSource was 'stream' (default), it's a pass-through.
                    if df is not None:
                        view_fields = cmd.config.viewFields or []
                        fields = [vf.field for vf in view_fields if getattr(vf, 'field', None)]
                        distinct_fields = [vf.field for vf in view_fields if getattr(vf, 'field', None) and getattr(vf, 'distinct', False)]

                        if fields:
                            seen = set()
                            deduped = []
                            for f in fields:
                                if f in seen:
                                    continue
                                seen.add(f)
                                deduped.append(f)
                            fields = deduped

                        if distinct_fields:
                            seen = set()
                            deduped = []
                            for f in distinct_fields:
                                if f in seen:
                                    continue
                                seen.add(f)
                                deduped.append(f)
                            distinct_fields = deduped

                        # If any distinct is set, only keep those fields for deterministic output
                        selected_fields = distinct_fields if distinct_fields else fields
                        if selected_fields:
                            valid = [f for f in selected_fields if f in df.columns]
                            if valid:
                                df = df[valid]

                        if distinct_fields:
                            valid_distinct = [f for f in distinct_fields if f in df.columns]
                            if valid_distinct:
                                df = df.drop_duplicates(subset=valid_distinct)

                        sort_fields = []
                        sort_dirs = []
                        if cmd.config.viewSorts:
                            seen = set()
                            for s in cmd.config.viewSorts:
                                if s.field and s.field in df.columns:
                                    if s.field in seen:
                                        continue
                                    seen.add(s.field)
                                    sort_fields.append(s.field)
                                    sort_dirs.append(s.ascending is not False)
                        elif cmd.config.viewSortField and cmd.config.viewSortField in df.columns:
                            sort_fields.append(cmd.config.viewSortField)
                            sort_dirs.append(cmd.config.viewSortAscending is not False)

                        if sort_fields:
                            df = df.sort_values(by=sort_fields, ascending=sort_dirs)

                        limit = cmd.config.viewLimit
                        if isinstance(limit, int) and limit >= 0:
                            df = df.head(limit)

                # 3. Variable Logic
                if cmd.type == 'save' and df is not None:
                     field, var_name = cmd.config.field, cmd.config.value
                     is_distinct = cmd.config.distinct if cmd.config.distinct is not None else True
                     if field and var_name and field in df.columns:
                         if is_distinct:
                             variables[str(var_name)] = df[field].unique().tolist()
                         else:
                             variables[str(var_name)] = df[field].tolist()
                elif cmd.type == 'define_variable':
                    v_name = cmd.config.variableName
                    v_val = cmd.config.variableValue
                    if v_name:
                        variables[v_name] = v_val
                
                # 4. Processing Logic (Skip if df is None)
                if df is not None:
                    if cmd.type == 'filter': df = self._apply_filter(df, cmd, variables)
                    elif cmd.type == 'join': df = self._apply_join(df, cmd, session_id, tree)
                    elif cmd.type == 'sort': df = self._apply_sort(df, cmd)
                    elif cmd.type == 'group' or cmd.type == 'aggregate':
                        df = self._apply_group(df, cmd, session_id)
                        source_stream_reusable = False
                    elif cmd.type == 'transform': df = self._apply_transform(df, cmd)
                    # multi_table and view are pass-throughs for the stream itself (data loaded via context above)
                    if df is not None and current_source_name:
                        df.attrs["_source_name"] = current_source_name
                        df.attrs["_source_stream_reusable"] = source_stream_reusable

                # Check Stop Condition
                if limit_command_id and cmd.id == limit_command_id:
                    break

            except Exception as e:
                print(f"Error executing command {cmd.id} ({cmd.type}): {e}")
                continue
                
        return df

    def _apply_filter(self, df: pd.DataFrame, cmd: Command, variables: Dict[str, Any]) -> pd.DataFrame:
        root = cmd.config.filterRoot
        if not root:
            # Fallback for single condition legacy support
            c = cmd.config
            if not c.field: return df
            mask = self._get_condition_mask(df, {
                "field": c.field,
                "operator": c.operator,
                "value": c.value,
                "dataType": c.dataType
            }, variables)
            return df[mask]

        mask = self._get_group_mask(df, root, variables)
        return df[mask]

    def _get_group_mask(self, df: pd.DataFrame, group: Dict[str, Any], variables: Dict[str, Any]) -> pd.Series:
        logical_op = group.get('logicalOperator', 'AND')
        conditions = group.get('conditions', [])
        
        if not conditions:
            return pd.Series([True] * len(df), index=df.index)

        masks = []
        for item in conditions:
            if item.get('type') == 'group':
                masks.append(self._get_group_mask(df, item, variables))
            else:
                masks.append(self._get_condition_mask(df, item, variables))

        if logical_op == 'AND':
            res = masks[0]
            for m in masks[1:]: res = res & m
            return res
        else:
            res = masks[0]
            for m in masks[1:]: res = res | m
            return res

    def _get_condition_mask(self, df: pd.DataFrame, cond: Dict[str, Any], variables: Dict[str, Any]) -> pd.Series:
        field = cond.get('field')
        op = cond.get('operator')
        val = cond.get('value')
        value_type = cond.get('valueType')

        if op == 'always_true':
            return pd.Series([True] * len(df), index=df.index)
        if op == 'always_false':
            return pd.Series([False] * len(df), index=df.index)
        if field not in df.columns: 
            return pd.Series([True] * len(df), index=df.index)

        series = df[field]
        if op == 'is_null':
            return series.isnull()
        if op == 'is_not_null':
            return ~series.isnull()
        
        # Variable Resolution in Value
        target_val = val
        
        if value_type == 'variable':
             var_name = str(val)
             if var_name.startswith('{') and var_name.endswith('}'):
                 var_name = var_name[1:-1]
             if var_name in variables:
                 target_val = variables[var_name]
        elif isinstance(target_val, str) and target_val.startswith('{') and target_val.endswith('}'):
            var_name = target_val[1:-1]
            if var_name in variables:
                target_val = variables[var_name]

        if op == 'in_variable':
            # Support direct variable name reference OR resolved list from above
            target_list = []
            if isinstance(target_val, list):
                target_list = target_val
            else:
                target_list = variables.get(str(target_val), [])
            return series.isin(target_list)
            
        if op == 'not_in_variable':
            target_list = []
            if isinstance(target_val, list):
                target_list = target_val
            else:
                target_list = variables.get(str(target_val), [])
            return ~series.isin(target_list)

        try:
             # Use resolved target_val
             if cond.get('dataType') == 'number' or isinstance(target_val, (int, float)):
                 v = float(target_val)
                 if op == '>': return series > v
                 if op == '>=': return series >= v
                 if op == '<': return series < v
                 if op == '<=': return series <= v
                 if op == '=': return series == v
                 if op == '!=': return series != v
             else:
                 v = str(target_val) # Fallback string representation for simple ops
                 s_str = series.astype(str)
                 if op == '=': return s_str == v
                 if op == '!=': return s_str != v
                 
                 if op == 'contains': 
                     vals = []
                     if isinstance(target_val, list):
                         vals = [str(x) for x in target_val]
                     else:
                         vals = [x.strip() for x in str(target_val).split(',')]
                     
                     if len(vals) > 0:
                         mask = pd.Series([False] * len(df), index=df.index)
                         for val in vals:
                             mask = mask | s_str.str.contains(val, case=False, na=False)
                         return mask
                     return pd.Series([False] * len(df), index=df.index)

                 if op == 'not_contains': 
                     vals = []
                     if isinstance(target_val, list):
                         vals = [str(x) for x in target_val]
                     else:
                         vals = [x.strip() for x in str(target_val).split(',')]

                     if len(vals) > 0:
                         mask = pd.Series([False] * len(df), index=df.index)
                         for val in vals:
                             mask = mask | s_str.str.contains(val, case=False, na=False)
                         return ~mask
                     return pd.Series([True] * len(df), index=df.index)
                 
                 if op == 'in_list':
                     vals = []
                     if isinstance(target_val, list):
                         vals = [str(x) for x in target_val]
                     else:
                         vals = [x.strip() for x in str(target_val).split(',')]
                         
                     if cond.get('dataType') == 'number':
                         try:
                             num_vals = [float(x) for x in vals if x.strip()]
                             return series.isin(num_vals)
                         except:
                             pass
                     return s_str.isin(vals)

                 if op == 'not_in_list':
                     vals = []
                     if isinstance(target_val, list):
                         vals = [str(x) for x in target_val]
                     else:
                         vals = [x.strip() for x in str(target_val).split(',')]

                     if cond.get('dataType') == 'number':
                         try:
                             num_vals = [float(x) for x in vals if x.strip()]
                             return ~series.isin(num_vals)
                         except:
                             pass
                     return ~s_str.isin(vals)

                 if op == 'starts_with': return s_str.str.startswith(v, na=False)
                 if op == 'ends_with': return s_str.str.endswith(v, na=False)
                 if op == 'is_empty': return series.isna() | (s_str == '')
                 if op == 'is_not_empty': return (~series.isna()) & (s_str != '')
        except: pass
        
        return pd.Series([True] * len(df), index=df.index)

    def _apply_join(self, df: pd.DataFrame, cmd: Command, session_id: str, tree: OperationNode) -> pd.DataFrame:
        join_type_map = {'left': 'left', 'right': 'right', 'inner': 'inner', 'full': 'outer'}
        join_type = join_type_map.get((cmd.config.joinType or 'left').lower(), 'left')
        suffix = cmd.config.joinSuffix or "_joined"

        target_df = None
        if cmd.config.joinTargetType == 'node' and cmd.config.joinTargetNodeId:
            target_df = self.execute(session_id, tree, cmd.config.joinTargetNodeId)
        elif cmd.config.joinTable:
             target_df = storage.get_full_dataset(session_id, cmd.config.joinTable)

        if target_df is None or target_df.empty: return df

        on_clause = cmd.config.on
        try:
            if on_clause and '=' in on_clause:
                left_on, right_on = [x.strip() for x in on_clause.split('=')]
                # Strip table prefixes if present
                if '.' in left_on: left_on = left_on.split('.')[-1]
                if '.' in right_on: right_on = right_on.split('.')[-1]
                
                return pd.merge(df, target_df, left_on=left_on, right_on=right_on, how=join_type, suffixes=('', suffix))
            elif on_clause:
                col = on_clause.strip()
                if '.' in col: col = col.split('.')[-1]
                return pd.merge(df, target_df, on=col, how=join_type, suffixes=('', suffix))
        except Exception as e:
            print(f"Join Error: {e}")
            pass
        return df

    def _apply_group(self, df: pd.DataFrame, cmd: Command, session_id: str) -> pd.DataFrame:
        c = cmd.config
        group_fields = c.groupByFields or (c.groupBy if c.groupBy else [])
        aggregations = c.aggregations or []
        if not aggregations and c.field and c.aggFunc:
             aggregations = [{'field': c.field, 'func': c.aggFunc, 'alias': f'{c.aggFunc}_{c.field}'}]

        if not group_fields and not aggregations: return df
        valid_group_cols = [f for f in group_fields if f in df.columns]
        
        if valid_group_cols:
            grouped = df.groupby(valid_group_cols)
            agg_dict = {}
            for agg in aggregations:
                f, func, alias = agg.get('field'), agg.get('func'), agg.get('alias')
                if f == '*' or not f: 
                    agg_dict[alias] = pd.NamedAgg(column=df.columns[0], aggfunc='count')
                elif f in df.columns:
                    agg_dict[alias] = pd.NamedAgg(column=f, aggfunc=func)
            result_df = grouped.agg(**agg_dict).reset_index()
        else:
            res_row = {}
            for agg in aggregations:
                f, func, alias = agg.get('field'), agg.get('func'), agg.get('alias')
                if f == '*' or not f: res_row[alias] = len(df)
                elif f in df.columns:
                    if func == 'sum': res_row[alias] = df[f].sum()
                    elif func == 'mean': res_row[alias] = df[f].mean()
                    elif func == 'min': res_row[alias] = df[f].min()
                    elif func == 'max': res_row[alias] = df[f].max()
                    else: res_row[alias] = df[f].count()
            result_df = pd.DataFrame([res_row])

        having = c.havingConditions or []
        for h in having:
            metric = h.get('metricAlias')
            op = h.get('operator')
            val = h.get('value')
            if metric in result_df.columns:
                series = result_df[metric]
                try:
                    v = float(val)
                    if op == '>': result_df = result_df[series > v]
                    elif op == '>=': result_df = result_df[series >= v]
                    elif op == '<': result_df = result_df[series < v]
                    elif op == '<=': result_df = result_df[series <= v]
                    elif op == '=': result_df = result_df[series == v]
                    elif op == '!=': result_df = result_df[series != v]
                except (ValueError, TypeError):
                    v = str(val)
                    s_str = series.astype(str)
                    if op == '=': result_df = result_df[s_str == v]
                    elif op == '!=': result_df = result_df[s_str != v]
                    elif op == 'contains': result_df = result_df[s_str.str.contains(v, case=False, na=False)]

        if c.outputTableName: storage.add_dataset(session_id, c.outputTableName, result_df)
        return result_df

    def _apply_transform(self, df: pd.DataFrame, cmd: Command) -> pd.DataFrame:
        mappings = cmd.config.mappings
        if mappings:
            mapping_funcs = {}
            runtime_config = runtime_config_module.load_runtime_config()
            for m in mappings:
                if not m.outputField or not m.expression: continue
                if m.mode == 'python':
                    try:
                        mapping_funcs[m.outputField] = compile_python_transform(
                            m.expression,
                            allow_unsafe=runtime_config.unsafe_python_transform_enabled,
                        )
                    except Exception as e:
                        print(f"Compilation Error: {e}")
            def apply_row(row):
                new_data = {}
                row_dict = row.to_dict()
                for m in mappings:
                    if not m.outputField or not m.expression: continue
                    if m.mode == 'python':
                        func = mapping_funcs.get(m.outputField)
                        if func:
                            try: new_data[m.outputField] = func(row_dict)
                            except: new_data[m.outputField] = None
                        else: new_data[m.outputField] = None
                    else:
                        try: new_data[m.outputField] = simple_eval(m.expression, names=row_dict)
                        except: new_data[m.outputField] = None
                return pd.Series(new_data)
            new_cols = df.apply(apply_row, axis=1)
            return pd.concat([df, new_cols], axis=1)
        return df

    def _apply_sort(self, df: pd.DataFrame, cmd: Command) -> pd.DataFrame:
        f = cmd.config.field
        asc = cmd.config.ascending if cmd.config.ascending is not None else True
        if f and f in df.columns: return df.sort_values(by=f, ascending=asc)
        return df
