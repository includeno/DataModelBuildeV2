
import pandas as pd
import numpy as np
import math
import datetime
import re
from typing import List, Optional, Dict, Set, Any, Union
from models import Command, OperationNode
from storage import storage
from simpleeval import simple_eval

class ExecutionEngine:
    def execute(self, session_id: str, tree: OperationNode, target_node_id: str) -> pd.DataFrame:
        path = self._find_path_to_node(tree, target_node_id)
        if not path:
            raise ValueError("Target node not found in operation tree")

        df = None
        variables: Dict[str, Any] = {} 

        for node in path:
            if node.enabled:
                df = self._apply_node_commands(df, node.commands, session_id, variables, tree)
        
        if df is None:
            return pd.DataFrame()

        return df

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

    def _apply_node_commands(self, df: Optional[pd.DataFrame], commands: List[Command], session_id: str, variables: Dict[str, Any], tree: OperationNode) -> Optional[pd.DataFrame]:
        sorted_cmds = sorted(commands, key=lambda x: x.order)
        
        for idx, cmd in enumerate(sorted_cmds):
            try:
                source = cmd.config.dataSource
                if source and source != 'stream':
                     df = storage.get_full_dataset(session_id, source)
                
                if cmd.type == 'source' or (cmd.type not in ['join', 'group'] and cmd.config.mainTable):
                    table_name = cmd.config.mainTable
                    if table_name:
                        df = storage.get_full_dataset(session_id, table_name)
                    continue

                if cmd.type == 'save' and df is not None:
                     field, var_name = cmd.config.field, cmd.config.value
                     is_distinct = cmd.config.distinct if cmd.config.distinct is not None else True
                     if field and var_name and field in df.columns:
                         if is_distinct:
                             variables[str(var_name)] = df[field].unique().tolist()
                         else:
                             variables[str(var_name)] = df[field].tolist()
                     continue

                if df is None: continue

                if cmd.type == 'filter': df = self._apply_filter(df, cmd, variables)
                elif cmd.type == 'join': df = self._apply_join(df, cmd, session_id, tree)
                elif cmd.type == 'sort': df = self._apply_sort(df, cmd)
                elif cmd.type == 'group' or cmd.type == 'aggregate': 
                    df = self._apply_group(df, cmd, session_id)
                elif cmd.type == 'transform': df = self._apply_transform(df, cmd)
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
        if field not in df.columns: 
            return pd.Series([True] * len(df), index=df.index)

        series = df[field]
        if op == 'in_variable':
            target_list = variables.get(str(val), [])
            return series.isin(target_list)
        if op == 'not_in_variable':
            target_list = variables.get(str(val), [])
            return ~series.isin(target_list)

        try:
             if cond.get('dataType') == 'number' or isinstance(val, (int, float)):
                 v = float(val)
                 if op == '>': return series > v
                 if op == '>=': return series >= v
                 if op == '<': return series < v
                 if op == '<=': return series <= v
                 if op == '=': return series == v
                 if op == '!=': return series != v
             else:
                 v = str(val)
                 s_str = series.astype(str)
                 if op == '=': return s_str == v
                 if op == '!=': return s_str != v
                 if op == 'contains': return s_str.str.contains(v, case=False, na=False)
                 if op == 'not_contains': return ~s_str.str.contains(v, case=False, na=False)
                 if op == 'starts_with': return s_str.str.startswith(v, na=False)
                 if op == 'ends_with': return s_str.str.endswith(v, na=False)
                 if op == 'is_empty': return (s_str == '') | series.isna()
                 if op == 'is_not_empty': return (s_str != '') & ~series.isna()
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
                return pd.merge(df, target_df, left_on=left_on, right_on=right_on, how=join_type, suffixes=('', suffix))
            elif on_clause:
                return pd.merge(df, target_df, on=on_clause.strip(), how=join_type, suffixes=('', suffix))
        except: pass
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
            exec_globals = {'np': np, 'pd': pd, 'math': math, 'datetime': datetime, 're': re}
            mapping_funcs = {}
            for m in mappings:
                if not m.outputField or not m.expression: continue
                if m.mode == 'python':
                    try:
                        local_scope = {}
                        exec(m.expression, exec_globals, local_scope)
                        if 'transform' in local_scope: mapping_funcs[m.outputField] = local_scope['transform']
                        else:
                            for k, v in local_scope.items():
                                if callable(v): mapping_funcs[m.outputField] = v; break
                    except Exception as e: print(f"Compilation Error: {e}")
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
