
import pandas as pd
import numpy as np
import math
import datetime
import re
import duckdb
from typing import List, Optional, Dict, Set, Any, Union
from models import Command, OperationNode
from storage import storage
from simpleeval import simple_eval

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

    def generate_sql(self, session_id: str, tree: OperationNode, target_node_id: str, target_command_id: str) -> str:
        from sql_generator import generate_sql_for_command
        
        path = self._find_path_to_node(tree, target_node_id)
        if not path: raise ValueError("Target node not found")
        
        df = None
        variables: Dict[str, Any] = {}
        
        ctes = []
        step_counter = 0
        current_sql_input = "initial_source"
        found_target = False
        
        for node in path:
            if not node.enabled: continue
            
            for cmd in node.commands:
                # Execute command to update variables/state (needed for variable resolution in SQL)
                # We pass [cmd] to reuse the existing logic which handles single command execution
                df = self._apply_node_commands(df, [cmd], session_id, variables, tree)
                
                # Generate SQL for this command
                cmd_sql = generate_sql_for_command(cmd, variables, current_sql_input)
                
                # If SQL is valid (not a comment), add to CTE chain
                if not cmd_sql.strip().startswith("--"):
                    step_name = f"step_{step_counter}"
                    ctes.append(f"{step_name} AS ({cmd_sql})")
                    current_sql_input = step_name
                    step_counter += 1
                
                if node.id == target_node_id and cmd.id == target_command_id:
                    found_target = True
                    break
            
            if found_target:
                break
                
        if not found_target:
            raise ValueError("Target command not found")

        if not ctes:
            return "-- No SQL generated (or all commands were unsupported)"
            
        cte_string = ",\n".join(ctes)
        return f"WITH {cte_string}\nSELECT * FROM {current_sql_input}"

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

        # 3. Perform the filter join using DuckDB
        con = duckdb.connect(":memory:")
        try:
            con.register('main_table', df)
            
            # Get the sub table
            sub_df = storage.get_full_dataset(session_id, sub_config.table)
            if sub_df is None:
                raise ValueError(f"Dataset {sub_config.table} not found")
            
            con.register('sub_table', sub_df)
            
            # Construct Query: SELECT * FROM sub_table sub WHERE EXISTS (SELECT 1 FROM main_table main WHERE condition)
            condition = sub_config.on
            
            query = f"""
                SELECT sub.* 
                FROM sub_table sub
                WHERE EXISTS (
                    SELECT 1 
                    FROM main_table main 
                    WHERE {condition}
                )
            """
            result = con.execute(query).df()
            return result
        except Exception as e:
            raise ValueError(f"Failed to execute sub-table query: {str(e)}")
        finally:
            con.close()

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

    def _apply_node_commands(self, df: Optional[pd.DataFrame], commands: List[Command], session_id: str, variables: Dict[str, Any], tree: OperationNode, limit_command_id: str = None) -> Optional[pd.DataFrame]:
        sorted_cmds = sorted(commands, key=lambda x: x.order)
        
        for idx, cmd in enumerate(sorted_cmds):
            try:
                # 1. Handle Context/Source Loading (Common to all commands)
                source = cmd.config.dataSource
                # Only load source if we are at the start of the stream (df is None)
                # OR if it is a View command explicitly asking to view a table
                if source and source != 'stream' and (df is None or cmd.type == 'view'):
                     # Try to resolve source as a linkId first
                     resolved_table = self._resolve_table_from_link_id(tree, source)
                     if resolved_table:
                         source = resolved_table
                     
                     # If loading a specific source, replace df
                     df = storage.get_full_dataset(session_id, source)
                
                # Legacy fallback for Source Type
                if cmd.type == 'source':
                    table_name = cmd.config.mainTable
                    if table_name:
                        df = storage.get_full_dataset(session_id, table_name)
                elif df is None and (cmd.type not in ['join', 'group', 'multi_table', 'view', 'define_variable'] and cmd.config.mainTable):
                    table_name = cmd.config.mainTable
                    if table_name:
                        df = storage.get_full_dataset(session_id, table_name)
                    # If it's purely a source command, we are done with this step logic, but check loop exit below
                    if cmd.type == 'source':
                        pass # Continue to check exit condition

                # 2. View Command Logic
                if cmd.type == 'view':
                    # View command explicitly sets the dataframe to the dataSource, done above by generic handler.
                    # If dataSource was 'stream' (default), it's a pass-through.
                    pass 

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
                    elif cmd.type == 'group' or cmd.type == 'aggregate': df = self._apply_group(df, cmd, session_id)
                    elif cmd.type == 'transform': df = self._apply_transform(df, cmd)
                    # multi_table and view are pass-throughs for the stream itself (data loaded via context above)

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

        if field not in df.columns: 
            return pd.Series([True] * len(df), index=df.index)

        series = df[field]
        
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
