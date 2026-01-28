import pandas as pd
import numpy as np
import math
from typing import List, Optional
from .models import Command, OperationNode
from .storage import storage

class ExecutionEngine:
    def execute(self, tree: OperationNode, target_node_id: str, session_id: str) -> pd.DataFrame:
        path = self._find_path_to_node(tree, target_node_id)
        if not path:
            raise ValueError("Target node not found in operation tree")

        # Start with the first dataset available if no explicit source is defined
        # In a real system, the root node might define the source
        datasets = storage.list_datasets(session_id)
        if not datasets:
            return pd.DataFrame()

        # Default source: the first uploaded dataset
        initial_df = storage.get_dataset(session_id, datasets[0])
        if initial_df is None:
            return pd.DataFrame()
            
        df = initial_df.copy()

        for node in path:
            if node.enabled:
                df = self._apply_node_commands(df, node.commands, session_id)
        
        return df

    def _find_path_to_node(self, root: OperationNode, target_id: str) -> Optional[List[OperationNode]]:
        if root.id == target_id:
            return [root]
        if root.children:
            for child in root.children:
                path = self._find_path_to_node(child, target_id)
                if path:
                    return [root] + path
        return None

    def _apply_node_commands(self, df: pd.DataFrame, commands: List[Command], session_id: str) -> pd.DataFrame:
        sorted_cmds = sorted(commands, key=lambda x: x.order)
        for cmd in sorted_cmds:
            try:
                if cmd.type == 'filter':
                    df = self._apply_filter(df, cmd)
                elif cmd.type == 'join':
                    df = self._apply_join(df, cmd, session_id)
                elif cmd.type == 'sort':
                    df = self._apply_sort(df, cmd)
                elif cmd.type == 'aggregate':
                    df = self._apply_aggregate(df, cmd)
                elif cmd.type == 'select':
                    df = self._apply_select(df, cmd)
                elif cmd.type == 'transform':
                    df = self._apply_transform(df, cmd)
            except Exception as e:
                print(f"Error executing command {cmd.id} ({cmd.type}): {e}")
                # Continue execution even if one command fails
                continue
        return df

    def _apply_filter(self, df: pd.DataFrame, cmd: Command) -> pd.DataFrame:
        c = cmd.config
        field = c.field
        op = c.operator
        val = c.value
        dtype = c.dataType or 'string'

        if field not in df.columns:
            return df

        if dtype == 'number':
            try:
                val = float(val)
                if op == '>': return df[df[field] > val]
                if op == '>=': return df[df[field] >= val]
                if op == '<': return df[df[field] < val]
                if op == '<=': return df[df[field] <= val]
                if op == '=': return df[df[field] == val]
                if op == '!=': return df[df[field] != val]
            except:
                pass
        
        elif dtype == 'boolean':
            try:
                # Normalize boolean value from string or bool
                if isinstance(val, str):
                    bool_val = val.lower() == 'true'
                else:
                    bool_val = bool(val)
                
                # Ensure column is boolean for comparison
                series = df[field].astype(bool)
                
                if op == '=' or op == 'true' or op == 'false': 
                    # Handle "is true" / "is false" operators which might not strictly use 'val'
                    if op == 'true': return df[series == True]
                    if op == 'false': return df[series == False]
                    return df[series == bool_val]
                
                if op == '!=': return df[series != bool_val]
            except:
                pass

        elif dtype == 'date' or dtype == 'timestamp':
            try:
                # Convert column to datetime if not already
                series = pd.to_datetime(df[field], errors='coerce')
                target = pd.to_datetime(val)
                
                if op == 'before': return df[series < target]
                if op == 'after': return df[series > target]
                if op == '=': return df[series == target] 
            except:
                pass

        else: # String and others
            val = str(val)
            series = df[field].astype(str)
            if op == '=': return df[series == val]
            if op == '!=': return df[series != val]
            if op == 'contains': return df[series.str.contains(val, case=False, na=False, regex=False)]
            if op == 'starts_with': return df[series.str.startswith(val, na=False)]
            if op == 'ends_with': return df[series.str.endswith(val, na=False)]

        return df

    def _apply_join(self, df: pd.DataFrame, cmd: Command, session_id: str) -> pd.DataFrame:
        target_name = cmd.config.joinTable
        if not target_name:
            return df
        
        other_df = storage.get_dataset(session_id, target_name)
        if other_df is None:
            return df

        # Fix: Map frontend 'FULL' to pandas 'outer'
        join_type_map = {
            'left': 'left',
            'right': 'right',
            'inner': 'inner',
            'full': 'outer'
        }
        raw_type = (cmd.config.joinType or 'left').lower()
        join_type = join_type_map.get(raw_type, 'left')
        
        on_clause = cmd.config.on
        
        if on_clause and '=' in on_clause:
            left_on, right_on = [x.strip() for x in on_clause.split('=')]
            # Check if columns exist to prevent crash
            if left_on in df.columns and right_on in other_df.columns:
                return pd.merge(df, other_df, left_on=left_on, right_on=right_on, how=join_type)
        elif on_clause:
             clean_on = on_clause.strip()
             if clean_on in df.columns and clean_on in other_df.columns:
                return pd.merge(df, other_df, on=clean_on, how=join_type)
        
        return df

    def _apply_sort(self, df: pd.DataFrame, cmd: Command) -> pd.DataFrame:
        field = cmd.config.field
        ascending = cmd.config.ascending if cmd.config.ascending is not None else True
        if field and field in df.columns:
            return df.sort_values(by=field, ascending=ascending)
        return df

    def _apply_aggregate(self, df: pd.DataFrame, cmd: Command) -> pd.DataFrame:
        group_cols = cmd.config.groupBy
        agg_func = cmd.config.aggFunc or 'count'
        field = cmd.config.field # The field to aggregate on

        if not group_cols or not field:
            return df

        valid_cols = [c for c in group_cols if c in df.columns]
        if not valid_cols:
            return df

        grouped = df.groupby(valid_cols)
        
        if field in df.columns:
            if agg_func == 'sum':
                res = grouped[field].sum().reset_index()
            elif agg_func == 'mean':
                res = grouped[field].mean().reset_index()
            elif agg_func == 'max':
                res = grouped[field].max().reset_index()
            elif agg_func == 'min':
                res = grouped[field].min().reset_index()
            else:
                res = grouped[field].count().reset_index()
            return res
            
        return df

    def _apply_select(self, df: pd.DataFrame, cmd: Command) -> pd.DataFrame:
        fields = cmd.config.fields
        if fields:
            valid_fields = [f for f in fields if f in df.columns]
            if valid_fields:
                return df[valid_fields]
        return df

    def _apply_transform(self, df: pd.DataFrame, cmd: Command) -> pd.DataFrame:
        output_field = cmd.config.outputField
        expression = cmd.config.expression

        if not output_field or not expression:
            return df

        try:
            # RESTRICTED EVAL ENVIRONMENT
            # Expose 'row', 'math', 'np' (numpy), 'pd' (pandas)
            # This allows expressions like: row['salary'] * 1.2 or row['first_name'] + ' ' + row['last_name']
            
            allowed_globals = {
                "math": math,
                "np": np,
                "len": len,
                "str": str,
                "int": int,
                "float": float
            }

            def eval_wrapper(row):
                # Provide row as local context
                return eval(expression, {"__builtins__": None}, {**allowed_globals, "row": row})

            # Apply row-wise
            df[output_field] = df.apply(eval_wrapper, axis=1)
        except Exception as e:
            print(f"Transform expression failed for field '{output_field}': {e}")
            # Optionally set nulls or let the error propagate up to the loop
            pass
            
        return df
