
from typing import Dict, Any, List, Union
from models import Command, CommandConfig
from sql_utils import quote_identifier, quote_table_ref

def generate_sql_for_command(cmd: Command, variables: Dict[str, Any], input_table: str = "input_table") -> str:
    c = cmd.config
    
    if cmd.type == 'filter':
        if c.filterRoot:
            where_clause = _build_group_condition(c.filterRoot, variables)
        else:
            # Legacy single condition
            where_clause = _build_single_condition({
                "field": c.field,
                "operator": c.operator,
                "value": c.value,
                "valueType": "raw" # Assume raw for legacy
            }, variables)
            
        table_ref = quote_table_ref(input_table)
        if not where_clause:
            return f"SELECT * FROM {table_ref}"
        return f"SELECT * FROM {table_ref} WHERE {where_clause}"
        
    elif cmd.type == 'join':
        join_type = (c.joinType or 'LEFT').upper()
        target = c.joinTable or "other_table"
        if c.joinTargetType == 'node':
            return f"-- SQL generation not supported for dynamic Node joins (Node ID: {c.joinTargetNodeId})"
            
        on = c.on or "1=1"
        on = _substitute_variables(on, variables)
        
        suffix = c.joinSuffix or "_joined"
        
        table_ref = quote_table_ref(input_table)
        target_ref = quote_identifier(target)
        return f"SELECT t1.*, t2.* FROM {table_ref} t1 {join_type} JOIN {target_ref} t2 ON {on}"
        
    elif cmd.type == 'group' or cmd.type == 'aggregate':
        dims = c.groupByFields or (c.groupBy if c.groupBy else [])
        aggs = c.aggregations or []
        
        select_parts = [quote_identifier(d) for d in dims]
        for agg in aggs:
            func = (agg.get('func') or 'count').upper()
            field = agg.get('field') or '*'
            alias = agg.get('alias') or f"{func}_{field}"
            field_sql = "*" if field == "*" else quote_identifier(field)
            select_parts.append(f"{func}({field_sql}) AS {quote_identifier(alias)}")
            
        if not select_parts:
            select_parts = ["*"]
            
        group_by_clause = f"GROUP BY {', '.join([quote_identifier(d) for d in dims])}" if dims else ""
        
        having_parts = []
        for h in (c.havingConditions or []):
            metric = h.get('metricAlias')
            op = h.get('operator')
            val = h.get('value')
            val = _resolve_value(val, variables)
            metric_sql = quote_identifier(metric) if metric else metric
            having_parts.append(f"{metric_sql} {op} {val}")
            
        having_clause = f"HAVING {' AND '.join(having_parts)}" if having_parts else ""
        
        table_ref = quote_table_ref(input_table)
        return f"SELECT {', '.join(select_parts)} FROM {table_ref} {group_by_clause} {having_clause}"
        
    elif cmd.type == 'sort':
        field = c.field
        table_ref = quote_table_ref(input_table)
        if not field: return f"SELECT * FROM {table_ref}"
        direction = "ASC" if c.ascending is not False else "DESC"
        return f"SELECT * FROM {table_ref} ORDER BY {quote_identifier(field)} {direction}"
        
    elif cmd.type == 'transform':
        selects = ["*"]
        for m in (c.mappings or []):
            if m.mode == 'python':
                return "-- SQL generation not supported for Python transformations"
            if m.expression and m.outputField:
                expr = _substitute_variables(m.expression, variables)
                selects.append(f"{expr} AS {m.outputField}")
        table_ref = quote_table_ref(input_table)
        return f"SELECT {', '.join(selects)} FROM {table_ref}"
        
    elif cmd.type == 'save':
        field = c.field
        if not field: return f"-- Invalid Save Command"
        distinct = "DISTINCT " if c.distinct else ""
        table_ref = quote_table_ref(input_table)
        return f"SELECT {distinct}{quote_identifier(field)} FROM {table_ref}"

    elif cmd.type == 'source':
        table = c.mainTable or input_table
        alias = (c.alias or "").strip() if hasattr(c, 'alias') else ""
        table_ref = quote_identifier(table)
        if alias:
            return f"SELECT * FROM {table_ref} AS {quote_identifier(alias)}"
        return f"SELECT * FROM {table_ref}"

    elif cmd.type == 'view':
        view_fields = c.viewFields or []
        fields = [vf.field for vf in view_fields if getattr(vf, 'field', None)]
        distinct_fields = [vf.field for vf in view_fields if getattr(vf, 'field', None) and getattr(vf, 'distinct', False)]

        if distinct_fields:
            select_fields = distinct_fields
            distinct = "DISTINCT "
        else:
            select_fields = fields
            distinct = ""

        if select_fields:
            seen = set()
            deduped = []
            for f in select_fields:
                if f in seen:
                    continue
                seen.add(f)
                deduped.append(f)
            select_fields = deduped

        if not select_fields:
            select_fields = ["*"]

        table_ref = quote_table_ref(input_table)
        quoted_fields = ["*" if f == "*" else quote_identifier(f) for f in select_fields]
        base_sql = f"SELECT {distinct}{', '.join(quoted_fields)} FROM {table_ref}"

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
            base_sql = f"{base_sql} ORDER BY {', '.join(sort_parts)}"

        limit = c.viewLimit
        if isinstance(limit, int) and limit > 0:
            base_sql = f"{base_sql} LIMIT {limit}"

        return base_sql
        
    return f"-- SQL generation not supported for {cmd.type}"

def _build_group_condition(group: Dict[str, Any], variables: Dict[str, Any]) -> str:
    conditions = group.get('conditions', [])
    if not conditions:
        return ""
        
    parts = []
    for item in conditions:
        if item.get('type') == 'group':
            sub = _build_group_condition(item, variables)
            if sub: parts.append(f"({sub})")
        else:
            sub = _build_single_condition(item, variables)
            if sub: parts.append(sub)
            
    if not parts: return ""
    
    op = f" {group.get('logicalOperator', 'AND')} "
    return op.join(parts)

def _build_single_condition(cond: Dict[str, Any], variables: Dict[str, Any]) -> str:
    field = cond.get('field')
    op = cond.get('operator')
    val = cond.get('value')
    val_type = cond.get('valueType')
    
    if op == 'always_true': return ""
    if op == 'always_false': return "1=0"
    if not field: return ""
    field_sql = quote_identifier(field)
    
    # Resolve value
    resolved_val = val
    if val_type == 'variable':
        var_name = str(val)
        if var_name in variables:
            resolved_val = variables[var_name]
    elif isinstance(val, str) and val.startswith('{') and val.endswith('}'):
        # Try to resolve {var} syntax even if not explicitly marked as variable
        var_name = val[1:-1]
        if var_name in variables:
            resolved_val = variables[var_name]
            
    sql_val = _format_sql_value(resolved_val)
    
    if op == '=': return f"{field_sql} = {sql_val}"
    if op == '!=': return f"{field_sql} != {sql_val}"
    if op == '>': return f"{field_sql} > {sql_val}"
    if op == '>=': return f"{field_sql} >= {sql_val}"
    if op == '<': return f"{field_sql} < {sql_val}"
    if op == '<=': return f"{field_sql} <= {sql_val}"
    
    if op == 'contains': return f"{field_sql} LIKE '%{str(resolved_val)}%'"
    if op == 'not_contains': return f"{field_sql} NOT LIKE '%{str(resolved_val)}%'"
    if op == 'starts_with': return f"{field_sql} LIKE '{str(resolved_val)}%'"
    if op == 'ends_with': return f"{field_sql} LIKE '%{str(resolved_val)}'"
    
    if op == 'in_list' or op == 'in_variable':
        if isinstance(resolved_val, list):
            vals = ", ".join([_format_sql_value_inner(x) for x in resolved_val])
            return f"{field_sql} IN ({vals})"
        return f"{field_sql} IN ({sql_val})"
        
    if op == 'not_in_list' or op == 'not_in_variable':
        if isinstance(resolved_val, list):
            vals = ", ".join([_format_sql_value_inner(x) for x in resolved_val])
            return f"{field_sql} NOT IN ({vals})"
        return f"{field_sql} NOT IN ({sql_val})"
        
    if op == 'is_null': return f"({field_sql} IS NULL)"
    if op == 'is_not_null': return f"({field_sql} IS NOT NULL)"
    if op == 'is_empty': return f"({field_sql} = '')"
    if op == 'is_not_empty': return f"({field_sql} != '')"
    
    return f"{field_sql} {op} {sql_val}"

def _substitute_variables(text: str, variables: Dict[str, Any]) -> str:
    if not text: return ""
    # Create a copy to avoid modifying while iterating if we were doing that
    result = text
    for k, v in variables.items():
        # Determine how to format the value based on its type
        if isinstance(v, (list, tuple)):
            # If it's a list, format as (v1, v2, v3)
            val_str = f"({', '.join([_format_sql_value_inner(x) for x in v])})"
        else:
            val_str = _format_sql_value_inner(v)
            
        # Replace {var} with value
        result = result.replace(f"{{{k}}}", str(val_str))
    return result

def _resolve_value(val: Any, variables: Dict[str, Any]) -> Any:
    if isinstance(val, str) and val.startswith('{') and val.endswith('}'):
        var_name = val[1:-1]
        return variables.get(var_name, val)
    return val

def _format_sql_value(val: Any) -> str:
    if isinstance(val, (list, tuple)):
        return f"({', '.join([_format_sql_value_inner(x) for x in val])})"
    return _format_sql_value_inner(val)

def _format_sql_value_inner(val: Any) -> str:
    if val is None: return "NULL"
    if isinstance(val, (int, float)): return str(val)
    if isinstance(val, str): return f"'{val}'" 
    return str(val)
