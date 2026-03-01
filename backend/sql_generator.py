
from typing import Dict, Any, List, Union
from models import Command, CommandConfig

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
            
        if not where_clause:
            where_clause = "1=1"
            
        return f"SELECT * FROM {input_table} WHERE {where_clause}"
        
    elif cmd.type == 'join':
        join_type = (c.joinType or 'LEFT').upper()
        target = c.joinTable or "other_table"
        if c.joinTargetType == 'node':
            return f"-- SQL generation not supported for dynamic Node joins (Node ID: {c.joinTargetNodeId})"
            
        on = c.on or "1=1"
        on = _substitute_variables(on, variables)
        
        suffix = c.joinSuffix or "_joined"
        
        return f"SELECT t1.*, t2.* FROM {input_table} t1 {join_type} JOIN {target} t2 ON {on}"
        
    elif cmd.type == 'group':
        dims = c.groupByFields or (c.groupBy if c.groupBy else [])
        aggs = c.aggregations or []
        
        select_parts = list(dims)
        for agg in aggs:
            func = (agg.get('func') or 'count').upper()
            field = agg.get('field') or '*'
            alias = agg.get('alias') or f"{func}_{field}"
            select_parts.append(f"{func}({field}) AS {alias}")
            
        if not select_parts:
            select_parts = ["*"]
            
        group_by_clause = f"GROUP BY {', '.join(dims)}" if dims else ""
        
        having_parts = []
        for h in (c.havingConditions or []):
            metric = h.get('metricAlias')
            op = h.get('operator')
            val = h.get('value')
            val = _resolve_value(val, variables)
            having_parts.append(f"{metric} {op} {val}")
            
        having_clause = f"HAVING {' AND '.join(having_parts)}" if having_parts else ""
        
        return f"SELECT {', '.join(select_parts)} FROM {input_table} {group_by_clause} {having_clause}"
        
    elif cmd.type == 'sort':
        field = c.field
        if not field: return f"SELECT * FROM {input_table}"
        direction = "ASC" if c.ascending is not False else "DESC"
        return f"SELECT * FROM {input_table} ORDER BY {field} {direction}"
        
    elif cmd.type == 'transform':
        selects = ["*"]
        for m in (c.mappings or []):
            if m.mode == 'python':
                return "-- SQL generation not supported for Python transformations"
            if m.expression and m.outputField:
                expr = _substitute_variables(m.expression, variables)
                selects.append(f"{expr} AS {m.outputField}")
        return f"SELECT {', '.join(selects)} FROM {input_table}"
        
    elif cmd.type == 'save':
        field = c.field
        if not field: return f"-- Invalid Save Command"
        distinct = "DISTINCT " if c.distinct else ""
        return f"SELECT {distinct}{field} FROM {input_table}"

    elif cmd.type == 'source':
        return f"SELECT * FROM {c.mainTable}"
        
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
    
    if not field: return ""
    
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
    
    if op == '=': return f"{field} = {sql_val}"
    if op == '!=': return f"{field} != {sql_val}"
    if op == '>': return f"{field} > {sql_val}"
    if op == '>=': return f"{field} >= {sql_val}"
    if op == '<': return f"{field} < {sql_val}"
    if op == '<=': return f"{field} <= {sql_val}"
    
    if op == 'contains': return f"{field} LIKE '%{str(resolved_val)}%'"
    if op == 'not_contains': return f"{field} NOT LIKE '%{str(resolved_val)}%'"
    if op == 'starts_with': return f"{field} LIKE '{str(resolved_val)}%'"
    if op == 'ends_with': return f"{field} LIKE '%{str(resolved_val)}'"
    
    if op == 'in_list' or op == 'in_variable':
        if isinstance(resolved_val, list):
            vals = ", ".join([_format_sql_value_inner(x) for x in resolved_val])
            return f"{field} IN ({vals})"
        return f"{field} IN ({sql_val})"
        
    if op == 'not_in_list' or op == 'not_in_variable':
        if isinstance(resolved_val, list):
            vals = ", ".join([_format_sql_value_inner(x) for x in resolved_val])
            return f"{field} NOT IN ({vals})"
        return f"{field} NOT IN ({sql_val})"
        
    if op == 'is_empty': return f"({field} IS NULL OR {field} = '')"
    if op == 'is_not_empty': return f"({field} IS NOT NULL AND {field} != '')"
    
    return f"{field} {op} {sql_val}"

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
