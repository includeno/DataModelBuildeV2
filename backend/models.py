
from pydantic import BaseModel, Field
from typing import List, Any, Optional, Dict, Union

class FieldInfo(BaseModel):
    type: str
    format: Optional[str] = None

class MappingRule(BaseModel):
    # id is optional for backward compatibility with older payloads/tests
    id: Optional[str] = None
    expression: str
    outputField: str
    mode: Optional[str] = None

class SubTableConfig(BaseModel):
    id: str
    table: str
    on: str
    label: str

class ViewFieldConfig(BaseModel):
    field: str
    distinct: Optional[bool] = False

class ViewSortConfig(BaseModel):
    field: str
    ascending: Optional[bool] = True

class CommandConfig(BaseModel):
    # New: Context selection
    dataSource: Optional[str] = "stream" # 'stream' or table_name

    field: Optional[str] = None
    operator: Optional[str] = None
    value: Any = None
    
    # Variable extraction
    distinct: Optional[bool] = True
    
    # Source configs
    mainTable: Optional[str] = None
    alias: Optional[str] = None
    linkId: Optional[str] = None

    # Variable Definition
    variableName: Optional[str] = None
    variableType: Optional[str] = None # 'text' or 'list'
    variableValue: Optional[Union[str, List[str]]] = None

    # Join configs
    joinTargetType: Optional[str] = "table" # 'table' or 'node'
    joinTable: Optional[str] = None
    joinTargetNodeId: Optional[str] = None
    joinType: Optional[str] = None
    on: Optional[str] = None
    joinSuffix: Optional[str] = "_joined" 
    
    # Sort/Transform configs
    ascending: Optional[bool] = True
    fields: Optional[List[str]] = None
    
    # Aggregation configs
    groupBy: Optional[List[str]] = None
    aggFunc: Optional[str] = None # sum, mean, count
    
    # Transform
    outputField: Optional[str] = None
    expression: Optional[str] = None
    mappings: Optional[List[MappingRule]] = None
    
    dataType: Optional[str] = None
    
    # New Aggregation Structure
    groupByFields: Optional[List[str]] = None
    aggregations: Optional[List[Dict[str, str]]] = None
    havingConditions: Optional[List[Dict[str, Any]]] = None
    outputTableName: Optional[str] = None
    
    # Recursive Filter Root
    filterRoot: Optional[Dict[str, Any]] = None

    # Multi Table
    subTables: Optional[List[SubTableConfig]] = None
    # View command
    viewFields: Optional[List[ViewFieldConfig]] = None
    viewSortField: Optional[str] = None
    viewSortAscending: Optional[bool] = True
    viewSorts: Optional[List[ViewSortConfig]] = None
    viewLimit: Optional[int] = None

class Command(BaseModel):
    id: str
    type: str
    config: CommandConfig
    # default to 0 so missing order doesn't break validation or sorting
    order: int = 0

class OperationNode(BaseModel):
    id: str
    type: str
    operationType: Optional[str] = "process"
    name: str
    enabled: bool
    commands: List[Command]
    children: Optional[List['OperationNode']] = None

try:
    OperationNode.model_rebuild()
except AttributeError:
    OperationNode.update_forward_refs()

class ExecuteRequest(BaseModel):
    session_id: str = Field(..., alias="sessionId")
    tree: OperationNode
    targetNodeId: str
    targetCommandId: Optional[str] = None
    includeCommandMeta: bool = False
    page: int = 1
    pageSize: int = 50
    viewId: str = "main" # 'main' or specific subTable ID

class ExecuteSqlRequest(BaseModel):
    session_id: str = Field(..., alias="sessionId")
    query: str
    page: int = 1
    pageSize: int = 50

class AnalyzeRequest(BaseModel):
    session_id: str = Field(..., alias="sessionId")
    tree: OperationNode
    parentNodeId: str
