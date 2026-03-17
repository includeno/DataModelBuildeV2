
from pydantic import BaseModel, Field
try:
    from pydantic import model_validator
except ImportError:  # pragma: no cover - pydantic v1 fallback
    model_validator = None
    from pydantic import root_validator
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
    onConditionGroup: Optional[Dict[str, Any]] = None
    conditionGroup: Optional[Dict[str, Any]] = None

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
    note: Optional[str] = None

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


class ExecutionContextRequest(BaseModel):
    session_id: Optional[str] = Field(None, alias="sessionId")
    project_id: Optional[str] = Field(None, alias="projectId")

    if model_validator is not None:
        @model_validator(mode="before")
        @classmethod
        def _require_context_id(cls, values):
            if values.get("sessionId") or values.get("projectId"):
                return values
            raise ValueError("sessionId or projectId is required")
    else:  # pragma: no cover - pydantic v1 fallback
        @root_validator(pre=True)
        def _require_context_id(cls, values):
            if values.get("sessionId") or values.get("projectId"):
                return values
            raise ValueError("sessionId or projectId is required")

    @property
    def context_id(self) -> str:
        return str(self.project_id or self.session_id or "")


class ExecuteRequest(ExecutionContextRequest):
    tree: Optional[OperationNode] = None
    targetNodeId: str
    targetCommandId: Optional[str] = None
    includeCommandMeta: bool = False
    page: int = 1
    pageSize: int = 50
    viewId: str = "main" # 'main' or specific subTable ID

class ExecuteSqlRequest(ExecutionContextRequest):
    query: str
    page: int = 1
    pageSize: int = 50

class AnalyzeRequest(ExecutionContextRequest):
    tree: Optional[OperationNode] = None
    parentNodeId: str


class RegisterRequest(BaseModel):
    email: str
    password: str
    displayName: Optional[str] = ""


class LoginRequest(BaseModel):
    email: str
    password: str


class CreateProjectRequest(BaseModel):
    name: str
    description: Optional[str] = ""
    orgId: Optional[str] = None


class AddProjectMemberRequest(BaseModel):
    memberEmail: str
    role: str = "viewer"


class UpdateProjectMemberRequest(BaseModel):
    role: str


class CreateOrganizationRequest(BaseModel):
    name: str


class AddOrganizationMemberRequest(BaseModel):
    memberEmail: str
    role: str = "member"


class UpdateOrganizationMemberRequest(BaseModel):
    role: str


class CommitPatch(BaseModel):
    op: str
    state: Optional[Dict[str, Any]] = None
    key: Optional[str] = None
    value: Any = None


class CommitProjectStateRequest(BaseModel):
    baseVersion: int
    state: Optional[Dict[str, Any]] = None
    clientOpId: Optional[str] = None
    patches: Optional[List[CommitPatch]] = None


class RefreshTokenRequest(BaseModel):
    refreshToken: str
