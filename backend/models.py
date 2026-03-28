
from pydantic import BaseModel, Field
try:
    from pydantic import ConfigDict, model_validator
except ImportError:  # pragma: no cover - pydantic v1 fallback
    ConfigDict = None
    model_validator = None
    from pydantic import root_validator
from typing import List, Any, Optional, Dict, Union


class StrictRequestModel(BaseModel):
    if ConfigDict is not None:
        model_config = ConfigDict(extra="forbid")
    else:  # pragma: no cover - pydantic v1 fallback
        class Config:
            extra = "forbid"

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

class ValidationRule(BaseModel):
    id: str
    field: str
    rule: str  # 'not_null' | 'unique' | 'range' | 'regex' | 'enum' | 'type_check'
    min: Optional[float] = None
    max: Optional[float] = None
    pattern: Optional[str] = None
    enumValues: Optional[List[str]] = None
    expectedType: Optional[str] = None
    message: Optional[str] = None

class ValidationReportDetail(BaseModel):
    ruleId: str
    field: str
    failedRowCount: int
    sampleValues: List[Any] = []

class ValidationReport(BaseModel):
    passed: bool
    totalChecks: int
    failedChecks: int
    details: List[ValidationReportDetail] = []

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

    # Validate command
    validationRules: Optional[List[ValidationRule]] = None
    validationMode: Optional[str] = "warn"  # 'fail' | 'warn' | 'flag'

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


# ── Import-time cleaning models ──────────────────────────────────────────

class DedupConfig(BaseModel):
    enabled: bool = True
    fields: Union[List[str], str] = "all"  # list of field names or "all"
    keep: str = "first"  # "first" | "last"

class ImportFillRule(BaseModel):
    field: str  # specific field name or "*number" / "*string" / "*date"
    strategy: str  # "mean" | "median" | "mode" | "constant" | "forward" | "drop_row"
    constantValue: Optional[str] = None

class FillMissingConfig(BaseModel):
    enabled: bool = True
    rules: List[ImportFillRule] = [
        ImportFillRule(field="*number", strategy="median"),
        ImportFillRule(field="*string", strategy="constant", constantValue=""),
        ImportFillRule(field="*date", strategy="drop_row"),
    ]

class OutlierConfig(BaseModel):
    enabled: bool = False
    method: str = "iqr"  # "iqr" | "zscore"
    threshold: float = 1.5
    action: str = "flag"  # "flag" | "remove"
    targetFields: Union[List[str], str] = "numeric"  # list of field names or "numeric"

class TrimWhitespaceConfig(BaseModel):
    enabled: bool = True
    fields: Union[List[str], str] = "string"  # list of field names or "string"

class ImportCleanConfig(BaseModel):
    dedup: DedupConfig = DedupConfig()
    fillMissing: FillMissingConfig = FillMissingConfig()
    outlier: OutlierConfig = OutlierConfig()
    trimWhitespace: TrimWhitespaceConfig = TrimWhitespaceConfig()


class ExecutionContextRequest(StrictRequestModel):
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


class RegisterRequest(StrictRequestModel):
    email: str
    password: str
    displayName: Optional[str] = ""


class LoginRequest(StrictRequestModel):
    email: str
    password: str


class CreateProjectRequest(StrictRequestModel):
    name: str
    description: Optional[str] = ""
    orgId: Optional[str] = None


class AddProjectMemberRequest(StrictRequestModel):
    memberEmail: str
    role: str = "viewer"


class UpdateProjectMemberRequest(StrictRequestModel):
    role: str


class CreateOrganizationRequest(StrictRequestModel):
    name: str


class AddOrganizationMemberRequest(StrictRequestModel):
    memberEmail: str
    role: str = "member"


class UpdateOrganizationMemberRequest(StrictRequestModel):
    role: str


class CommitPatch(StrictRequestModel):
    op: str
    state: Optional[Dict[str, Any]] = None
    key: Optional[str] = None
    value: Any = None


class CommitProjectStateRequest(StrictRequestModel):
    baseVersion: int
    state: Optional[Dict[str, Any]] = None
    clientOpId: Optional[str] = None
    patches: Optional[List[CommitPatch]] = None


class RefreshTokenRequest(StrictRequestModel):
    refreshToken: str
