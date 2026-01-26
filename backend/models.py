from pydantic import BaseModel
from typing import List, Any, Optional, Dict, Union

class CommandConfig(BaseModel):
    field: Optional[str] = None
    operator: Optional[str] = None
    value: Any = None
    
    # Join configs
    joinTable: Optional[str] = None
    joinType: Optional[str] = None
    on: Optional[str] = None
    
    # Sort/Transform configs
    ascending: Optional[bool] = True
    fields: Optional[List[str]] = None
    
    # Aggregation configs
    groupBy: Optional[List[str]] = None
    aggFunc: Optional[str] = None # sum, mean, count
    
    dataType: Optional[str] = None

class Command(BaseModel):
    id: str
    type: str
    config: CommandConfig
    order: int

class OperationNode(BaseModel):
    id: str
    type: str
    name: str
    enabled: bool
    commands: List[Command]
    children: Optional[List['OperationNode']] = None

OperationNode.update_forward_refs()

class ExecuteRequest(BaseModel):
    tree: OperationNode
    targetNodeId: str
