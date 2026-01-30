
export type CommandType = 'filter' | 'join' | 'transform' | 'group' | 'sort' | 'pivot' | 'export' | 'source' | 'custom' | 'save';

export type OperationType = 'dataset' | 'process';

export type DataType = 'string' | 'number' | 'boolean' | 'date' | 'timestamp' | 'json';

export interface FieldInfo {
  type: DataType;
  format?: string;
}

export interface AggregationConfig {
  field: string;
  func: 'sum' | 'mean' | 'count' | 'min' | 'max' | 'first' | 'last';
  alias: string;
}

export interface HavingCondition {
  id: string;
  metricAlias: string;
  operator: string;
  value: string | number;
}

export interface MappingRule {
  id: string;
  mode: 'simple' | 'python';
  expression: string;
  outputField: string;
}

// New recursive filter structures
export interface FilterCondition {
  id: string;
  type: 'condition';
  field: string;
  operator: string;
  value: string | number | string[];
  dataType?: DataType;
}

export interface FilterGroup {
  id: string;
  type: 'group';
  logicalOperator: 'AND' | 'OR';
  conditions: (FilterCondition | FilterGroup)[];
}

export interface CommandConfig {
  dataSource?: string; 

  // Recursive Filter Root
  filterRoot?: FilterGroup;

  // Legacy/Single-field support
  field?: string;
  operator?: string;
  value?: string | number | string[];
  
  distinct?: boolean; 
  mainTable?: string;

  // Join configs
  joinTargetType?: 'table' | 'node'; 
  joinTable?: string; 
  joinTargetNodeId?: string; 
  joinType?: 'INNER' | 'LEFT' | 'RIGHT' | 'FULL';
  on?: string; 
  joinSuffix?: string; 

  expression?: string;
  outputField?: string;
  mappings?: MappingRule[];
  ascending?: boolean;
  
  groupByFields?: string[];
  aggregations?: AggregationConfig[];
  havingConditions?: HavingCondition[];
  outputTableName?: string;

  groupBy?: string[];
  aggFunc?: string;
  
  dataType?: DataType; 
  [key: string]: any;
}

export interface Command {
  id: string;
  type: CommandType;
  config: CommandConfig;
  order: number;
}

export interface OperationNode {
  id: string;
  type: 'operation';
  operationType: OperationType; 
  name: string;
  enabled: boolean;
  commands: Command[];
  children?: OperationNode[];
  pageSize?: number; 
}

export interface Dataset {
  id: string;
  name: string;
  rows: any[];
  fields: string[];
  fieldTypes?: Record<string, FieldInfo>; 
  totalCount?: number;
}

export interface SessionMetadata {
  createdAt: number; 
  sessionId: string;
  displayName?: string; 
}

export interface SessionConfig {
  cascadeDisable: boolean;
  panelPosition?: 'right' | 'left' | 'bottom' | 'top';
  [key: string]: any;
}

export interface SessionMetadataDetail {
    displayName: string;
    settings: SessionConfig;
}

export interface ExecutionResult {
  rows: any[];
  totalCount: number;
  columns?: string[];
  page: number;
  pageSize: number;
}

export interface ApiConfig {
  baseUrl: string;
  isMock: boolean;
}
