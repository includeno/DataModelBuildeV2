

export type CommandType = 'filter' | 'join' | 'transform' | 'group' | 'sort' | 'pivot' | 'export' | 'source' | 'custom' | 'save' | 'multi_table' | 'view' | 'define_variable';

export type OperationType = 'dataset' | 'process' | 'setup' | 'root';

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

export interface SubTableConfig {
  id: string;
  table: string;
  on: string; // Join condition e.g. main.id = sub.user_id
  label: string;
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
  alias?: string; // Added for Source commands to define output alias

  // Variable Definition (Setup Node)
  variableName?: string;
  variableType?: 'text' | 'list';
  variableValue?: string | string[];

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

  // Multi Table Display
  subTables?: SubTableConfig[];

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

export interface SqlHistoryItem {
  id: string;
  timestamp: number;
  query: string;
  status: 'success' | 'error';
  durationMs?: number;
  rowCount?: number;
  errorMessage?: string;
}

export interface SessionState {
  tree: OperationNode;
  datasets: Dataset[];
  sqlHistory?: SqlHistoryItem[];
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
  // If result is from a multi_table command
  isMultiTable?: boolean;
  activeViewId?: string; // 'main' or subTableId
}

export interface ApiConfig {
  baseUrl: string;
  isMock: boolean;
}

export interface AppearanceConfig {
  textSize: number;
  textColor: string;
  guideLineColor: string;
  showGuideLines: boolean;
}