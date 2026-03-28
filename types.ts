

export type CommandType = 'filter' | 'join' | 'transform' | 'group' | 'sort' | 'pivot' | 'export' | 'source' | 'custom' | 'save' | 'multi_table' | 'view' | 'define_variable' | 'validate';

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
  value: string | number | string[] | null;
  valueType?: 'raw' | 'variable';
  dataType?: DataType;
}

export interface FilterGroup {
  id: string;
  type: 'group';
  logicalOperator: 'AND' | 'OR';
  conditions: (FilterCondition | FilterGroup)[];
}

export interface SubTableLinkCondition {
  id: string;
  type: 'condition';
  field: string;
  operator: string;
  mainField: string;
}

export interface SubTableConditionGroup {
  id: string;
  type: 'group';
  logicalOperator: 'AND' | 'OR';
  conditions: (SubTableLinkCondition | SubTableConditionGroup)[];
}

export interface SubTableConfig {
  id: string;
  table: string;
  on: string; // Join condition e.g. main.id = sub.user_id
  label: string;
  // ON multiple conditions builder (preferred)
  onConditionGroup?: SubTableConditionGroup;
  // Backward compatibility for older saved sessions
  conditionGroup?: SubTableConditionGroup;
}

export interface ViewFieldConfig {
  field: string;
  distinct?: boolean;
}

export interface ViewSortConfig {
  field: string;
  ascending?: boolean;
}

export interface ValidationRule {
  id: string;
  field: string;
  rule: 'not_null' | 'unique' | 'range' | 'regex' | 'enum' | 'type_check';
  min?: number;
  max?: number;
  pattern?: string;
  enumValues?: string[];
  expectedType?: DataType;
  message?: string;
}

export interface CommandConfig {
  dataSource?: string; 

  // Recursive Filter Root
  filterRoot?: FilterGroup;

  // Legacy/Single-field support
  field?: string;
  operator?: string;
  value?: string | number | string[] | null;
  
  distinct?: boolean; 
  mainTable?: string;
  alias?: string; // Added for Source commands to define output alias

  // Variable Definition (Setup Node)
  variableName?: string;
  variableType?: 'text' | 'list';
  variableValue?: string | string[];
  // Shared note for setup source / define_variable commands
  note?: string;

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

  // View Command
  viewFields?: ViewFieldConfig[];
  viewSortField?: string;
  viewSortAscending?: boolean;
  viewSorts?: ViewSortConfig[];
  viewLimit?: number;

  groupBy?: string[];
  aggFunc?: string;

  dataType?: DataType;

  // Validate Command
  validationRules?: ValidationRule[];
  validationMode?: 'fail' | 'warn' | 'flag';

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

export interface ImportHistoryItem {
  timestamp: number;
  originalFileName: string;
  datasetName: string;
  tableName: string;
  rows: number;
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

export type ProjectRole = 'owner' | 'admin' | 'editor' | 'viewer';

export interface ProjectMetadata {
  id: string;
  orgId?: string;
  name: string;
  description?: string;
  role: ProjectRole;
  archived?: boolean;
  createdAt: number;
  updatedAt: number;
}

export interface ProjectMember {
  userId: string;
  email: string;
  displayName?: string;
  role: ProjectRole;
  createdAt: number;
  updatedAt: number;
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

export interface ProjectMetadataDetail {
  displayName: string;
  settings: SessionConfig;
}

export interface SessionDiagnosticsReport {
  sessionId?: string;
  projectId?: string;
  generatedAt: string;
  sources: Array<{ id: string; mainTable?: string; alias?: string; linkId?: string; note?: string }>;
  sourceMap: Array<{ identifier: string; table: string }>;
  datasets: Array<{ id?: string; name?: string; totalCount?: number; fieldCount?: number }>;
  operations: Array<{ id: string; name?: string; operationType?: string; commands: Array<{ id: string; type: string; order: number; dataSource?: string | null }> }>;
  dataSourceResolution: Array<{ commandId: string; dataSource: string; resolved: string; status: 'ok' | 'missing' }>;
  warnings: string[];
}

// ── Import-time cleaning types ──────────────────────────────────────────

export interface ImportFillRule {
  field: string; // specific field name or '*number' / '*string' / '*date'
  strategy: 'mean' | 'median' | 'mode' | 'constant' | 'forward' | 'drop_row';
  constantValue?: string;
}

export interface DedupConfig {
  enabled: boolean;
  fields: string[] | 'all';
  keep: 'first' | 'last';
}

export interface FillMissingConfig {
  enabled: boolean;
  rules: ImportFillRule[];
}

export interface OutlierConfig {
  enabled: boolean;
  method: 'zscore' | 'iqr';
  threshold: number;
  action: 'remove' | 'flag';
  targetFields: string[] | 'numeric';
}

export interface TrimWhitespaceConfig {
  enabled: boolean;
  fields: string[] | 'string';
}

export interface ImportCleanConfig {
  dedup: DedupConfig;
  fillMissing: FillMissingConfig;
  outlier: OutlierConfig;
  trimWhitespace: TrimWhitespaceConfig;
}

export interface CleanPreviewReport {
  duplicateRowCount: number;
  missingValueCounts: Record<string, number>;
  outlierCounts: Record<string, number>;
  whitespaceFieldCount: number;
}

export interface CleanReport {
  dedupRemoved: number;
  fillApplied: Record<string, number>;
  outlierFlagged: Record<string, number>;
  outlierRemoved: number;
  trimApplied: number;
  originalRowCount: number;
  finalRowCount: number;
}

export interface LineageStep {
  nodeId: string;
  commandId: string;
  commandType: string;
  expression?: string | null;
}

export interface FieldLineage {
  fieldName: string;
  originTable: string;
  originField: string;
  transformations: LineageStep[];
}

export type LineageMap = Record<string, FieldLineage>;

export interface ValidationReport {
  passed: boolean;
  totalChecks: number;
  failedChecks: number;
  details: { ruleId: string; field: string; failedRowCount: number; sampleValues: any[] }[];
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
  validationReport?: ValidationReport;
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
  showNodeIds?: boolean;
  showOperationIds?: boolean;
  showCommandIds?: boolean;
  showDatasetIds?: boolean;
}

export interface ProjectSnapshot {
  tree: OperationNode;
  datasets: Dataset[];
  sqlHistory: SqlHistoryItem[];
  metadata: ProjectMetadataDetail;
}

export interface ProjectStateEnvelope {
  projectId: string;
  version: number;
  state: Partial<ProjectSnapshot>;
  updatedBy?: string;
  updatedAt?: number;
}

export interface ProjectConflictInfo {
  latestVersion: number;
  remoteState: ProjectSnapshot;
  pendingPatchesCount: number;
  message: string;
}

export type ProjectSaveStatus = 'idle' | 'dirty' | 'saving' | 'saved' | 'conflict' | 'error';

export interface ProjectJobError {
  code?: string | null;
  category?: string | null;
  message: string;
}

export interface ProjectJob {
  id: string;
  projectId: string;
  type: 'execute' | 'export' | string;
  status: 'queued' | 'running' | 'completed' | 'failed' | 'canceled' | string;
  progress: number;
  payload?: Record<string, any>;
  result?: Record<string, any> | null;
  error?: ProjectJobError | null;
  cancelRequested?: boolean;
  createdBy?: string;
  createdAt: number;
  updatedAt: number;
  startedAt?: number | null;
  finishedAt?: number | null;
  downloadUrl?: string;
}
