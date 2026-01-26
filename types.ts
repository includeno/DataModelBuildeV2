export type CommandType = 'filter' | 'join' | 'transform' | 'aggregate' | 'sort' | 'pivot' | 'export' | 'custom';

export interface CommandConfig {
  field?: string;
  operator?: string;
  value?: string | number | string[];
  mainTable?: string;
  joinTable?: string;
  joinType?: 'INNER' | 'LEFT' | 'RIGHT' | 'FULL';
  expression?: string;
  outputField?: string;
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
  name: string;
  enabled: boolean;
  commands: Command[];
  children?: OperationNode[];
}

export interface Dataset {
  id: string;
  name: string;
  rows: any[];
  fields: string[];
}

export interface SessionMetadata {
  createdAt: string;
  sessionId: string;
}

export interface ExecutionResult {
  rows: any[];
  totalCount: number;
}