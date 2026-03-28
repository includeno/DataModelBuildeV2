# DataFlow Engine 功能演进路线图 v2

> 编写日期：2026-03-24
> 基于当前项目状态的功能建议与优先级规划

---

## 一、数据处理能力增强

### 1.1 导入时数据清洗

**目标**：在数据导入阶段（`DataImportModal`）集成数据清洗配置，上传文件后、存入 DuckDB 前自动完成去重、缺失值填充、异常值处理。用户可调整配置，也可直接使用默认配置一键导入。

**当前导入流程**（需改造的环节）：
1. 用户在 `DataImportModal`（`components/DataImport.tsx`）选择文件
2. 前端调用 `api.upload()` → 后端 `POST /projects/{id}/upload`
3. 后端 `_read_uploaded_dataframe()`（`main.py:2373`）解析文件为 DataFrame
4. 后端列名清洗（`main.py:1555`，仅 strip + 空格转下划线）
5. 后端 `_build_dataframe_schema()`（`main.py:504`）推断类型
6. 存入 DuckDB（`storage.add_dataset()`，`storage.py:576`）

**改造方案**：在步骤 4 和 5 之间插入清洗流程，清洗配置随上传请求一起提交。

#### 前端任务

- **T1.1.1** 在 `types.ts` 中新增导入清洗配置接口：
  ```typescript
  interface ImportCleanConfig {
    // 去重
    dedup: {
      enabled: boolean;         // 默认 true
      fields: string[] | 'all'; // 默认 'all'（全字段去重）
      keep: 'first' | 'last';  // 默认 'first'
    };
    // 缺失值处理
    fillMissing: {
      enabled: boolean;         // 默认 true
      rules: ImportFillRule[];  // 默认按类型自动生成
    };
    // 异常值处理
    outlier: {
      enabled: boolean;         // 默认 false（需用户主动开启）
      method: 'zscore' | 'iqr'; // 默认 'iqr'
      threshold: number;        // 默认 1.5（IQR 模式）
      action: 'remove' | 'flag'; // 默认 'flag'
      targetFields: string[] | 'numeric'; // 默认 'numeric'（所有数值列）
    };
    // 空白值处理
    trimWhitespace: {
      enabled: boolean;         // 默认 true
      fields: string[] | 'string'; // 默认 'string'（所有字符串列）
    };
  }

  interface ImportFillRule {
    field: string | '*number' | '*string' | '*date'; // 具体字段名或类型通配
    strategy: 'mean' | 'median' | 'mode' | 'constant' | 'forward' | 'drop_row';
    constantValue?: string;
  }
  ```

- **T1.1.2** 定义默认清洗配置工厂函数 `buildDefaultCleanConfig(schema)`（放在 `utils/importDefaults.ts`）：
  ```typescript
  function buildDefaultCleanConfig(
    fields: string[],
    fieldTypes: Record<string, FieldInfo>
  ): ImportCleanConfig {
    return {
      dedup: { enabled: true, fields: 'all', keep: 'first' },
      fillMissing: {
        enabled: true,
        rules: [
          { field: '*number', strategy: 'median' },   // 数值列用中位数填充
          { field: '*string', strategy: 'constant', constantValue: '' }, // 字符串列用空字符串
          { field: '*date', strategy: 'drop_row' },    // 日期列缺失则删除行
        ],
      },
      outlier: {
        enabled: false,  // 默认关闭，避免误删数据
        method: 'iqr',
        threshold: 1.5,
        action: 'flag',
        targetFields: 'numeric',
      },
      trimWhitespace: { enabled: true, fields: 'string' },
    };
  }
  ```
  - 默认配置的设计原则：**不丢数据优先**（去重保留首条、异常值仅标记不删除、异常值检测默认关闭）

- **T1.1.3** 改造 `components/DataImport.tsx` 的 `DataImportModal`，在文件上传和确认之间新增清洗配置步骤：
  - **流程变为三步**：① 选择文件 → ② 预览 & 配置清洗 → ③ 确认导入
  - 步骤 ② 的实现：
    - 先调用新增的 `POST /projects/{id}/upload/preview` 端点，上传文件但不入库，返回 DataFrame 预览（前 50 行）+ 自动推断的 schema + 清洗预报告
    - 显示数据预览表格（复用现有预览逻辑）
    - 在预览下方展示清洗配置面板（折叠式，默认收起，标题显示「数据清洗 — 已启用 N 项」）

- **T1.1.4** 新增 `components/ImportCleanPanel.tsx` 清洗配置面板：
  - **去重区域**：
    - 开关（默认开）+ 当前数据预报告（如「检测到 12 行完全重复」）
    - 字段选择：全部字段 / 自定义选择特定字段
    - 保留策略：首条 / 末条
  - **缺失值区域**：
    - 开关（默认开）+ 预报告（如「3 个字段共 45 个缺失值」）
    - 规则列表：每行显示 字段/类型通配 → 策略下拉 → 常量输入（仅 constant 策略显示）
    - 默认规则已按类型预填，用户可增删改
    - 每条规则旁显示影响行数（如「将填充 23 个值」）
  - **异常值区域**：
    - 开关（默认关）+ 开启后显示配置
    - 方法选择（IQR / Z-score）+ 阈值滑块
    - 处理方式：标记（新增 `_outlier` 列）/ 移除
    - 目标字段：所有数值列 / 自定义选择
    - 预报告（如「将标记 8 行异常值」）
  - **空白值区域**：
    - 开关（默认开）
    - 作用于：所有字符串列 / 自定义选择
  - **底部汇总栏**：
    - 清洗后预估行数（原始行数 → 清洗后行数）
    - 「重置为默认」按钮
    - 「跳过清洗」按钮（等价于全部关闭）

- **T1.1.5** 修改 `handleUpload()`（`DataImport.tsx:108`）：
  - 将 `ImportCleanConfig` JSON 序列化后附加到 FormData（字段名 `cleanConfig`）
  - 用户点击「跳过清洗」时传 `cleanConfig = null`（后端跳过清洗步骤）

- **T1.1.6** 在 `utils/api.ts` 中新增方法：
  ```typescript
  // 预览上传（不入库），返回预览数据 + 清洗预报告
  uploadPreview(config: ApiConfig, file: File, projectId: string): Promise<{
    fields: string[];
    fieldTypes: Record<string, FieldInfo>;
    rows: any[];           // 前50行预览
    totalCount: number;
    cleanReport: CleanPreviewReport;
  }>
  ```
  ```typescript
  interface CleanPreviewReport {
    duplicateRowCount: number;        // 完全重复行数
    missingValueCounts: Record<string, number>; // 每字段缺失值数
    outlierCounts: Record<string, number>;      // 每字段异常值数（仅数值列）
    whitespaceFieldCount: number;     // 含前后空白的字符串字段数
  }
  ```

- **T1.1.7** 为 `ImportCleanPanel` 和默认配置生成逻辑编写 Vitest 单元测试：
  - 测试 `buildDefaultCleanConfig()` 对纯数值/纯字符串/混合 schema 的输出
  - 测试面板在不同 `CleanPreviewReport` 下的预报告文案渲染
  - 测试「跳过清洗」按钮将 cleanConfig 设为 null

#### 后端任务

- **T1.1.8** 在 `backend/models.py` 中新增清洗配置 Pydantic 模型：
  ```python
  class DedupConfig(BaseModel):
      enabled: bool = True
      fields: Union[List[str], Literal['all']] = 'all'
      keep: Literal['first', 'last'] = 'first'

  class ImportFillRule(BaseModel):
      field: str                 # 具体字段名或 '*number' / '*string' / '*date'
      strategy: Literal['mean', 'median', 'mode', 'constant', 'forward', 'drop_row']
      constantValue: Optional[str] = None

  class FillMissingConfig(BaseModel):
      enabled: bool = True
      rules: List[ImportFillRule] = [
          ImportFillRule(field='*number', strategy='median'),
          ImportFillRule(field='*string', strategy='constant', constantValue=''),
          ImportFillRule(field='*date', strategy='drop_row'),
      ]

  class OutlierConfig(BaseModel):
      enabled: bool = False
      method: Literal['zscore', 'iqr'] = 'iqr'
      threshold: float = 1.5
      action: Literal['remove', 'flag'] = 'flag'
      targetFields: Union[List[str], Literal['numeric']] = 'numeric'

  class TrimWhitespaceConfig(BaseModel):
      enabled: bool = True
      fields: Union[List[str], Literal['string']] = 'string'

  class ImportCleanConfig(BaseModel):
      dedup: DedupConfig = DedupConfig()
      fillMissing: FillMissingConfig = FillMissingConfig()
      outlier: OutlierConfig = OutlierConfig()
      trimWhitespace: TrimWhitespaceConfig = TrimWhitespaceConfig()
  ```

- **T1.1.9** 新增 `backend/import_cleaner.py` 模块，实现清洗逻辑：
  ```python
  class ImportCleaner:
      def clean(self, df: pd.DataFrame, config: ImportCleanConfig, schema: dict) -> Tuple[pd.DataFrame, CleanReport]:
          """执行清洗并返回清洗后的 DataFrame 和报告"""

      def preview(self, df: pd.DataFrame, schema: dict) -> CleanPreviewReport:
          """仅分析不修改，返回预报告"""
  ```
  - **去重**（`_apply_dedup`）：
    - `fields='all'`：`df.drop_duplicates(keep=config.keep)`
    - 指定字段：`df.drop_duplicates(subset=config.fields, keep=config.keep)`
    - 记录去除行数到报告
  - **空白值处理**（`_apply_trim`，先于填充执行）：
    - `fields='string'`：遍历所有 object/string dtype 列，`df[col].str.strip()`
    - 将纯空白字符串替换为 NaN（使后续缺失值处理能识别）
  - **缺失值填充**（`_apply_fill`）：
    - 展开通配规则：`*number` → 匹配所有 int/float 列，`*string` → 匹配所有 object 列，`*date` → 匹配所有 datetime 列
    - 具体字段规则优先级高于通配规则
    - 各策略实现：
      - `mean`：`df[col].fillna(df[col].mean())`
      - `median`：`df[col].fillna(df[col].median())`
      - `mode`：`df[col].fillna(df[col].mode()[0])` （取众数第一个）
      - `constant`：`df[col].fillna(constantValue)`
      - `forward`：`df[col].fillna(method='ffill')`
      - `drop_row`：`df.dropna(subset=[col])`
    - 记录每字段填充/删除数到报告
  - **异常值处理**（`_apply_outlier`，最后执行）：
    - `targetFields='numeric'`：自动选择所有 int/float 列
    - IQR 方法：`Q1 = df[col].quantile(0.25)`, `Q3 = df[col].quantile(0.75)`, `IQR = Q3 - Q1`，标记 `< Q1 - threshold*IQR` 或 `> Q3 + threshold*IQR`
    - Z-score 方法：`z = (df[col] - mean) / std`，标记 `abs(z) > threshold`
    - `action='flag'`：新增 `_{col}_outlier` 布尔列
    - `action='remove'`：直接删除异常行
    - 记录每字段异常值数到报告
  - **执行顺序固定**：去重 → 空白值 → 缺失值 → 异常值（顺序有依赖关系）

- **T1.1.10** 新增预览端点 `POST /projects/{project_id}/upload/preview`（`main.py`）：
  - 接收文件，调用 `_read_uploaded_dataframe()` 解析
  - 调用 `ImportCleaner.preview(df, schema)` 生成预报告
  - **不存入 DuckDB**，返回预览数据 + schema + 预报告
  - 将解析后的 DataFrame 缓存到内存/临时文件（key = 随机 token），供后续确认导入时复用（避免二次解析）
  - 缓存有效期 10 分钟，过期自动清理

- **T1.1.11** 修改现有上传端点 `POST /projects/{project_id}/upload`（`main.py:1534`）：
  - 新增可选 Form 参数 `cleanConfig: Optional[str]`（JSON 字符串）
  - 新增可选 Form 参数 `previewToken: Optional[str]`（复用预览缓存的 DataFrame）
  - 处理逻辑：
    - 如有 `previewToken`，从缓存取 DataFrame（跳过文件解析）；否则正常解析文件
    - 如有 `cleanConfig`，解析为 `ImportCleanConfig` → 调用 `ImportCleaner.clean(df, config, schema)`
    - 如 `cleanConfig` 为空或未传，使用默认配置 `ImportCleanConfig()`（即默认去重 + 填充 + trim，不做异常值检测）
    - 清洗后的 DataFrame 继续原有流程存入 DuckDB
  - 在返回结果中新增 `cleanReport` 字段，告知用户实际清洗了多少数据：
    ```json
    {
      "cleanReport": {
        "dedupRemoved": 12,
        "fillApplied": { "salary": 3, "name": 0 },
        "outlierFlagged": { "salary": 2 },
        "trimApplied": 45,
        "originalRowCount": 500,
        "finalRowCount": 488
      }
    }
    ```

- **T1.1.12** 保持向后兼容：如前端未传 `cleanConfig` 参数（老版本前端），上传行为与当前完全一致（仅做列名清洗，不做数据清洗），确保升级无破坏

- **T1.1.13** 编写 `backend/test_import_cleaner.py` 单元测试：
  - **去重测试**：10 行中 3 行完全重复 → `dedup.enabled=True` → 验证结果 7 行，报告 `dedupRemoved=3`
  - **缺失值测试**：数值列含 NaN → `median` 策略 → 验证 NaN 被中位数替换
  - **缺失值 drop_row 测试**：日期列含 NaN → `drop_row` → 验证行被删除
  - **异常值 flag 测试**：salary 列含极端值 → `iqr, flag` → 验证新增 `_salary_outlier` 列
  - **异常值 remove 测试**：salary 列含极端值 → `iqr, remove` → 验证行被删除
  - **空白值测试**：字符串列含 `"  hello  "` 和 `"   "` → trim → 验证前后空白去除、纯空白转 NaN
  - **通配规则测试**：`*number` 规则应匹配所有 int/float 列但不匹配 string 列
  - **执行顺序测试**：含重复 + 缺失 + 异常的混合数据 → 全部启用 → 验证去重先于填充、填充先于异常值检测
  - **默认配置测试**：不传 cleanConfig → 验证使用默认配置且不丢失非异常数据
  - **跳过清洗测试**：`cleanConfig=null` → 验证 DataFrame 原样入库

- **T1.1.14** 编写导入端点集成测试（`backend/test_api_execution.py`）：
  - 上传含脏数据的 CSV + cleanConfig → 验证返回的 cleanReport 正确
  - 上传后查询数据集 → 验证存入 DuckDB 的数据已清洗
  - 预览端点测试：上传 → 获取预报告 → 使用 previewToken 确认导入 → 验证不重复解析

---

### 1.2 数据验证规则

**目标**：为数据集字段设置约束条件，在执行前或执行后进行校验，输出验证报告。

#### 前端任务

- **T1.2.1** 在 `types.ts` 的 `CommandType` 中新增 `'validate'`
- **T1.2.2** 在 `CommandConfig` 中新增验证配置：
  ```typescript
  validationRules?: ValidationRule[];
  validationMode?: 'fail' | 'warn' | 'flag'; // 失败中断 / 仅警告 / 标记行
  ```
  新增 `ValidationRule` 接口：
  ```typescript
  interface ValidationRule {
    id: string;
    field: string;
    rule: 'not_null' | 'unique' | 'range' | 'regex' | 'enum' | 'type_check';
    min?: number;       // range 模式
    max?: number;       // range 模式
    pattern?: string;   // regex 模式
    enumValues?: string[]; // enum 模式
    expectedType?: DataType; // type_check 模式
    message?: string;   // 自定义错误提示
  }
  ```
- **T1.2.3** 在 `CommandEditor.tsx` 中新增 `ValidateCommandPanel`：
  - 规则列表编辑器（增/删/排序）
  - 每条规则：选择字段 → 选择规则类型 → 动态表单（范围输入、正则输入等）
  - 底部选择验证模式（中断 / 警告 / 标记）
- **T1.2.4** 在 `ExecutionResult`（types.ts:252）中新增可选字段 `validationReport?: ValidationReport`：
  ```typescript
  interface ValidationReport {
    passed: boolean;
    totalChecks: number;
    failedChecks: number;
    details: { ruleId: string; field: string; failedRowCount: number; sampleValues: any[] }[];
  }
  ```
- **T1.2.5** 在数据预览区域新增验证报告面板：用红/黄/绿色卡片展示每条规则的通过情况

#### 后端任务

- **T1.2.6** 在 `backend/models.py` 中新增 `ValidationRule` Pydantic 模型和 `ValidationReport` 响应模型
- **T1.2.7** 在 `backend/engine.py` 中新增 `_apply_validate` 方法：
  - 遍历 rules，对 DataFrame 逐条检查
  - `not_null`：`df[field].isna().sum()`
  - `unique`：`df[field].duplicated().sum()`
  - `range`：`((df[field] < min) | (df[field] > max)).sum()`
  - `regex`：`~df[field].astype(str).str.match(pattern)`
  - `enum`：`~df[field].isin(enumValues)`
  - `fail` 模式下任一失败抛出 `ValidationError`
  - `flag` 模式下新增 `_validation_failed` 列
  - 收集并返回 `ValidationReport`
- **T1.2.8** 修改执行返回结构，在 `main.py` 的 `/execute` 响应中嵌入 `validationReport`
- **T1.2.9** 编写后端测试覆盖 6 种规则类型 × 3 种模式组合

---

### 1.3 数据血缘追踪

**目标**：记录每个字段从源头到当前节点的完整变换链路，支持前端可视化展示。

#### 后端任务

- **T1.3.1** 新增 `backend/lineage.py` 模块，定义血缘数据结构：
  ```python
  class FieldLineage:
      field_name: str
      origin_table: str
      origin_field: str
      transformations: list[LineageStep]  # 每一步变换记录

  class LineageStep:
      node_id: str
      command_id: str
      command_type: str
      expression: str | None  # transform 表达式
  ```
- **T1.3.2** 在 `engine.py` 的 `_apply_node_commands` 中，每执行一个命令后追加血缘记录：
  - `source`：记录原始字段 → 自身映射
  - `join`：记录右表引入的新字段
  - `transform`：记录 outputField 的来源表达式和输入字段
  - `group`：记录 groupBy 字段和聚合字段的来源
  - `view`：记录字段筛选
- **T1.3.3** 新增 API 端点 `GET /v2/projects/{id}/lineage?nodeId=xxx`，返回目标节点所有字段的血缘链
- **T1.3.4** 编写测试：构建 source → join → transform → group 流水线 → 验证最终字段血缘正确

#### 前端任务

- **T1.3.5** 在 `utils/api.ts` 中新增 `getLineage(config, projectId, nodeId)` 方法
- **T1.3.6** 新增 `components/LineagePanel.tsx` 组件：
  - 以有向图（DAG）形式展示字段血缘关系
  - 点击某字段高亮其完整变换路径
  - 使用 SVG 或 Canvas 绘制节点连线（可考虑轻量库如 dagre-d3 或纯 SVG）
- **T1.3.7** 在操作树节点右键菜单或工具栏中新增「查看血缘」入口，弹窗展示 `LineagePanel`

---

### 1.4 模糊匹配 Join

**目标**：支持基于字符串相似度的近似连接，适用于数据清洗场景中名称不完全一致的匹配。

#### 后端任务

- **T1.4.1** 在 `backend/models.py` 的 `CommandConfig` 中新增字段：
  ```python
  joinFuzzy: bool = False
  fuzzyAlgorithm: str = "levenshtein"  # levenshtein | soundex | jaro_winkler
  fuzzyThreshold: float = 0.8           # 相似度阈值 0-1
  fuzzyMaxMatches: int = 1              # 每行最多匹配数
  ```
- **T1.4.2** 在 `backend/engine.py` 的 `_apply_join` 方法中新增模糊分支：
  - 当 `joinFuzzy=True` 时，不走 DuckDB SQL JOIN
  - 使用 `rapidfuzz` 库（需新增依赖）计算相似度矩阵
  - 基于阈值筛选匹配对，生成笛卡尔积后过滤
  - 新增 `_fuzzy_score` 列标示匹配得分
  - 处理一对多匹配时按 `fuzzyMaxMatches` 截断
- **T1.4.3** 在 `requirements.txt` 中新增 `rapidfuzz>=3.0.0`
- **T1.4.4** 在 `sql_generator.py` 中模糊 Join 生成注释性 SQL（因 DuckDB 不直接支持模糊 JOIN）：
  ```sql
  -- Fuzzy JOIN (levenshtein, threshold=0.8) cannot be expressed as standard SQL
  -- Executed via Python rapidfuzz engine
  ```
- **T1.4.5** 编写测试：上传两张含近似名称的表（如 "Apple Inc" vs "Apple Inc."） → 模糊 Join → 验证匹配成功

#### 前端任务

- **T1.4.6** 在 `types.ts` 的 `CommandConfig` 中新增对应 TypeScript 字段
- **T1.4.7** 在 `CommandEditor.tsx` 的 Join 配置面板中新增「启用模糊匹配」开关：
  - 开启后显示算法选择下拉框、相似度阈值滑块（0-1）、最大匹配数输入
  - 开启后隐藏精确 ON 条件配置（互斥）
- **T1.4.8** 在数据预览中高亮 `_fuzzy_score` 列，用色阶表示匹配质量

---

### 1.5 窗口函数支持

**目标**：新增 `window` 命令类型，支持在可视化编辑器中配置窗口函数（ROW_NUMBER、RANK、LAG/LEAD 等）。

#### 前端任务

- **T1.5.1** 在 `types.ts` 中新增 `'window'` 到 `CommandType`
- **T1.5.2** 在 `CommandConfig` 中新增窗口函数配置：
  ```typescript
  windowFunction?: 'row_number' | 'rank' | 'dense_rank' | 'lag' | 'lead' | 'sum' | 'avg' | 'count' | 'min' | 'max' | 'first_value' | 'last_value';
  windowPartitionBy?: string[];   // PARTITION BY 字段列表
  windowOrderBy?: string;         // ORDER BY 字段
  windowOrderAsc?: boolean;       // 排序方向
  windowOffset?: number;          // LAG/LEAD 的偏移量，默认1
  windowDefault?: string;         // LAG/LEAD 的默认值
  windowOutputAlias?: string;     // 输出列别名
  windowFrameType?: 'rows' | 'range'; // 窗口帧类型
  windowFrameStart?: string;      // 如 'UNBOUNDED PRECEDING'
  windowFrameEnd?: string;        // 如 'CURRENT ROW'
  ```
- **T1.5.3** 新增 `WindowCommandPanel` 组件：
  - 函数选择下拉框（分组显示：排名类 / 偏移类 / 聚合类）
  - PARTITION BY：多选字段标签
  - ORDER BY：单选字段 + 方向切换
  - LAG/LEAD 模式下显示偏移量和默认值输入
  - 聚合模式下显示窗口帧配置（可选高级设置折叠区）
  - 输出别名输入框
  - 实时 SQL 预览面板展示生成的窗口函数 SQL
- **T1.5.4** 编写单元测试验证各函数类型的配置对象正确性

#### 后端任务

- **T1.5.5** 在 `backend/models.py` 的 `CommandConfig` 中新增对应 Pydantic 字段
- **T1.5.6** 在 `backend/engine.py` 新增 `_apply_window` 方法：
  - 构造 DuckDB SQL 窗口函数语句
  - 在当前 DataFrame 对应的 DuckDB 临时表上执行
  - 将结果列追加到 DataFrame
  - 示例 SQL：`SELECT *, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn FROM df_table`
- **T1.5.7** 在 `sql_generator.py` 中新增 `window` 类型的 SQL 生成：
  - 拼接 `{function}({args}) OVER (PARTITION BY ... ORDER BY ... {frame})` 语法
  - 处理 LAG/LEAD 的 offset 和 default 参数
- **T1.5.8** 编写测试：上传员工薪资表 → 配置 `rank() OVER (PARTITION BY dept ORDER BY salary DESC)` → 验证排名正确

---

### 1.6 行列转置（Unpivot）

**目标**：补充已有的 Pivot 操作，支持将宽表转为长表（列转行）。

#### 前端任务

- **T1.6.1** 在 `types.ts` 的 `CommandType` 中新增 `'unpivot'`
- **T1.6.2** 在 `CommandConfig` 中新增配置：
  ```typescript
  unpivotIdFields?: string[];     // 保持不变的标识列
  unpivotValueFields?: string[];  // 需要转置的值列
  unpivotNameColumn?: string;     // 转置后的名称列名（默认 'variable'）
  unpivotValueColumn?: string;    // 转置后的值列名（默认 'value'）
  ```
- **T1.6.3** 新增 `UnpivotCommandPanel` 组件：
  - 左侧：标识列多选（拖拽排序）
  - 右侧：值列多选（拖拽排序）
  - 底部：名称列 / 值列的列名输入
  - 实时预览转置后的前 5 行结构

#### 后端任务

- **T1.6.4** 在 `engine.py` 新增 `_apply_unpivot`：使用 `pd.melt(df, id_vars=..., value_vars=..., var_name=..., value_name=...)`
- **T1.6.5** 在 `sql_generator.py` 中生成 DuckDB UNPIVOT 语法：
  ```sql
  UNPIVOT table ON col1, col2, col3 INTO NAME variable VALUE value
  ```
- **T1.6.6** 编写测试：宽表（id, jan, feb, mar）→ 长表（id, month, value）

---

### 1.7 正则提取/替换

**目标**：新增 `regex` 命令类型，支持对字符串字段进行正则提取、分割、替换。

#### 前端任务

- **T1.7.1** 在 `types.ts` 中新增 `'regex'` 到 `CommandType`
- **T1.7.2** 在 `CommandConfig` 中新增：
  ```typescript
  regexMode?: 'extract' | 'replace' | 'split';
  regexPattern?: string;         // 正则表达式
  regexSourceField?: string;     // 源字段
  regexReplacement?: string;     // 替换字符串（replace 模式）
  regexOutputField?: string;     // 输出字段名
  regexGroupIndex?: number;      // 提取捕获组索引（extract 模式，默认0）
  regexSplitIndex?: number;      // 分割后取第几段（split 模式）
  regexFlags?: string;           // 正则标志（i, m, s 等）
  ```
- **T1.7.3** 新增 `RegexCommandPanel` 组件：
  - 模式切换（提取 / 替换 / 分割）
  - 正则输入框 + 语法高亮 + 实时验证（是否合法正则）
  - 测试区域：输入示例文本 → 实时展示匹配结果
  - 源字段选择 + 输出字段名输入
  - 替换模式下显示替换字符串输入
  - 提取模式下显示捕获组索引选择
  - 分割模式下显示分割段索引选择

#### 后端任务

- **T1.7.4** 在 `engine.py` 新增 `_apply_regex`：
  - `extract`：`df[field].str.extract(pattern, expand=False)` 或按 groupIndex 提取
  - `replace`：`df[field].str.replace(pattern, replacement, regex=True)`
  - `split`：`df[field].str.split(pattern).str[splitIndex]`
  - 正则安全校验：限制回溯深度，拒绝 ReDoS 风险模式
- **T1.7.5** 在 `sql_generator.py` 中使用 DuckDB 的 `regexp_extract` / `regexp_replace` 函数
- **T1.7.6** 编写测试：邮箱字段提取域名、电话号码脱敏替换、地址分割省市区

---

### 1.8 数据库连接器

**目标**：支持从外部关系型数据库（MySQL、PostgreSQL）直连查询数据并注入为数据集。

#### 后端任务

- **T1.8.1** 新增 `backend/connectors/` 目录和基类：
  ```python
  class BaseConnector(ABC):
      def test_connection(self) -> bool
      def list_tables(self) -> list[str]
      def get_schema(self, table: str) -> dict[str, str]
      def query(self, sql: str, limit: int = 1000) -> pd.DataFrame
      def close(self)
  ```
- **T1.8.2** 实现 `MySQLConnector`（基于 `pymysql`）和 `PostgreSQLConnector`（基于 `psycopg2`）
- **T1.8.3** 新增连接管理模型：
  ```python
  class ConnectionConfig(BaseModel):
      id: str
      name: str
      type: Literal['mysql', 'postgresql']
      host: str
      port: int
      database: str
      username: str
      password: str  # 存储时加密
      ssl: bool = False
  ```
- **T1.8.4** 新增 API 端点：
  - `POST /v2/connections` — 创建连接（先测试连通性）
  - `GET /v2/connections` — 列出当前项目的连接
  - `DELETE /v2/connections/{id}` — 删除连接
  - `POST /v2/connections/{id}/test` — 测试连通性
  - `GET /v2/connections/{id}/tables` — 列出表
  - `GET /v2/connections/{id}/schema?table=xxx` — 获取表结构
  - `POST /v2/connections/{id}/query` — 执行查询并返回 DataFrame（带行数限制）
  - `POST /v2/connections/{id}/import` — 将查询结果导入为项目数据集
- **T1.8.5** 连接密码使用 Fernet 对称加密存储，密钥从环境变量 `DB_ENCRYPTION_KEY` 读取
- **T1.8.6** 查询执行添加超时保护（默认 30s）和行数限制（默认 10000 行）
- **T1.8.7** 在 `requirements.txt` 中新增 `pymysql>=1.1.0`, `psycopg2-binary>=2.9.0`, `cryptography>=41.0.0`

#### 前端任务

- **T1.8.8** 在 `utils/api.ts` 中新增连接管理相关 API 方法
- **T1.8.9** 新增 `components/ConnectionManager.tsx` 组件：
  - 连接列表（卡片形式，显示类型图标、名称、状态灯）
  - 新建连接表单：类型选择 → 主机/端口/库名/用户名/密码 → 测试按钮 → 保存
  - 连接详情：可浏览表列表 → 点击表查看字段结构 → SQL 查询框 → 导入按钮
- **T1.8.10** 在 `source` 命令的配置面板中新增数据源类型切换（文件数据集 / 数据库查询），选择数据库时展示连接和表选择

---

### 1.9 API 数据源

**目标**：支持通过 HTTP 请求从 REST API 拉取 JSON 数据并注入为数据集。

#### 后端任务

- **T1.9.1** 新增 `backend/connectors/http_connector.py`：
  ```python
  class HttpConnector:
      def fetch(self, config: HttpSourceConfig) -> pd.DataFrame
  ```
- **T1.9.2** 新增配置模型：
  ```python
  class HttpSourceConfig(BaseModel):
      url: str
      method: Literal['GET', 'POST'] = 'GET'
      headers: dict[str, str] = {}
      params: dict[str, str] = {}       # URL 查询参数
      body: dict | None = None           # POST body
      jsonPath: str = "$"                # JSONPath 提取数据数组
      timeout: int = 30
      pagination: PaginationConfig | None = None

  class PaginationConfig(BaseModel):
      type: Literal['offset', 'cursor', 'page']
      pageParam: str = "page"
      limitParam: str = "limit"
      maxPages: int = 10
  ```
- **T1.9.3** 实现 JSON 数据展平：嵌套 JSON → 扁平列结构（使用 `pd.json_normalize`）
- **T1.9.4** 分页支持：根据配置自动翻页请求，合并所有页数据
- **T1.9.5** 安全限制：禁止请求内网地址（SSRF 防护），限制响应大小（默认 50MB）
- **T1.9.6** 新增 API 端点 `POST /v2/datasources/http-import`，接受配置 → 拉取 → 返回预览 → 确认后存为数据集

#### 前端任务

- **T1.9.7** 新增 `components/HttpSourcePanel.tsx`：
  - URL 输入 + 方法选择
  - Headers 键值对编辑器（可增删行）
  - Body 编辑器（JSON 编辑器，仅 POST 时显示）
  - JSONPath 输入 + 实时预览提取结果
  - 分页配置折叠区
  - 「预览」按钮：拉取第一页数据展示
  - 「导入」按钮：确认后全量拉取并存为数据集

---

### 1.10 在线表格同步

**目标**：支持 Google Sheets 双向读写，可作为数据源导入或将结果导出回表格。

#### 后端任务

- **T1.10.1** 新增 `backend/connectors/gsheet_connector.py`，基于 Google Sheets API v4
- **T1.10.2** 实现 OAuth2 授权流程：
  - `GET /v2/integrations/google/auth-url` — 返回 Google OAuth 授权链接
  - `GET /v2/integrations/google/callback` — 接收回调，存储 refresh_token
  - `POST /v2/integrations/google/revoke` — 撤销授权
- **T1.10.3** 实现读取功能：
  - `GET /v2/integrations/google/sheets` — 列出用户的 Spreadsheet 列表
  - `GET /v2/integrations/google/sheets/{id}/tabs` — 列出工作表标签页
  - `POST /v2/integrations/google/sheets/{id}/import` — 读取指定标签页为 DataFrame → 存为数据集
- **T1.10.4** 实现写回功能：
  - `POST /v2/integrations/google/sheets/{id}/export` — 将指定数据集/执行结果写入指定标签页
  - 支持追加模式和覆盖模式
- **T1.10.5** 在 `requirements.txt` 中新增 `google-api-python-client>=2.100.0`, `google-auth-oauthlib>=1.1.0`

#### 前端任务

- **T1.10.6** 新增 `components/GoogleSheetsPanel.tsx`：
  - 授权状态展示（已连接 / 未连接）
  - 授权按钮 → 打开 OAuth 弹窗 → 回调后刷新状态
  - Spreadsheet 列表 → 选择标签页 → 预览数据 → 导入
  - 导出弹窗：选择目标 Spreadsheet + 标签页 + 写入模式

---

## 二、可视化与分析

### 2.1 内置图表节点

**目标**：新增 `chart` 命令类型，支持在操作树节点上挂载图表配置，实时预览中间结果。

#### 前端任务

- **T2.1.1** 在 `types.ts` 的 `CommandType` 中新增 `'chart'`
- **T2.1.2** 在 `CommandConfig` 中新增图表配置：
  ```typescript
  chartType?: 'line' | 'bar' | 'pie' | 'scatter' | 'heatmap' | 'histogram';
  chartXField?: string;           // X 轴字段
  chartYFields?: string[];        // Y 轴字段（支持多系列）
  chartGroupField?: string;       // 分组/颜色维度
  chartTitle?: string;
  chartXLabel?: string;
  chartYLabel?: string;
  chartWidth?: number;
  chartHeight?: number;
  chartColors?: string[];         // 自定义色板
  chartShowLegend?: boolean;
  chartShowGrid?: boolean;
  chartStacked?: boolean;         // 堆叠模式（bar/line）
  chartBins?: number;             // histogram 分箱数
  ```
- **T2.1.3** 选择轻量图表库并集成（推荐 `recharts`，基于 React，无额外 DOM 依赖）
- **T2.1.4** 新增 `components/ChartCommandPanel.tsx`：
  - 图表类型选择（图标网格）
  - 字段映射配置区：X轴 / Y轴 / 分组 下拉选择（根据当前 schema 填充选项）
  - 样式配置折叠区：标题、标签、色板、网格、图例
  - 实时图表预览区（使用当前节点的执行结果数据）
- **T2.1.5** 在数据预览区域（`DataPreview.tsx`）新增「图表」Tab 页，展示当前节点的图表命令渲染结果
- **T2.1.6** 在 `package.json` 中新增 `recharts` 依赖

#### 后端任务

- **T2.1.7** `chart` 命令在后端为透传类型（不修改 DataFrame），仅在前端渲染
- **T2.1.8** 在 `engine.py` 的命令分发中新增 `chart` 分支，直接跳过（`pass`）
- **T2.1.9** 在 `sql_generator.py` 中 `chart` 命令生成注释：`-- Chart: {chartType} (rendered client-side)`

#### 导出任务

- **T2.1.10** 新增图表导出功能：使用 `recharts` 的 `toSVG()` 或 `html2canvas` 截图导出 PNG/SVG
- **T2.1.11** 在图表预览区右上角增加下载按钮（PNG / SVG / PDF）

---

### 2.2 描述性统计面板

**目标**：一键生成数据集的统计概要（分布、分位数、偏度、峰度），直接在前端展示。

#### 后端任务

- **T2.2.1** 新增 API 端点 `POST /v2/projects/{id}/statistics`：
  ```python
  class StatisticsRequest(BaseModel):
      nodeId: str
      fields: list[str] | None = None  # None 表示全部字段

  class FieldStatistics(BaseModel):
      field: str
      type: str
      count: int
      nullCount: int
      uniqueCount: int
      # 数值字段额外信息
      mean: float | None
      std: float | None
      min: float | None
      max: float | None
      q25: float | None
      q50: float | None  # 中位数
      q75: float | None
      skewness: float | None
      kurtosis: float | None
      # 字符串字段额外信息
      avgLength: float | None
      maxLength: int | None
      topValues: list[dict] | None  # [{value, count}] 前10频率
  ```
- **T2.2.2** 实现统计计算：
  - 数值字段：使用 `df.describe()` + `scipy.stats.skew/kurtosis`
  - 字符串字段：`value_counts().head(10)`、`str.len().mean()`
  - 日期字段：min/max/范围天数

#### 前端任务

- **T2.2.3** 在 `utils/api.ts` 中新增 `getStatistics(config, projectId, nodeId, fields?)` 方法
- **T2.2.4** 新增 `components/StatisticsPanel.tsx`：
  - 卡片网格布局，每个字段一张卡片
  - 数值字段卡片：迷你直方图 + 统计指标（均值/中位数/标准差/偏度/峰度）
  - 字符串字段卡片：Top 10 值柱状图 + 唯一值计数
  - 日期字段卡片：时间范围 + 分布时间线
  - 顶部显示总览：总行数、总字段数、缺失值比例
- **T2.2.5** 在数据预览区新增「统计」Tab 页入口

---

### 2.3 相关性矩阵

**目标**：对数值字段自动计算 Pearson/Spearman 相关系数，以热力图形式展示。

#### 后端任务

- **T2.3.1** 新增 API 端点 `POST /v2/projects/{id}/correlation`：
  - 请求参数：`nodeId`, `fields[]`, `method` (pearson | spearman | kendall)
  - 使用 `df[fields].corr(method=method)` 计算
  - 返回矩阵 JSON：`{ fields: string[], matrix: number[][] }`

#### 前端任务

- **T2.3.2** 新增 `components/CorrelationMatrix.tsx`：
  - 使用 `recharts` 或 SVG 渲染热力图
  - 颜色映射：-1（蓝）→ 0（白）→ +1（红）
  - 鼠标悬停显示具体系数值
  - 可切换 Pearson / Spearman / Kendall 方法
  - 可选择参与计算的字段子集
- **T2.3.3** 在统计面板底部嵌入相关性矩阵

---

### 2.4 时间序列分析

**目标**：为日期/时间字段提供趋势分解、移动平均、同比/环比计算。

#### 后端任务

- **T2.4.1** 新增 API 端点 `POST /v2/projects/{id}/timeseries`：
  ```python
  class TimeSeriesRequest(BaseModel):
      nodeId: str
      dateField: str
      valueField: str
      operations: list[Literal['moving_avg', 'yoy', 'mom', 'decompose']]
      movingAvgWindow: int = 7
      granularity: Literal['day', 'week', 'month', 'quarter', 'year'] = 'day'
  ```
- **T2.4.2** 实现各分析方法：
  - 移动平均：`df[field].rolling(window).mean()`
  - 同比（YoY）：与去年同期对比百分比变化
  - 环比（MoM）：与上期对比百分比变化
  - 趋势分解：使用 `statsmodels.tsa.seasonal_decompose`（可选依赖）
- **T2.4.3** 返回数据包含原始序列 + 各分析结果列

#### 前端任务

- **T2.4.4** 新增 `components/TimeSeriesPanel.tsx`：
  - 日期字段 + 数值字段选择器
  - 分析方法多选（移动平均 / 同比 / 环比 / 趋势分解）
  - 折线图叠加展示：原始数据 + 各分析结果用不同颜色/线型
  - 移动平均窗口大小滑块
  - 时间粒度切换（日/周/月/季/年）

---

## 三、工作流与自动化

### 3.1 定时任务调度

**目标**：支持将操作树配置为定时任务，按 Cron 表达式周期执行并输出结果。

#### 后端任务

- **T3.1.1** 新增 `backend/scheduler.py` 模块，使用 `APScheduler` 实现任务调度：
  ```python
  class TaskScheduler:
      def add_job(self, project_id, config: ScheduleConfig) -> str  # 返回 job_id
      def remove_job(self, job_id: str)
      def list_jobs(self, project_id: str) -> list[ScheduledJob]
      def get_job_history(self, job_id: str) -> list[JobExecution]
  ```
- **T3.1.2** 新增调度配置模型：
  ```python
  class ScheduleConfig(BaseModel):
      name: str
      cron: str                # Cron 表达式（如 "0 8 * * 1"）
      targetNodeId: str
      outputMode: Literal['overwrite_dataset', 'append_dataset', 'export_csv', 'webhook']
      outputDatasetName: str | None = None
      webhookUrl: str | None = None
      enabled: bool = True
      retryCount: int = 0
      timeoutSeconds: int = 120

  class JobExecution(BaseModel):
      id: str
      jobId: str
      startedAt: datetime
      completedAt: datetime | None
      status: Literal['running', 'completed', 'failed', 'timeout']
      rowCount: int | None
      error: str | None
  ```
- **T3.1.3** 新增 API 端点：
  - `POST /v2/projects/{id}/schedules` — 创建定时任务
  - `GET /v2/projects/{id}/schedules` — 列出项目的定时任务
  - `PATCH /v2/projects/{id}/schedules/{jobId}` — 更新配置/启停
  - `DELETE /v2/projects/{id}/schedules/{jobId}` — 删除
  - `POST /v2/projects/{id}/schedules/{jobId}/run` — 手动触发一次
  - `GET /v2/projects/{id}/schedules/{jobId}/history` — 执行历史
- **T3.1.4** 任务执行逻辑：复用 `ExecutionEngine.execute()`，捕获异常记录到历史
- **T3.1.5** 在 `requirements.txt` 中新增 `APScheduler>=3.10.0`
- **T3.1.6** 编写测试：创建定时任务 → 手动触发 → 验证执行历史正确记录

#### 前端任务

- **T3.1.7** 新增 `components/ScheduleManager.tsx`：
  - 任务列表：名称、Cron 表达式（中文可读描述）、下次执行时间、状态灯
  - 新建任务表单：名称 → 选择目标节点 → Cron 编辑器（提供常用预设 + 自定义输入）→ 输出方式
  - 执行历史面板：时间线展示每次执行的状态、耗时、行数
  - 操作按钮：启用/停用、立即执行、编辑、删除

---

### 3.2 执行通知

**目标**：任务执行完成后通过 Webhook 或邮件通知相关人员。

#### 后端任务

- **T3.2.1** 新增 `backend/notifier.py` 模块：
  ```python
  class Notifier:
      async def send_webhook(self, url: str, payload: dict) -> bool
      async def send_email(self, to: str, subject: str, body: str) -> bool
  ```
- **T3.2.2** Webhook 通知 payload 格式：
  ```json
  {
    "event": "job_completed",
    "jobId": "xxx",
    "projectId": "xxx",
    "status": "completed",
    "rowCount": 1500,
    "durationMs": 3200,
    "timestamp": "2026-03-24T08:00:00Z"
  }
  ```
- **T3.2.3** 邮件通知使用 SMTP（配置通过环境变量 `SMTP_HOST`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASS`）
- **T3.2.4** 在 `ScheduleConfig` 中新增通知配置：
  ```python
  notifyOnSuccess: bool = False
  notifyOnFailure: bool = True
  notifyWebhookUrl: str | None = None
  notifyEmails: list[str] = []
  ```

#### 前端任务

- **T3.2.5** 在定时任务编辑表单中新增「通知」Tab 页：
  - 通知触发条件：成功时 / 失败时（复选框）
  - Webhook URL 输入 + 测试按钮
  - 邮件地址列表编辑器

---

### 3.3 宏节点（Macro）

**目标**：将操作树的一个子树封装为可复用模板，支持参数化和跨项目引用。

#### 前端任务

- **T3.3.1** 在 `types.ts` 中新增：
  ```typescript
  interface MacroDefinition {
    id: string;
    name: string;
    description: string;
    parameters: MacroParameter[];
    tree: OperationNode;  // 子树快照
    createdBy: string;
    createdAt: string;
  }

  interface MacroParameter {
    name: string;
    type: 'string' | 'number' | 'field' | 'dataset';
    defaultValue?: string;
    description?: string;
    required: boolean;
  }
  ```
- **T3.3.2** 在操作树节点右键菜单中新增「保存为宏」选项：
  - 弹窗：输入宏名称 / 描述
  - 自动检测子树中的外部引用（数据集名、变量名等），提取为参数
  - 用户可编辑参数列表（名称、类型、默认值、描述）
  - 确认后将子树 + 参数定义存为 MacroDefinition
- **T3.3.3** 在侧边栏新增「宏库」面板：
  - 列出当前项目可用的宏（项目级 + 全局级）
  - 拖拽宏到操作树 → 弹出参数填写表单 → 确认后展开为子树并替换参数
- **T3.3.4** 宏实例化后保留原始宏 ID 的引用，支持「更新到最新版本」

#### 后端任务

- **T3.3.5** 新增 API 端点：
  - `POST /v2/projects/{id}/macros` — 创建宏定义
  - `GET /v2/projects/{id}/macros` — 列出可用宏
  - `GET /v2/macros/{macroId}` — 获取宏详情
  - `POST /v2/macros/{macroId}/instantiate` — 传入参数 → 返回展开后的子树
- **T3.3.6** 宏定义存储在项目 SQLite 的 `macros` 表中

---

### 3.4 版本对比

**目标**：对比操作树的两个版本（或两个不同节点的命令链），以 Diff 形式展示差异。

#### 前端任务

- **T3.4.1** 新增 `components/TreeDiffViewer.tsx`：
  - 左右分栏展示两个版本的操作树
  - 节点级差异：新增（绿色）、删除（红色）、修改（黄色）
  - 命令级差异：展开节点后对比命令列表，显示新增/删除/修改的命令
  - 配置级差异：修改的命令展示字段级 Diff（类似 JSON Diff）
- **T3.4.2** 实现 Tree Diff 算法：
  - 以节点 ID 为基准匹配
  - 递归对比 children 数组
  - 对比 commands 数组（按 ID 匹配）
  - 对比 CommandConfig 各字段值
- **T3.4.3** 在协作界面的版本历史中新增「对比版本」按钮
- **T3.4.4** 在合并冲突面板中嵌入 Diff Viewer 辅助解决冲突

#### 后端任务

- **T3.4.5** 新增 API 端点 `GET /v2/projects/{id}/diff?v1=xxx&v2=yyy`，返回两个版本的操作树快照

---

## 四、协作与权限

### 4.1 节点级评论与标注

**目标**：允许用户在操作树节点上添加评论，支持 @提及 和讨论串。

#### 后端任务

- **T4.1.1** 在项目 SQLite 中新增 `comments` 表：
  ```sql
  CREATE TABLE comments (
      id TEXT PRIMARY KEY,
      project_id TEXT NOT NULL,
      node_id TEXT NOT NULL,
      command_id TEXT,          -- 可选，精确到命令级
      user_id TEXT NOT NULL,
      content TEXT NOT NULL,
      parent_id TEXT,           -- 回复的评论 ID（支持讨论串）
      resolved BOOLEAN DEFAULT FALSE,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  );
  ```
- **T4.1.2** 新增 API 端点：
  - `POST /v2/projects/{id}/comments` — 创建评论
  - `GET /v2/projects/{id}/comments?nodeId=xxx` — 获取节点评论列表
  - `PATCH /v2/projects/{id}/comments/{commentId}` — 编辑/标记已解决
  - `DELETE /v2/projects/{id}/comments/{commentId}` — 删除（仅作者或管理员）
- **T4.1.3** @提及解析：从 content 中提取 `@username` 模式 → 在项目成员中匹配 → 存储 mention 记录
- **T4.1.4** 通过 WebSocket 推送新评论事件给在线成员

#### 前端任务

- **T4.1.5** 在操作树节点上新增评论气泡图标（有评论时显示数量角标）
- **T4.1.6** 新增 `components/CommentThread.tsx`：
  - 评论列表：头像 + 用户名 + 时间 + 内容
  - 输入框支持 @提及自动补全（下拉匹配项目成员列表）
  - 回复功能：缩进展示讨论串
  - 「标记为已解决」按钮（折叠已解决讨论）
- **T4.1.7** 点击节点评论图标 → 弹出侧边评论面板

---

### 4.2 审批流程

**目标**：对关键操作（如数据导出、定时任务上线）设置审批门槛，需指定审批人确认后才能执行。

#### 后端任务

- **T4.2.1** 新增 `approvals` 表和模型：
  ```python
  class ApprovalRequest(BaseModel):
      id: str
      projectId: str
      type: Literal['export', 'schedule_enable', 'schema_change', 'custom']
      description: str
      requestedBy: str
      approvers: list[str]       # 需要审批的用户 ID 列表
      status: Literal['pending', 'approved', 'rejected', 'expired']
      decisions: list[ApprovalDecision]
      expiresAt: datetime
      payload: dict              # 关联的操作详情
  ```
- **T4.2.2** 新增 API 端点：
  - `POST /v2/projects/{id}/approvals` — 提交审批请求
  - `GET /v2/projects/{id}/approvals` — 查看待审批列表
  - `POST /v2/projects/{id}/approvals/{approvalId}/decide` — 批准/拒绝
- **T4.2.3** 在需要审批的操作端点中嵌入检查逻辑：操作前检查是否需要审批 → 是则创建审批请求并返回 `202 Accepted`
- **T4.2.4** 审批通过后自动执行原始操作

#### 前端任务

- **T4.2.5** 新增 `components/ApprovalPanel.tsx`：
  - 我的审批请求列表（发起的）
  - 待我审批列表（需要我审批的）
  - 审批详情：操作描述 + 关联内容预览 + 批准/拒绝按钮 + 评论输入
- **T4.2.6** 在需要审批的操作按钮旁显示状态标签（待审批 / 已通过 / 已拒绝）

---

### 4.3 数据集级权限

**目标**：控制不同角色的用户可以访问哪些数据集，在 API 层强制拦截。

#### 后端任务

- **T4.3.1** 在项目 SQLite 中新增 `dataset_permissions` 表：
  ```sql
  CREATE TABLE dataset_permissions (
      dataset_id TEXT NOT NULL,
      role TEXT NOT NULL,         -- 'viewer' | 'editor' | 'admin' | 'owner'
      access TEXT NOT NULL,       -- 'full' | 'read' | 'none'
      PRIMARY KEY (dataset_id, role)
  );
  ```
- **T4.3.2** 在 `/execute` 和 `/query` 端点中新增权限检查中间件：
  - 解析操作树中引用的 dataSource → 查询权限表 → 拒绝无权访问的请求
  - 返回 `403 Forbidden` 及具体被拒的数据集名称
- **T4.3.3** 新增 API 端点 `PUT /v2/projects/{id}/datasets/{datasetId}/permissions`，允许项目管理员设置
- **T4.3.4** 默认策略：owner/admin 全部 full，editor 全部 read，viewer 按需配置

#### 前端任务

- **T4.3.5** 在数据集管理面板中每个数据集卡片新增「权限」按钮
- **T4.3.6** 权限编辑弹窗：角色 × 数据集矩阵表格，单元格下拉选择 full/read/none

---

### 4.4 字段级脱敏

**目标**：对敏感字段自动掩码显示，保护 PII（个人可识别信息）。

#### 后端任务

- **T4.4.1** 新增敏感字段标记模型：
  ```python
  class FieldMaskConfig(BaseModel):
      field: str
      maskType: Literal['phone', 'email', 'id_card', 'name', 'custom']
      customPattern: str | None = None  # custom 模式的正则替换
  ```
- **T4.4.2** 内置脱敏规则：
  - `phone`：`138****1234`（保留前3后4）
  - `email`：`a***@example.com`（保留首字母和域名）
  - `id_card`：`110***********1234`（保留前3后4）
  - `name`：`张*`（保留姓氏）
  - `custom`：用户自定义正则
- **T4.4.3** 在 `/execute` 返回前，根据用户角色和脱敏配置对结果 DataFrame 进行掩码
  - admin/owner 可看原始数据（可配置）
  - editor/viewer 自动脱敏
- **T4.4.4** 新增 API 端点 `PUT /v2/projects/{id}/datasets/{datasetId}/masking`，配置脱敏规则

#### 前端任务

- **T4.4.5** 在数据集 Schema 编辑器（`SchemaEditor`）中每个字段新增「脱敏」下拉选择
- **T4.4.6** 在数据预览表格中脱敏字段用虚线下划线 + 锁图标标示
- **T4.4.7** admin 用户在预览时可点击「显示原始值」临时查看

---

### 4.5 只读分享

**目标**：生成带 Token 的只读链接，供外部人员无需登录即可查看指定节点的执行结果。

#### 后端任务

- **T4.5.1** 新增 `share_links` 表：
  ```sql
  CREATE TABLE share_links (
      token TEXT PRIMARY KEY,     -- 随机 UUID
      project_id TEXT NOT NULL,
      node_id TEXT NOT NULL,
      created_by TEXT NOT NULL,
      expires_at TIMESTAMP,
      password_hash TEXT,         -- 可选密码保护
      view_count INTEGER DEFAULT 0,
      max_views INTEGER,          -- 可选最大访问次数
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  );
  ```
- **T4.5.2** 新增 API 端点：
  - `POST /v2/projects/{id}/shares` — 创建分享链接（可设过期时间、密码、最大访问次数）
  - `GET /v2/shares/{token}` — 无需认证，返回节点执行结果（检查过期/密码/次数限制）
  - `DELETE /v2/projects/{id}/shares/{token}` — 撤销分享
  - `GET /v2/projects/{id}/shares` — 列出项目所有分享链接
- **T4.5.3** 分享端点返回的数据同样应用字段脱敏规则

#### 前端任务

- **T4.5.4** 在操作树节点右键菜单新增「分享结果」选项
- **T4.5.5** 分享弹窗：
  - 生成的链接（可复制）
  - 过期时间设置（1小时 / 1天 / 7天 / 自定义 / 永不）
  - 可选密码保护
  - 最大访问次数设置
  - 已创建分享列表 + 撤销按钮
- **T4.5.6** 新增 `/share/{token}` 前端页面：精简只读界面，展示数据表格 + 图表（如有）

---

## 五、性能与体验

### 5.1 增量执行

**目标**：识别操作树中未变更的节点，跳过其执行，只重新计算变更节点及下游。

#### 后端任务

- **T5.1.1** 为每次执行缓存节点结果：
  - 计算节点指纹（hash）：基于 `commands` JSON + 输入 DataFrame 行数/字段列表
  - 将结果 DataFrame 存入 DuckDB 临时表（key = `cache_{node_id}_{hash}`）
- **T5.1.2** 在 `engine.py` 的 `execute` 方法中新增缓存检查逻辑：
  - 遍历执行路径上的每个节点
  - 计算当前指纹 → 与缓存指纹对比
  - 匹配则直接读取缓存结果，跳过执行
  - 不匹配则执行并更新缓存
- **T5.1.3** 缓存失效策略：
  - 数据集变更时清除所有引用该数据集的缓存
  - 节点命令修改时清除该节点及其所有下游缓存
  - 手动触发全量清除
- **T5.1.4** 在执行响应中新增 `cacheHit: boolean` 和 `skippedNodes: string[]` 字段
- **T5.1.5** 编写测试：首次执行 → 不修改 → 再次执行 → 验证 cacheHit=true 且耗时显著降低

#### 前端任务

- **T5.1.6** 在操作树节点上显示缓存状态图标（绿色勾 = 已缓存 / 灰色 = 未缓存 / 橙色 = 已过期）
- **T5.1.7** 在执行结果区域显示「命中缓存，跳过 N 个节点」提示
- **T5.1.8** 在工具栏新增「清除缓存」按钮

---

### 5.2 执行计划预览

**目标**：执行前展示生成的 SQL 执行计划（EXPLAIN），帮助用户发现慢查询。

#### 后端任务

- **T5.2.1** 新增 API 端点 `POST /v2/projects/{id}/explain`：
  - 接受与 `/execute` 相同的请求参数
  - 对每个节点生成 SQL → 执行 `EXPLAIN` → 返回计划
  - 返回格式：`{ nodes: [{ nodeId, sql, plan: string, estimatedRows: number }] }`
- **T5.2.2** 在 `engine.py` 中新增 `explain` 方法，复用 `generate_sql` 逻辑但不实际执行

#### 前端任务

- **T5.2.3** 在「运行」按钮旁新增「预览执行计划」按钮
- **T5.2.4** 新增 `components/ExplainPanel.tsx`：
  - 按节点展示 SQL 和执行计划
  - 对预估行数过大或全表扫描的节点用黄色/红色警告标记
  - 提供优化建议文案（如「建议在 xxx 字段上添加过滤条件」）

---

### 5.3 流式分页加载

**目标**：对超大结果集使用虚拟滚动 + 按需加载，避免前端内存溢出。

#### 前端任务

- **T5.3.1** 在 `DataPreview.tsx` 中引入虚拟滚动（推荐 `@tanstack/react-virtual`）：
  - 只渲染可视区域内的行（±缓冲行）
  - 滚动到边界时触发下一页数据请求
  - 显示滚动条进度指示器（当前行 / 总行数）
- **T5.3.2** 在 `utils/api.ts` 的 `execute` 方法中支持分页请求参数传递（已有 `page`/`pageSize`）
- **T5.3.3** 实现分页缓存：前端缓存已加载的页数据，避免重复请求
- **T5.3.4** 在 `package.json` 中新增 `@tanstack/react-virtual` 依赖
- **T5.3.5** 列宽自适应：根据字段类型和数据内容自动计算最优列宽

#### 后端任务

- **T5.3.6** 确保 `/execute` 端点在 `pageSize` 较大时使用流式返回（`StreamingResponse`），避免单次序列化过大 JSON
- **T5.3.7** 对总行数超过 `MAX_SYNC_RESULT_ROWS`（当前 5000）的结果自动启用分页

---

### 5.4 撤销/重做

**目标**：为操作树编辑操作实现全局 Undo/Redo 栈。

#### 前端任务

- **T5.4.1** 在 `utils/projectStore.ts` 中实现 Undo/Redo 管理器：
  ```typescript
  interface UndoManager {
    undoStack: ProjectPatch[][];  // 每次操作产生的 patches 组
    redoStack: ProjectPatch[][];
    maxStackSize: number;         // 默认 50
  }
  ```
- **T5.4.2** 修改 `projectStoreReducer`：
  - 每次 dispatch 产生 state 变更时，计算反向 patch 并推入 undoStack
  - UNDO action：弹出 undoStack 顶部 → 应用反向 patch → 推入 redoStack
  - REDO action：弹出 redoStack 顶部 → 应用正向 patch → 推入 undoStack
  - 新操作发生时清空 redoStack
- **T5.4.3** 在 `ProjectEditorAction` 中新增 `UNDO` 和 `REDO` 类型
- **T5.4.4** 绑定全局键盘快捷键：
  - `Ctrl+Z` / `Cmd+Z` → dispatch UNDO
  - `Ctrl+Shift+Z` / `Cmd+Shift+Z` → dispatch REDO
  - 在 `App.tsx` 中注册 `useEffect` 监听 keydown 事件
- **T5.4.5** 在工具栏新增撤销/重做按钮（灰色 = 栈为空不可用）
- **T5.4.6** 编写 Vitest 测试：执行多步编辑 → Undo 逐步回退 → Redo 恢复 → 验证状态正确

---

### 5.5 键盘快捷键

**目标**：为常用操作绑定键盘快捷键，提升高级用户操作效率。

#### 前端任务

- **T5.5.1** 新增 `utils/shortcuts.ts` 快捷键注册中心：
  ```typescript
  interface ShortcutConfig {
    key: string;           // 如 'n', 'r', 'Delete'
    ctrl?: boolean;
    shift?: boolean;
    alt?: boolean;
    action: string;        // 对应的 action 名
    description: string;   // 中文说明
    scope: 'global' | 'tree' | 'editor' | 'sql';
  }
  ```
- **T5.5.2** 内置默认快捷键映射：
  | 快捷键 | 作用域 | 说明 |
  |--------|--------|------|
  | `Ctrl+Z` | 全局 | 撤销 |
  | `Ctrl+Shift+Z` | 全局 | 重做 |
  | `Ctrl+Enter` | 编辑器/SQL | 运行当前节点/SQL |
  | `Ctrl+N` | 操作树 | 在当前节点下新建子节点 |
  | `Delete` | 操作树 | 删除选中节点 |
  | `Ctrl+D` | 操作树 | 复制当前节点 |
  | `Ctrl+S` | 全局 | 手动保存/提交 |
  | `Ctrl+/` | 全局 | 显示快捷键帮助 |
  | `Ctrl+1/2/3` | 全局 | 切换视图（工作流/SQL/数据） |
  | `↑/↓` | 操作树 | 节点间导航 |
- **T5.5.3** 新增 `components/ShortcutHelp.tsx` 快捷键帮助弹窗（按 `Ctrl+/` 触发）
- **T5.5.4** 避免与浏览器/系统快捷键冲突（需在 Tauri 桌面端和 Web 端分别测试）

---

### 5.6 暗色主题

**目标**：支持 Light/Dark 主题切换，利用 Tailwind CSS 的 dark mode。

#### 前端任务

- **T5.6.1** 在 `tailwind.config.js` 中启用 `darkMode: 'class'`
- **T5.6.2** 定义暗色主题色板变量（在 `index.css` 或 Tailwind 配置中）：
  - 背景色：`#1a1a2e` → `#16213e` → `#0f3460` 三级层次
  - 文字色：`#e0e0e0`（主）/ `#a0a0a0`（次）
  - 边框色：`#2a2a3e`
  - 强调色：保持与亮色主题一致
- **T5.6.3** 为所有组件补充 `dark:` 前缀类名：
  - 遍历 `components/` 目录下所有 `.tsx` 文件
  - 为 `bg-white` 添加 `dark:bg-gray-900`
  - 为 `text-gray-700` 添加 `dark:text-gray-200`
  - 为 `border-gray-200` 添加 `dark:border-gray-700`
  - 等（需逐个组件调整确保视觉一致性）
- **T5.6.4** 在 `AppearanceConfig`（App.tsx:53）中新增 `theme: 'light' | 'dark' | 'system'`
- **T5.6.5** 在设置面板中新增主题切换（三态开关：亮色/暗色/跟随系统）
- **T5.6.6** 实现系统主题监听：`window.matchMedia('(prefers-color-scheme: dark)')` + 事件监听
- **T5.6.7** 将主题偏好持久化到 `localStorage`

---

### 5.7 操作树小地图

**目标**：当操作树节点数量较多时，提供缩略导航图辅助快速定位。

#### 前端任务

- **T5.7.1** 新增 `components/TreeMinimap.tsx`：
  - 固定在操作树面板右下角（可拖拽位置）
  - 缩略渲染完整树结构（仅方块 + 连线，不显示文字）
  - 当前可视区域用半透明矩形标示
  - 点击/拖拽小地图矩形 → 操作树主视图同步滚动到对应位置
  - 节点颜色与操作树中的类型颜色一致（dataset=蓝、process=绿、setup=紫）
- **T5.7.2** 当节点总数 < 15 时自动隐藏小地图（节点少时无需辅助导航）
- **T5.7.3** 使用 Canvas 绘制以保证大量节点时的渲染性能

---

### 5.8 自然语言转操作树（AI 辅助）

**目标**：用户用中文描述数据处理需求，调用 LLM 自动生成操作树节点配置。

#### 后端任务

- **T5.8.1** 新增 `backend/ai_assistant.py` 模块：
  ```python
  class AIAssistant:
      def generate_operations(
          self,
          prompt: str,              # 用户自然语言描述
          available_datasets: list,  # 当前可用数据集及 schema
          existing_tree: dict,       # 当前操作树结构
      ) -> list[OperationNode]
  ```
- **T5.8.2** 设计 System Prompt：
  - 描述 DataFlow Engine 的操作树结构和所有命令类型
  - 提供各命令的 JSON 配置示例
  - 约束输出格式为合法的 `OperationNode[]` JSON
- **T5.8.3** 新增 API 端点 `POST /v2/projects/{id}/ai/generate`：
  - 接受用户 prompt + 当前数据集 schema
  - 调用 LLM API（Claude / OpenAI，通过环境变量配置）
  - 返回生成的节点列表 + 解释文本
- **T5.8.4** 配置项：`AI_API_KEY`, `AI_MODEL`, `AI_BASE_URL`（通过环境变量设置）

#### 前端任务

- **T5.8.5** 在工具栏新增「AI 助手」按钮（魔法棒图标）
- **T5.8.6** 新增 `components/AIAssistantPanel.tsx`：
  - 聊天式输入框：用户输入中文需求描述
  - 示例引导：提供预设 Prompt 模板（如「筛选销售额大于1000的订单并按月汇总」）
  - AI 返回后展示生成的节点列表预览
  - 用户可逐个勾选/取消 → 确认后批量插入操作树
  - 显示 AI 的解释文本（为什么这样配置）
- **T5.8.7** 加载状态：Streaming 展示 AI 响应过程

---

### 5.9 智能字段推荐

**目标**：在配置 Join/Filter 时，根据字段名语义和数据类型自动推荐最可能的匹配字段。

#### 前端任务

- **T5.9.1** 新增 `utils/fieldMatcher.ts` 字段匹配工具：
  ```typescript
  function suggestJoinFields(
    leftFields: Record<string, DataType>,
    rightFields: Record<string, DataType>
  ): { leftField: string; rightField: string; confidence: number }[]
  ```
- **T5.9.2** 匹配规则（优先级递减）：
  1. 完全同名同类型 → confidence: 1.0
  2. 同名不同类型（如 string vs number）→ 0.7
  3. 包含关系（如 `user_id` 与 `id`）→ 0.6
  4. 语义近似（如 `customer_name` 与 `name`）→ 0.4
  5. 仅类型相同 → 0.2
- **T5.9.3** 在 Join 配置面板的 ON 条件字段选择器中：
  - 选择左表字段后，右表字段下拉按 confidence 排序
  - Top 推荐字段用星标标记
- **T5.9.4** 在 Filter 配置中：选择字段后根据类型自动推荐合适的操作符

---

### 5.10 异常数据解释

**目标**：对执行结果中的异常值、空值、极端值自动生成解释性文字。

#### 后端任务

- **T5.10.1** 新增 API 端点 `POST /v2/projects/{id}/insights`：
  - 接受 `nodeId` + 可选 `fields[]`
  - 自动分析数据集，生成结构化 insights 列表：
    ```python
    class DataInsight:
        type: Literal['outlier', 'null_ratio', 'distribution_skew', 'cardinality', 'constant_field']
        severity: Literal['info', 'warning', 'critical']
        field: str
        message: str      # 中文描述
        detail: dict      # 具体数值信息
    ```
- **T5.10.2** 实现检测规则：
  - 空值比例 > 30% → warning
  - 空值比例 > 80% → critical
  - 数值字段存在 > 3σ 离群值 → warning
  - 字段基数为 1（常量字段）→ info
  - 分布偏度 > 2 → info

#### 前端任务

- **T5.10.3** 在数据预览表头旁显示 insight 图标（有异常时显示警告色）
- **T5.10.4** 点击图标展开 insight 卡片列表，每张卡片展示一条分析结论
- **T5.10.5** 在统计面板中也嵌入 insights 区域

---

## 六、优先级矩阵

### P0 — 核心体验（建议近期实施）

| 功能 | 任务量 | 理由 |
|------|--------|------|
| 撤销/重做（5.4） | 前端 6 项 | 编辑操作树时的基础体验保障，用户预期的标配功能 |
| 增量执行（5.1） | 前端 3 项 + 后端 5 项 | 操作树规模增大后性能瓶颈明显，直接影响日常使用 |
| 数据验证规则（1.2） | 前端 5 项 + 后端 4 项 | 数据质量是生产可用的基础门槛 |

### P1 — 功能补全（建议中期实施）

| 功能 | 任务量 | 理由 |
|------|--------|------|
| 内置图表节点（2.1） | 前端 7 项 + 后端 3 项 | 形成"处理 → 可视化"闭环 |
| 数据库连接器（1.8） | 前端 3 项 + 后端 7 项 | 企业用户刚需，数据源扩展 |
| 字段级脱敏（4.4） | 前端 3 项 + 后端 4 项 | 多人协作下数据安全必备 |
| 数据血缘追踪（1.3） | 前端 3 项 + 后端 4 项 | 复杂流水线排查数据问题的关键能力 |
| 窗口函数支持（1.5） | 前端 4 项 + 后端 4 项 | 分析查询高频需求 |

### P2 — 效率提升（建议下一阶段实施）

| 功能 | 任务量 | 理由 |
|------|--------|------|
| 宏节点（3.3） | 前端 4 项 + 后端 2 项 | 减少重复劳动 |
| 定时任务（3.1） | 前端 1 项 + 后端 6 项 | 生产级流水线场景 |
| 版本对比（3.4） | 前端 4 项 + 后端 1 项 | 协作中对比变更的基础能力 |
| 键盘快捷键（5.5） | 前端 4 项 | 提升高级用户效率 |
| 暗色主题（5.6） | 前端 7 项 | 长时间使用的视觉舒适度 |
| 导入时数据清洗（1.1） | 前端 7 项 + 后端 7 项 | 导入阶段自动清洗，带默认配置，零门槛提升数据质量 |

### P3 — 差异化竞争力（建议远期规划）

| 功能 | 任务量 | 理由 |
|------|--------|------|
| AI 辅助（5.8） | 前端 3 项 + 后端 4 项 | 降低使用门槛 |
| 模糊匹配 Join（1.4） | 前端 3 项 + 后端 5 项 | 高级数据清洗需求 |
| 在线表格同步（1.10） | 前端 1 项 + 后端 5 项 | 第三方依赖多，维护成本高 |
| 评论与标注（4.1） | 前端 3 项 + 后端 4 项 | 团队协作辅助功能 |
| 审批流程（4.2） | 前端 2 项 + 后端 4 项 | 企业级管控需求 |

---

## 七、技术前置条件

以下基础设施建议在功能迭代前完成：

### 7.1 前端状态管理规范化
- **T7.1.1** 将 `App.tsx`（1650行）中的状态逻辑拆分为独立 store 模块：
  - `stores/uiStore.ts`：模态框状态、当前视图、选中节点
  - `stores/executionStore.ts`：执行结果、SQL 历史
  - `stores/settingsStore.ts`：外观配置、连接配置
- **T7.1.2** 保持 `projectStore.ts` 作为项目数据核心 store 不变

### 7.2 命令验证框架
- **T7.2.1** 新增 `utils/commandValidator.ts`，为每种命令类型定义 Zod schema
- **T7.2.2** 在 `CommandEditor.tsx` 保存命令前调用验证，阻止无效配置
- **T7.2.3** 在后端 `models.py` 中使用 Pydantic validator 做服务端二次校验

### 7.3 插件化架构
- **T7.3.1** 定义命令类型注册接口：
  ```typescript
  interface CommandPlugin {
    type: CommandType;
    label: string;
    icon: LucideIcon;
    panel: React.ComponentType<CommandPanelProps>;  // 配置面板
    validator: ZodSchema;                            // 配置校验
    defaultConfig: Partial<CommandConfig>;
  }
  ```
- **T7.3.2** 重构 `CommandEditor.tsx` 中的 switch-case 为插件注册表 lookup

### 7.4 监控与可观测性
- **T7.4.1** 在后端新增 `/metrics` 端点（Prometheus 格式），暴露：
  - 请求计数和延迟（按端点分组）
  - 活跃会话数
  - 执行引擎任务队列长度
  - DuckDB 查询耗时分布
- **T7.4.2** 前端错误上报：捕获未处理异常 → POST 到后端日志端点
