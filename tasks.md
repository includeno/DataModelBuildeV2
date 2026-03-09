# Command Builder 改造任务清单（审查后版本）

## 使用说明

- 每个任务/子任务都可独立勾选完成（`[x]`）。
- 本版已去掉当前阶段不合理或过重的任务（如 schema 版本化、全量埋点体系）。
- 本版补充了更贴近当前目标的可落地任务（命令一致性、可编辑、可校验、可回归）。

---

## Phase 0（对齐与基线）

- [ ] T00 建立“命令能力矩阵”基线
  - [ ] T00.1 列出所有已实现命令类型（前端构建器/SQL Builder/SQL 导出/执行引擎）
  - [ ] T00.2 标注每种命令的“可编辑/可校验/可导出/可解析”状态
  - [ ] T00.3 明确本轮目标覆盖范围与非目标
  - [ ] T00.4 验收：团队对“支持范围”无歧义

- [ ] T01 建立回归基线
  - [ ] T01.1 固化 parser 测试集合（含纯 SQL 推断用例）
  - [ ] T01.2 固化 SQL Builder UI 测试集合
  - [ ] T01.3 固化后端 SQL 生成测试集合
  - [ ] T01.4 验收：改造前基线全绿

---

## Phase 1（核心功能修复）

- [x] T02 SQL Builder 补齐命令编辑能力（`join/group/having/transform/save`）
  - [x] T02.1 `join`：target/joinType/on/字段辅助构建可编辑
  - [x] T02.2 `group`：groupByFields/aggregations 可编辑
  - [x] T02.3 `having`：metricAlias/operator/value 可编辑
  - [x] T02.4 `transform`：mapping 列表、mode、表达式、输出字段可编辑
  - [x] T02.5 `save`：field/distinct/value 可编辑
  - [x] T02.6 验收：导入 SQL 后可在弹窗内完成修正并直接 Apply

- [ ] T03 SQL Builder 校验补全（Apply 阻断）
  - [ ] T03.1 增加 `join` 校验（target 必填、ON 合法）
  - [ ] T03.2 增加 `group/having` 校验（字段存在、having 引用 metric 合法）
  - [ ] T03.3 增加 `transform` 校验（expression/outputField 必填）
  - [ ] T03.4 增加 `save` 校验（field/value 合法）
  - [ ] T03.5 错误映射到命令卡片并可定位
  - [ ] T03.6 验收：不合法命令时 Apply 必须禁用

- [ ] T04 抽离统一 `commandValidator`（主编辑器 + SQL Builder + Run 前）
  - [ ] T04.1 新建共享校验模块（按命令类型分规则）
  - [ ] T04.2 `CommandEditor` 接入共享校验结果
  - [ ] T04.3 `SqlBuilderModal` 接入共享校验结果
  - [ ] T04.4 `Run/Run to step` 接入共享校验入口
  - [ ] T04.5 验收：同一命令在不同入口得到一致校验结论

- [x] T05 修复 `viewLimit=0` 全链路一致性
  - [x] T05.1 前端显示逻辑保留 `0`（替换 `||` 判空）
  - [x] T05.2 summary 文案显示 `Limit 0`
  - [x] T05.3 SQL 导出支持 `LIMIT 0`
  - [x] T05.4 UI 输入规则允许 `0`，避免与 `min=1` 冲突
  - [x] T05.5 验收：`SQL -> 命令 -> SQL` 中 `LIMIT 0` 不丢失

- [ ] T06 命令类型切换防误操作（确认 + 撤销）
  - [ ] T06.1 切换前提示“将重置哪些配置”
  - [ ] T06.2 提供单步撤销（恢复原 type/config）
  - [ ] T06.3 验收：误触切换可恢复

- [ ] T07 强约束 `multi_table` 必须末尾
  - [ ] T07.1 禁止拖拽到非末尾位置
  - [ ] T07.2 非法插入自动吸附到末尾
  - [ ] T07.3 验收：任意交互都不会形成“中间 multi_table”

- [ ] T08 变量上下文一致性（SQL Builder 与主编辑器）
  - [ ] T08.1 传入当前作用域变量到 SQL Builder
  - [ ] T08.2 Filter 编辑器支持变量插入与校验
  - [ ] T08.3 验收：变量体验与主编辑器一致

---

## Phase 2（一致性与正确性增强）

- [ ] T09 数据源解析与展示统一（`linkId/alias/sourceTable`）
  - [ ] T09.1 抽离统一解析函数（source id -> dataset）
  - [ ] T09.2 抽离统一展示函数（用户可读标签）
  - [ ] T09.3 替换多处重复逻辑
  - [ ] T09.4 验收：显示目标与执行目标一致

- [ ] T10 “Consider Existing” 去重升级（语义去重）
  - [ ] T10.1 条件规范化（大小写、值格式、同义操作符）
  - [ ] T10.2 扩展去重范围到 `join/group/save/transform`
  - [ ] T10.3 保留“命中原因”提示
  - [ ] T10.4 验收：重复命令识别更稳定、误删可控

- [ ] T11 冲突检测保留分组语义（不只 flatten）
  - [ ] T11.1 以表达式树做冲突判断
  - [ ] T11.2 支持括号和 AND/OR 语义
  - [ ] T11.3 验收：复杂嵌套条件冲突判断正确

- [ ] T12 Parser 边界行为优化（不新增重模式）
  - [ ] T12.1 对不支持语法给出更精确 warning（含片段）
  - [ ] T12.2 保证降级后命令顺序稳定且可解释
  - [ ] T12.3 对 JOIN/HAVING/GROUP 的降级路径补单测
  - [ ] T12.4 验收：复杂 SQL 导入可预期、可修复

- [ ] T13 SQL 导出一致性修复（按命令类型逐一对齐）
  - [ ] T13.1 `join/group/having/transform/save/view` 导出规则与构建器一致
  - [ ] T13.2 修复文案支持但导出不支持的细项
  - [ ] T13.3 补齐后端单测与 API 测试
  - [ ] T13.4 验收：构建器配置与导出 SQL 行为一致

- [ ] T14 round-trip 一致性（可支持子集）
  - [ ] T14.1 定义 round-trip 支持子集
  - [ ] T14.2 新增 `commands -> SQL(meta) -> commands` 测试
  - [ ] T14.3 新增纯 SQL 子集 round-trip 测试
  - [ ] T14.4 验收：子集内 round-trip 稳定等价

---

## Phase 3（体验增强）

- [ ] T15 SQL Builder 错误/警告可定位
  - [ ] T15.1 warning 与命令 ID 绑定
  - [ ] T15.2 点击 warning 可展开并定位命令
  - [ ] T15.3 验收：用户可快速定位修复点

- [ ] T16 SQL Builder 差异预览（Diff）
  - [ ] T16.1 显示将新增/替换/删除的命令摘要
  - [ ] T16.2 高亮与已有命令冲突项
  - [ ] T16.3 验收：Apply 前可清晰理解改动

- [ ] T17 SQL Builder 支持“仅应用选中命令”
  - [ ] T17.1 命令列表增加选择态
  - [ ] T17.2 Apply 时仅提交选中命令
  - [ ] T17.3 验收：部分导入流程可用

- [ ] T18 destructive 操作统一撤销
  - [ ] T18.1 删除命令可撤销
  - [ ] T18.2 批量 prune 可撤销
  - [ ] T18.3 type 切换重置可撤销
  - [ ] T18.4 验收：关键误操作可恢复

- [ ] T19 主编辑器脏变更保护
  - [ ] T19.1 识别未应用改动
  - [ ] T19.2 关闭/切换操作节点前确认
  - [ ] T19.3 验收：无提示丢改动问题消失

- [ ] T20 自动推断行为显式化
  - [ ] T20.1 对 `save_` 推断结果加标识
  - [ ] T20.2 对降级解析结果加标识
  - [ ] T20.3 验收：用户能区分“显式配置”和“自动推断”

---

## Phase 4（测试与交付）

- [ ] T21 测试矩阵补全（UI + Parser + Backend）
  - [x] T21.1 SQL Builder UI：`join/group/having/transform/save` 编辑+校验
  - [ ] T21.2 Parser：边界语法与降级路径
  - [ ] T21.3 Backend：SQL 导出一致性测试
  - [ ] T21.4 round-trip：metadata + 纯 SQL 子集
  - [ ] T21.5 验收：新增路径均有自动化覆盖

- [ ] T22 端到端流程回归
  - [ ] T22.1 “Build from SQL -> Edit -> Apply -> Run -> Generate SQL” 主路径 E2E
  - [ ] T22.2 覆盖插入位置（中间插入/末尾插入）
  - [ ] T22.3 覆盖 Consider Existing + 部分应用路径
  - [ ] T22.4 验收：关键用户流程稳定

- [ ] T23 文档与发布说明
  - [ ] T23.1 更新用户文档（支持 SQL 子集、降级说明）
  - [ ] T23.2 更新开发文档（共享 validator、测试策略）
  - [ ] T23.3 输出变更清单与已知限制
  - [ ] T23.4 验收：文档可指导使用与维护

---

## 里程碑勾选

- [ ] M0（Phase 0 完成）
- [ ] M1（Phase 1 完成）
- [ ] M2（Phase 2 完成）
- [ ] M3（Phase 3 完成）
- [ ] M4（Phase 4 完成）
- [ ] M5（全量回归：`npm run test:frontend` + `pytest -q backend`）
