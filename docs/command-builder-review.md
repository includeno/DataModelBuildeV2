# Command 构建器问题与改进建议（2026-03-10）

## 1. 范围

本评审覆盖：

- 主构建器（`components/CommandEditor.tsx`）
- SQL 构建器弹窗（`components/command-editor/SqlBuilderModal.tsx`）
- SQL 解析器（`components/command-editor/sqlParser.ts`）
- SQL 导出链路（`backend/engine.py`、`backend/sql_generator.py`）

目标：识别影响用户体验和一致性的关键问题，并给出可执行改进建议。

---

## 2. 主要问题（按优先级）

### P0（高优先，直接影响流程可用性）

1. SQL Builder 的“可编辑能力”与命令覆盖不一致  
现状：弹窗内仅 `filter/sort/view` 可编辑，`join/group/transform/save` 只能看详情。  
影响：用户导入 SQL 后无法在同一弹窗闭环修正，必须跳回主编辑器，流程中断。

2. SQL Builder 校验覆盖不足  
现状：`Apply` 阻断逻辑只覆盖 `filter/sort/view` 字段与数据源，不覆盖 `join/group/having/transform/save`。  
影响：用户能点击 Apply，但运行时才报错，错误发现滞后。

3. 配置语义不一致（`viewLimit=0`）  
现状：解析器支持 `LIMIT 0`，但 UI 展示/输入与后端 SQL 导出对 `0` 处理不一致。  
影响：用户看到的命令与导出的 SQL 不一致，降低可信度。

### P1（中高优先，影响易用性与容错）

4. 切换命令类型会静默重置配置  
现状：type 变更后整段 config 被重置（含 `dataSource`）。  
影响：误操作造成数据丢失，缺少确认/撤销。

5. `multi_table` “必须最后一步”仅提示不强约束  
现状：有提示文案，但仍可被拖拽到中间。  
影响：容易构造无效流水线，导致预期外行为。

6. 运行前校验弱，更多错误在执行期暴露  
现状：Run 按钮主要检查 dataSource 是否缺失。  
影响：字段缺失、表达式空值、join/on 不完整等在执行时才失败。

7. “Consider Existing” 去重能力偏窄  
现状：重点在 filter 条件 + sort/view 去重，其他命令类型重复不处理。  
影响：用户认为“已去重”，实际仍可能重复 join/group/save。

### P2（中优先，影响体验完整性）

8. SQL Builder 内 filter 编辑不支持变量上下文  
现状：`availableVariables` 传空数组。  
影响：主编辑器可用的变量能力在弹窗里缺失。

9. 纯 SQL 覆盖面仍有边界  
现状：`UNION/OFFSET`、`FROM/JOIN` 子查询、`HAVING OR`、`GROUP BY` 表达式仍有降级或不支持。  
影响：复杂 SQL 导入时只能“部分成功”，需大量手修。

10. 反馈信息颗粒度不够  
现状：warning 多为文本列表，缺少“关联到具体命令/字段”的定位。  
影响：用户修复成本高。

---

## 3. 最影响用户体验的问题

1. SQL Builder 不能完整编辑和校验所有已实现 command。  
2. 切换命令类型后静默丢配置。  
3. `viewLimit=0` 在解析/UI/导出不一致。

---

## 4. 已有建议（来自上一轮）

1. 把 SQL Builder 编辑器和校验扩展到 `join/group/having/transform/save`，与主编辑器同等能力。  
2. 建立统一命令 schema 校验层，`Run/Apply` 共用。  
3. 修复 `viewLimit=0` 全链路一致性（显示、输入、summary、SQL 导出）。  
4. 切换命令类型增加确认弹窗或撤销机制。  
5. 对 `multi_table` 做结构约束：禁止拖到非末尾或自动吸附到末尾。  
6. SQL Builder 注入当前作用域变量，和主编辑器一致。  

---

## 5. 新增建议（更多）

### A. 交互与可用性

1. SQL Builder 增加“命令级错误定位”  
将 warning/error 绑定到命令卡片与具体字段，并支持一键跳转。

2. SQL Builder 增加“仅应用选中命令”  
允许用户只 Apply 部分解析结果，降低大批量导入风险。

3. SQL Builder 增加“预览差异（Diff）”  
显示“将新增/替换/删除哪些命令”，避免误覆盖。

4. 主编辑器增加“脏变更保护”  
在关闭弹窗或切换操作节点前提示未保存编辑。

5. 把“自动推断行为”显式化  
例如 `save_` 别名触发 save 命令，应在 UI 标注“由别名约定推断”。

6. 对关键 destructive 操作提供撤销  
删除命令、批量 prune、type 切换重置，提供短时撤销。

### B. 一致性与正确性

7. 建立 round-trip 一致性目标  
`commands -> SQL -> commands` 在支持子集上应等价（字段顺序、distinct、limit、sort）。

8. 统一命令 summary 语义  
避免 `viewLimit=0` 这种“实际有值但 summary 不显示”的分歧。

9. 统一数据源显示规则  
避免 UI 里混用 `linkId/alias/sourceTable` 导致用户看到的标签与实际执行目标不一致。

10. 去重逻辑升级为“语义去重”  
目前是结构签名去重，建议引入条件规范化（排序、同义操作符统一）后再比较。

11. 保留 filter 组逻辑语义  
当前冲突检测 flatten 条件，可能忽略括号语义；建议按表达式树做冲突判断。

12. 对 parser 降级场景做“明确降级策略”  
如 `HAVING OR` 现在“尽量转 AND + warning”，建议允许用户选择“严格失败”或“宽松降级”模式。

### C. 测试与质量保障

13. 补全 SQL Builder UI 校验测试矩阵  
新增 `join/group/having/transform/save` 的“可编辑 + 可校验 + Apply 阻断”测试。

14. 增加 round-trip 自动化测试  
覆盖 plain SQL 和 `-- DMB_COMMAND` 混合输入场景。

15. 增加 parser 模糊测试（fuzz/property-based）  
重点覆盖括号、引号、嵌套表达式、边界 limit/order/group/having 组合。

16. 增加“错误文案稳定性”测试  
保障关键错误提示不被回归改坏（对客服和用户引导很关键）。

### D. 工程与维护

17. 抽离共享校验模块  
把主编辑器和 SQL Builder 的校验逻辑集中到 `commandValidator`，杜绝双份实现漂移。

18. 抽离共享“字段可用性解析”  
统一 `dataSource -> dataset -> fieldNames` 映射，减少多处重复推导逻辑。

19. 抽离共享“命令签名/去重”模块  
避免 UI 侧自定义签名与后端语义冲突。

20. 引入命令 schema 版本化  
为将来命令字段演进（如 join builder 的结构化 on）预留迁移机制。

### E. 观测与运营

21. 埋点关键行为  
记录 parse 成功率、warning 分布、Apply 后回滚率、运行失败类型，指导优化优先级。

22. 埋点“用户修复成本”  
统计从 Parse 到成功 Apply 的编辑次数、耗时、失败重试次数。

23. 新增“诊断导出”  
一键导出当前命令树、解析 warning、校验错误，便于问题复现与支持。

---

## 6. 推荐落地顺序

### 阶段 1（先解决痛点）

1. SQL Builder 扩展 `join/group/having/transform/save` 编辑与校验。  
2. 修复 `viewLimit=0` 全链路一致性。  
3. 命令类型切换加确认与撤销。

### 阶段 2（提升可靠性）

4. 引入统一 validator，并接入 Run/Apply 双入口。  
5. 强约束 `multi_table` 必须末尾。  
6. 升级去重与冲突检测为语义级。

### 阶段 3（提升体验与可观测）

7. SQL Builder 差异预览 + 部分应用。  
8. 增加 round-trip 与 fuzz 测试。  
9. 增加埋点与诊断导出能力。

