# 2026-11-27 · 18 项待办的本地验证与设计建议

基线为 `test` 的 `e42134e`。从 Git 导出源码快照，在空模型凭据环境运行四模块 `make test`，并补跑嵌套存档 12/12、Plan Web mock SSE、A2A HITL 冒烟及下列定向回归。`make test-integration` 的 9 个教学 mock 通过，12 个真模型用例跳过。模型替身访问本机 HTTP，路由替身使用仓内样本。当前上线计划对共用存档的验收仍暂缓。[18 项待办](../../code/todo.md) · [冒烟命令](../smoke-test-recipes.md)

## 7 项验收的本地结果

| ID | 本次已验证 | 仍需验收的条件 |
| --- | --- | --- |
| A02 | PostgreSQL 16.9 和 MySQL 8.0.35 均通过发版集成命令及 race 检查，覆盖三个 pgx 执行模式和两种 MySQL 参数模式。Redis 7.2.6 实库通过旧键迁移、GraphID 核对及 AG-UI 双任务的租约令牌校验。SQLite 用 `05f5bb9` 和 `e42134e` 两个二进制验证旧档续跑、恢复备份后用旧版续跑，以及同一逻辑图下旧版续跑新版挂起档。三次续跑均沿用原问题 ID 并清空剩余存档。 | 发布 SHA 对应的 PostgreSQL/MySQL CI、实际启用的后端用真实存量档演练、按部署范围验收回退。 [集成测试](../../../sql/conformance_integration_test.go) · [Redis AG-UI 回归](../../../redis/agui_session_test.go) · [升级说明](../../release/next.md) |
| A03 | 离线回放 458 条仓内构造样本，为 Jev、百炼、Qwen 写入共 1,374 条模拟预测。评测走原有输入和配置指纹、分层、错路由率、接管比例及 P95 报告、门槛扫描。样本文件名中的 `real` 指构造的对话式样本。 | 业务提供脱敏问题、人工标注的正确分支，并指定可用候选模型。冻结样本与模型版本后，再选错路由率、覆盖率和端到端 P95 门槛。 [样本约定](../../../example/decision/routing_eval/dataset.go) · [回放回归](../../../example/decision/routing_eval/replay_test.go) |
| A05 | 真实 Qwen 客户端接本地 SSE，确认工具参数跨分片拼接、A2A 挂起、按原 ID 补答以及续跑 artifact。另跑 A2A HITL 冒烟及服务端测试。 | 用实际 Qwen 模型跑编排、Skill Action、工具审批、存档及 A2A 的发版矩阵。 [端到端回归](../../../example/a2a/llm_smoke/main_test.go) · [冒烟命令](../smoke-test-recipes.md) |
| A06 | Anthropic 客户端的 HTTP/SSE 回归覆盖空闲、续计时、`message_stop`、计费拒绝分类和上下文清理。新探针用相同工具回合对照原前缀、改 System、改工具定义，分别核对默认 `drop_block` 和严格 `error` 的六份请求，检查两种清理与服务端压缩可按预期编码。 | 实际模型的流间隔、计费拒绝与超窗错误原文、真实编辑统计、组合压缩和缓存用量。严格模式下的前缀错误在本地由测试桩返回。 [流式回归](../../../model/anthropic/stream_idle_test.go) · [上下文清理回归](../../../model/anthropic/context_editing_test.go) · [真模型入口](../../../example/model/anthropic_smoke/context_editing_test.go) |
| A07 | Qwen 和 DeepSeek 的 HTTP 桩覆盖生产 `BasicAgent` 的三轮推理回放、单次调用关停 thinking、消费端提前关闭流。Anthropic 签名块另有原样回放和编辑后重建的回归。 | 各供应商实际模型与版本组合的请求兼容性、推理表现及流取消。 [跨轮回归](../../../agent_v2/thinking_replay_http_test.go) · [签名回放回归](../../../model/anthropic/replay_test.go) |
| A09 | 本地 HTTPS 反代覆盖 Cookie 隔离、挂起和终态 TTL，后端用 miniredis。SSE 背压与成功终态拒写、确认丢失后的恢复通过。同一测试进程内的两个 Plan Web HTTP 服务另接 Redis 7.2.6 实库，验证挂起和接手续跑。 | 目标入口鉴权、代理缓冲和超时、真实流量和故障恢复。共享存档实际部署验收随上线范围决定。 [入口及 TTL 回归](../../../example/orchestration/plan_chat_web/server_test.go) · [SSE](../../../example/orchestration/plan_chat_web/sse_internal_test.go) · [终态故障注入](../../../example/orchestration/plan_chat_web/completion_recovery_test.go) |
| A15 | 捕获 `CountInputTokens` 实际 HTTP 请求，核对工具定义、System、签名 thinking 块、缓存选项和出错路径。建议接近窗口阈值时才调用计数端点，并按模型和完整请求指纹复用估算。 | 用真模型对照签名回放、可见文本、本地估算及实际 input/cache usage，给各模型留出经实测的预算余量。计数端点的估算不含实际缓存判定。 [计数回归](../../../model/anthropic/token_count_test.go) · [真模型入口](../../../example/model/anthropic_smoke/anthropic_smoke_test.go) · [Anthropic Token Counting](https://platform.claude.com/docs/en/build-with-claude/token-counting) |

## 11 项设计的取舍

### Q01 · Sandbox 后端一致性

本地 `NewBashTool` 回归覆盖参数转发、退出码、传输错误、空结果及输出标识。当前后端缺少退出码时也返回零，不能据此判断命令成功。OpenAI Agents SDK 用 capabilities 和 permissions 表达可用能力，按部署选客户端和隔离后端。[crew-go 接口](../../../sandbox/sandbox.go) · [工具回归](../../../sandbox/tool_test.go) · [OpenAI Sandbox Concepts](https://openai.github.io/openai-agents-python/sandbox/guide/) · [Sandbox Clients](https://openai.github.io/openai-agents-python/sandbox/clients/)

**建议保留窄接口，随首个实际后端引入能力声明和 conformance。** 先定义工具确实依赖的可观察契约，包括输出上限、退出状态是否已知、调用取消后远端进程是否终止，以及挂载、网络和进程的授权边界。接入方显式选择后端并声明能力。用受控命令测试终止和权限，调用方收到 `context.Canceled` 只能验证调用端取消。

### Q05 · 消息协议

本地 MCP 结构化音频经 checkpoint Resume 重放，Anthropic 与 OpenAI Responses 的工具结果均有序列化断言。Eino v0.9 保留默认 `Message` 路径，并新增有序内容块的 `AgenticMessage` 与泛型 ADK。LangChain 提供标准内容块视图，消息仍能承载 provider 原生内容。[MCP 回放](../../../mcp/tool_media_resume_test.go) · [适配器回归](../../../mcp/tool_media_test.go) · [Eino v0.9](https://www.cloudwego.io/docs/eino/release_notes_and_migration/eino_v0.9._agentic-runtime/) · [LangChain Messages](https://docs.langchain.com/oss/python/langchain/messages)

**建议保留 `schema.Message`、现有结构化内容与 provider 原生回放。** 若两家实际适配器都出现有序的服务端工具调用、结果或引用无法完整往返，先用同一用例验证流式聚合、历史写读和旧 checkpoint 读取，再决定扩展内容块或增加有类型的协议。单一供应商的原生字段优先在适配层保真。

### Q06 · 故障分析 RCA

本地用 `compose_v2` 从 HTTP 桩的三个端点读指标、调用链与变更。checkout 的 503 有下游 inventory 503 和同服务变更时给出带三个证据 ID 的候选。缺调用链或只观测到无关服务变更时均输出证据不足。Datadog Bits 采取提出假设、查询遥测并继续验证的循环，Google SRE 的事件响应流程优先恢复服务。[当前提案](../../v2/fault-analysis-system.md) · [Datadog Bits 调查](https://docs.datadoghq.com/bits_ai/bits_investigation/investigate_issues/) · [Google SRE Incident Response](https://sre.google/workbook/incident-response/) · [OTel Trace Context Fields](https://opentelemetry.io/docs/specs/otel/logs/data-model/#trace-context-fields)

**建议先做一类故障的只读固定图。** 以服务、时间窗和依赖关系关联证据，输出候选原因、来源 ID 和证据不足状态。用标注事故比较首选原因准确率、错误归因率和调查时间。等同一批事故证明自适应取证有收益，再让受预算约束的 `PlanAgent` 补查缺失证据。

### Q08 · Claude 签名回放与缓存

六组本地请求确认现有适配器对 adaptive thinking 默认发送 `drop_block`，严格模式可以显式指定，工具调用对应的原有 thinking 块仍随请求发送。Anthropic 校验绑定前缀，部分模型和 thinking 设置有适用条件。缓存沿 tools、System、messages 逐层判定，工具定义变化会使后续层失效。[客户端配置](../../../model/anthropic/client.go) · [签名回放回归](../../../model/anthropic/replay_test.go) · [Thinking 排障](https://platform.claude.com/docs/en/build-with-claude/thinking-troubleshooting) · [工具与缓存](https://platform.claude.com/docs/en/agents-and-tools/tool-use/tool-use-with-prompt-caching)

**建议沿用默认策略，优先稳定同一工具回合的实际送模前缀。** 保留原始签名块的顺序和工具配对，减少无意的 System、工具顺序和 schema 变化。确需变更前缀时，在完整工具组之后切到新的回放边界，或按策略淘汰旧签名。宿主派发始终执行当前权限。用现有 `input_transformations` 标注和缓存读写量核对代价，严格模式用于兼容性验收和必须保留推理的调用。遇到确定性的绑定错误时，改变回放窗口或策略才重发。

### Q11 · 工具审批与风险策略

本地验证共享 MCP 目录按 Handoff 成员隔离检索范围、工具绑定及审批，允许记忆不跨任务，续跑使用原审批对应的参数。OpenAI Agents SDK 让宿主按解析后的参数决定是否审批，LangChain 支持 approve/edit/reject。MCP 要求将不受信服务器的工具标注视为不可信输入。[成员隔离](../../../agent_v2/tool_search_member_policy_test.go) · [按任务隔离审批](../../../agent_v2/approval_cross_task_test.go) · [OpenAI Approvals](https://openai.github.io/openai-agents-python/human_in_the_loop/) · [LangChain HITL](https://docs.langchain.com/oss/python/langchain/human-in-the-loop) · [MCP Tools](https://modelcontextprotocol.io/specification/2025-11-25/server/tools)

**建议沿现有审批入口增加应用可选的派发前风险策略。** 宿主根据调用者、工具身份和已固定的实际参数决定允许、确认或拒绝。模型可以申请确认并说明理由，异步发现风险时在动作开始前切换到同步确认。把确认绑定到目标、规范参数和策略版本，修改参数视为另一次待授权调用。用相同名称的不同工具、参数改写、审批后权限收紧覆盖策略回归。

### Q12 · 调度约束与副作用重放

本地后台任务回归覆盖阶段和证据持久化、挂起与租约接手。`WithRerunnable` 回归覆盖条件否决后有意重做子调用。LangGraph 用 task 结果与幂等操作控制重执行，Temporal 的 Activity 由业务下游用稳定的执行与活动身份去重，Google ADK 在运行配置中设模型调用上限。[后台进度](../../v2/durable-background-agent.md#阶段进度与证据) · [子调用重做](../../../agent_v2/nested_call_reject_test.go) · [LangGraph 重执行](https://docs.langchain.com/oss/python/langgraph/graph-api#re-execution-and-idempotency) · [Temporal Activity](https://docs.temporal.io/activity-definition) · [Google ADK RunConfig](https://adk.dev/runtime/runconfig/)

**建议把重放策略放在实际调用边界，预算放在任务边界。** 读取允许重试。有副作用的调用优先用下游保障的稳定幂等键。缺少幂等保证时，从权威回执核对结果，确认此前尝试未生效且无法再提交后才重试，结果不确定时挂起。`visit` 继续表示执行位置，同一动作的重试复用键，获批重做的动作有新身份。持久任务的模型和工具额度随执行进度恢复，时限单独表达。Goal 由应用显式激活并设停止条件。用外部效果已提交、checkpoint 尚未保存的故障窗口测试接手。

### Q13 · 历史权威与送模投影

本地回归验证嵌套挂起输入的拒写和确认丢失后恢复，旧档删除失败时仍能原 ID 续跑，跨分区放弃时成功删旧档后才补工具历史。常规历史路径从 `HistoryStore` 投影上文，读取失败可回退 Events，普通异步写入失败记录告警。OpenAI Agents SDK 在加载会话后允许编辑送模输入，Google ADK 把事件追加和会话状态更新放在 SessionService 边界。[嵌套恢复](../../../agent_v2/nested_input_recovery_test.go) · [删除失败后续跑](../../../agui/replacement_test.go) · [两分区回归](../../../agent_v2/abandoned_partition_test.go) · [OpenAI Sessions](https://openai.github.io/openai-agents-python/sessions/) · [Google ADK Sessions](https://adk.dev/sessions/session/)

**建议让 `HistoryStore` 拥有有序的消息审计记录，由框架投影已提交内容送模。** `RunSession.Events` 用于运行内观测和既有上文回退，完整审计依赖持久记录。条件否决保留已追加的原记录和尝试状态，送模时过滤被否决的尝试。对明确要求耐久的部署，为关键写入使用可恢复待写记录和按调用出现位置的确认，保留原始分区。按应用脱敏和保留策略记录用户实际输入，框架生成的放弃原因与工具收尾带独立状态。回归应覆盖确认丢失重试只追加一次、工具体不重复执行及前文位置保持。

### Q14 · Code Mode 信任边界

本地已有工具检索的候选范围恢复、成员级 ToolFilter 和审批派发回归。Cloudflare Code Mode 将隔离 executor、显式连接器与耐久运行时分开。Anthropic 的 MCP 代码执行范式先选需要的工具，再用程序组合结果。[检索和成员策略回归](../../../agent_v2/tool_search_member_policy_test.go) · [Cloudflare Code Mode](https://developers.cloudflare.com/agents/tools/codemode/how-it-works/) · [Anthropic MCP Code Execution](https://www.anthropic.com/research/code-execution-with-mcp)

**建议先用类型化的只读工具序列验证收益。** 每个调用由宿主校验工具、参数、权限与预算，并记录回执供续跑重放。脚本执行留到多调用场景实测节省模型轮数、耗时或上下文，而且有通过 Q01 契约的执行后端。产生副作用的请求沿用 Q11 的宿主审批。

### Q17 · 保留期、导出与存储成本

本次 SQLite 转存保留期和读取回归通过。重新用 PostgreSQL 16.9 测 6 万条合成记录，五分之一的 score 用 JSON 字符串。固定 `ns` 并过滤 score 大于 9770 且小于 9780，三种计划得到相同的 32 个 ID。无索引的自定义计划约 16.10 ms，带表达式索引约 0.62 ms。强制通用计划约 18.44 ms，其索引只约束 namespace，仍检查 4 万行。本地单次取样适合核对计划形状，部署选型需要真实负载。LangSmith 分别配置 checkpoint 与 Store 的 TTL，PostgreSQL 表达式索引同时有写入维护成本。[SQLite 回归](../../../sql/microcompact_retention_test.go) · [既有 PostgreSQL 探针](../../code/backlog-audit.md#2026-11-24-本地待办续做) · [LangSmith TTL](https://docs.langchain.com/langsmith/configure-ttl) · [PostgreSQL 表达式索引](https://www.postgresql.org/docs/16/indexes-expressional.html)

**建议独立配置 Store TTL、HistoryStore 留存与导出、存档和压缩转存保留。** 历史默认沿用保留策略，业务按需开启清理。先模拟删除范围并完成可重跑的导出，再清理历史。挂起 checkpoint 或压缩摘要仍引用的正文按依赖延长保留。统计 AutoCompact 边界的大小和读取代价，按真实查询的选择性、通用计划及写放大决定是否添加表达式索引。

### Q18 · Plan、Reflect 与 Routing 的决策契约

本地同一组三个固定任务中，固定图和真实 `NewLLMPlanner` 加模型桩均得到正确输出，固定图按样本省去 2、3、3 次模型调用。LangGraph 的结构化路由、分派工作者、评估再生成是不同工作流，分别表达分类、任务分解和结果评价。[固定图与规划对照](../../../example/decision/trajectory_eval/plan_comparison_test.go) · [LangGraph 工作流模式](https://docs.langchain.com/oss/python/langgraph/workflows-agents)

**建议复用评测日志和运行约束，由各决策点定义自己的结果。** Routing 继续按分支和错路由成本校准。Plan 在可枚举任务里走固定图，开放任务使用结构化计划并在执行前核对成员、预算与需人审动作。Reflect 按任务 rubric 输出判定、证据和修改要求，设最大轮数及退让路径。用同样本比较成功率、误放行、调用数和总耗时后，再决定各自的接管门槛。

### Q27 · 挂起版本与执行位置

SQLite、PostgreSQL 和 MySQL 的版本回归固定逻辑图名、节点和 channel，改变审批后的静态边。缺原版本时返回 409，原审批仍可继续。配置了原版本工厂时旧任务只走旧分支，新任务走新分支。LangGraph 明确了挂起节点改名和状态类型变化的兼容限制。OpenAI Agents SDK 建议保存流程定义版本并选择相应代码路径，Temporal 提供 Worker Deployment Version 固定与回放测试。[crew-go 版本回归](../../../sql/agui_workflow_version_test.go) · [LangGraph Graph migrations](https://docs.langchain.com/oss/python/langgraph/graph-api#graph-migrations) · [OpenAI 挂起版本](https://openai.github.io/openai-agents-python/human_in_the_loop/#versioning-pending-tasks) · [Temporal Worker Versioning](https://docs.temporal.io/best-practices/worker#use-worker-versioning-to-safely-deploy-new-workflow-code) · [Temporal Go 回放测试](https://docs.temporal.io/develop/go/best-practices/testing-suite)

**建议先为挂起任务保留原定义，遇到实际业务状态变更时再引入显式迁移。** 共享会话使用 `agui.WithSessionStore` 和 `agui.WithWorkflowVersions` 选择工厂，独立存档入口由应用保留并按任务版本选择原定义。迁移由明确的源与目标版本运行可重试转换。`SaveNS` 会拒绝非空 GraphID 冲突，变更身份需提供显式的原子迁移入口，在同一任务租约内比较存档世代再提交。验证保留问题 ID、已提交工具账及历史归属，再安排旧版本下线。按执行 namespace 查询作为可选诊断过滤，owner 和会话界定访问范围，任务和 agent 继续用作历史过滤。

## 实施顺序建议

1. **Q08、Q13**已有明确的复现与边界。先测真实 Claude 前缀变更开销，同时落实耐久历史的写入失败策略与按提交状态送模的质量门槛。
2. **Q01、Q11、Q14**以一个后端和一组只读工具串起 conformance、权限与审批。需要持久副作用或发布跨版本续跑的业务，再按 **Q12、Q27** 验重放及迁移。
3. **Q17**拿实际查询与保留负载选阈值。**Q06、Q18**先选一类业务对照测收益。**Q05**等两家适配器出现同类表示缺口再演进协议。

## 复跑入口

运行 SQL 集成需配置本地测试库的 `CREW_SQL_POSTGRES_DSN` 和 `CREW_SQL_MYSQL_DSN`。Redis 的实库用例读取 `CREW_TEST_REDIS_ADDR`，Plan Web 读取 `PLAN_WEB_TEST_REDIS_URL`。Checkpoint 租约过期测试用 miniredis 加速时钟，AG-UI 的租约令牌用例走实际配置的后端。

```bash
make test
go test -tags=integration -timeout=600s -run '^TestAdv_.*_Mock$' ./example/tutorial
go -C sql test -race -tags=integration -count=1 -timeout=5m -run '^Test(Postgres|MySQL)Integration$' .
go -C redis test -race -run '^TestRedis(Checkpointer_(Conformance|TaskLeases|MigrateLegacyKeys.*)|Store_Conformance|AGUISessionStoreFencesBothTasks|AGUICancellationSurvivesProcessExit)$' .
go test -race ./example/orchestration/plan_chat_web -run '^TestPlanWebSharedSessionLifecycle$'
```

本次对照核读本地 Eino `4a4b7186`、LangGraph `c6335a425`、OpenAI Agents SDK `6c4e4a29` 和 Google ADK `2b90353` 的相关接口与路径。行业做法以各节链接的上游文档为依据。
