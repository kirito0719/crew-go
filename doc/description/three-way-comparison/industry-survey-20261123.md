# Agent 框架、运行时与论文增量调查（2026-11-23）

本轮接续 [11 月 12 日调查](industry-survey-2026q4.md)，核对上次快照至北京时间 2026-11-23 的远端代码。
论文补读范围为 2026-10 至 11 月。版本状态以官方 release 为准，运行行为对照本地源码、提交差异和测试。
crew-go 调研基线是 `85a140b`。此后 `bb3749c` 已合入失败模型调用的用量统计、评测逐轮记录和可取回的上下文折叠，`7ebad49` 已修复 AutoCompact 并发历史边界。业务样本验收和后续实验见[项目待办](../../code/todo.md)。

本轮更值得跟进的是可恢复的上下文、跨多轮执行的成本统计、多次运行的行为评测，以及后台任务的控制和授权。
crew-go 已有持久后台子 Agent、AG-UI 回执与恢复、嵌套后台工具、上下文清理和路由校准。本轮建议沿这些入口补验证和能力。
[后台子 Agent](../../v2/durable-background-agent.md) · [现行待办](../../code/todo.md) · [近期待办核验](../../code/backlog-audit.md)

## 一、本地参考仓同步

对 15 个已有参考仓执行同步，14 个成功取得远端状态。其中 11 个 checkout 快进，eino 稳定分支和 Grok Build 已对齐，
AgentScope 更新远端引用。MAF 的 fetch 返回认证读取失败，公开仓地址核对也返回 404，本次保留原有快照。

源码根目录为 `/Users/gupengfei/program/go/src`。下表的起点是同步前的跟踪引用。新增数由
`git rev-list --count OLD..NEW` 计算，包含 merge 带入的历史。它表示新到达的提交，不能直接换算为新功能数量。

| 仓库与分支 | 原快照 → 本次引用 | 新到达提交 | 同步结果 |
| --- | --- | --- | --- |
| langgraph `main` | [`b4cb0f19e` → `c6335a425`](https://github.com/langchain-ai/langgraph/compare/b4cb0f19e3d8984e8a57f85667b8d1bec018f0ab...c6335a4255dc1d8abc25a7677f9c072c7ab27b9c) | 33 | checkout 已快进 |
| eino `main` | [`4a4b71866` → `4a4b71866`](https://github.com/cloudwego/eino/commit/4a4b7186693d9805539c7f0c36db7d03a3485c6a) | 0 | checkout 已对齐，无新增 |
| langchain `master` | [`771d11ba6` → `8ff3cd30e`](https://github.com/langchain-ai/langchain/compare/771d11ba650b6fc43306d8f303e574814e433804...8ff3cd30eebfe37712a91f11e3f4b61a545e7a37) | 82 | checkout 已快进 |
| adk-python `main` | [`9620b4296` → `2b90353cd`](https://github.com/google/adk-python/compare/9620b42965305294dba886ff38757dae3478c401...2b90353cd2f1ee0a8a52262a7843333b6f7d2c8d) | 83 | checkout 已快进 |
| MAF `main` | `22b5e752b` | — | fetch 失败，公开地址返回 404 |
| openai-agents-python `main` | [`64fbde5d0` → `6c4e4a29c`](https://github.com/openai/openai-agents-python/compare/64fbde5d02b42ae5f9db668eb0184cdb3fc9d775...6c4e4a29cd08eef454db6980d74da6afc8939d24) | 30 | checkout 已快进 |
| crewAI `main` | [`7e4efa91c` → `56c26a7fb`](https://github.com/crewAIInc/crewAI/compare/7e4efa91c57a02646c9e7b7b7355d149f0783126...56c26a7fb3ece62c64de52e8b4228d15c9c81492) | 73 | checkout 已快进 |
| agentscope `origin/main` | [`3fef100ee` → `ba9e9d04e`](https://github.com/agentscope-ai/agentscope/compare/3fef100ee13d70905723d0000608bcce2d7e4a0d...ba9e9d04eb3788b3dc8d8148d27c0948a3b7f43e) | 94 | 远端引用已更新，保留本地分叉 |
| codex `main` | [`0702ebed4` → `e0a6d0532`](https://github.com/openai/codex/compare/0702ebed4d6878c40bb90e01ef33039b21c6e1fc...e0a6d0532deefa65eb7e86287fffadebbfda7591) | 594 | checkout 已快进 |
| deepseek-harness `master` | [`e8f7b93a3` → `4b399c39a`](https://github.com/deepseek-ai/deepseek-harness/compare/e8f7b93a344964de3dca1ace9e72b6e4d6de87cb...4b399c39a6d1479cd84ca591b4d7c88e24d38a96) | 2201 | checkout 已快进 |
| grok-build `main` | [`2bdd1d6a6` → `2bdd1d6a6`](https://github.com/xai-org/grok-build/commit/2bdd1d6a6369de0e8c68132ea4539e9abd9e14a8) | 0 | checkout 已对齐，无新增 |
| claude-agent-sdk-python `main` | [`3cf3446b0` → `318436039`](https://github.com/anthropics/claude-agent-sdk-python/compare/3cf3446b0fa6c97042e85b245bba07459f22f946...3184360393a92ca8a6ec7bbbdee571afd1ce1c56) | 17 | checkout 已快进 |
| pi `main` | [`a40abecaa` → `69a699348`](https://github.com/earendil-works/pi/compare/a40abecaaf39e5a11e6c4850ee5cd8ecda4958cc...69a6993486acee850d14a4a1547daa4a167228ad) | 314 | checkout 已快进 |
| ag-ui `main` | [`f6f6c5664` → `c31e1e819`](https://github.com/ag-ui-protocol/ag-ui/compare/f6f6c566466c92baf6a8067b5ff8aa059a2b18a9...c31e1e819e3a3c3b4af14add04f5b9a9a0208cae) | 158 | checkout 已快进 |
| otel-semconv-genai `main` | [`e6658f14c` → `4181b1ebe`](https://github.com/open-telemetry/semantic-conventions-genai/compare/e6658f14c9d9ea0760825f029cb82e7673f48fab...4181b1ebe55da7edb3538636c45ae64af369b5ef) | 18 | checkout 已快进 |

AgentScope 的本地 `main` 仍有 29 个自身提交，本轮分析读取 `origin/main` 的 `ba9e9d04e`。
eino 另同步 `origin/alpha/10`，从 `50c4becb` 变为 `269a188d`。该分支重写了历史，
两侧独有提交为 185 / 196。实际树差异为 30 个文件、1,593 行增加和 202 行删除，含稳定分支改动的合入。
alpha 的 HEAD 提交时间为 11 月 12 日，后文将它作为重新核对的预发布快照。
[alpha 树差异](https://github.com/cloudwego/eino/compare/50c4becb9f38f1704c3d28cde3d01f2ccd66c5ed..269a188d37822a00f0e93683058ff1ff5e8c39e2)

## 二、框架和协议的重点变化

### LangGraph 1.2.24 与 prebuilt 1.1.3

`Send` 派发的工具节点现在取得与普通边相同的、按节点输入 schema 转换的 state。另一处修复让
`wrap_tool_call` 对 state 的覆盖真正传给工具。两条修复共同说明，动态派发、钩子可见状态和工具实际收到的输入需要逐入口对账。
本地已核对 `pregel/_algo.py`、`prebuilt/tool_node.py` 及新增回归。
[动态工具状态](https://github.com/langchain-ai/langgraph/commit/292106d22302cbeb27725a61d91848bd0fd19459) · [钩子覆盖](https://github.com/langchain-ai/langgraph/commit/bbd52757e72d8611b07ae2831b9529a6ba03f2a1)

独立 thread 上调用的子图在调用方明确开启 `subgraphs` 流时仍向它交付事件。
DeltaChannel 补齐空 checkpoint 和清空消息的 reducer 行为。Postgres async saver 使用连接池时跳过实例级锁，
单次 store 读取也减少 pipeline 开销。这些优化各有适用的连接或状态条件。
[子图流](https://github.com/langchain-ai/langgraph/commit/ccfb0a1bf82ac6d56e909ca782779574362bb8c4) · [DeltaChannel](https://github.com/langchain-ai/langgraph/commit/8aea1fea7fb4d23344b5c3714ab86694f7744a26) · [连接池](https://github.com/langchain-ai/langgraph/commit/8fd97561d678341b979a4dccbc6c187c5bdc338b) · [1.2.24](https://github.com/langchain-ai/langgraph/releases/tag/1.2.24)

### eino 稳定版与 alpha

稳定版仍为 v0.9.23。重新取得的 alpha 快照包含 AGENTS.md 的每文件字节上限、总预算截断，以及多媒体
`FileID` 在工具结果转换中的往返。Runner 存档在中断信息和持久中断 state 保存相同字节时只保存一份，并在载入时恢复引用。
递归取消的 AgentTool 流也新增了恢复回归。这些都是与旧快照的实际树差异，其中部分源自更早的稳定分支提交。
[AGENTS.md 实现](https://github.com/cloudwego/eino/blob/269a188d37822a00f0e93683058ff1ff5e8c39e2/adk/middlewares/agentsmd/agentsmd.go) · [存档编码](https://github.com/cloudwego/eino/blob/269a188d37822a00f0e93683058ff1ff5e8c39e2/adk/interrupt.go) · [取消恢复测试](https://github.com/cloudwego/eino/blob/269a188d37822a00f0e93683058ff1ff5e8c39e2/adk/cancel_edge_test.go)

### Google ADK 2.15

`BufferedSessionService` 将事件写入按 session 排队，在后台批量提交，并为共享的 app/user state 保留跨会话顺序。
应用需要在报告本轮完成前调用 `flush_session`，关闭时调用 `aclose`。写失败、过期版本与重读的处理均有明确契约。
crew-go 的 HistoryStore 写入器已有异步追加和 drain，接入收益需从实际历史后端延迟与失败策略评估。
[2.15 发布](https://github.com/google/adk-python/releases/tag/v2.15.0) · [缓冲会话指南](https://github.com/google/adk-python/blob/2b90353cd2f1ee0a8a52262a7843333b6f7d2c8d/docs/guides/sessions/buffered_session_service/index.md) · [本仓历史写入](../../../agent_v2/history.go)

评测增加跨运行汇总，区分至少一次成功的 `pass@k` 和k 次均成功的 `pass^k`，将逐 case 汇总写进结果文件。
环境资源增加 shared/user/session 作用域并按作用域锁初始化。主分支另加入 GroundingPlugin，
可继续跟踪其证据核验接口。
[多次运行汇总](https://github.com/google/adk-python/commit/287c0b0) · [环境隔离](https://github.com/google/adk-python/commit/691ecbe) · [GroundingPlugin](https://github.com/google/adk-python/commit/9efe125)

crew-go 的 `trajectory_eval` 已有 `-runs`、逐 case 成功计数、`pass@k`、`pass^k`、P95、token 与单价成本估算。
后续可沿该入口加入复杂任务验收和逐轮结构化记录。 [现有统计](../../../example/decision/trajectory_eval/main.go)

### OpenAI Agents SDK 0.25 与 LangChain 1.5.2

OpenAI Agents 的会话提交保留尚未完成的异步工具调用，流式重试保留 pending input。
失败或未完成的 Responses 调用也会携带已消耗的 usage。后台等待、断线重试和计费因此有各自要保留的状态。
[0.25.0](https://github.com/openai/openai-agents-python/releases/tag/v0.25.0) · [异步工具会话](https://github.com/openai/openai-agents-python/commit/f48003a6) · [重试输入](https://github.com/openai/openai-agents-python/commit/9e05bd5b) · [失败用量](https://github.com/openai/openai-agents-python/commit/6e6707cd)

LangChain 1.5.2 为 HITL 合成的工具消息添加来源 metadata。
provider 适配器继续修复流式 Responses 工具参数和可重试的流内错误。这里可复用的做法是让合成内容携带可识别来源，
并把终帧、增量参数与错误恢复一起测试。
[1.5.2](https://github.com/langchain-ai/langchain/releases/tag/langchain==1.5.2) · [合成消息来源](https://github.com/langchain-ai/langchain/commit/c6f426a) · [流式参数](https://github.com/langchain-ai/langchain/commit/8978935)

### CrewAI 与 AgentScope

CrewAI 1.15.44 引入可版本化、可分享的文件 artifact，默认 Knowledge 存储转向本地 Qdrant，Chroma 成为可选 extra。
主分支随后将副作用型的 `save_artifact` 结果明确设为跳过缓存。Flow 还修复人工反馈恢复后的前序输出保留，以及共享声明中嵌套默认值的隔离。
[artifact](https://github.com/crewAIInc/crewAI/commit/94abb18e0) · [默认存储](https://github.com/crewAIInc/crewAI/commit/ea5616d03) · [副作用工具缓存](https://github.com/crewAIInc/crewAI/commit/70a31b667) · [Flow 恢复](https://github.com/crewAIInc/crewAI/commit/ea57411e1)

AgentScope 2.0.12 后的主分支使 MCP 工具说明按名称稳定排序，可由客户端决定是否信任服务端自报的 annotations。
运行时 headers 改变、连接关闭会使工具发现缓存失效。Steering 修复确认不确定时的输入保留。
这些变化分别落在 prompt 前缀稳定性、能力声明的可信来源和控制输入的交付上。
[MCP annotations](https://github.com/agentscope-ai/agentscope/commit/74d34ba5) · [工具排序](https://github.com/agentscope-ai/agentscope/commit/dae83ed9) · [发现缓存](https://github.com/agentscope-ai/agentscope/commit/54708bb3) · [Steering](https://github.com/agentscope-ai/agentscope/commit/fe7069e7)

### AG-UI

OpenAI Agents 适配器将审批 token 绑定到具体暂停它的 agent 和精确参数，在消费时核对过期，并允许共享已决审批。
LangGraph 的 continue 以 `input=None` 续最新 checkpoint 的待执行任务，输入变化时从 START 开始新 Run。
客户端可传的运行选项也收敛到允许列表。
TS client 将 reducer state 放到每个 subscription 内，避免订阅间串状态。这里的修复属于具体适配器和客户端。
[审批绑定](https://github.com/ag-ui-protocol/ag-ui/commit/9040277f) · [消费时过期校验](https://github.com/ag-ui-protocol/ag-ui/commit/f38615cd) · [checkpoint 续跑](https://github.com/ag-ui-protocol/ag-ui/commit/92c9f2e4) · [输入变化](https://github.com/ag-ui-protocol/ag-ui/commit/78fe0038) · [订阅隔离](https://github.com/ag-ui-protocol/ag-ui/commit/48ec882d)

### Codex、DeepSeek Harness、Pi 与 Claude Agent SDK

Codex 主分支在工具实际派发前重新核对共享 rollout 预算，子 agent 可显式开启 item 边界中断。
app-server 恢复时的请求重放加背压，并为子 agent 生命周期增加 OTel 观测。这些代码提供了预算检查点、
恢复队列和子任务统计的对照。
[工具前预算](https://github.com/openai/codex/commit/f226c9ccf) · [子任务中断](https://github.com/openai/codex/commit/cff061fca) · [恢复背压](https://github.com/openai/codex/commit/6633c6e09) · [子任务观测](https://github.com/openai/codex/commit/65454c7b9)

DeepSeek Harness 取得 `dsh-v0.2.3-alpha.2` 快照，阶段为预发布。窗口内修复共享子 agent 并发容量，
补充 Team 任务写入范围和 claim 诊断。同步中带入了较早分支的历史，因此表中的 2,201 个提交需结合具体日期阅读。
[共享容量](https://github.com/deepseek-ai/deepseek-harness/commit/d72d80bea7) · [任务 claim](https://github.com/deepseek-ai/deepseek-harness/commit/4ae3ccffba)

Pi 1.6.0 附近的 Durable API 增加 `onAbort: "keep"`，可在会话中止后保留排队输入。
Stop 时保留排队的子 agent 与 peer 消息，并修复 compaction 后超大会话的加载。
Claude Agent SDK Python 更新到 v0.2.187，暴露用户消息 UUID，修复权限更新的 CLI 线格式。
[Pi 队列策略](https://github.com/earendil-works/pi/commit/e67aa1a4e) · [Stop 输入](https://github.com/earendil-works/pi/commit/87661cf35) · [长会话](https://github.com/earendil-works/pi/commit/cb1205e46) · [Claude UUID](https://github.com/anthropics/claude-agent-sdk-python/commit/4720d36) · [权限更新](https://github.com/anthropics/claude-agent-sdk-python/commit/8ed58af)

### Strands 与 OTel 的补充对照

Strands 的官方仓发布页现指向 `harness-sdk`。本轮额外查阅 Python v1.65.0 和后续发布记录，
关注异步 agent builder、被过滤或拒绝的子任务作为工具错误返回，以及由 offload 条件决定内容处理粒度。
这些条目适合结合本仓后台子任务和工具结果转存做专项对照。
[Python 发布](https://github.com/strands-agents/harness-sdk/releases/tag/python/v1.65.0) · [异步 builder](https://github.com/strands-agents/harness-sdk/commit/3d741b4) · [Python offload](https://github.com/strands-agents/harness-sdk/commit/84d814b)

OTel GenAI 独立仓发布 v0.1.0，相关语义条目仍标为 Development。新增说明要求按操作或服务聚合 usage，
同一次 token 消耗可能出现在 agent 与 inference 等多类 span 上。主分支还将 prompt variables 设为结构化对象，
MCP 使用 `rpc.status_code`。跟进时需要核对本仓实际用到的字段和看板消费者。
[v0.1.0](https://github.com/open-telemetry/semantic-conventions-genai/releases/tag/v0.1.0) · [token 聚合说明](https://github.com/open-telemetry/semantic-conventions-genai/commit/afd3ff6b) · [prompt variables](https://github.com/open-telemetry/semantic-conventions-genai/commit/2e5c892a) · [MCP 状态](https://github.com/open-telemetry/semantic-conventions-genai/commit/7cb940f2)

## 三、新论文与可复用技术

本节六篇为 arXiv 预印本，日期采用首发标记，版本另列。实验结果均为作者报告，crew-go 的业务收益需要在本项目任务上复测。

| 论文与日期 | 方法和实验范围 | 对 crew-go 的意义 |
| --- | --- | --- |
| Folio，11-12，v1 | 运行中形成三层关联记录，L1 是本轮短注，L2 是调用清单，L3 是原始资料。运行时选择不同分辨率折叠旧上下文，模型通过稳定句柄取回原文。 | 可在现有摘要和 offload 上评估分层视图、按页恢复和缓存前缀稳定性。作者用单模型在 75 个 LOCA-bench 任务上报告 66 次成功，对照同运行时摘要方案为 54 次。 [论文](https://arxiv.org/abs/2611.16627) |
| Evaluating Authorization and Trust Boundaries，11-16，v1 | 用同一个 gpt-4o-mini 在四种框架上做 12 类实验，覆盖工具链授权、记忆污染和 agent 身份传播。受保护配置能阻止部分越权操作。 | 将授权主体、工具能力和检索内容来源分别核对，可为 Sandbox、MCP 和 Handoff 收窄提供攻击场景。结果适用于论文中的配置。 [论文](https://arxiv.org/abs/2611.21259) |
| WorkflowArena，11-09，v1 | 145 个任务、8 个领域，用隐藏 verifier 验集成交付，同时记录拆分、委派和协作。在 32 个困难任务子集上固定执行模型，比较不同来源的 workflow，使用连续分数度量收益。 | 行为评测可同时度量产出、派发次数、协作结构与成本，帮助判断采用哪个编排。 [论文](https://arxiv.org/abs/2611.12856) |
| Proactive Agentic Orchestration，10-09，v1 | POrchestra 允许子 agent 带状态、请求和证据主动申请调整任务、上下文、工具与预算，保持本地执行连续。OPOD 用回顾反馈训练编排决策。 | 后台任务的下一步可研究进度上报和有授权的调整。推理期通信和训练期 OPOD 各有接入条件。 [论文与代码入口](https://arxiv.org/abs/2610.13485) |
| AgentOrch，10-24，v2 为 10-29 | 650 个 SOP、3,220 个运行变体。Global 在看具体运行输入前生成一次 workflow 并跨变体复用，逐步方式每个变体都能看具体输入与执行反馈。所测模型上逐步方式准确率更高。 | 用既有 GraphAgent 和 PlanAgent 做同任务对照，量化减少规划调用后的正确率、调用数与延迟，再决定是否增加完整计划编译入口。 [论文](https://arxiv.org/abs/2610.33375) |
| REMORY，10-08，v3 为 10-21 | 在文本摘要后补一个有界的 soft memory token 序列，用训练后的记忆网络帮助冻结的 actor 近似全文上下文续写。 | 需要可接收 soft token 的推理后端与额外训练。可借它的评测维度核对证据归因、同任务同工具的重复输出和工具错误。重复输出的原定义排除规划与轮询。 [论文](https://arxiv.org/abs/2610.11287) |

Folio 的性能对照使用一个 benchmark、一个模型和价格结构，每条任务链只运行一次。closed harness 的成本没有同口径比较。
评估本仓方案时，保留历史原文和稳定引用后，分别测完成率、证据命中、重复调用、缓存 token、折叠耗时和恢复读取量。
[论文限制](https://arxiv.org/html/2611.16627v1)

## 四、对 crew-go 的跟进建议

这些建议按 `85a140b` 的现状提出，作为后续实施或实验的输入。现行待办已有相应主题。

| 建议顺序 | 建议与当前入口 | 完成条件 |
| --- | --- | --- |
| 1 | 在已有 `trajectory_eval -runs` 统计上扩充复杂任务验收和逐轮结构化结果，借鉴 WorkflowArena 的过程记录。 [现有统计](../../../example/decision/trajectory_eval/main.go) · [实际调用记录](../../../example/decision/trajectory_eval/runner.go) | 同一组业务输入固定模型、工具和判定器，保存逐轮结果、委派关系、可验证产物和资源消耗，再用现有 `pass@k`、`pass^k`、P95 与成本指标比较方案。业务路由标定继续沿 A03。 |
| 2 | 核对失败模型调用的 usage 载体。调研基线的 Responses `convert` 在非 completed 状态先返回错误，成功分支才把 usage 放入消息。对照 OpenAI SDK 的失败用量处理。 [转换实现](../../../model/openai/responses_stream.go) | 用 mock 注入已计费的 failed/incomplete 响应和随后成功的重试，验证错误保真、消费量保留、重试总量和 OTel 聚合。 |
| 3 | 在 MicroCompact、AutoCompact 和转存读取上验证 Folio 式分层恢复。已有稳定引用和摘要边界可复用，原文保留期限归 Q03/Q17。 [上下文压缩](../../v2/context-compaction.md) | 固定资料和任务，对比当前策略与分层视图。测证据可恢复性和实际成本，同时覆盖引用过期、首次写失败、跨 Run 及 Resume。 |
| 4 | 为后台子任务研究持续进度上报、证据记录和安全点上的授权调整。当前 `background.Manager` 已有派单、查询、审批、取消和完成通知，提问与补答可复用 `Ask` / `Answer`。 [当前接口](../../../agent_v2/background/task.go) | 子任务持续上报进度和证据，父任务的调整按版本和权限在安全点生效。与已有提问补答做对照，验证恢复身份、重复投递、旧请求、暂停和取消。关联 Q12、C18。 |
| 5 | 用 AgentOrch 的方法量化预生成图的收益。已有固定前驱汇合、图收尾屏障和 PlanAgent 可作基础。 [图执行](../../../compose_v2/graph_loop.go) · [PlanAgent](../../../agent_v2/plan_agent.go) | 固定 SOP 与运行变体，声明生成阶段能否看具体输入、workflow 跨变体的复用范围和成本摊销，再比较正确率、模型调用次数、超时和重试副作用。 |
| 6 | 本仓 MCP 已默认不信任 annotations，显式信任后才采用只读提示。结合授权边界论文和 ADK 的 EnvironmentScope 核对跨委派、缓存与审批。 [MCP 只读判定](../../../mcp/tool_adapter.go) · [Sandbox](../../v2/sandbox.md) · [Handoff](../../v2/handoff.md) | 用两个调用方验证工具目录缓存、审批和任务归属。模拟 MCP 自报只读、headers 更新、委派来源变化及记忆污染，核对实际执行边界。关联 Q01/Q11/Q14、C18。 |

现有多次运行入口可用 `go run ./example/decision/trajectory_eval -runs 3` 复核确定性 fixture，
接入业务评测时再使用真实候选模型和标注数据。
跨实例恢复、真实 provider 的计费及压缩收益沿既有验收条件推进。
[待办与验收范围](../../code/todo.md)

## 五、核验记录

本次记录各参考仓同步前后 SHA、分叉和失败，并读取相关实现、提交差异及对应回归。
论文核对 arXiv 摘要、正文方法、评测范围与版本。框架发布核对官方页面和源码版本。
对改动的两个 Markdown 文件做了相对路径、标题锚点与 `commentaudit` 增量校验。
文档分别复审源码与版本、论文实验口径、crew-go 当前能力，按原始证据修正。
现有 `trajectory_eval -runs 3` 的 mock 实跑结果为 `trials=12/12`、
`pass@3=4/4`、`pass^3=4/4`。该结果验证已有评测入口，真实模型和论文性能留待相同任务与环境复测。
