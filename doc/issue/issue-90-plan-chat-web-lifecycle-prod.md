# ISSUE-90 · Plan Web 会话生命周期

**状态**　已实现会话查询、Redis 多副本续跑、删除和临时会话。

## 配置

`go run ./example/orchestration/plan_chat_web -mock` 使用模拟模型。Redis URL 为空时，会话存储使用进程内存。
部署多个副本时，各副本配置相同的 `PLAN_WEB_REDIS_URL`（也可用 `-redis-url`）。会话元数据和 Checkpointer 使用同一个 Redis，
执行、续跑和删除共用任务租约。Redis 模式在超步边界同步保存进度。生产配置应开启 Redis 持久化。
经 HTTPS 反向代理部署时，可加 `-secure-cookie`。 [Redis 接入](../../redis/doc.go)

## HTTP 接口

打开首页或开始第一场会话时服务端设置 `crew_plan_owner` Cookie。浏览器和 API 客户端
凭同一个 Cookie 查找会话，另一浏览器的 Cookie 对该会话得到 404。应用需要组织级账号时，
由部署网关管理身份和访问权限。
会话详情的成功响应和 404 均带 `Cache-Control: no-store`，防止代理把一次浏览器的查询结果用于其他请求。

| 接口 | 行为 |
| --- | --- |
| `GET /api/chat?q=...` | 开启 SSE 会话，可加 `ephemeral=true` |
| `GET /api/chat?session_id=...&action=continue` | 从存档续跑，`action=abort` 清档并标为 cancelled |
| `POST /api/interrupt?session_id=...` | 标记暂停，执行该任务的副本在下一次图内钩子检查时处理 |
| `GET /api/sessions?status=interrupted&limit=20` | 查询本 Cookie 的会话索引，按创建时间倒序。limit 最大 100 |
| `GET /api/sessions/{id}` | 查询状态、模式、时间和历史计数 |
| `DELETE /api/sessions/{id}` | 删除会话和存档。任务仍持有执行许可时返回 409，成功返回 204 |

确认清档后，终态默认保留 60 分钟。Redis 使用键的过期时间，内存模式使用 sweeper。挂起任务保留供续跑。
临时会话确认清档后即从索引移除。终态会话仍在索引时，Resume 返回 410。

## 恢复边界

会话索引保存状态及时间，SSE 事件由当前连接消费。进度队列满时可以丢弃增量。连接仍可写时，
已排队的进度事件发完后会发送请求结果和 `done`。
续跑因请求取消退出且允许恢复时，服务端按实际 Checkpoint
核对可续工作和提问身份。读取失败或已有未确认的续跑准入时保留存档，会话状态提交后报告 `resume_unconfirmed`。
持有会话 ID 的客户端可以先发起核对或显式放弃。
核对得到普通可续档时发布当前挂起状态，再由下一次请求提交答复和继续指令。
Agent 成功返回后，Plan Web 在任务租约内先保存含历史计数的完成凭据，再将会话更新为 `success` 和
`cleanup_pending`，最后清理存档并确认清理结果。进程在其间退出时，其他实例可通过详情查询或续跑请求核对
完成凭据和会话记录，补写终态或补清存档。已确认成功的会话拒绝继续执行和放弃，返回 410。
补清期间通过同一任务租约排除重叠操作，待清理的记录保留到确认完成。

若前一次未提交终态且缺少完成凭据，取得执行许可的请求核对原存档。普通可续档可从保存的进度恢复，
续跑准入仍未确认的情况继续报告 `resume_unconfirmed`。
旧版遗留的 `running` 记录若已经丢失存档，续跑时报告 `error`。
外部工具应按自己的幂等键处理重试。 [Plan Web 冒烟](../description/smoke-test-recipes.md#2-plan_chat_web-mock--hitl-resume30-秒)
