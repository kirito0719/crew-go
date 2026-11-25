package agui_test

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"crew-go/agent_v2"
	"crew-go/agui"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
	"crew-go/tool"
)

type evictionAbortCP struct {
	*failAbortCleanupCP
	wantSID string
	badCtx  bool
}

func (c *evictionAbortCP) DeleteSubtree(ctx context.Context, sid, ns string, condition ...compose_v2.CheckpointDeleteCondition) (int32, error) {
	if sid != c.wantSID {
		return c.InMemoryCheckpointer.DeleteSubtree(ctx, sid, ns, condition...)
	}
	deadline, bounded := ctx.Deadline()
	if ctx.Err() != nil || compose_v2.GetOwner(ctx) != "test-owner" || compose_v2.GetThreadID(ctx) != "old" || !bounded || time.Until(deadline) <= 0 || time.Until(deadline) > 11*time.Second {
		c.badCtx = true
	}
	return c.failAbortCleanupCP.DeleteSubtree(ctx, sid, ns, condition...)
}

func TestHandler_EvictionCompletesTaskHistory(t *testing.T) {
	for _, tc := range []struct {
		name                                string
		byTTL, customOwner                  bool
		cancelled, retry, withoutCheckpoint bool
	}{
		{name: "cancelled_capacity", cancelled: true},
		{name: "cancelled_TTL_other_owner", cancelled: true, byTTL: true},
		{name: "cancelled_capacity_custom_history_owner", cancelled: true, customOwner: true},
		{name: "cancelled_TTL_custom_history_owner", cancelled: true, byTTL: true, customOwner: true},
		{name: "capacity", customOwner: true},
		{name: "TTL_other_owner", byTTL: true, customOwner: true},
		{name: "capacity_retry", customOwner: true, retry: true},
		{name: "capacity_without_checkpoint", customOwner: true, withoutCheckpoint: true},
		{name: "TTL_without_checkpoint", byTTL: true, customOwner: true, withoutCheckpoint: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				hs := &localReplacementHistory{HistoryStore: agent_v2.NewInMemoryHistoryStore()}
				if tc.retry {
					hs.failFirst = 1
				}
				cp := &evictionAbortCP{failAbortCleanupCP: &failAbortCleanupCP{InMemoryCheckpointer: compose_v2.NewInMemoryCheckpointer()}}
				runs := 0
				tk := tool.NewToolkit()
				tk.Register(tool.NewFuncTool("transfer", "transfer", map[string]any{"type": "object"}, func(context.Context, map[string]any) (*tool.Result, error) { runs++; return tool.Success("done"), nil }))
				var opts []agui.Option
				if !tc.withoutCheckpoint {
					opts = append(opts, agui.WithCheckpointer(cp))
				}
				if tc.byTTL {
					opts = append(opts, agui.WithSessionTTL(time.Second))
				} else {
					opts = append(opts, agui.WithMaxSessions(1))
				}
				historyOwner := "test-owner"
				historyOpt := agent_v2.WithHistory(hs, "tenant")
				if tc.customOwner {
					historyOwner = "history-test-owner"
					historyOpt = agent_v2.WithHistory(hs, "", agent_v2.WithHistoryOwnerFunc(func(ctx context.Context) string {
						owner, _ := ctx.Value(testOwnerKey{}).(string)
						return "history-" + owner
					}))
				}
				h := ownerAwareHandler(func(hooks hook.Hooks) agent_v2.Agent {
					return agent_v2.NewBasicAgent(agent_v2.WithName("evict"), agent_v2.WithModel(transferModel{}), agent_v2.WithToolkit(tk), agent_v2.WithApprovalMode(agent_v2.ApprovalAsk), historyOpt, agent_v2.WithHooks(hooks))
				}, opts...)
				iid := findInterruptID(parseSSE(postAs(h, "test-owner", `{"threadId":"old","messages":[{"role":"user","content":"转账"}]}`).Body.String()))
				if iid == "" {
					t.Fatal("no interrupt")
				}
				sid := cp.sid
				cp.wantSID = sid
				key := agent_v2.HistoryKey{Owner: historyOwner, ConvID: "old"}
				load := func() []*schema.Message {
					t.Helper()
					history, err := agent_v2.LoadAllHistory(t.Context(), hs, key)
					if err != nil {
						t.Fatal(err)
					}
					return history
				}
				checkpointExists := func() bool {
					t.Helper()
					exists, err := cp.ExistsNS(t.Context(), sid, "")
					if err != nil {
						t.Fatal(err)
					}
					return exists
				}
				oldHistory := load()
				if !hasToolCall(oldHistory, "c1") || (!tc.withoutCheckpoint && !checkpointExists()) {
					t.Fatal("旧任务应留下工具调用和已配置的存档")
				}
				if tc.withoutCheckpoint {
					for _, msg := range oldHistory {
						if task, _ := msg.Annotations[agent_v2.HistoryTaskAnnotation].(string); task != "" {
							sid = task
							break
						}
					}
					if sid == "" {
						t.Fatal("旧历史应记录任务身份")
					}
				}
				trigger := func() {
					t.Helper()
					newOwner := "test-owner"
					if tc.byTTL {
						time.Sleep(time.Second + time.Nanosecond)
						newOwner = "other-owner"
					}
					resp := postAs(h, newOwner, `{"threadId":"new","messages":[{"role":"user","content":"hello"}]}`)
					if outcomeType(parseSSE(resp.Body.String())) != "success" {
						t.Fatalf("触发回收的新线程应成功完成：%s", resp.Body)
					}
				}
				cp.fail = true
				if tc.cancelled {
					body := postAs(h, "test-owner", `{"threadId":"old","resume":[{"interruptId":"`+iid+`","status":"cancelled"}]}`).Body.String()
					if !strings.Contains(body, `"code":"abort_failed"`) {
						t.Fatalf("cleanup did not fail: %s", body)
					}
				} else if tc.retry {
					trigger()
					if !checkpointExists() || hs.attempts.Load() != 0 {
						t.Fatal("回收删档失败时应保留原档，历史补写应为零次")
					}
				}
				cp.fail = false
				trigger()
				if !tc.withoutCheckpoint && checkpointExists() {
					t.Fatal("回收应删除旧任务存档")
				}
				answer := `{"threadId":"old","resume":[{"interruptId":"` + iid + `","status":"resolved","payload":true}]}`
				if tc.retry {
					if hasToolResult(load(), "c1") || hs.attempts.Load() != 1 {
						t.Fatal("首次补写失败后应保留待收尾状态")
					}
					resp := postAs(h, "test-owner", answer)
					if resp.Code != http.StatusConflict || !strings.Contains(resp.Body.String(), `"code":"cleanup_pending"`) {
						t.Fatalf("已清档任务应拒绝迟到的审批：%d %s", resp.Code, resp.Body)
					}
					trigger()
				}
				results := 0
				for _, msg := range load() {
					if schema.GetToolResultID(msg) == "c1" {
						results++
						if msg.Annotations[agent_v2.HistoryTaskAnnotation] != sid {
							t.Fatalf("补写结果的任务身份不符：%v", msg.Annotations)
						}
					}
				}
				if results != 1 || runs != 0 {
					t.Fatalf("回收应为原历史补写一个结果且不执行工具：结果=%d 执行=%d", results, runs)
				}
				if !tc.withoutCheckpoint {
					resp := postAs(h, "test-owner", answer)
					if resp.Code != http.StatusNotFound || !strings.Contains(resp.Body.String(), `"code":"no_session"`) {
						t.Fatalf("完成回收后应移除原挂起入口：%d %s", resp.Code, resp.Body)
					}
				}
				if cp.badCtx {
					t.Fatal("cleanup lost victim owner/thread identity or independent deadline")
				}
			})
		})
	}
}
