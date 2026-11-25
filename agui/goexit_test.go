package agui_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/agui"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

// TestServeRun_GoexitStillSettles 钉住：`ag.Run` 里发生 `runtime.Goexit` 时，前端仍拿到
// 终态事件、cp 不漏。
//
// 业务钩子可以通过 testify.Require 的失败分支或 Zap 的 fatal hook 调用 Goexit。
// 它**跳过所有后续普通语句**，而
// `ServeHTTP` 的兜底用 `recover()`——**Goexit 下 recover 返 nil**，那道兜底整个不触发。
//
// 修之前实测：事件序列停在 `RUN_STARTED`、客户端 `unexpected EOF`（违反 AG-UI 生命周期
// 契约），且每次漏一份**没有回收路径**的 cp——`storeInterrupt` 没跑 ⟹ 空壳 entry 的
// `sess == nil` ⟹ LRU 淘汰它时取不到 sid、连 cp 一起清不掉。
//
// a2a 那侧结构上免疫（收尾跑在另一条 goroutine），agui 是 inline 调用所以要显式补。
func TestServeRun_GoexitStillSettles(t *testing.T) {
	cp := compose_v2.NewInMemoryCheckpointer()
	h := ownedHandler(func(hooks hook.Hooks) agent_v2.Agent {
		return agent_v2.NewBasicAgent(agent_v2.WithName("x"),
			agent_v2.WithModel(&goexitModel{}), agent_v2.WithHooks(hooks),
			agent_v2.WithMaxRounds(2))
	}, agui.WithCheckpointer(cp))

	// **必须跑在独立 goroutine 上**：`runtime.Goexit` 会终止当前 goroutine，在测试主
	// goroutine 上直接调 ServeHTTP 会让 test runner 崩（"test executed panic(nil) or
	// runtime.Goexit"）。真实部署里 handler 跑在 net/http 的连接 goroutine 上，用真
	// server 才是那个形状。
	srv := httptest.NewServer(h)
	defer srv.Close()
	post := func(body string) []map[string]any {
		resp, err := http.Post(srv.URL, "application/json", strings.NewReader(body))
		if err != nil {
			return nil // 连接被掐断（正是 Goexit 未收尾时的现象）
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return parseSSE(string(b))
	}
	ev := post(`{"threadId":"t1","runId":"r1","messages":[{"role":"user","content":"hi"}]}`)
	types := eventTypes(ev)

	// 1) 生命周期必须闭合——只有 RUN_STARTED 就是半截流。
	hasTerminal := false
	for _, ty := range types {
		if ty == "RUN_ERROR" || ty == "RUN_FINISHED" {
			hasTerminal = true
		}
	}
	if !hasTerminal {
		t.Errorf("Goexit 之后没有终态事件，前端收到半截流：%v", types)
	}

	// 2) cp 不能漏——那份档谁也够不着（Status=failed 列不出来、sess==nil 让 LRU 清不了）。
	//
	// sid 是服务端铸的，测试里拿不到；改从「后续同 threadId 的 run 能否正常跑」侧面验：
	// 空壳 entry 若没清掉，它占着那个会话键且 sess==nil。
	ev2 := post(`{"threadId":"t1","runId":"r2","messages":[{"role":"user","content":"再来"}]}`)
	if code := findErrorCode(ev2); code == "busy" {
		t.Error("Goexit 留下的空壳 entry 没清——同 threadId 的后续请求被判成 busy")
	}
}

// goexitModel：Generate 里直接 Goexit（模拟业务 hook 里的 fatal）。
type goexitModel struct{}

func (m *goexitModel) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	runtime.Goexit()
	return nil, nil
}

func (m *goexitModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	runtime.Goexit()
	return nil, nil
}

func (m *goexitModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

type exitAfterInterrupt struct {
	hook.NoOpHooks
	id string
}

func (h *exitAfterInterrupt) OnAgentEnd(ctx context.Context, _ *hook.AgentInfo, _ *hook.AgentOutput) context.Context {
	if interrupted, ok := compose_v2.IsInterrupt(agent_v2.GetEndError(ctx)); ok {
		h.id = interrupted.AllQueries()[0].InterruptID
		runtime.Goexit()
	}
	return ctx
}

func TestServeRun_GoexitPreservesLocalHistoryCleanup(t *testing.T) {
	cp := &failAbortCleanupCP{InMemoryCheckpointer: compose_v2.NewInMemoryCheckpointer(), fail: true}
	history := &localReplacementHistory{HistoryStore: agent_v2.NewInMemoryHistoryStore(), failFirst: 1}
	exit := &exitAfterInterrupt{}
	toolRuns := 0
	kit := tool.NewToolkit()
	kit.Register(tool.NewFuncTool("transfer", "转账", map[string]any{"type": "object"}, func(context.Context, map[string]any) (*tool.Result, error) {
		toolRuns++
		return tool.Success("已转账"), nil
	}))
	oldKey := agent_v2.HistoryKey{Owner: "old-history-owner", ConvID: "old"}
	base := ownerAwareHandler(func(hooks hook.Hooks) agent_v2.Agent {
		return agent_v2.NewBasicAgent(agent_v2.WithName("goexit"), agent_v2.WithModel(transferModel{}),
			agent_v2.WithToolkit(kit), agent_v2.WithApprovalMode(agent_v2.ApprovalAsk), agent_v2.WithHooks(hook.NewChainHooks(hooks, exit)))
	}, agui.WithCheckpointer(cp))
	withHistory := func(store agent_v2.HistoryStore, owner string) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			base.ServeHTTP(w, r.WithContext(agent_v2.WithHistoryCtx(r.Context(), store, "",
				agent_v2.WithHistoryOwnerFunc(func(context.Context) string { return owner }))))
		})
	}
	old := withHistory(history, oldKey.Owner)
	newHistory := agent_v2.NewInMemoryHistoryStore()
	next := withHistory(newHistory, "new-history-owner")
	req := httptest.NewRequestWithContext(context.WithValue(t.Context(), testOwnerKey{}, "owner"), http.MethodPost, "/agui",
		strings.NewReader(`{"threadId":"old","runId":"start","messages":[{"role":"user","content":"转账"}]}`))
	resp := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		defer close(done)
		old.ServeHTTP(resp, req)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("异常运行未退出")
	}
	sid := cp.sid
	if exit.id == "" || sid == "" || !strings.Contains(resp.Body.String(), `"code":"agent_error"`) {
		t.Fatalf("需要先挂起再异常退出，task=%q interrupt=%q response=%s", sid, exit.id, resp.Body)
	}
	load := func(store agent_v2.HistoryStore, key agent_v2.HistoryKey) []*schema.Message {
		t.Helper()
		msgs, err := agent_v2.LoadAllHistory(t.Context(), store, key)
		if err != nil {
			t.Fatal(err)
		}
		return msgs
	}
	exists := func() bool {
		t.Helper()
		found, err := cp.ExistsNS(t.Context(), sid, "")
		if err != nil {
			t.Fatal(err)
		}
		return found
	}
	if !exists() || !hasToolCall(load(history, oldKey), "c1") || history.attempts.Load() != 0 {
		t.Fatal("失败的删档应保留原档和工具声明")
	}
	answer := `{"threadId":"old","resume":[{"interruptId":"` + exit.id + `","status":"resolved","payload":true}]}`
	late := postAs(next, "owner", answer)
	if late.Code != http.StatusConflict || !strings.Contains(late.Body.String(), `"code":"cleanup_pending"`) {
		t.Fatalf("终态任务应拒绝迟到的审批，实际 %d %s", late.Code, late.Body)
	}
	cp.fail = false
	replacement := `{"threadId":"old","runId":"next","messages":[{"role":"user","content":"查余额"}]}`
	first := postAs(next, "owner", replacement)
	if first.Code != http.StatusConflict || !strings.Contains(first.Body.String(), "cleanup_pending") || exists() ||
		history.attempts.Load() != 1 || hasToolResult(load(history, oldKey), "c1") {
		t.Fatalf("首次补写失败应保留已清档进度并阻止新运行，实际 %d %s", first.Code, first.Body)
	}
	success := postAs(next, "owner", replacement)
	if outcomeType(parseSSE(success.Body.String())) != "success" {
		t.Fatalf("重试应收尾并继续新任务，实际 %s", success.Body)
	}
	closed := 0
	for _, msg := range load(history, oldKey) {
		if schema.GetToolResultID(msg) == "c1" {
			closed++
			if msg.Annotations[agent_v2.HistoryTaskAnnotation] != sid {
				t.Fatalf("补写应该归属旧任务，实际 %+v", msg.Annotations)
			}
		}
	}
	newRecords := load(newHistory, agent_v2.HistoryKey{Owner: "new-history-owner", ConvID: "old"})
	if closed != 1 || toolRuns != 0 || len(newRecords) == 0 || hasToolResult(newRecords, "c1") {
		t.Fatalf("应只向原历史补写，收尾数=%d 执行数=%d 新记录=%+v", closed, toolRuns, newRecords)
	}
}
