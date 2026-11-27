package agui_test

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
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

type checkpointRouteKey struct{}

type fixedCheckpointHandle struct{ compose_v2.Checkpointer }

type localCheckpointRouter struct {
	*compose_v2.InMemoryCheckpointer
	original    *replacementDeleteFailure
	useOriginal func(context.Context) bool
}

func (r *localCheckpointRouter) ResolveCheckpointPartition(ctx context.Context) (compose_v2.Checkpointer, error) {
	if r.useOriginal(ctx) {
		return fixedCheckpointHandle{r.original}, nil
	}
	return r.InMemoryCheckpointer, nil
}

type checkpointPartitionFixture struct {
	router   *localCheckpointRouter
	handler  http.Handler
	executed atomic.Int32
}

func newCheckpointPartitionFixture(opts ...agui.Option) *checkpointPartitionFixture {
	f := &checkpointPartitionFixture{router: &localCheckpointRouter{
		InMemoryCheckpointer: compose_v2.NewInMemoryCheckpointer(),
		original:             &replacementDeleteFailure{InMemoryCheckpointer: compose_v2.NewInMemoryCheckpointer()},
		useOriginal: func(ctx context.Context) bool {
			original, _ := ctx.Value(checkpointRouteKey{}).(bool)
			return original
		},
	}}
	kit := tool.NewToolkit()
	kit.Register(tool.NewFuncTool("transfer", "转账", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			f.executed.Add(1)
			return tool.Success("已转账"), nil
		}))
	f.handler = ownedHandler(func(hooks hook.Hooks) agent_v2.Agent {
		return agent_v2.NewBasicAgent(agent_v2.WithName("partition-agent"), agent_v2.WithModel(transferModel{}),
			agent_v2.WithToolkit(kit), agent_v2.WithApprovalMode(agent_v2.ApprovalAsk), agent_v2.WithHooks(hooks))
	}, append(opts, agui.WithCheckpointer(f.router))...)
	return f
}

func (f *checkpointPartitionFixture) suspend(t *testing.T, thread string) (string, string) {
	t.Helper()
	first := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := context.WithValue(r.Context(), checkpointRouteKey{}, true)
		f.handler.ServeHTTP(w, r.WithContext(ctx))
	})
	reply := postAGUI(first, fmt.Sprintf(`{"threadId":%q,"runId":"initial","messages":[{"role":"user","content":"转账"}]}`, thread))
	interruptID := findInterruptID(parseSSE(reply.Body.String()))
	sid := f.router.original.lastRoot
	if interruptID == "" || sid == "" || f.executed.Load() != 0 {
		t.Fatalf("首次运行应在原分区存档并等待审批，实际 %s", reply.Body)
	}
	if exists, err := f.router.InMemoryCheckpointer.ExistsNS(t.Context(), sid, ""); err != nil || exists {
		t.Fatalf("首次运行写入了另一分区，exists=%v err=%v", exists, err)
	}
	if err := f.router.InMemoryCheckpointer.SaveNS(t.Context(), sid, "", &compose_v2.Checkpoint{
		GraphID: "separate-partition",
	}); err != nil {
		t.Fatal(err)
	}
	return sid, interruptID
}

func (f *checkpointPartitionFixture) requireCheckpoints(t *testing.T, sid string, originalExists bool) {
	t.Helper()
	if exists, err := f.router.original.ExistsNS(t.Context(), sid, ""); err != nil || exists != originalExists {
		t.Errorf("原分区存档应存在=%t，实际 exists=%v err=%v", originalExists, exists, err)
	}
	other, err := f.router.InMemoryCheckpointer.LoadNS(t.Context(), sid, "")
	if err != nil || other == nil || other.GraphID != "separate-partition" {
		t.Errorf("另一分区的同名槽应保留，实际 checkpoint=%v err=%v", other, err)
	}
}

func TestHandler_ResumeUsesOriginalCheckpointPartition(t *testing.T) {
	f := newCheckpointPartitionFixture()
	sid, id := f.suspend(t, "resume-partition")

	out := resumeEntries(t, f.handler, "resume-partition", "resume", resolved(id, true))
	if outcomeType(out) != "success" || f.executed.Load() != 1 {
		t.Errorf("续跑应批准旧任务并执行工具一次，实际事件=%v 执行=%d", out, f.executed.Load())
	}
	f.requireCheckpoints(t, sid, false)
}

func TestHandler_ReplacementUsesOriginalCheckpointPartition(t *testing.T) {
	f := newCheckpointPartitionFixture()
	sid, _ := f.suspend(t, "replace-partition")
	f.router.original.failTask = sid

	failed := postAGUI(f.handler, `{"threadId":"replace-partition","runId":"replace-first","messages":[{"role":"user","content":"查余额"}]}`)
	out := parseSSE(failed.Body.String())
	if len(out) != 2 || out[0]["type"] != "RUN_STARTED" || out[1]["code"] != "agent_error" ||
		f.router.original.failures != 1 || f.executed.Load() != 0 {
		t.Errorf("原分区删档失败应阻止新运行，实际 %s，删档失败=%d 执行=%d",
			failed.Body, f.router.original.failures, f.executed.Load())
	}
	f.requireCheckpoints(t, sid, true)

	f.router.original.failTask = ""
	retried := postAGUI(f.handler, `{"threadId":"replace-partition","runId":"replace-retry","messages":[{"role":"user","content":"查余额"}]}`)
	if outcomeType(parseSSE(retried.Body.String())) != "success" || f.executed.Load() != 0 {
		t.Errorf("存储恢复后应能替换旧任务，实际 %s，执行=%d", retried.Body, f.executed.Load())
	}
	f.requireCheckpoints(t, sid, false)
}

func TestHandler_CancelUsesOriginalCheckpointPartition(t *testing.T) {
	f := newCheckpointPartitionFixture()
	sid, id := f.suspend(t, "cancel-partition")

	out := resumeEntries(t, f.handler, "cancel-partition", "cancel", map[string]any{"interruptId": id, "status": "cancelled"})
	if outcomeType(out) != "cancelled" || f.executed.Load() != 0 {
		t.Errorf("取消应收尾旧任务，实际事件=%v 执行=%d", out, f.executed.Load())
	}
	f.requireCheckpoints(t, sid, false)
}

func TestHandler_EvictionUsesOriginalCheckpointPartition(t *testing.T) {
	f := newCheckpointPartitionFixture(agui.WithMaxSessions(1))
	sid, _ := f.suspend(t, "evict-partition")

	next := postAGUI(f.handler, `{"threadId":"next","runId":"new","messages":[{"role":"user","content":"查余额"}]}`)
	if outcomeType(parseSSE(next.Body.String())) != "success" || f.executed.Load() != 0 {
		t.Errorf("容量淘汰后新任务应能执行，实际 %s，执行=%d", next.Body, f.executed.Load())
	}
	f.requireCheckpoints(t, sid, false)
}

type partitionModel func(context.Context) (*schema.Message, error)

func (m partitionModel) Generate(ctx context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	return m(ctx)
}

func (partitionModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}

func (m partitionModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

func TestHandler_DetachedTaskUsesItsOwnCheckpointPartition(t *testing.T) {
	independent := agent_v2.NewRunSession()
	router := &localCheckpointRouter{
		InMemoryCheckpointer: compose_v2.NewInMemoryCheckpointer(),
		original:             &replacementDeleteFailure{InMemoryCheckpointer: compose_v2.NewInMemoryCheckpointer()},
		useOriginal: func(ctx context.Context) bool {
			return compose_v2.GetSessionID(ctx) == independent.RunID
		},
	}
	const question = "继续独立任务吗？"
	side := agent_v2.NewBasicAgent(agent_v2.WithName("independent"),
		agent_v2.WithModel(partitionModel(func(ctx context.Context) (*schema.Message, error) {
			answer, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: question})
			if err != nil {
				return nil, err
			}
			return schema.AssistantMessage(answer.Content), nil
		})))
	var interruptID string
	outer := partitionModel(func(ctx context.Context) (*schema.Message, error) {
		ctx, cancel := context.WithTimeout(agent_v2.DetachTask(ctx), 5*time.Second)
		defer cancel()
		_, err := side.Run(ctx, independent, schema.UserMessage("独立输入"))
		if interrupt, ok := compose_v2.IsInterrupt(err); ok && interrupt.Query != nil && interrupt.Query.Question == question {
			interruptID = interrupt.Query.InterruptID
			return schema.AssistantMessage("主任务完成"), nil
		}
		return nil, fmt.Errorf("detached task did not pause: %v", err)
	})
	handler := ownedHandler(func(hooks hook.Hooks) agent_v2.Agent {
		return agent_v2.NewBasicAgent(agent_v2.WithName("outer"), agent_v2.WithModel(outer), agent_v2.WithHooks(hooks))
	}, agui.WithCheckpointer(router))

	reply := postAGUI(handler, `{"threadId":"outer","runId":"start","messages":[{"role":"user","content":"开始"}]}`)
	if interruptID == "" || !strings.Contains(reply.Body.String(), "主任务完成") ||
		outcomeType(parseSSE(reply.Body.String())) != "success" {
		t.Fatalf("主任务应完成，独立任务仍挂起，实际 %s", reply.Body)
	}
	fixed, fixedErr := router.InMemoryCheckpointer.ExistsNS(t.Context(), independent.RunID, "")
	selected, selectedErr := router.original.ExistsNS(t.Context(), independent.RunID, "")
	if fixed || !selected || fixedErr != nil || selectedErr != nil {
		t.Errorf("独立任务应只写入自己选中的分区，主分区=%v (%v)，独立分区=%v (%v)", fixed, fixedErr, selected, selectedErr)
	}

	ctx, cancel := context.WithTimeout(compose_v2.WithCheckpointerCtx(t.Context(), router), 5*time.Second)
	defer cancel()
	events, err := side.Resume(ctx, independent.RunID, agent_v2.WithResumeUserInputs(map[string]*schema.Message{
		interruptID: schema.UserMessage("继续"),
	}))
	if final := agent_v2.FinalMessage(events); err != nil || final == nil || final.Content != "继续" {
		t.Errorf("按独立任务身份应能从实际分区补答，实际 final=%v err=%v", final, err)
	}
}
