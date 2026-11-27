package agent_v2_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
)

type historyAttemptModel struct {
	requests [][]*schema.Message
}

func (m *historyAttemptModel) Generate(_ context.Context, messages []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.requests = append(m.requests, append([]*schema.Message(nil), messages...))
	switch len(m.requests) {
	case 1:
		return schema.AssistantMessage("BAD-待否决的方案"), nil
	case 2:
		return schema.AssistantMessage("通过校验的方案"), nil
	default:
		return schema.AssistantMessage("后续任务"), nil
	}
}

func (m *historyAttemptModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}

func (m *historyAttemptModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

func TestHistoryProjectsOnlyCommittedRerunnableAttempts(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	cp := compose_v2.NewInMemoryCheckpointer()
	history := agent_v2.NewInMemoryHistoryStore()
	ctx = compose_v2.WithThreadIDCtx(ctx, "plan-history")
	ctx = compose_v2.WithCheckpointerCtx(agent_v2.WithHistoryCtx(ctx, history, "owner"), cp)
	m := &historyAttemptModel{}
	planner := agent_v2.NewBasicAgent(agent_v2.WithName("planner"), agent_v2.WithModel(m))
	parent := rerunHost(t, func(ctx context.Context, input *schema.Message) (string, error) {
		events, err := agent_v2.RunSubAgent(ctx, "planner", planner, input, agent_v2.WithRerunnable())
		return nestedFinal(events), err
	})
	sess := agent_v2.NewRunSession()
	if _, err := parent.Run(ctx, sess, schema.UserMessage("请拟计划")); err == nil {
		t.Fatal("首个方案应被父图否决")
	}
	cold := reopenDirectChildStore(t, ctx, cp, sess.RunID)
	ctx = compose_v2.WithCheckpointerCtx(ctx, cold)
	events, err := parent.Resume(ctx, sess.RunID)
	if err != nil || nestedFinal(events) != "通过校验的方案" {
		t.Fatalf("续跑应生成可用方案，实际 %q，错误 %v", nestedFinal(events), err)
	}
	if _, err := parent.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("检查历史")); err != nil {
		t.Fatal(err)
	}
	if len(m.requests) != 3 {
		t.Fatalf("应调用模型三次，实际 %d 次", len(m.requests))
	}
	if prompt := dumpHist(m.requests[1]); strings.Contains(prompt, "BAD-待否决的方案") {
		t.Fatalf("重做时读取到了被否决的方案：%s", prompt)
	}
	if prompt := dumpHist(m.requests[2]); strings.Contains(prompt, "BAD-待否决的方案") ||
		!strings.Contains(prompt, "通过校验的方案") {
		t.Fatalf("下个任务应只读取通过校验的方案：%s", prompt)
	}
	raw, err := agent_v2.LoadAllHistory(ctx, history, agent_v2.HistoryKey{Owner: "owner", ConvID: "plan-history"})
	if err != nil || !strings.Contains(dumpHist(raw), "BAD-待否决的方案") {
		t.Fatalf("原始历史应保留被否决的方案，实际 %s，错误 %v", dumpHist(raw), err)
	}
	fork, err := agent_v2.LoadCommittedHistory(ctx, history, agent_v2.HistoryKey{Owner: "owner", ConvID: "plan-history"})
	if view := dumpHist(fork); err != nil || strings.Contains(view, "BAD-待否决的方案") || !strings.Contains(view, "通过校验的方案") {
		t.Fatalf("分叉前文应保留通过校验的方案，实际 %s，错误 %v", view, err)
	}
}
