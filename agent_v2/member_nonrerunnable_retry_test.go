package agent_v2_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
	"crew-go/tool"
)

func TestNonrerunnableMemberEntryKeepsToolReceiptAcrossRetryAndResume(t *testing.T) {
	for _, tc := range []struct {
		name          string
		cancelAt      int
		defaultPolicy bool
		firstAttempts int
	}{
		{name: "否决直至重试耗尽", firstAttempts: 3},
		{name: "第二次判断取消", cancelAt: 2, firstAttempts: 2},
		{name: "默认策略取消", defaultPolicy: true, cancelAt: 1, firstAttempts: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, stop := context.WithTimeout(t.Context(), 5*time.Second)
			defer stop()
			cp := compose_v2.NewInMemoryCheckpointer()
			ctx = compose_v2.WithCheckpointerCtx(ctx, cp)
			first, cancel := context.WithCancel(ctx)
			defer cancel()
			writes := 0
			kit := tool.NewToolkit()
			kit.Register(tool.NewFuncTool("write_order", "登记订单", map[string]any{"type": "object"},
				func(context.Context, map[string]any) (*tool.Result, error) {
					writes++
					return rdMeta(tool.Success(fmt.Sprintf("receipt-%d", writes))), nil
				}))
			model := newScopeStub(oneToolCall("write_order"))
			cashier := agent_v2.NewBasicAgent(agent_v2.WithName("cashier"), agent_v2.WithModel(model), agent_v2.WithToolkit(kit))
			receipt := compose_v2.LastValue[*schema.Message]("receipt")
			b := compose_v2.NewBuilder().WithName("order-review")
			compose_v2.WithChannel(b, receipt)
			var inputs []string
			var accepted int
			resuming := false
			b.AddNode("accept", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
				accepted++
				return nil, nil
			})
			rejected := errors.New("order under review")
			var edgeOpts []compose_v2.CondOption
			if !tc.defaultPolicy {
				edgeOpts = append(edgeOpts, compose_v2.WithCondFailPolicy(compose_v2.CondFailRerunFrom))
			}
			b.AddConditionalEdge(compose_v2.StartNode, func(ctx context.Context, chs compose_v2.Channels) (compose_v2.NodeID, error) {
				msg, _ := compose_v2.Get(chs, receipt)
				inputs = append(inputs, msg.GetContent())
				if resuming {
					return "accept", nil
				}
				if len(inputs) == tc.cancelAt {
					cancel()
					return "", ctx.Err()
				}
				return "", rejected
			}, edgeOpts...).AddEdge("accept", compose_v2.EndNode)
			g, err := b.Compile()
			if err != nil {
				t.Fatal(err)
			}
			review := agent_v2.NewGraphAgent(g, agent_v2.WithGraphAgentName("reviewer"),
				agent_v2.WithGraphInputWriter(func(m *schema.Message) []compose_v2.ChannelWrite {
					return []compose_v2.ChannelWrite{compose_v2.Write(receipt, m)}
				}),
				agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
					msg, _ := compose_v2.Get(chs, receipt)
					return schema.AssistantMessage(msg.GetContent())
				}))
			seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("purchase"), agent_v2.WithSequentialRetryConfig(compose_v2.SimpleRetry(2))).
				AddStep("cashier", cashier).AddStep("review", review)
			session := agent_v2.NewRunSession()
			_, err = seq.Run(first, session, schema.UserMessage("创建订单"))
			var exhausted *compose_v2.RetriesExhaustedError
			if tc.cancelAt != 0 {
				if !errors.Is(err, context.Canceled) || errors.As(err, &exhausted) {
					t.Fatalf("应在重试中取消，实际 %v", err)
				}
			} else if !errors.Is(err, rejected) || !errors.As(err, &exhausted) {
				t.Fatalf("应耗尽成员的重试预算，实际 %v", err)
			}
			archives, err := cp.LoadAllInSession(ctx, session.RunID)
			if err != nil || len(archives) != 1 || archives[0].NS != "" || len(archives[0].Checkpoint.CurrentFrontier) != 1 ||
				archives[0].Checkpoint.CurrentFrontier[0].NodeID != "review" || archives[0].Checkpoint.Status != compose_v2.CPStatusPending ||
				len(inputs) != tc.firstAttempts || accepted != 0 || writes != 1 || model.count() != 1 {
				t.Fatalf("首次执行 后端错误=%v 存档=%+v 入口收到=%v 已接收=%d 工具写入=%d 模型=%d", err, archives, inputs, accepted, writes, model.count())
			}
			resuming = true
			events, err := seq.Resume(ctx, session.RunID)
			final := agent_v2.FinalMessage(events)
			const want = "原始目标：创建订单\n\n上一步（cashier）的产出：\nreceipt-1"
			if err != nil || final == nil || final.Role != schema.RoleAssistant || final.Content != want || accepted != 1 || writes != 1 || model.count() != 1 ||
				len(inputs) != tc.firstAttempts+1 || slices.ContainsFunc(inputs, func(s string) bool { return s != want }) {
				t.Fatalf("续跑 err=%v 最终回复=%v 入口收到=%v 已接收=%d 工具写入=%d 模型=%d", err, final, inputs, accepted, writes, model.count())
			}
			if remaining, err := cp.LoadAllInSession(ctx, session.RunID); err != nil || len(remaining) != 0 {
				t.Fatalf("成功后剩余存档=%v err=%v", remaining, err)
			}
		})
	}
}
