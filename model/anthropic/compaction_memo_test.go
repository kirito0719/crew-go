package anthropic_test

import (
	"context"
	"maps"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/model/anthropic"
	"crew-go/schema"
	"crew-go/store"
	"crew-go/tool"
)

const protectedReply = "有配对工具结果的历史答复"

type replayMemoHook struct {
	hook.NoOpHooks
	strip bool
}

func (h replayMemoHook) OnModelStart(ctx context.Context, info *hook.AgentInfo, in *schema.ModelInput) (context.Context, error) {
	for i, msg := range in.Messages {
		if msg.Content != protectedReply {
			continue
		}
		current := *msg
		current.Annotations = maps.Clone(msg.Annotations)
		current.Annotations["test_round"] = info.Round
		if h.strip && info.Round > 0 {
			delete(current.Annotations, anthropic.AssistantContentAnnotation)
		}
		in.Messages[i] = &current
	}
	return ctx, nil
}

type replayMemoArchive struct {
	store.Store
	puts atomic.Int32
}

func (s *replayMemoArchive) Put(ctx context.Context, ns store.Namespace, key string, value any, opts ...store.PutOption) error {
	s.puts.Add(1)
	return s.Store.Put(ctx, ns, key, value, opts...)
}

func TestBasicAgent_CompactionMemoHonorsClaudeReplayRemoval(t *testing.T) {
	for _, tc := range []struct {
		name string
		drop bool
	}{
		{name: "removed", drop: true},
		{name: "unchanged"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const (
				signature = "historical-thinking-signature"
				archived  = "只允许从归档索引读取的原文"
				lookupID  = "history-lookup"
				activeID  = "current-lookup"
				worker    = "claude-replay-memo"
				thread    = "replay-memo-history"
			)
			server, requests := newServer(t, func(w http.ResponseWriter, r *http.Request, n int) {
				switch n {
				case 1:
					replyJSON(w, http.StatusOK, message([]any{
						map[string]any{"type": "thinking", "thinking": "", "signature": signature},
						map[string]any{"type": "text", "text": protectedReply},
						map[string]any{"type": "tool_use", "id": lookupID, "name": "lookup", "input": map[string]any{}},
					}, "tool_use", nil))
				case 2:
					replySSE(w, sseFor([]any{
						map[string]any{"type": "tool_use", "id": activeID, "name": "lookup", "input": map[string]any{}},
					}, "tool_use")...)
				default:
					replySSE(w, sseFor([]any{map[string]any{"type": "text", "text": "已核对历史"}}, "end_turn")...)
				}
			})
			ctx := compose_v2.WithThreadIDCtx(t.Context(), thread)
			client := newClient(server)
			signed, err := client.Generate(ctx, []*schema.Message{schema.UserMessage("准备供应商签名快照")})
			if err != nil {
				t.Fatal(err)
			}
			keptResult := schema.ToolResultMessage(lookupID, "历史工具结果索引")
			keptResult.Annotations = map[string]any{agent_v2.MicroCompactOffloadedAnnotation: true}
			prior := []*schema.Message{
				schema.UserMessage("查历史记录"), signed, keptResult,
				schema.UserMessage(strings.Repeat("旧工具前文 ", 2400) + archived),
				schema.AssistantMessage("旧轮结论"),
			}
			history := agent_v2.NewInMemoryHistoryStore()
			key := agent_v2.HistoryKey{Owner: "tenant", ConvID: thread}
			for _, m := range prior {
				if m.Annotations == nil {
					m.Annotations = make(map[string]any)
				}
				m.Annotations[agent_v2.HistoryAgentAnnotation] = worker
			}
			if err := history.Append(ctx, key, prior); err != nil {
				t.Fatal(err)
			}
			archive := &replayMemoArchive{Store: store.NewInMemoryStore()}
			kit := tool.NewToolkit()
			var executions atomic.Int32
			kit.Register(tool.NewFuncTool("lookup", "查找资料", map[string]any{"type": "object"},
				func(context.Context, map[string]any) (*tool.Result, error) {
					executions.Add(1)
					return tool.Success("当前工具确实执行过"), nil
				}))
			a := agent_v2.NewBasicAgent(
				agent_v2.WithName(worker), agent_v2.WithModel(client), agent_v2.WithHistory(history, key.Owner),
				agent_v2.WithHooks(replayMemoHook{strip: tc.drop}), agent_v2.WithToolkit(kit),
				agent_v2.WithContextRecovery(&agent_v2.ContextRecoveryConfig{
					Store: archive, MaxTokens: 1200, KeepRecentRounds: 1,
				}),
			)
			events, err := a.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("核对上下文"))
			if reply := agent_v2.FinalMessage(events); err != nil || reply == nil || reply.Content != "已核对历史" ||
				requests.count() != 3 || executions.Load() != 1 {
				t.Fatalf("应完成一次复现工具往返，请求=%d 工具=%d 答复=%+v 错误=%v", requests.count(), executions.Load(), reply, err)
			}
			first := canon(t, requests.at(t, 1).body["messages"])
			if !strings.Contains(first, signature) || !strings.Contains(first, "[此前上下文已归档") || strings.Contains(first, archived) {
				t.Fatal("首轮应保留签名并确实折叠其他历史")
			}
			second := canon(t, requests.at(t, 2).body["messages"])
			if !strings.Contains(second, protectedReply) || !strings.Contains(second, `"tool_use_id":"`+lookupID+`"`) ||
				!strings.Contains(second, "当前工具确实执行过") || strings.Contains(second, archived) {
				t.Fatal("下一轮应保留历史工具配对和当前工具结果，并继续使用归档视图")
			}
			if strings.Contains(second, signature) == tc.drop {
				t.Fatalf("下一轮实际请求的签名有误，钩子已删除=%v", tc.drop)
			}
			if !tc.drop && archive.puts.Load() != 1 {
				t.Fatalf("只改非请求标注时应复用折叠备忘，归档写了 %d 次", archive.puts.Load())
			}
		})
	}
}
