package agent_v2

import (
	"fmt"
	"slices"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

func TestHistoryWindowRecoversInputAcrossInterleavedPages(t *testing.T) {
	ctx := compose_v2.WithThreadIDCtx(t.Context(), "review")
	hs := NewInMemoryHistoryStore()
	key := HistoryKey{Owner: "tenant", ConvID: "review"}
	const member = "actor"
	stamp := func(message *schema.Message, owner, task, trail string) *schema.Message {
		out := stampHistoryAgent(message, owner, owner, task)
		out.Annotations[historyScopeAnnotation] = trail
		return out
	}
	msgs := []*schema.Message{
		stamp(schema.UserMessage("本次审阅说明"), member, "retained", "current"),
		stamp(schema.UserMessage("同一任务更早的说明"), member, "retained", "older"),
		stamp(schema.UserMessage("其他成员的说明"), "neighbor", "retained", "current"),
		stamp(schema.UserMessage("其他任务的说明"), member, "different", "current"),
	}
	for n := range historyPageMin + 4 {
		msgs = append(msgs, stamp(schema.AssistantMessage("并发审阅"), member, fmt.Sprintf("other-%d", n), "current"))
	}
	call := schema.ToolCall{ID: "receipt", Function: schema.FunctionCall{Name: "inspect", Arguments: "{}"}}
	msgs = append(msgs,
		stamp(schema.AssistantToolCallMessage([]schema.ToolCall{call}), member, "retained", "current"),
		stamp(schema.ToolResultMessage("receipt", "核验记录"), member, "retained", "current"),
		stamp(schema.AssistantMessage("已核验"), member, "retained", "current"),
	)
	if err := hs.Append(ctx, key, msgs); err != nil {
		t.Fatal(err)
	}
	prior, ok := historyPrior(ctx, &historyConfig{store: hs, owner: key.Owner}, member, HandoffHistorySelfOnly, 2)
	if !ok {
		t.Fatal("应使用持久会话历史")
	}
	window := truncatePriorPreservingMarkers(prior, 2)
	var content []string
	for _, m := range window {
		if m.Content != "" {
			content = append(content, m.Content)
		}
	}
	if !slices.Equal(content, []string{"本次审阅说明", "核验记录", "已核验"}) || len(schema.GetToolCalls(window[1])) != 1 {
		t.Fatalf("只应补回同一成员调用的旧输入和配对结果，实际 %v", content)
	}
}
