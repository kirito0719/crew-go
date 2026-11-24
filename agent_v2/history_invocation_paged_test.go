package agent_v2_test

import (
	"context"
	"fmt"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
	"crew-go/tool"
)

func TestHistoryWindowPagedMemberProjections(t *testing.T) {
	for _, tt := range []struct {
		name string
		mode agent_v2.HandoffHistoryMode
	}{{"self", agent_v2.HandoffHistorySelfOnly}, {"raw", agent_v2.HandoffHistoryRaw}, {"rewrite", agent_v2.HandoffHistoryRewrite}} {
		for _, rounds := range []int{1, 37} {
			t.Run(fmt.Sprintf("%s/%d-rounds", tt.name, rounds), func(t *testing.T) {
				ctx := compose_v2.WithThreadIDCtx(t.Context(), "paged")
				hs := agent_v2.NewInMemoryHistoryStore()
				tk := tool.NewToolkit()
				effects := 0
				tk.Register(tool.NewFuncTool("lookup", "lookup", map[string]any{"type": "object"}, func(context.Context, map[string]any) (*tool.Result, error) {
					effects++
					return tool.Success(fmt.Sprintf("LOOKUP_%d", effects)), nil
				}))
				script := []*schema.Message{schema.AssistantMessage("RETIRED_OUTPUT")}
				for i := 0; i < rounds; i++ {
					script = append(script, schema.AssistantToolCallMessage([]schema.ToolCall{{ID: fmt.Sprintf("lookup-%d", i), Type: "function", Function: schema.FunctionCall{Name: "lookup", Arguments: "{}"}}}))
				}
				script = append(script, schema.AssistantMessage("RETAINED_OUTPUT"), schema.AssistantMessage("NEW_OUTPUT"))
				m := &promptRecorder{resp: script}
				member := agent_v2.NewBasicAgent(agent_v2.WithName("member"), agent_v2.WithHistory(hs, "owner"), agent_v2.WithModel(m), agent_v2.WithToolkit(tk), agent_v2.WithMaxHistoryPairs(1), agent_v2.WithMaxRounds(60))
				planner := func(_ context.Context, _ *agent_v2.RunSession, goal string, completed []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
					tasks := []string{"NEXT_ACTION"}
					if goal == "BEFORE" {
						tasks = []string{"RETIRED_INSTRUCTION", "UNIQUE_RETAINED_INSTRUCTION"}
					}
					if len(completed) == len(tasks) {
						return nil, "OK", nil
					}
					return []agent_v2.PlanStep{{Description: tasks[len(completed)], SubAgentName: "member"}}, "", nil
				}
				p := agent_v2.NewPlanAgent(planner, agent_v2.PlanName("plan")).AddSubAgent("member", historyViewAgent{Agent: member, mode: tt.mode})
				// 作者在 worker 的包装器后面，顶层显式配置真正的历史层。
				ctx = agent_v2.WithHistoryCtx(ctx, hs, "owner")
				if _, e := p.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("BEFORE")); e != nil {
					t.Fatal(e)
				}
				rows, e := agent_v2.LoadAllHistory(ctx, hs, agent_v2.HistoryKey{Owner: "owner", ConvID: "paged"})
				if e != nil || !containsMsg(rows, "UNIQUE_RETAINED_INSTRUCTION") || !containsMsg(rows, "RETAINED_OUTPUT") || effects != rounds {
					t.Fatalf("原任务未走完全套: rows=%v err=%v,%d次", len(rows), e, effects)
				}
				if _, e = p.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("AFTER")); e != nil {
					t.Fatal(e)
				}
				got := m.call(len(script) - 1)
				if !containsMsg(got, "RETAINED_OUTPUT") || !containsMsg(got, "NEXT_ACTION") {
					t.Fatalf("观察位置错误: %v", contentsOfTest(got))
				}
				if !containsMsg(got, "UNIQUE_RETAINED_INSTRUCTION") || containsMsg(got, "RETIRED_INSTRUCTION") {
					t.Fatalf("同一任务保留的第二个调用未严格匹配（见到正确输入=%t，误取旧调用=%t）", containsMsg(got, "UNIQUE_RETAINED_INSTRUCTION"), containsMsg(got, "RETIRED_INSTRUCTION"))
				}
			})
		}
	}
}
