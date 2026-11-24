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

// 真实编排把稀有指令交给成员。成员调工具后完成；用第二个 Run 观察它的下一份模型实参。
func TestHistoryWindowRetainsInstructionForKeptMemberOutput(t *testing.T) {
	for _, kind := range []string{"reflect", "plan"} {
		for _, lim := range []int{0, 1} {
			t.Run(fmt.Sprintf("%s/limit%d", kind, lim), func(t *testing.T) {
				hs := agent_v2.NewInMemoryHistoryStore()
				ctx := compose_v2.WithThreadIDCtx(t.Context(), "one-thread")
				var written int
				kit := tool.NewToolkit()
				kit.Register(tool.NewFuncTool("lookup", "look up", map[string]any{"type": "object"}, func(context.Context, map[string]any) (*tool.Result, error) {
					written++
					return tool.Success("REAL_LOOKUP"), nil
				}))
				script := []*schema.Message{oneToolCall("lookup"), schema.AssistantMessage("PRIOR_RESULT"), schema.AssistantMessage("NEXT_RESULT")}
				if kind == "reflect" {
					script = append([]*schema.Message{schema.AssistantMessage("DRAFT")}, script...)
				}
				model := &promptRecorder{resp: script}
				worker := agent_v2.NewBasicAgent(agent_v2.WithName("worker"), agent_v2.WithModel(model), agent_v2.WithToolkit(kit), agent_v2.WithHistory(hs, "tenant"), agent_v2.WithMaxHistoryPairs(lim))
				const unique = "INSTRUCTION_ONLY_IN_PRIOR"
				var host agent_v2.Agent
				if kind == "reflect" {
					host = agent_v2.NewReflectAgent(agent_v2.WithReflectName("reflect"), agent_v2.WithReflectMaxRounds(2), agent_v2.WithReflector(func(_ context.Context, goal, draft *schema.Message) (*agent_v2.ReflectOpinion, error) {
						if goal.Content == "EARLIER" && draft.Content == "DRAFT" {
							return &agent_v2.ReflectOpinion{GoalMatched: false, Opinion: unique}, nil
						}
						return &agent_v2.ReflectOpinion{GoalMatched: true}, nil
					})).WithActor(worker)
				} else {
					host = agent_v2.NewPlanAgent(func(_ context.Context, _ *agent_v2.RunSession, goal string, done []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
						if len(done) > 0 {
							return nil, "DONE", nil
						}
						task := "ANOTHER_TASK"
						if goal == "EARLIER" {
							task = unique
						}
						return []agent_v2.PlanStep{{Description: task, SubAgentName: "worker"}}, "", nil
					}, agent_v2.PlanName("plan")).AddSubAgent("worker", worker)
				}
				if _, err := host.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("EARLIER")); err != nil {
					t.Fatal(err)
				}
				hist, err := agent_v2.LoadAllHistory(ctx, hs, agent_v2.HistoryKey{Owner: "tenant", ConvID: "one-thread"})
				if err != nil || !containsMsg(hist, unique) || !containsMsg(hist, "PRIOR_RESULT") || written != 1 {
					t.Fatalf("存档/工具未覆盖 %v，实写 %d，历史 %v", err, written, contentsOfTest(hist))
				}
				if _, err = host.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("CURRENT")); err != nil {
					t.Fatal(err)
				}
				seen := model.call(len(script) - 1)
				t.Logf("后继实参 %v", contentsOfTest(seen))
				if !containsMsg(seen, "PRIOR_RESULT") {
					t.Fatal("未见上一任务产出，未抵达区分边界")
				}
				if !containsMsg(seen, unique) {
					t.Fatal("后继保留旧产出，但其唯一的派生指令丢失")
				}
			})
		}
	}
}
