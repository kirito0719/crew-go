package agent_v2_test

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

func TestPlanApprovalBindsToCandidateAcrossInterrupt(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	ctx = compose_v2.WithCheckpointerCtx(ctx, compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	member := "first"
	llm := agent_v2.NewLLMPlanner(&fnModel{fn: func(_ context.Context, msgs []*schema.Message) (*schema.Message, error) {
		if containsMsg(msgs, "复核回执") {
			return makeToolCallMsg("respond", `{"answer":"已复核"}`), nil
		}
		return makeToolCallMsg("plan", fmt.Sprintf(`{"steps":[{"description":"复核同一笔款项","sub_agent":%q}]}`, member)), nil
	}})
	var approvals, dispatched []string
	flow := agent_v2.NewPlanAgent(func(ctx context.Context, sess *agent_v2.RunSession, goal string, executed []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
		steps, final, err := llm(ctx, sess, goal, executed)
		if err != nil || len(steps) == 0 {
			return steps, final, err
		}
		candidate := steps[0].SubAgentName
		askCtx := compose_v2.WithResumeAnswerScope(ctx, candidate)
		answer, err := compose_v2.Ask(askCtx, &compose_v2.AskQuery{Question: "是否交给 " + candidate})
		if err != nil {
			return nil, "", err
		}
		approvals = append(approvals, candidate+"="+answer.GetContent())
		return steps, final, nil
	}, agent_v2.PlanName("review"))
	for _, name := range []string{"first", "second"} {
		worker := agent_v2.NewBasicAgent(agent_v2.WithName(name), agent_v2.WithModel(&fnModel{fn: func(_ context.Context, _ []*schema.Message) (*schema.Message, error) {
			dispatched = append(dispatched, name)
			return schema.AssistantMessage("复核回执"), nil
		}}))
		flow.AddSubAgent(name, worker)
	}

	_, err := flow.Run(ctx, sess, schema.UserMessage("核对款项"))
	first := decisionRetryQuestion(t, err, "是否交给 first")
	_, err = flow.Resume(ctx, sess.RunID)
	if repeated := decisionRetryQuestion(t, err, "是否交给 first"); repeated != first {
		t.Fatalf("同一候选重复挂起，问题从 %q 变为 %q", first, repeated)
	}
	member = "second"
	answers := map[string]*schema.Message{first: schema.UserMessage("同意 first")}
	_, err = flow.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(answers))
	second := decisionRetryQuestion(t, err, "是否交给 second")
	if first == second || len(approvals) != 0 || len(dispatched) != 0 {
		t.Fatalf("更换候选后已消费旧答复，ID=%q/%q 批准=%v 派发=%v", first, second, approvals, dispatched)
	}
	answers[second] = schema.UserMessage("同意 second")
	events, err := flow.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(answers))
	if err != nil || agent_v2.FinalMessage(events).GetContent() != "已复核" ||
		!slices.Equal(approvals, []string{"second=同意 second"}) || !slices.Equal(dispatched, []string{"second"}) {
		t.Fatalf("批准=%v 派发=%v 最终答复=%v 错误=%v", approvals, dispatched, agent_v2.FinalMessage(events), err)
	}
}
