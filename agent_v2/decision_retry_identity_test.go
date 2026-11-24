package agent_v2_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

func decisionRetryQuestion(t *testing.T, err error, want string) string {
	t.Helper()
	stop, ok := compose_v2.IsInterrupt(err)
	if !ok || len(stop.AllQueries()) != 1 {
		t.Fatalf("期望询问 %q，实际 err=%v", want, err)
	}
	q := stop.AllQueries()[0]
	if q.Question != want || q.InterruptID == "" {
		t.Fatalf("期望问题 %q，实际 %+v", want, q)
	}
	return q.InterruptID
}

// 真实规划器首次产出未注册的成员，调度器在本次决定已经拿到答复之后才拒绝派发。
// 同一计划节点再次执行时换了成员，调用方照常携带先前所有答案。
func TestPlanRejectedDecisionAsksAboutChangedCandidate(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	ctx = compose_v2.WithCheckpointerCtx(ctx, compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	target, plannerCalls := "retired", 0
	llm := agent_v2.NewLLMPlanner(&fnModel{fn: func(_ context.Context, msgs []*schema.Message) (*schema.Message, error) {
		plannerCalls++
		if containsMsg(msgs, "worker-receipt") {
			return makeToolCallMsg("respond", `{"answer":"done"}`), nil
		}
		return makeToolCallMsg("plan", fmt.Sprintf(`{"steps":[{"description":"执行复核","sub_agent":%q,"depends_on":[]}]}`, target)), nil
	}})
	var approvals []string
	planner := func(ctx context.Context, sess *agent_v2.RunSession, goal string, executed []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
		steps, final, err := llm(ctx, sess, goal, executed)
		if err != nil || len(steps) == 0 {
			return steps, final, err
		}
		candidate := steps[0].SubAgentName
		answer, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "交给 " + candidate + " 吗"})
		if err != nil {
			return nil, "", err
		}
		approvals = append(approvals, candidate+"="+answer.GetContent())
		return steps, final, nil
	}
	workerModel := &promptRecorder{resp: []*schema.Message{schema.AssistantMessage("worker-receipt")}}
	worker := agent_v2.NewBasicAgent(agent_v2.WithName("worker"), agent_v2.WithModel(workerModel))
	flow := agent_v2.NewPlanAgent(planner, agent_v2.PlanName("review"), agent_v2.WithPlanRetryConfig(compose_v2.SimpleRetry(2))).AddSubAgent("worker", worker)
	_, err := flow.Run(ctx, sess, schema.UserMessage("复核本次结算"))
	old := decisionRetryQuestion(t, err, "交给 retired 吗")
	answers := map[string]*schema.Message{old: schema.UserMessage("第一次同意")}
	_, err = flow.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(answers))
	if err == nil || !strings.Contains(err.Error(), "retired") || !slices.Equal(approvals, []string{"retired=第一次同意"}) || plannerCalls != 2 {
		t.Fatalf("调度器应拒绝首个决定，规划=%d 确认=%v err=%v", plannerCalls, approvals, err)
	}
	target = "worker"
	_, err = flow.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(answers))
	if err == nil || len(approvals) != 1 || len(workerModel.call(0)) != 0 {
		t.Fatalf("新候选必须先获得自己的答复，规划=%d 确认=%v 工作模型=%s err=%v", plannerCalls, approvals, workerModel.dump(), err)
	}
	next := decisionRetryQuestion(t, err, "交给 worker 吗")
	if next == old {
		t.Fatalf("两个不同候选共用问题身份 %q", old)
	}
	answers[next] = schema.UserMessage("第二次同意")
	events, err := flow.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(answers))
	if err != nil || agent_v2.FinalMessage(events).GetContent() != "done" || plannerCalls != 5 ||
		!slices.Equal(approvals, []string{"retired=第一次同意", "worker=第二次同意"}) || len(workerModel.call(0)) == 0 || len(workerModel.call(1)) != 0 {
		t.Fatalf("最终答复=%v 规划=%d 确认=%v 工作模型=%s err=%v", agent_v2.FinalMessage(events), plannerCalls, approvals, workerModel.dump(), err)
	}
}

func TestPlanAnswerGenerationAfterRejectedOrIncompletePlanner(t *testing.T) {
	unavailable := errors.New("规划服务不到位")
	for _, tc := range []struct {
		name       string
		response   *schema.Message
		modelErr   error
		wantErr    error
		incomplete bool
	}{
		{name: "没有工具调用", response: schema.AssistantMessage("内部草稿"), wantErr: agent_v2.ErrPlannerNoToolCall},
		{name: "未知规划工具", response: makeToolCallMsg("other", "{}"), wantErr: agent_v2.ErrPlannerBadToolCall},
		{name: "空步骤", response: makeToolCallMsg("plan", `{"steps":[]}`), wantErr: agent_v2.ErrPlannerEmptySteps},
		{name: "参数无效", response: makeToolCallMsg("plan", "{"), wantErr: agent_v2.ErrPlannerParseArgs},
		{name: "缺默认执行器", response: makeToolCallMsg("plan", `{"steps":[{"description":"需人工处理"}]}`)},
		{name: "同一规划尚未完成", modelErr: unavailable, wantErr: unavailable, incomplete: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			ctx = compose_v2.WithCheckpointerCtx(ctx, compose_v2.NewInMemoryCheckpointer())
			sess := agent_v2.NewRunSession()
			var received []string
			calls, working := 0, false
			q := "是否采用初始方案"
			planner := agent_v2.NewLLMPlanner(&fnModel{fn: func(ctx context.Context, msgs []*schema.Message) (*schema.Message, error) {
				calls++
				if containsMsg(msgs, "已处理") {
					return makeToolCallMsg("respond", `{"answer":"结算完毕"}`), nil
				}
				ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: q})
				if err != nil {
					return nil, err
				}
				received = append(received, ans.GetContent())
				if !working {
					return tc.response, tc.modelErr
				}
				return makeToolCallMsg("plan", `{"steps":[{"description":"处理本次任务","sub_agent":"worker"}]}`), nil
			}})
			model := &promptRecorder{resp: []*schema.Message{schema.AssistantMessage("已处理")}}
			flow := agent_v2.NewPlanAgent(planner, agent_v2.PlanName("plan")).AddSubAgent("worker",
				agent_v2.NewBasicAgent(agent_v2.WithName("worker"), agent_v2.WithModel(model)))
			_, err := flow.Run(ctx, sess, schema.UserMessage("结算"))
			old := decisionRetryQuestion(t, err, q)
			answers := map[string]*schema.Message{old: schema.UserMessage("同意原案")}
			_, err = flow.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(answers))
			if err == nil || tc.wantErr != nil && !errors.Is(err, tc.wantErr) || tc.wantErr == nil && !strings.Contains(err.Error(), "no default executor") ||
				calls != 2 || len(model.call(0)) != 0 {
				t.Fatalf("第一次决定应中止，规划=%d 工作模型=%s err=%v", calls, model.dump(), err)
			}
			working = true
			wantAnswers, wantCalls := []string{"同意原案", "同意原案"}, 4
			if !tc.incomplete {
				q = "是否采用新方案"
				_, err = flow.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(answers))
				next := decisionRetryQuestion(t, err, q)
				if next == old || len(received) != 1 || len(model.call(0)) != 0 {
					t.Fatalf("新方案应挂起等待新答案，问题=%s 答案=%v 工作模型=%s", next, received, model.dump())
				}
				answers[next] = schema.UserMessage("同意新案")
				wantAnswers, wantCalls = []string{"同意原案", "同意新案"}, 5
			}
			events, err := flow.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(answers))
			if err != nil || agent_v2.FinalMessage(events).GetContent() != "结算完毕" || calls != wantCalls ||
				!slices.Equal(received, wantAnswers) || len(model.call(0)) == 0 || len(model.call(1)) != 0 {
				t.Fatalf("最终答复=%v 规划=%d 答案=%v 工作模型=%s err=%v", agent_v2.FinalMessage(events), calls, received, model.dump(), err)
			}
		})
	}
}

// 评估结果已提交后，下一轮 actor 失败并耗尽自己的重试；恢复该轮无需再执行前一轮评估。
func TestReflectDownstreamRetryKeepsEvaluationsAndAnswersByRound(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	ctx = compose_v2.WithCheckpointerCtx(ctx, compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	busy := errors.New("改稿暂不可用")
	actorCalls := 0
	actor := agent_v2.NewBasicAgent(agent_v2.WithName("author"), agent_v2.WithModel(&fnModel{fn: func(_ context.Context, msgs []*schema.Message) (*schema.Message, error) {
		actorCalls++
		if actorCalls == 1 {
			return schema.AssistantMessage("首稿"), nil
		}
		if !containsMsg(msgs, "补充证据") {
			return nil, errors.New("缺上一轮反馈")
		}
		if actorCalls <= 3 {
			return nil, busy
		}
		return schema.AssistantMessage("修订稿"), nil
	}}))
	var checks, approved []string
	reviewer := agent_v2.NewBasicAgent(agent_v2.WithName("reviewer"), agent_v2.WithModel(&fnModel{fn: func(ctx context.Context, msgs []*schema.Message) (*schema.Message, error) {
		draft := "首稿"
		if containsMsg(msgs, "修订稿") {
			draft = "修订稿"
		}
		checks = append(checks, draft)
		answer, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "如何评定" + draft})
		if err != nil {
			return nil, err
		}
		approved = append(approved, draft+"="+answer.GetContent())
		if draft == "首稿" {
			return schema.AssistantMessage("请补充证据"), nil
		}
		return schema.AssistantMessage("yes"), nil
	}}))
	flow := agent_v2.NewReflectAgent(agent_v2.WithReflectName("review"), agent_v2.WithEvaluator(reviewer),
		agent_v2.WithReflectMaxRounds(2), agent_v2.WithReflectRetryConfig(compose_v2.SimpleRetry(1))).WithActor(actor)
	_, err := flow.Run(ctx, sess, schema.UserMessage("提供报告"))
	old := decisionRetryQuestion(t, err, "如何评定首稿")
	answers := map[string]*schema.Message{old: schema.UserMessage("改")}
	_, err = flow.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(answers))
	var exhausted *compose_v2.RetriesExhaustedError
	if !errors.Is(err, busy) || !errors.As(err, &exhausted) || actorCalls != 3 || !slices.Equal(checks, []string{"首稿", "首稿"}) {
		t.Fatalf("改稿应耗尽两次尝试，actor=%d 评估=%v err=%v", actorCalls, checks, err)
	}
	_, err = flow.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(answers))
	next := decisionRetryQuestion(t, err, "如何评定修订稿")
	if next == old || !slices.Equal(approved, []string{"首稿=改"}) || actorCalls != 4 {
		t.Fatalf("修订稿要独立答复，旧问题=%s 新问题=%s actor=%d 已确认=%v", old, next, actorCalls, approved)
	}
	answers[next] = schema.UserMessage("通过")
	events, err := flow.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(answers))
	if err != nil || agent_v2.FinalMessage(events).GetContent() != "修订稿" || actorCalls != 4 ||
		!slices.Equal(checks, []string{"首稿", "首稿", "修订稿", "修订稿"}) || !slices.Equal(approved, []string{"首稿=改", "修订稿=通过"}) {
		t.Fatalf("最终答复=%v actor=%d 评估=%v 已确认=%v err=%v", agent_v2.FinalMessage(events), actorCalls, checks, approved, err)
	}
}
