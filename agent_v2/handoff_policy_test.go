package agent_v2_test

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

// policyToolModel 记录真正绑定给模型的 transfer_to_agent 枚举。
type policyToolModel struct {
	*scopeStub
	offered [][]string
}

func (m *policyToolModel) WithTools(tools []*schema.ToolInfo) (model.ChatModel, error) {
	for _, info := range tools {
		if info.Name != agent_v2.HandoffTransferToolName {
			continue
		}
		data, err := json.Marshal(info.Params)
		if err != nil {
			return nil, err
		}
		var params struct {
			Properties map[string]struct{ Enum []string }
		}
		if err := json.Unmarshal(data, &params); err != nil {
			return nil, err
		}
		m.offered = append(m.offered, params.Properties["agent_name"].Enum)
	}
	return m, nil
}

func (m *policyToolModel) checkTargets(t *testing.T, want ...string) {
	t.Helper()
	if (len(m.offered) == 0) != (len(want) == 0) {
		t.Fatalf("模型收到的转交工具候选 %v，期望候选 %v", m.offered, want)
	}
	for _, got := range m.offered {
		if !slices.Equal(got, want) {
			t.Fatalf("模型收到的转交候选 %v，期望 %v", got, want)
		}
	}
}

func TestHandoffPolicyFiltersModelSchemaAndDispatchByRegisteredSource(t *testing.T) {
	// 成员使用相同的 Agent 名，通过注册位置区分来源。各自先请求一个被禁止的目标，再作合法转交。
	first := &policyToolModel{scopeStub: newScopeStub(transferToolCall("first-rejected", "end"), transferToolCall("first-allowed", "middle"))}
	middle := &policyToolModel{scopeStub: newScopeStub(transferToolCall("middle-rejected", "front"), transferToolCall("middle-allowed", "end"))}
	enabled := func(m *policyToolModel) *agent_v2.BasicAgent {
		return agent_v2.NewBasicAgent(agent_v2.WithName("shared"), agent_v2.WithModel(m), agent_v2.WithHandoffTransfer(), agent_v2.WithMaxRounds(3))
	}
	end, endModel := scopeLeaf("end", "完成")
	targets := []string{"unused", "middle"}
	opt := agent_v2.WithHandoffTargets("front", targets...)
	targets[1] = "end"
	h := agent_v2.NewHandoffAgent(agent_v2.WithHandoffName("policy"), agent_v2.WithHandoffInitial("front"),
		opt, agent_v2.WithHandoffTargets("middle", "end")).
		AddAgent("front", enabled(first)).AddAgent("middle", enabled(middle)).AddAgent("end", end).
		AddAgent("unused", newProgAgent("unused", "未选择", ""))
	evs, err := h.Run(t.Context(), agent_v2.NewRunSession(), schema.UserMessage("转交"))
	if err != nil {
		t.Fatal(err)
	}
	first.checkTargets(t, "middle", "unused")
	middle.checkTargets(t, "end")
	final := agent_v2.FinalMessage(evs)
	if first.count() != 2 || middle.count() != 2 || endModel.count() != 1 || final == nil || final.Content != "完成" {
		t.Fatalf("实际模型调用 front=%d middle=%d end=%d，最终回复 %v", first.count(), middle.count(), endModel.count(), final)
	}
}

func TestHandoffPolicyProgrammaticAndReturnApplyPerCall(t *testing.T) {
	for _, initial := range []string{"allowed", "blocked"} {
		t.Run(initial, func(t *testing.T) {
			member := newProgAgent("same-source-object", "成员答复", "end")
			end := newProgAgent("end", "接手答复", "")
			h := agent_v2.NewHandoffAgent(agent_v2.WithHandoffName("policy"), agent_v2.WithHandoffInitial(initial),
				agent_v2.WithHandoffTargets("allowed", "end"), agent_v2.WithHandoffTargets("blocked"),
				agent_v2.WithDeterministicReturnTo("end")).
				AddAgent("allowed", member).AddAgent("blocked", member).AddAgent("end", end)
			for _, paused := range []bool{false, true, false} {
				var opts []agent_v2.RunOption
				if paused {
					opts = append(opts, agent_v2.WithHandoffPaused())
				}
				before := end.runs
				events, err := h.Run(t.Context(), agent_v2.NewRunSession(), schema.UserMessage("开始"), opts...)
				if err != nil {
					t.Fatal(err)
				}
				permitted := initial == "allowed" && !paused
				final := agent_v2.FinalMessage(events)
				if permitted {
					if member.lastErr != nil || end.runs-before != 1 || final == nil || final.Content != end.say {
						t.Fatalf("获准的转交 err=%v 接手次数=%d 最终消息=%v", member.lastErr, end.runs-before, final)
					}
				} else if !errors.Is(member.lastErr, compose_v2.ErrRunAborted) || end.runs != before || final == nil || final.Content != member.say {
					t.Fatalf("受限的转交 err=%v 接手次数=%d 最终消息=%v", member.lastErr, end.runs-before, final)
				}
			}
		})
	}
}

func TestHandoffPolicyRejectsUnknownRegisteredNameBeforeDispatch(t *testing.T) {
	for _, config := range []agent_v2.HandoffOption{
		agent_v2.WithHandoffTargets("unknown", "source"),
		agent_v2.WithHandoffTargets("source", "unknown"),
	} {
		worker := newProgAgent("source", "执行", "")
		h := agent_v2.NewHandoffAgent(agent_v2.WithHandoffName("policy"), agent_v2.WithHandoffInitial("source"), config).
			AddAgent("source", worker)
		if _, err := h.Run(t.Context(), agent_v2.NewRunSession(), schema.UserMessage("开始")); err == nil || worker.runs != 0 {
			t.Fatalf("未注册的来源或目标应在派发前报错，调用次数=%d err=%v", worker.runs, err)
		}
	}
}

func TestHandoffPolicyCheckpointUsesExactRegisteredNames(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	const first, second = "target\x80", "target\x81"
	identity := func(target string) string {
		t.Helper()
		front := agent_v2.NewBasicAgent(agent_v2.WithName("front"), agent_v2.WithModel(newScopeStub(askMsg("ask", "继续？"))), agent_v2.WithAskUser())
		h := agent_v2.NewHandoffAgent(agent_v2.WithHandoffName("exact-names"), agent_v2.WithHandoffInitial("front"), agent_v2.WithHandoffTargets("front", target)).
			AddAgent("front", front).AddAgent(first, newProgAgent("first", "一", "")).AddAgent(second, newProgAgent("second", "二", ""))
		cp, session := compose_v2.NewInMemoryCheckpointer(), agent_v2.NewRunSession()
		_, err := h.Run(compose_v2.WithCheckpointerCtx(ctx, cp), session, schema.UserMessage("开始"))
		if _, ok := compose_v2.IsInterrupt(err); !ok {
			t.Fatalf("应先挂起，实际 %v", err)
		}
		saved, err := cp.LoadNS(ctx, session.RunID, "")
		if err != nil || saved == nil || saved.GraphID == "" {
			t.Fatalf("缺少父图身份，err=%v", err)
		}
		return saved.GraphID
	}
	if a, b := identity(first), identity(second); a == b {
		t.Fatalf("分别授权两个不同注册名时，存档身份相同 %q", a)
	}
}

func TestHandoffPolicyRemovingImplicitTargetKeepsCommittedCheckpoint(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	calls := schema.GetToolCalls(transferToolCall("transfer", "billing"))
	batch := schema.AssistantToolCallMessage(append(calls, schema.GetToolCalls(askMsg("confirm", "确认转交？"))...))
	send := newScopeStub(batch, schema.AssistantMessage("重新生成"))
	billing := newProgAgent("billing", "已接手", "")
	build := func(includeBilling bool) *agent_v2.HandoffAgent {
		front := agent_v2.NewBasicAgent(agent_v2.WithName("front"), agent_v2.WithModel(send), agent_v2.WithAskUser(), agent_v2.WithHandoffTransfer())
		h := agent_v2.NewHandoffAgent(agent_v2.WithHandoffName("implicit"), agent_v2.WithHandoffInitial("front"), agent_v2.WithHandoffTargets("terminal")).
			AddAgent("front", front).AddAgent("terminal", newProgAgent("terminal", "终点", ""))
		if includeBilling {
			h.AddAgent("billing", billing)
		}
		return h
	}
	old, session := compose_v2.NewInMemoryCheckpointer(), agent_v2.NewRunSession()
	_, err := build(true).Run(compose_v2.WithCheckpointerCtx(ctx, old), session, schema.UserMessage("开始"))
	pause, ok := compose_v2.IsInterrupt(err)
	if !ok || len(pause.AllQueries()) != 1 {
		t.Fatalf("应在同批工具挂起，实际 %v", err)
	}
	ctx = compose_v2.WithCheckpointerCtx(ctx, reopenBackgroundCheckpoints(t, ctx, old, session.RunID))
	answer := agent_v2.WithResumeUserInputs(map[string]*schema.Message{pause.AllQueries()[0].InterruptID: schema.UserMessage("确认")})
	if _, err := build(false).Resume(ctx, session.RunID, answer); !errors.Is(err, compose_v2.ErrGraphIDConflict) {
		t.Fatalf("原来源默认可以转给已被移除的 billing，续跑应报告版本冲突，实际 %v", err)
	}
	events, err := build(true).Resume(ctx, session.RunID, answer)
	final := agent_v2.FinalMessage(events)
	if err != nil || final == nil || final.Content != billing.say || billing.runs != 1 || send.count() != 1 {
		t.Fatalf("恢复原候选表后 err=%v 最终回复=%v 接手=%d 来源=%d", err, final, billing.runs, send.count())
	}
}

func TestHandoffPolicyPauseIsolatedAcrossConcurrentRuns(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	pausedEntered := make(chan struct{})
	openEntered := make(chan struct{})
	pausedChecked := make(chan struct{})
	front := &effFunc{name: "front"}
	front.fn = func(ctx context.Context, self agent_v2.Agent, in *schema.Message, opts []agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
		if in.Content == "暂停" {
			close(pausedEntered)
			select {
			case <-openEntered:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			err := agent_v2.HandoffTo(ctx, opts, self, "end")
			close(pausedChecked)
			if errors.Is(err, compose_v2.ErrRunAborted) {
				return effExit("front", "留在前台"), nil
			}
			return effExit("front", "意外转交"), err
		}
		close(openEntered)
		select {
		case <-pausedChecked:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return effExit("front", "转交"), agent_v2.HandoffTo(ctx, opts, self, "end")
	}
	end := &effReply{name: "end", reply: "接手"}
	h := agent_v2.NewHandoffAgent(agent_v2.WithHandoffName("shared"), agent_v2.WithHandoffInitial("front")).
		AddAgent("front", front).AddAgent("end", end)
	done := make(chan struct{})
	var paused []*agent_v2.AgentEvent
	var pauseErr error
	go func() {
		defer close(done)
		paused, pauseErr = h.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("暂停"), agent_v2.WithHandoffPaused())
	}()
	select {
	case <-pausedEntered:
	case <-done:
		t.Fatalf("暂停调用在进入成员前退出 %v", pauseErr)
	case <-ctx.Done():
		<-done
		t.Fatal(ctx.Err())
	}
	open, openErr := h.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("执行"))
	<-done
	f1, f2 := agent_v2.FinalMessage(paused), agent_v2.FinalMessage(open)
	if pauseErr != nil || openErr != nil || f1 == nil || f1.Content != "留在前台" || f2 == nil || f2.Content != end.reply ||
		front.runs.Load() != 2 || end.runs.Load() != 1 {
		t.Fatalf("并发暂停=%v/%v 正常=%v/%v 前台=%d 接手=%d", f1, pauseErr, f2, openErr, front.runs.Load(), end.runs.Load())
	}
}

func TestHandoffPolicyScopesRegrantedOptionsToCurrentSource(t *testing.T) {
	for _, mode := range []string{"下一个来源", "下次调用暂停"} {
		t.Run(mode, func(t *testing.T) {
			stash := &grantOptStash{}
			probe := &policyToolModel{scopeStub: newScopeStub(transferToolCall("stale", "billing"), schema.AssistantMessage("委派者自己处理"))}
			delegate := agent_v2.NewBasicAgent(agent_v2.WithName("delegate"), agent_v2.WithModel(probe),
				agent_v2.WithHandoffTransfer(), agent_v2.WithMaxRounds(3))
			billing, billed := scopeLeaf("billing", "已接手")
			h := agent_v2.NewHandoffAgent(agent_v2.WithHandoffName("policy"), agent_v2.WithHandoffInitial("front"),
				agent_v2.WithHandoffTargets("front", "helper", "billing"), agent_v2.WithHandoffTargets("helper")).
				AddAgent("billing", billing)
			if mode == "下一个来源" {
				h.AddAgent("front", &regrantStashShell{name: "front", stash: stash, target: "helper", to: delegate}).
					AddAgent("helper", &grantReplayShell{name: "helper", stash: stash, inner: delegate})
			} else {
				h.AddAgent("front", &regrantTwoPhaseShell{name: "front", stash: stash, to: delegate}).
					AddAgent("helper", newProgAgent("helper", "未选择", ""))
			}
			events, err := h.Run(t.Context(), agent_v2.NewRunSession(), schema.UserMessage("开始"))
			if err != nil {
				t.Fatal(err)
			}
			if mode == "下次调用暂停" {
				if stash.n() == 0 || probe.count() != 0 || billed.count() != 0 {
					t.Fatal("首次调用应只保存转授")
				}
				events, err = h.Run(t.Context(), agent_v2.NewRunSession(), schema.UserMessage("新任务"), agent_v2.WithHandoffPaused())
				if err != nil {
					t.Fatal(err)
				}
			}
			probe.checkTargets(t)
			final := agent_v2.FinalMessage(events)
			if stash.n() == 0 || probe.count() != 2 || billed.count() != 0 || final == nil || final.Content != "委派者自己处理" {
				t.Fatalf("委派模型=%d 计费模型=%d 保存的授予=%d 最终回复=%v", probe.count(), billed.count(), stash.n(), final)
			}
		})
	}
}

func TestHandoffPolicySavedProgrammaticGrantCannotCrossPause(t *testing.T) {
	for _, tc := range []struct {
		name        string
		save, reuse []agent_v2.RunOption
	}{
		{name: "正常时保存再在暂停时使用", reuse: []agent_v2.RunOption{agent_v2.WithHandoffPaused()}},
		{name: "暂停时保存再在正常时使用", save: []agent_v2.RunOption{agent_v2.WithHandoffPaused()}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stash := &grantOptStash{}
			member := newProgAgent("delegate", "委派结果", "billing")
			billing := newProgAgent("billing", "接手", "")
			h := agent_v2.NewHandoffAgent(agent_v2.WithHandoffName("default-roster"), agent_v2.WithHandoffInitial("front")).
				AddAgent("front", &regrantTwoPhaseShell{name: "front", stash: stash, to: member}).
				AddAgent("billing", billing)
			if _, err := h.Run(t.Context(), agent_v2.NewRunSession(), schema.UserMessage("保存委派"), tc.save...); err != nil {
				t.Fatal(err)
			}
			if stash.n() == 0 || member.runs != 0 {
				t.Fatalf("首次保存的授权=%d 委派者执行=%d", stash.n(), member.runs)
			}
			events, err := h.Run(t.Context(), agent_v2.NewRunSession(), schema.UserMessage("新一轮"), tc.reuse...)
			if err != nil {
				t.Fatal(err)
			}
			final := agent_v2.FinalMessage(events)
			if member.runs != 1 || !errors.Is(member.lastErr, compose_v2.ErrRunAborted) || billing.runs != 0 || final == nil || final.Content != member.say {
				t.Fatalf("暂停后的委派次数=%d 转交错误=%v 接手次数=%d 最终回复=%v", member.runs, member.lastErr, billing.runs, final)
			}
		})
	}
}

func TestHandoffPolicyKeepsOnlyCommonTargetsWhenUsingSavedGrant(t *testing.T) {
	stash := &grantOptStash{}
	probe := &policyToolModel{scopeStub: newScopeStub(
		transferToolCall("old-only", "billing"), transferToolCall("current-only", "reports"), transferToolCall("both", "notify"))}
	delegate := agent_v2.NewBasicAgent(agent_v2.WithName("delegate"), agent_v2.WithModel(probe),
		agent_v2.WithHandoffTransfer(), agent_v2.WithMaxRounds(4))
	billing, reports := newProgAgent("billing", "账单", ""), newProgAgent("reports", "报表", "")
	notify := newProgAgent("notify", "通知", "")
	h := agent_v2.NewHandoffAgent(agent_v2.WithHandoffName("regrant"), agent_v2.WithHandoffInitial("front"),
		agent_v2.WithHandoffTargets("front", "helper", "billing", "notify"), agent_v2.WithHandoffTargets("helper", "reports", "notify")).
		AddAgent("front", &regrantStashShell{name: "front", stash: stash, target: "helper", to: delegate}).
		AddAgent("helper", &grantReplayShell{name: "helper", stash: stash, inner: delegate}).
		AddAgent("billing", billing).AddAgent("reports", reports).AddAgent("notify", notify)
	events, err := h.Run(t.Context(), agent_v2.NewRunSession(), schema.UserMessage("办理"))
	if err != nil {
		t.Fatal(err)
	}
	probe.checkTargets(t, "notify")
	final := agent_v2.FinalMessage(events)
	if probe.count() != 3 || billing.runs != 0 || reports.runs != 0 || notify.runs != 1 || final == nil || final.Content != notify.say {
		t.Fatalf("模型=%d 账单=%d 报表=%d 通知=%d 最终回复=%v", probe.count(), billing.runs, reports.runs, notify.runs, final)
	}
}

func TestHandoffPolicyColdResumeHonorsCommittedTransferAndPausesNewOnes(t *testing.T) {
	for _, mode := range []string{"正常续跑", "暂停转交"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			old := compose_v2.NewInMemoryCheckpointer()
			tools := tool.NewToolkit()
			var approved []string
			delayed := newProgAgent("delayed", "", "")
			var delayedErr error
			tools.Register(tool.NewFuncTool("confirm", "等待答复", map[string]any{"type": "object"},
				func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
					answer, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "确认该次转交？"})
					if err != nil {
						return nil, err
					}
					approved = append(approved, answer.Content)
					// 同批的第一项在挂起前已经提交；这项直到确认后才尝试转交。
					delayedErr = agent_v2.HandoffTo(ctx, agent_v2.RegrantFromNode(ctx, delayed), delayed, "next")
					return tool.Success(answer.Content), nil
				}))
			transfer := schema.AssistantToolCallMessage([]schema.ToolCall{
				schema.GetToolCalls(transferToolCall("transfer", "billing"))[0],
				{ID: "confirm", Type: "function", Function: schema.FunctionCall{Name: "confirm", Arguments: "{}"}},
			})
			send := newScopeStub(transfer, schema.AssistantMessage("重复调用来源"))
			build := func(targets []string, billing, next agent_v2.Agent) *agent_v2.HandoffAgent {
				front := agent_v2.NewBasicAgent(agent_v2.WithName("front"), agent_v2.WithModel(send),
					agent_v2.WithToolkit(tools), agent_v2.WithHandoffTransfer(), agent_v2.WithMaxRounds(3))
				return agent_v2.NewHandoffAgent(agent_v2.WithHandoffName("cold-policy"), agent_v2.WithHandoffInitial("front"),
					agent_v2.WithDeterministicReturnTo("next"), agent_v2.WithHandoffTargets("front", targets...),
					agent_v2.WithHandoffTargets("billing", "next")).
					AddAgent("front", front).AddAgent("billing", billing).AddAgent("next", next)
			}
			firstBilling := newProgAgent("billing", "账单", "next")
			firstNext := newProgAgent("next", "下一位", "")
			s := agent_v2.NewRunSession()
			original := build([]string{"billing", "next"}, firstBilling, firstNext)
			_, err := original.Run(compose_v2.WithCheckpointerCtx(ctx, old), s, schema.UserMessage("查询账单"))
			pause, ok := compose_v2.IsInterrupt(err)
			if !ok || len(pause.AllQueries()) != 1 || pause.AllQueries()[0].Question != "确认该次转交？" ||
				send.count() != 1 || firstBilling.runs != 0 || firstNext.runs != 0 || len(approved) != 0 {
				t.Fatalf("同批转交挂起 err=%v 来源=%d 账单=%d 下一位=%d 已批准=%v", err, send.count(), firstBilling.runs, firstNext.runs, approved)
			}
			reopened := reopenBackgroundCheckpoints(t, ctx, old, s.RunID)
			ctx = compose_v2.WithCheckpointerCtx(ctx, reopened)
			billing := newProgAgent("billing", "账单答复", "next")
			next := newProgAgent("next", "下一位答复", "")
			answer := agent_v2.WithResumeUserInputs(map[string]*schema.Message{pause.AllQueries()[0].InterruptID: schema.UserMessage("确认")})
			if _, err := build([]string{"next"}, billing, next).Resume(ctx, s.RunID, answer); !errors.Is(err, compose_v2.ErrGraphIDConflict) {
				t.Fatalf("变更静态名单应当保留原档并拒绝续跑，实际 %v", err)
			}
			opts := []agent_v2.ResumeOption{answer}
			paused := mode == "暂停转交"
			if paused {
				opts = append(opts, agent_v2.WithResumeHandoffPaused())
			}
			events, err := build([]string{"next", "billing", "next"}, billing, next).Resume(ctx, s.RunID, opts...)
			if err != nil {
				t.Fatal(err)
			}
			final := agent_v2.FinalMessage(events)
			if send.count() != 1 || !slices.Equal(approved, []string{"确认"}) || billing.runs != 1 || final == nil {
				t.Fatalf("冷恢复 来源=%d 已答=%v 账单=%d 最终消息=%v", send.count(), approved, billing.runs, final)
			}
			if paused {
				if !errors.Is(delayedErr, compose_v2.ErrRunAborted) || !errors.Is(billing.lastErr, compose_v2.ErrRunAborted) || next.runs != 0 || final.Content != billing.say {
					t.Fatalf("暂停后新作出的决定 同批=%v 账单=%v 下一位=%d 最终消息=%v", delayedErr, billing.lastErr, next.runs, final)
				}
			} else if delayedErr != nil || billing.lastErr != nil || next.runs != 1 || final.Content != next.say {
				t.Fatalf("正常续跑新作出的决定 同批=%v 账单=%v 下一位=%d 最终消息=%v", delayedErr, billing.lastErr, next.runs, final)
			}
			if left, err := reopened.LoadAllInSession(ctx, s.RunID); err != nil || len(left) != 0 {
				t.Fatalf("成功后剩余存档=%v err=%v", left, err)
			}
		})
	}
}
