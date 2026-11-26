package agent_v2_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
	"crew-go/store"
)

// withRunID 给一条 assistant 打上模型调用的 RunID：分组按它把每次模型调用分成一组。
func withRunID(m *schema.Message, runID string) *schema.Message {
	if m.Annotations == nil {
		m.Annotations = map[string]any{}
	}
	m.Annotations[compose_v2.RunIDAnnotationKey] = runID
	return m
}

func pendingUser(text, annotation string) *schema.Message {
	m := schema.UserMessage(text)
	m.Annotations = map[string]any{annotation: true}
	return m
}

// 消息流末尾是刚接进来、模型还没回应的插话 / 闸门反馈时，AutoCompact 原文保留的那一段要从它前面那一轮算起：
// 插话说的是刚才那批工具结果（「只看 2026 年的」），闸门反馈说的是刚才那条答复（「漏了价格」）——那一轮被摘要
// 掉，模型就只能对着摘要回应。
//
// sanity-revert：tail 从「最后一条 user 所在的组」起算、不往前带 ⟹ 末尾那条单独成组的插话把最近一轮挤进摘要，
// 输出里没有那批结果的原文，本用例红。
func TestAutoCompact_PendingUserInputKeepsPrecedingRound(t *testing.T) {
	big := strings.Repeat("超长内容 ABCDEFGH ", 100)
	for _, tc := range []struct {
		name    string
		tail    []*schema.Message
		keepRaw string // 那一轮的标记
		kept    bool   // 它该不该原文留在输出里
	}{
		{
			name: "插话接在工具结果后面",
			tail: []*schema.Message{
				withRunID(schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "c2", Function: schema.FunctionCall{Name: "search", Arguments: `{}`}}}), "r2"),
				schema.ToolResultMessage("c2", "LATEST_RESULT "+big),
				pendingUser("只看 2026 年的", agent_v2.InterjectionAnnotation),
			},
			keepRaw: "LATEST_RESULT",
			kept:    true,
		},
		{
			name: "闸门反馈加上最后一次的那句",
			tail: []*schema.Message{
				withRunID(schema.AssistantMessage("VETOED_ANSWER "+big), "r2"),
				pendingUser("漏了价格", agent_v2.TurnGateFeedbackAnnotation),
				pendingUser("这是最后一次被要求继续", agent_v2.TurnGateFeedbackAnnotation),
			},
			keepRaw: "VETOED_ANSWER",
			kept:    true,
		},
		{
			// 对照：用户真正的下一个问题照旧是新一轮的起点，它前面那一轮照常进摘要（只往前带「待回应」的那两类）
			name: "用户真正的下一个问题",
			tail: []*schema.Message{
				withRunID(schema.AssistantMessage("PREV_ROUND "+big), "r2"),
				schema.UserMessage("第二个问题"),
			},
			keepRaw: "PREV_ROUND",
			kept:    false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sm := &capturingSummaryModel{}
			cfg := &agent_v2.AutoCompactConfig{
				Enabled: true, Model: sm, ModelContextWindow: 500, SummaryMaxTokens: 100, SummaryBufferTokens: 80,
				MinCompactableTokens: 1,
			}
			msgs := append([]*schema.Message{
				schema.UserMessage("查一下 " + big),
				withRunID(schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "c1", Function: schema.FunctionCall{Name: "search", Arguments: `{}`}}}), "r1"),
				schema.ToolResultMessage("c1", "OLD_RESULT "+big),
			}, tc.tail...)
			out, err := agent_v2.AutoCompactRun(context.Background(), cfg, msgs)
			if err != nil {
				t.Fatal(err)
			}
			if len(sm.captured) == 0 {
				t.Fatal("没触发压缩（阈值构造失误），本用例测不到什么")
			}
			kept := false
			for _, m := range out {
				if strings.HasPrefix(m.Content, tc.keepRaw) {
					kept = true
				}
			}
			if kept != tc.kept {
				t.Errorf("%s 那一轮原文保留 = %v，该是 %v（%d 条输出）", tc.keepRaw, kept, tc.kept, len(out))
			}
			if orphans := orphanToolResults(out); len(orphans) > 0 {
				t.Errorf("输出含孤儿 tool_result %v", orphans)
			}
		})
	}
}

// 前面有一组受保护的记忆（业务以 user 消息注入、打 StoreInjectedAnnotation）、只有一轮真实的问答、末尾是待回应的
// 插话：往前带到那一轮之后，「前面全是受保护组 ⟹ 把 tail 重置成最后一组」那条兜底不能把它又顶回去。
//
// sanity-revert：往前带放在那条兜底之前 ⟹ tail 被重置成只剩插话，那一轮的工具结果被摘要掉，本用例红。
func TestAutoCompact_PendingTailSurvivesProtectedLeadingGroup(t *testing.T) {
	big := strings.Repeat("超长内容 ABCDEFGH ", 100)
	sm := &capturingSummaryModel{}
	cfg := &agent_v2.AutoCompactConfig{
		Enabled: true, Model: sm, ModelContextWindow: 500, SummaryMaxTokens: 100, SummaryBufferTokens: 80,
		MinCompactableTokens: 1,
	}
	mem := schema.UserMessage("记忆：用户偏好简洁")
	mem.Annotations = map[string]any{agent_v2.StoreInjectedAnnotation: true}
	msgs := []*schema.Message{
		mem,
		schema.UserMessage("查一下 " + big),
		withRunID(schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "c1", Function: schema.FunctionCall{Name: "search", Arguments: `{}`}}}), "r1"),
		schema.ToolResultMessage("c1", "LATEST_RESULT "+big),
		pendingUser("只看 2026 年的", agent_v2.InterjectionAnnotation),
	}
	out, err := agent_v2.AutoCompactRun(context.Background(), cfg, msgs)
	if err != nil {
		t.Fatal(err)
	}
	kept := false
	for _, m := range out {
		if strings.HasPrefix(m.Content, "LATEST_RESULT") {
			kept = true
		}
	}
	if !kept {
		t.Errorf("插话前面那一轮的工具结果该原文保留，实际被摘要掉了（摘要调用 %d 次，输出 %d 条）", len(sm.captured), len(out))
	}
	if orphans := orphanToolResults(out); len(orphans) > 0 {
		t.Errorf("输出含孤儿 tool_result %v", orphans)
	}
}

type appendPendingCompactionView struct {
	hook.NoOpHooks
	tail []*schema.Message
}

func (h appendPendingCompactionView) OnModelStart(ctx context.Context, _ *hook.AgentInfo, in *schema.ModelInput) (context.Context, error) {
	in.Messages = slices.Concat(in.Messages, h.tail)
	return ctx, nil
}

func TestCompaction_PendingInputsSurviveFrameworkSeparators(t *testing.T) {
	const name = "pending-compaction"
	old := "可压缩的旧历史 " + strings.Repeat("history ", 1200)
	recent := strings.Repeat("本轮工具证据 ", 700) + "LATEST_TOOL_EVIDENCE"
	first := strings.Repeat("第一次插话的修订细节 ", 500) + "FIRST_PENDING_DETAILS"
	second := "末次追加条件"
	tail := []*schema.Message{
		withRunID(schema.AssistantMessage("第一段处理"), "earlier-step"),
		withRunID(schema.AssistantToolCallMessage([]schema.ToolCall{{
			ID: "latest-read", Function: schema.FunctionCall{Name: "lookup", Arguments: `{}`},
		}}), "recent-step"),
		schema.ToolResultMessage("latest-read", recent),
		pendingUser(first, agent_v2.InterjectionAnnotation),
		pendingUser("框架工具提醒", agent_v2.RepeatToolReminderAnnotation),
		schema.SystemMessage("维护本次输出约束"),
		pendingUser(second, agent_v2.TurnGateFeedbackAnnotation),
	}
	cases := []struct {
		name string
		opt  agent_v2.Option
	}{
		{"auto", agent_v2.WithAutoCompact(&agent_v2.AutoCompactConfig{
			Enabled: true, Model: &countingSummarizer{}, ModelContextWindow: 600, SummaryMaxTokens: 100,
			SummaryBufferTokens: 80, MinCompactableTokens: 1,
		})},
		{"recovery", agent_v2.WithContextRecovery(&agent_v2.ContextRecoveryConfig{
			Store: store.NewInMemoryStore(), MaxTokens: 600, KeepRecentRounds: 1,
		})},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			llm := recoveryInspectModel{inspect: func(_ context.Context, msgs []*schema.Message) (*schema.Message, error) {
				view := inputText(msgs)
				if strings.Contains(view, old) {
					return nil, errors.New("fixture did not compact the previous history")
				}
				var call, result, firstInput, secondInput bool
				for _, m := range msgs {
					for _, tc := range schema.GetToolCalls(m) {
						call = call || tc.ID == "latest-read"
					}
					result = result || schema.GetToolResultID(m) == "latest-read" && m.Content == recent
					firstInput = firstInput || m.Role == schema.RoleUser && m.Content == first
					secondInput = secondInput || m.Role == schema.RoleUser && m.Content == second
				}
				if !call || !result || !firstInput || !secondInput {
					return nil, errors.New("compaction lost the preceding tool pair or an unanswered user input")
				}
				return schema.AssistantMessage("已按两条追加条件整理证据"), nil
			}}
			a := agent_v2.NewBasicAgent(agent_v2.WithName(name), agent_v2.WithModel(llm),
				agent_v2.WithHooks(appendPendingCompactionView{tail: tail}), tc.opt)
			sess := agent_v2.NewRunSession()
			sess.AddEvent(agent_v2.NewOutputEvent(name, schema.UserMessage(old)))
			sess.AddEvent(agent_v2.NewOutputEvent(name, schema.AssistantMessage("上一轮完成")))
			events, err := a.Run(t.Context(), sess, schema.UserMessage("按条件总结本轮证据"))
			if err != nil {
				t.Fatal(err)
			}
			if final := agent_v2.FinalMessage(events); final == nil || final.Content != "已按两条追加条件整理证据" {
				t.Fatalf("最终答复=%+v", final)
			}
		})
	}
}

func TestAutoCompact_PendingInputSurvivesTrailingNoteAfterProtectedPrefix(t *testing.T) {
	memory := pendingUser("长期记忆", agent_v2.StoreInjectedAnnotation)
	pending := pendingUser(strings.Repeat("尚未回应的补充 ", 800), agent_v2.InterjectionAnnotation)
	note := pendingUser("框架提示", agent_v2.RepeatToolReminderAnnotation)
	in := []*schema.Message{memory, pending, note}
	sm := &countingSummarizer{}
	out, err := agent_v2.AutoCompactRun(t.Context(), &agent_v2.AutoCompactConfig{
		Enabled: true, Model: sm, ModelContextWindow: 500, SummaryMaxTokens: 100,
		SummaryBufferTokens: 80, MinCompactableTokens: 1,
	}, in)
	if err != nil || !slices.Equal(out, in) || sm.count() != 0 {
		t.Fatalf("受保护前缀之后的待答输入应完整保留，输出 %d 条、摘要 %d 次，错误 %v", len(out), sm.count(), err)
	}
}

func TestAutoCompact_UnansweredInputInsideToolGroup(t *testing.T) {
	call := withRunID(schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID: "same", Function: schema.FunctionCall{Name: "lookup", Arguments: `{}`},
	}}), "r1")
	pending := pendingUser(strings.Repeat("追加条件 ", 500), agent_v2.InterjectionAnnotation)
	result := schema.ToolResultMessage("same", strings.Repeat("工具证据 ", 700))
	note := pendingUser("框架状态提示", agent_v2.RepeatToolReminderAnnotation)
	for _, answered := range []bool{false, true} {
		name := "待答"
		if answered {
			name = "已有模型回应"
		}
		t.Run(name, func(t *testing.T) {
			in := []*schema.Message{schema.UserMessage("查交易记录"), call, pending, result}
			if answered {
				in = append(in, withRunID(schema.AssistantMessage("已经按条件检查结果"), "r2"))
			}
			in = append(in, note)
			groups := compose_v2.GroupMessagesByAPIRound(in)
			if len(groups) < 2 || !slices.Equal(groups[0], in[:4]) {
				t.Fatal("插话应与工具调用和结果位于同一个 API 组")
			}
			sm := &countingSummarizer{}
			out, err := agent_v2.AutoCompactRun(t.Context(), &agent_v2.AutoCompactConfig{
				Enabled: true, Model: sm, ModelContextWindow: 500, SummaryMaxTokens: 100,
				SummaryBufferTokens: 80, MinCompactableTokens: 1,
			}, in)
			if err != nil {
				t.Fatal(err)
			}
			if answered {
				if sm.count() != 1 || slices.Contains(out, pending) {
					t.Fatalf("模型回应后可以摘要前一轮，摘要 %d 次，输出 %d 条", sm.count(), len(out))
				}
				return
			}
			if !slices.Equal(out, in) || sm.count() != 0 {
				t.Fatalf("待答输入和相邻工具往返应保留，摘要 %d 次，输出 %d 条", sm.count(), len(out))
			}
		})
	}
}

func TestCompaction_UnansweredInputsAndPairsSurviveLateToolReceipt(t *testing.T) {
	makeCall := func(run string) *schema.Message {
		return withRunID(schema.AssistantToolCallMessage([]schema.ToolCall{{
			ID: "same", Function: schema.FunctionCall{Name: "lookup", Arguments: `{}`},
		}}), run)
	}
	old := schema.UserMessage(strings.Repeat("可压缩的旧任务 ", 1000))
	firstCall, secondCall := makeCall("earlier"), makeCall("later")
	firstResult := schema.ToolResultMessage("same", "第一条工具回执")
	lateResult := schema.ToolResultMessage("same", "迟到的第二条回执")
	first := pendingUser(strings.Repeat("待答补充细节 ", 500), agent_v2.InterjectionAnnotation)
	second := pendingUser("第二个追加条件", agent_v2.TurnGateFeedbackAnnotation)
	in := []*schema.Message{old, withRunID(schema.AssistantMessage("旧任务完成"), "old"),
		schema.UserMessage("本次任务"), firstCall, secondCall, firstResult,
		first, pendingUser("框架提示", agent_v2.RepeatToolReminderAnnotation), lateResult, second,
	}
	groups := compose_v2.GroupMessagesByAPIRound(in)
	if len(groups) != 6 || !slices.Contains(groups[4], lateResult) || !slices.Contains(groups[3], first) {
		t.Fatal("迟到回执应在两条待答输入之间单独成组")
	}
	for _, mode := range []string{"auto", "reactive"} {
		t.Run(mode, func(t *testing.T) {
			var out []*schema.Message
			var err error
			if mode == "auto" {
				out, err = agent_v2.AutoCompactRun(t.Context(), &agent_v2.AutoCompactConfig{
					Enabled: true, Model: &countingSummarizer{}, ModelContextWindow: 500, SummaryMaxTokens: 100,
					SummaryBufferTokens: 80, MinCompactableTokens: 1,
				}, in)
			} else {
				out, err = agent_v2.ReactiveCompactRun(t.Context(), &agent_v2.ReactiveCompactConfig{Enabled: true},
					in, errors.New("Input is too long: 20000 input tokens (max 500)"))
			}
			if err != nil || slices.Contains(out, old) {
				t.Fatalf("应裁掉无关旧任务，输出 %d 条，错误 %v", len(out), err)
			}
			for _, m := range []*schema.Message{firstCall, secondCall, firstResult, lateResult, first, second} {
				if !slices.Contains(out, m) {
					t.Fatalf("丢失当前配对或待答输入，角色 %s，输出 %d 条", m.Role, len(out))
				}
			}
		})
	}
}
