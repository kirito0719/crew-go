package agent_v2_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
	"crew-go/tool"
)

func TestReactiveCompactPreservesCurrentInputAcrossToolRounds(t *testing.T) {
	const (
		name     = "reactive-current-input"
		question = "按我指定的两步查询整理结论"
		oldText  = "上一任务的超长记录"
	)
	overflow := errors.New("Input is too long: 24000 input tokens (max 4000)")
	attempts, executions := 0, 0
	llm := recoveryInspectModel{inspect: func(_ context.Context, in []*schema.Message) (*schema.Message, error) {
		calls, results := make(map[string]bool), make(map[string]bool)
		sawOld, sawQuestion := false, false
		for _, m := range in {
			sawOld = sawOld || strings.Contains(m.Content, oldText)
			sawQuestion = sawQuestion || m.Content == question
			for _, call := range schema.GetToolCalls(m) {
				calls[call.ID] = true
			}
			if id := schema.GetToolResultID(m); id != "" {
				results[id] = true
			}
		}
		if results["second"] {
			attempts++
			if sawOld {
				return nil, overflow
			}
			if !sawQuestion || !results["first"] || !calls["first"] || !calls["second"] {
				return nil, errors.New("retry lost the current question or one of its tool pairs")
			}
			return schema.AssistantMessage("完成两步查询"), nil
		}
		id := "first"
		if results[id] {
			id = "second"
		}
		return schema.AssistantToolCallMessage([]schema.ToolCall{{
			ID: id, Type: "function", Function: schema.FunctionCall{Name: "lookup", Arguments: `{}`},
		}}), nil
	}}
	kit := tool.NewToolkit()
	kit.Register(tool.NewFuncTool("lookup", "查询", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			executions++
			return tool.Success("查询成功"), nil
		}))
	agent := agent_v2.NewBasicAgent(agent_v2.WithName(name), agent_v2.WithModel(llm), agent_v2.WithToolkit(kit),
		agent_v2.WithReactiveCompact(&agent_v2.ReactiveCompactConfig{Enabled: true, MaxAttempts: 1}))
	session := agent_v2.NewRunSession()
	seedAsst(session, name, "old-run", oldText+strings.Repeat(" old fact", 1600))
	events, err := agent.Run(t.Context(), session, schema.UserMessage(question))
	if err != nil {
		t.Fatalf("应裁掉旧任务并使用本次请求回答，调用 %d 次，错误 %v", attempts, err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil || final.Content != "完成两步查询" || attempts != 2 || executions != 2 {
		t.Fatalf("final=%v，超窗轮请求=%d，实际工具执行=%d", final, attempts, executions)
	}
}

func TestReactiveCompactDropsOldRoundAheadOfProtectedCurrentRound(t *testing.T) {
	old := schema.UserMessage(strings.Repeat("上一轮 ", 2000))
	current := schema.UserMessage("继续按技能处理")
	call := schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID: "load-skill", Type: "function", Function: schema.FunctionCall{Name: "activate_skill", Arguments: `{}`},
	}})
	result := schema.ToolResultMessage("load-skill", "技能正文")
	result.Annotations = map[string]any{agent_v2.SkillBodyAnnotation: true}
	system := schema.SystemMessage("后置的业务约束")
	input := []*schema.Message{old, current, call, result, system}
	out, err := agent_v2.ReactiveCompactRun(t.Context(),
		&agent_v2.ReactiveCompactConfig{Enabled: true}, input, errors.New("prompt is too long"))
	if err != nil || len(out) != 4 {
		t.Fatalf("应裁掉唯一可裁的旧轮，原长度 %d，实际长度 %d，错误 %v", len(input), len(out), err)
	}
	for i, want := range input[1:] {
		if out[i] != want {
			t.Fatalf("当前轮和 system 应按原顺序保留，第 %d 条为 %+v", i, out[i])
		}
	}
}

func TestReactiveCompactKeepsLatestToolPairAndUnansweredInputs(t *testing.T) {
	old := schema.UserMessage(strings.Repeat("超长旧对话 ", 1500))
	current := schema.UserMessage("本次请求")
	first := schema.AssistantMessage("第一轮")
	first.Annotations = map[string]any{compose_v2.RunIDAnnotationKey: "first"}
	obsolete := schema.AssistantMessage("中间旧输出")
	obsolete.Annotations = map[string]any{compose_v2.RunIDAnnotationKey: "obsolete"}
	call := schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID: "last", Type: "function", Function: schema.FunctionCall{Name: "lookup", Arguments: `{}`},
	}})
	call.Annotations = map[string]any{compose_v2.RunIDAnnotationKey: "last"}
	result := schema.ToolResultMessage("last", "最新查询结果")
	interjection := schema.UserMessage("先查 2026 年")
	interjection.Annotations = map[string]any{agent_v2.InterjectionAnnotation: true}
	note := schema.UserMessage("本轮工具重复调用提醒")
	note.Annotations = map[string]any{agent_v2.RepeatToolReminderAnnotation: true}
	feedback := schema.UserMessage("还要包含价格")
	feedback.Annotations = map[string]any{agent_v2.TurnGateFeedbackAnnotation: true}
	system := schema.SystemMessage("约束")
	input := []*schema.Message{old, current, first, obsolete, call, result, interjection, note, feedback, system}
	out, err := agent_v2.ReactiveCompactRun(t.Context(), &agent_v2.ReactiveCompactConfig{Enabled: true},
		input, errors.New("Input is too long: 18000 input tokens (max 500)"))
	want := []*schema.Message{current, first, call, result, interjection, note, feedback, system}
	if err != nil || len(out) != len(want) {
		t.Fatalf("应只裁掉旧对话与中间输出，实际长度 %d，期望 %d，错误 %v", len(out), len(want), err)
	}
	for i, expected := range want {
		if out[i] != expected {
			t.Fatalf("第 %d 条内容 %q，期望 %q", i, out[i].Content, expected.Content)
		}
	}
}

func TestReactiveCompactContinuesTrimmingPastItsUserMarker(t *testing.T) {
	old := schema.AssistantMessage(strings.Repeat("很旧的答复 ", 1000))
	old.Annotations = map[string]any{compose_v2.RunIDAnnotationKey: "old"}
	middle := schema.AssistantMessage(strings.Repeat("中间的答复 ", 1000))
	middle.Annotations = map[string]any{compose_v2.RunIDAnnotationKey: "middle"}
	latest := schema.AssistantMessage("最新答复")
	latest.Annotations = map[string]any{compose_v2.RunIDAnnotationKey: "latest"}
	cfg := &agent_v2.ReactiveCompactConfig{Enabled: true, UserMarker: "继续处理"}
	overLimit := errors.New("prompt is too long")
	first, err := agent_v2.ReactiveCompactRun(t.Context(), cfg, []*schema.Message{old, middle, latest}, overLimit)
	if err != nil || len(first) != 3 || first[1] != middle || first[2] != latest {
		t.Fatalf("首次应删掉最旧答复并补提示，长度=%d，错误 %v", len(first), err)
	}
	if first[0].Role != schema.RoleUser || first[0].Content != cfg.UserMarker {
		t.Fatalf("重试提示错误：%+v", first[0])
	}
	second, err := agent_v2.ReactiveCompactRun(t.Context(), cfg, first, overLimit)
	if err != nil || len(second) != 2 || second[0].Content != cfg.UserMarker || second[1] != latest {
		t.Fatalf("重试提示之前没有真实用户输入，第二次应继续裁掉中间答复，长度=%d，错误 %v", len(second), err)
	}
}

func TestReactiveCompactCombinesSpillIndexesAcrossRetries(t *testing.T) {
	batch := func(id, archiveID string) []*schema.Message {
		call := schema.AssistantToolCallMessage([]schema.ToolCall{{
			ID: id, Type: "function", Function: schema.FunctionCall{Name: "lookup", Arguments: `{}`},
		}})
		call.Annotations = map[string]any{compose_v2.RunIDAnnotationKey: id}
		result := schema.ToolResultMessage(id, strings.Repeat("已转存的证据 ", 1200))
		result.Annotations = map[string]any{agent_v2.ToolResultSpillAnnotation: archiveID}
		return []*schema.Message{call, result}
	}
	old := schema.UserMessage("旧请求")
	current := schema.UserMessage("当前请求")
	first := schema.AssistantMessage("第一轮答复")
	first.Annotations = map[string]any{compose_v2.RunIDAnnotationKey: "first"}
	latest := schema.AssistantMessage("最后答复")
	latest.Annotations = map[string]any{compose_v2.RunIDAnnotationKey: "latest"}

	in := append([]*schema.Message{old}, batch("older", "sp_ab123")...)
	in = append(in, current, first)
	in = append(in, batch("middle", "sp_ab456")...)
	in = append(in, latest)
	cfg := &agent_v2.ReactiveCompactConfig{Enabled: true}
	overflow := errors.New("prompt is too long")
	firstRetry, err := agent_v2.ReactiveCompactRun(t.Context(), cfg, in, overflow)
	if err != nil || len(firstRetry) != 6 || !strings.Contains(firstRetry[0].Content, "sp_ab123") {
		t.Fatalf("首次应淘汰最旧工具往返并补索引，长度=%d，错误 %v", len(firstRetry), err)
	}
	secondRetry, err := agent_v2.ReactiveCompactRun(t.Context(), cfg, firstRetry, overflow)
	if err != nil || len(secondRetry) != 4 || compose_v2.EstimateMessageTokens(secondRetry) >= compose_v2.EstimateMessageTokens(firstRetry) {
		t.Fatalf("已有索引时下一次应继续裁短窗口，长度=%d，错误 %v", len(secondRetry), err)
	}
	index := secondRetry[0]
	if strings.Count(index.Content, "sp_ab123") != 1 || strings.Count(index.Content, "sp_ab456") != 1 ||
		secondRetry[1] != current || secondRetry[2] != first || secondRetry[3] != latest {
		t.Fatalf("新旧转存引用应合在一条索引并保留当前轮，模型实收：%s", renderRoles(secondRetry))
	}
}
