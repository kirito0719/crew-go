package agent_v2_test

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/store"
	"crew-go/tool"
)

type recoveryReaderModel struct {
	crossRun bool
	calls    int
	id       string
	bound    bool
	views    [][]*schema.Message
}

type recoveryInspectModel struct {
	inspect func(context.Context, []*schema.Message) (*schema.Message, error)
}

func (m recoveryInspectModel) Generate(ctx context.Context, in []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	return m.inspect(ctx, in)
}
func (m recoveryInspectModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}
func (m recoveryInspectModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

func TestContextRecoveryPreservesCurrentInputAndPendingInbox(t *testing.T) {
	var inbox agent_v2.Inbox
	if err := inbox.Push(schema.UserMessage(strings.Repeat("pending details ", 800) + "FIRST_PENDING_TAIL")); err != nil {
		t.Fatal(err)
	}
	if err := inbox.Push(schema.UserMessage("last pending input")); err != nil {
		t.Fatal(err)
	}
	called := false
	llm := recoveryInspectModel{inspect: func(_ context.Context, in []*schema.Message) (*schema.Message, error) {
		called = true
		var all strings.Builder
		for _, m := range in {
			all.WriteString(m.Content)
		}
		if !strings.Contains(all.String(), "ORIGINAL_CURRENT_TAIL") || !strings.Contains(all.String(), "FIRST_PENDING_TAIL") {
			return nil, errors.New("unanswered current input was folded")
		}
		return schema.AssistantMessage("answered all pending inputs"), nil
	}}
	a := agent_v2.NewBasicAgent(agent_v2.WithName("pending"), agent_v2.WithModel(llm),
		agent_v2.WithContextRecovery(&agent_v2.ContextRecoveryConfig{Store: store.NewInMemoryStore(), MaxTokens: 500, KeepRecentRounds: 1}))
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	_, err := a.Run(agent_v2.WithInboxCtx(ctx, &inbox), agent_v2.NewRunSession(), schema.UserMessage(strings.Repeat("current details ", 800)+"ORIGINAL_CURRENT_TAIL"))
	if err != nil || !called {
		t.Fatalf("pending input protection: called=%v err=%v", called, err)
	}
}

// A 已经读过历史但尚未完成时让 B 完成；A 的折叠不能遮掉 B 的新增记录。
func TestContextRecoveryConcurrentTaskKeepsNewHistory(t *testing.T) {
	for _, inherited := range []bool{false, true} {
		t.Run(fmt.Sprint(inherited), func(t *testing.T) { runConcurrentRecoveryHistory(t, inherited) })
	}
}

func runConcurrentRecoveryHistory(t *testing.T, inherited bool) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	ctx = compose_v2.WithOwner(compose_v2.WithThreadIDCtx(ctx, "concurrent-recovery"), "tenant")
	hs, archive := agent_v2.NewInMemoryHistoryStore(), store.NewInMemoryStore()
	if inherited {
		warm := agent_v2.NewBasicAgent(agent_v2.WithName("shared-agent"), agent_v2.WithModel(&compactLoopModel{rounds: 30}), agent_v2.WithToolkit(grepToolkit()), agent_v2.WithMaxRounds(400),
			agent_v2.WithHistory(hs, "tenant"), agent_v2.WithMaxHistoryPairs(0), agent_v2.WithAutoCompact(smallWindow(&countingSummarizer{})))
		if _, err := warm.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("warm old summary")); err != nil {
			t.Fatal(err)
		}
		prior, err := agent_v2.LoadAllHistory(ctx, hs, agent_v2.HistoryKey{Owner: "tenant", ConvID: "concurrent-recovery"})
		if err != nil || countBoundaries(prior) == 0 {
			t.Fatalf("missing inherited summary: %v", err)
		}
	}
	kit := tool.NewToolkit()
	kit.Register(tool.NewFuncTool("dump", "导出", map[string]any{"type": "object"}, func(context.Context, map[string]any) (*tool.Result, error) {
		return tool.Success(strings.Repeat("large A facts ", 1400)), nil
	}))
	build := func(m model.ChatModel, extra ...agent_v2.Option) *agent_v2.BasicAgent {
		opts := []agent_v2.Option{agent_v2.WithName("shared-agent"), agent_v2.WithModel(m), agent_v2.WithHistory(hs, "tenant"), agent_v2.WithMaxHistoryPairs(0), agent_v2.WithToolkit(kit)}
		return agent_v2.NewBasicAgent(append(opts, extra...)...)
	}
	ready, gate, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var aErr error
	released := false
	release := func() {
		if !released {
			released = true
			close(gate)
		}
	}
	n, folds := 0, 0
	am := recoveryInspectModel{inspect: func(ctx context.Context, in []*schema.Message) (*schema.Message, error) {
		if n < 3 {
			n++
			return schema.AssistantToolCallMessage([]schema.ToolCall{{ID: fmt.Sprintf("a%d", n), Type: "function", Function: schema.FunctionCall{Name: "dump", Arguments: `{}`}}}), nil
		}
		for _, m := range in {
			if m.Annotations[agent_v2.ContextRecoveryAnnotation] != nil {
				folds++
			}
		}
		close(ready)
		select {
		case <-gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return schema.AssistantMessage("A_FINAL"), nil
	}}
	go func() {
		defer close(done)
		_, err := build(am, agent_v2.WithContextRecovery(&agent_v2.ContextRecoveryConfig{Store: archive, MaxTokens: 900, KeepRecentRounds: 1})).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("A_INPUT"))
		aErr = err
	}()
	// 启动方在所有退出路径都释放屏障并等待模型 goroutine。
	defer func() { release(); cancel(); <-done }()
	select {
	case <-ready:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	bm := recoveryInspectModel{inspect: func(context.Context, []*schema.Message) (*schema.Message, error) {
		return schema.AssistantMessage("B_ONLY_RESULT"), nil
	}}
	if _, err := build(bm).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("B_ONLY_INPUT")); err != nil {
		t.Fatal(err)
	}
	release()
	<-done
	if aErr != nil {
		t.Fatal(aErr)
	}
	sawB := false
	cm := recoveryInspectModel{inspect: func(_ context.Context, in []*schema.Message) (*schema.Message, error) {
		for _, m := range in {
			if strings.Contains(m.Content, "B_ONLY_RESULT") {
				sawB = true
			}
		}
		return schema.AssistantMessage("C_FINAL"), nil
	}}
	if _, err := build(cm).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("C_INPUT")); err != nil {
		t.Fatal(err)
	}
	if folds == 0 || !sawB {
		t.Fatalf("folds=%d next run saw concurrent task=%v", folds, sawB)
	}
}

var recoveryIDPattern = regexp.MustCompile(`id="(cx_[0-9a-f]+)"`)

func (m *recoveryReaderModel) Generate(_ context.Context, in []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.views = append(m.views, in)
	n := m.calls
	m.calls++
	call := func(name, args string) (*schema.Message, error) {
		return schema.AssistantToolCallMessage([]schema.ToolCall{{ID: fmt.Sprintf("call-%d", n), Type: "function", Function: schema.FunctionCall{Name: name, Arguments: args}}}), nil
	}
	if n < 3 {
		return call("dump", `{}`)
	}
	var id string
	for _, msg := range in {
		if match := recoveryIDPattern.FindStringSubmatch(msg.Content); match != nil {
			id = match[1]
			break
		}
	}
	if id == "" || !m.bound {
		return nil, errors.New("archive view or recovery tool is missing")
	}
	if m.id != "" && m.id != id {
		return nil, errors.New("archive handle changed after resume")
	}
	m.id = id
	if n == 3 {
		if m.crossRun {
			return schema.AssistantMessage("first done"), nil
		}
		return call("pause", `{}`)
	}
	if n == 4 {
		return call(agent_v2.ReadContextToolName, fmt.Sprintf(`{"id":%q,"query":"needle_47"}`, id))
	}
	last := in[len(in)-1]
	if schema.GetToolResultID(last) == "" || !strings.Contains(last.Content, "needle_47") {
		return nil, fmt.Errorf("reader did not recover the old fact: %s", last.Content)
	}
	return schema.AssistantMessage("recovered needle_47"), nil
}
func (m *recoveryReaderModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}
func (m *recoveryReaderModel) WithTools(tools []*schema.ToolInfo) (model.ChatModel, error) {
	for _, info := range tools {
		if info.Name == agent_v2.ReadContextToolName {
			m.bound = true
		}
	}
	return m, nil
}

func TestContextRecoveryBasicAgentResumeKeepsArchiveAndFullHistory(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	ctx = compose_v2.WithThreadIDCtx(compose_v2.WithOwner(ctx, "tenant"), "conversation")
	ctx = compose_v2.WithCheckpointerCtx(ctx, compose_v2.NewInMemoryCheckpointer())
	st := store.NewInMemoryStore()
	hs := agent_v2.NewInMemoryHistoryStore()
	llm := &recoveryReaderModel{}
	kit := tool.NewToolkit()
	dumps := 0
	kit.Register(tool.NewFuncTool("dump", "导出", map[string]any{"type": "object"}, func(context.Context, map[string]any) (*tool.Result, error) {
		dumps++
		return tool.Success(strings.Repeat("long archived tool content ", 700) + "needle_47"), nil
	}))
	kit.Register(tool.NewFuncTool("pause", "继续前提问", map[string]any{"type": "object"}, func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
		ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "继续恢复？"})
		if err != nil {
			return nil, err
		}
		return tool.Success(ans.Content), nil
	}))
	a := agent_v2.NewBasicAgent(agent_v2.WithName("recovery"), agent_v2.WithModel(llm), agent_v2.WithToolkit(kit), agent_v2.WithMaxRounds(8),
		agent_v2.WithHistory(hs, "tenant"), agent_v2.WithMaxHistoryPairs(0),
		agent_v2.WithContextRecovery(&agent_v2.ContextRecoveryConfig{Store: st, MaxTokens: 900, KeepRecentRounds: 1}))
	sess := agent_v2.NewRunSession()
	_, err := a.Run(ctx, sess, schema.UserMessage("find historical needle"))
	interrupt, ok := compose_v2.IsInterrupt(err)
	if !ok || len(interrupt.AllQueries()) != 1 || llm.id == "" || dumps != 3 {
		t.Fatalf("run: dumps=%d id=%s err=%v", dumps, llm.id, err)
	}
	id := llm.id
	events, err := a.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess), agent_v2.WithResumeUserInputs(map[string]*schema.Message{
		interrupt.AllQueries()[0].InterruptID: schema.UserMessage("继续"),
	}))
	if err != nil || agent_v2.FinalMessage(events) == nil || agent_v2.FinalMessage(events).Content != "recovered needle_47" || dumps != 3 || id != llm.id {
		t.Fatalf("resume: dumps=%d err=%v", dumps, err)
	}
	// 历史中的原始大结果保留；折叠视图不能覆盖工具账本。
	full := 0
	history, loadErr := agent_v2.LoadAllHistory(ctx, hs, agent_v2.HistoryKey{Owner: "tenant", ConvID: "conversation"})
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	for _, msg := range history {
		if msg.Role == schema.RoleTool && strings.HasPrefix(msg.Content, "long archived tool content") {
			full++
		}
	}
	if full != 3 {
		t.Fatalf("full tool history entries=%d, want 3", full)
	}
	if len(llm.views) < 6 {
		t.Fatalf("model calls=%d", len(llm.views))
	}
	for _, view := range llm.views[3:] {
		pending := map[string]bool{}
		for _, msg := range view {
			for _, call := range schema.GetToolCalls(msg) {
				pending[call.ID] = true
			}
			if result := schema.GetToolResultID(msg); result != "" {
				if !pending[result] {
					t.Fatalf("orphan tool result %s", result)
				}
				delete(pending, result)
			}
		}
		if len(pending) > 0 {
			t.Fatalf("dangling tool calls: %v", pending)
		}
	}
}

// Loop 的每轮执行 namespace 会变，归档必须跟稳定的历史位置一起继承。
func TestContextRecoveryLoopUsesStableHistorySlot(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	ctx = compose_v2.WithThreadIDCtx(compose_v2.WithOwner(ctx, "tenant"), "loop-context")
	st := store.NewInMemoryStore()
	hs := agent_v2.NewInMemoryHistoryStore()
	llm := &recoveryReaderModel{crossRun: true}
	kit := tool.NewToolkit()
	dumps := 0
	kit.Register(tool.NewFuncTool("dump", "导出", map[string]any{"type": "object"}, func(context.Context, map[string]any) (*tool.Result, error) {
		dumps++
		return tool.Success(strings.Repeat("old tool content ", 1200) + "needle_47"), nil
	}))
	a := agent_v2.NewBasicAgent(agent_v2.WithName("body"), agent_v2.WithModel(llm), agent_v2.WithToolkit(kit), agent_v2.WithMaxRounds(8),
		agent_v2.WithHistory(hs, "tenant"), agent_v2.WithMaxHistoryPairs(0),
		agent_v2.WithContextRecovery(&agent_v2.ContextRecoveryConfig{Store: st, MaxTokens: 900, KeepRecentRounds: 1}))
	loop := agent_v2.NewLoopAgent(agent_v2.WithLoopName("loop"), agent_v2.WithLoopMaxIterations(2)).WithBody(a)
	events, err := loop.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("find old fact"))
	if err != nil || dumps != 3 || llm.calls != 6 || agent_v2.FinalMessage(events) == nil || agent_v2.FinalMessage(events).Content != "recovered needle_47" {
		t.Fatalf("cross-iteration recovery failed: dumps=%d calls=%d err=%v", dumps, llm.calls, err)
	}
}

type mergeRecoveryResults struct{ hook.NoOpHooks }

func (mergeRecoveryResults) OnModelStart(ctx context.Context, _ *hook.AgentInfo, in *schema.ModelInput) (context.Context, error) {
	for i := 0; i+1 < len(in.Messages); i++ {
		a, b := in.Messages[i], in.Messages[i+1]
		if schema.GetToolResultID(a) != "lookup-first" || schema.GetToolResultID(b) != "lookup-second" {
			continue
		}
		merged := *a
		merged.ContentBlocks = slices.Concat(a.ContentBlocks, b.ContentBlocks)
		in.Messages = slices.Concat(in.Messages[:i], []*schema.Message{&merged}, in.Messages[i+2:])
		break
	}
	return ctx, nil
}

func TestContextRecoveryBasicAgentArchivesMergedToolResults(t *testing.T) {
	const (
		name         = "recovery-merged-results"
		originalFact = "SECOND_LOOKUP_EVIDENCE"
	)
	key := agent_v2.HistoryKey{Owner: "tenant", ConvID: "merged-tool-results"}
	ctx := compose_v2.WithOwner(compose_v2.WithThreadIDCtx(t.Context(), key.ConvID), key.Owner)
	history := agent_v2.NewInMemoryHistoryStore()
	prior := []*schema.Message{
		schema.UserMessage("上次查两份证据"),
		schema.AssistantToolCallMessage([]schema.ToolCall{
			{ID: "lookup-first", Function: schema.FunctionCall{Name: "lookup", Arguments: `{"n":1}`}},
			{ID: "lookup-second", Function: schema.FunctionCall{Name: "lookup", Arguments: `{"n":2}`}},
		}),
		schema.ToolResultMessage("lookup-first", strings.Repeat("first older evidence ", 500)),
		schema.ToolResultMessage("lookup-second", strings.Repeat("second older evidence ", 500)+originalFact),
	}
	for _, m := range prior {
		m.Annotations = map[string]any{agent_v2.HistoryAgentAnnotation: name, agent_v2.HistoryTaskAnnotation: "old-task"}
	}
	if err := history.Append(ctx, key, prior); err != nil {
		t.Fatal(err)
	}

	var archiveID string
	llm := recoveryInspectModel{inspect: func(_ context.Context, in []*schema.Message) (*schema.Message, error) {
		if archiveID != "" {
			if last := in[len(in)-1]; schema.GetToolResultID(last) == "read-old" && strings.Contains(last.Content, originalFact) {
				return schema.AssistantMessage("找到了第二份证据"), nil
			}
			return nil, errors.New("read_context did not return the second result from the archive")
		}
		for _, m := range in {
			match := recoveryIDPattern.FindStringSubmatch(m.Content)
			if match == nil {
				continue
			}
			if !strings.Contains(m.Content, "result=lookup-first") || !strings.Contains(m.Content, "result=lookup-second") {
				return nil, errors.New("recovery manifest did not list both result blocks")
			}
			archiveID = match[1]
			return schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "read-old", Function: schema.FunctionCall{
				Name:      agent_v2.ReadContextToolName,
				Arguments: fmt.Sprintf(`{"id":%q,"query":%q}`, archiveID, originalFact),
			}}}), nil
		}
		for _, m := range in {
			if schema.GetToolResultID(m) == "lookup-first" && len(m.ContentBlocks) == 2 {
				return nil, errors.New("complete merged tool results were not archived")
			}
		}
		return nil, errors.New("test did not present the merged tool result to the model")
	}}
	a := agent_v2.NewBasicAgent(
		agent_v2.WithName(name), agent_v2.WithHistory(history, key.Owner), agent_v2.WithModel(llm),
		agent_v2.WithHooks(mergeRecoveryResults{}),
		agent_v2.WithContextRecovery(&agent_v2.ContextRecoveryConfig{
			Store: store.NewInMemoryStore(), MaxTokens: 600, KeepRecentRounds: 1,
		}),
	)
	events, err := a.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("找回第二份证据"))
	if err != nil {
		t.Fatal(err)
	}
	if final := agent_v2.FinalMessage(events); archiveID == "" || final == nil || final.Content != "找到了第二份证据" {
		t.Fatalf("定位符 %q，最终答复 %+v", archiveID, final)
	}
	retained, err := agent_v2.LoadAllHistory(ctx, history, key)
	if err != nil {
		t.Fatal(err)
	}
	if len(retained) < len(prior) {
		t.Fatalf("历史只剩 %d 条，至少应保留原有 %d 条", len(retained), len(prior))
	}
	for i, original := range prior[2:] {
		m := retained[i+2]
		if schema.GetToolResultID(m) != schema.GetToolResultID(original) || m.Content != original.Content || len(m.ContentBlocks) != 1 {
			t.Fatalf("旧结果 %d 的原始历史被改写", i)
		}
	}
}

type switchRecoveryNote struct {
	hook.NoOpHooks
	note, older, recent *schema.Message
}

func (h switchRecoveryNote) OnModelStart(ctx context.Context, info *hook.AgentInfo, in *schema.ModelInput) (context.Context, error) {
	note := *h.note
	if info.Round == 0 {
		note.Annotations = map[string]any{agent_v2.RepeatToolReminderAnnotation: true}
	}
	in.Messages = slices.Concat(in.Messages[:1], []*schema.Message{&note, h.older, h.recent}, in.Messages[1:])
	return ctx, nil
}

func TestContextRecoveryUserClassificationInvalidatesMemo(t *testing.T) {
	note := schema.UserMessage(strings.Repeat("待确认的输入 ", 1000) + "USER_INPUT_TAIL")
	h := switchRecoveryNote{
		note:   note,
		older:  withRunID(schema.AssistantMessage("旧的工具提示之后的说明"), "older"),
		recent: withRunID(schema.AssistantMessage("最新的执行状态"), "recent"),
	}
	var calls int
	llm := recoveryInspectModel{inspect: func(_ context.Context, msgs []*schema.Message) (*schema.Message, error) {
		calls++
		var sawArchive, sawUser bool
		for _, m := range msgs {
			sawArchive = sawArchive || recoveryIDPattern.MatchString(m.Content)
			sawUser = sawUser || m.ID == note.ID && m.Role == schema.RoleUser &&
				!agent_v2.IsFrameworkNote(m) && m.Content == note.Content
		}
		if calls == 1 {
			if !sawArchive || strings.Contains(inputText(msgs), "USER_INPUT_TAIL") {
				return nil, errors.New("framework note was not folded in the first round")
			}
			return schema.AssistantToolCallMessage([]schema.ToolCall{{
				ID: "verify-note", Function: schema.FunctionCall{Name: "lookup", Arguments: `{}`},
			}}), nil
		}
		if !sawUser {
			return nil, errors.New("old recovery memo hid the input after it became a user message")
		}
		return schema.AssistantMessage("已收到本轮改为用户输入的内容"), nil
	}}
	kit := tool.NewToolkit()
	kit.Register(tool.NewFuncTool("lookup", "查询", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) { return tool.Success("已验证"), nil }))
	a := agent_v2.NewBasicAgent(agent_v2.WithName("recovery-note"), agent_v2.WithModel(llm),
		agent_v2.WithHooks(h), agent_v2.WithToolkit(kit), agent_v2.WithContextRecovery(&agent_v2.ContextRecoveryConfig{
			Store: store.NewInMemoryStore(), MaxTokens: 600, KeepRecentRounds: 1,
		}))
	events, err := a.Run(t.Context(), agent_v2.NewRunSession(), schema.UserMessage("初始输入"))
	if err != nil {
		t.Fatal(err)
	}
	if final := agent_v2.FinalMessage(events); calls != 2 || final == nil || final.Content != "已收到本轮改为用户输入的内容" {
		t.Fatalf("模型调用 %d 次，最终答复 %+v", calls, final)
	}
}
