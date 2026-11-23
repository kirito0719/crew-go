package agent_v2_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
)

// boundaryAgent：开了会话历史、关掉轮数截断、靠 AutoCompact 控制上下文——压缩边界服务的正是这种配置。
func boundaryAgent(name string, hs agent_v2.HistoryStore, m *compactLoopModel, s *countingSummarizer) *agent_v2.BasicAgent {
	return agent_v2.NewBasicAgent(agent_v2.WithName(name), agent_v2.WithModel(m),
		agent_v2.WithToolkit(grepToolkit()), agent_v2.WithMaxRounds(400),
		agent_v2.WithHistory(hs, "t"), agent_v2.WithMaxHistoryPairs(0),
		agent_v2.WithAutoCompact(smallWindow(s)))
}

func countBoundaries(msgs []*schema.Message) int {
	n := 0
	for _, m := range msgs {
		if _, ok := m.Annotations[agent_v2.HistoryCompactionAnnotation]; ok {
			n++
		}
	}
	return n
}

// TestCompactionBoundary_NextRunStartsFromBoundary：上一次 Run 压缩过，下一次 Run 从最新的压缩边界往后
// 读——读回的条数是「边界之后」的量级，第一次模型调用看到的是上次的摘要而不是最早的原文，也不用在第一轮
// 把整场历史重新摘要一遍。
//
// 断言选在三个量上：Run 2 读回了多少条历史（没有边界时是整场）；Run 2 第一次模型调用里有没有上次的摘要；
// Run 2 要摘要时摘的是哪一段（没有边界时 prior 是整场原文、必然超阈值，摘要输入里是整场的工具结果）。Run 1 收尾时
// 离阈值多近随轮数而定，Run 2 摘不摘要不是断言的量。
func TestCompactionBoundary_NextRunStartsFromBoundary(t *testing.T) {
	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-boundary")
	key := agent_v2.HistoryKey{Owner: "t", ConvID: "conv-boundary"}
	st := &pagedStore{inner: agent_v2.NewInMemoryHistoryStore()}

	m1 := &compactLoopModel{rounds: 300}
	s1 := &countingSummarizer{}
	if _, err := boundaryAgent("coder", st, m1, s1).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("第一件事")); err != nil {
		t.Fatal(err)
	}
	all, _ := agent_v2.LoadAllHistory(ctx, st, key)
	if countBoundaries(all) == 0 {
		t.Fatalf("fixture 没搭对：Run 1 摘要了 %d 次，历史里却没有压缩边界", s1.count())
	}
	lastSummary := fmt.Sprintf("SUMMARY-S%d", s1.count())

	m2 := &compactLoopModel{rounds: 1}
	s2 := &countingSummarizer{}
	st.msgs.Store(0)
	if _, err := boundaryAgent("coder", st, m2, s2).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("第二件事")); err != nil {
		t.Fatal(err)
	}
	first := inputText(m2.inputs[0])
	if !strings.Contains(first, lastSummary) {
		t.Fatalf("Run 2 第一次模型调用没看到 Run 1 最后那份摘要 %s：\n%.300s", lastSummary, first)
	}
	if strings.Contains(first, "第一件事") && !strings.Contains(first, "SUMMARY") {
		t.Fatalf("Run 2 看到的是 Run 1 的原文而不是摘要")
	}
	if s2.count() > 0 {
		total := strings.Count(inputText(all), "src/a.go:12")
		if got := strings.Count(inputText(s2.inputs[0]), "src/a.go:12"); got*3 > total {
			t.Fatalf("Run 2 在整场原文上重新摘要了一遍（摘要输入里 %d 行旧工具结果，整场 %d 行）", got, total)
		}
	}
	if n := st.msgs.Load(); n*3 > int64(len(all)) {
		t.Fatalf("会话 %d 条，Run 2 读回 %d 条——没从压缩边界开始读", len(all), n)
	} else {
		t.Logf("会话 %d 条（含 %d 条边界），Run 2 读回 %d 条", len(all), countBoundaries(all), n)
	}
}

// TestCompactionBoundary_OtherAgentsBoundaryIgnored：一条边界只对写下它的那个 agent 成立。别的 agent
// 读自己的视图时既不在别人的边界处停、也看不到它（它不是谁说的话）。
func TestCompactionBoundary_OtherAgentsBoundaryIgnored(t *testing.T) {
	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-two")
	key := agent_v2.HistoryKey{Owner: "t", ConvID: "conv-two"}
	hs := agent_v2.NewInMemoryHistoryStore()

	// b 先说几句，a 再跑一个会压缩的长任务
	mb := &compactLoopModel{rounds: 0}
	if _, err := boundaryAgent("b", hs, mb, &countingSummarizer{}).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("B-ONLY-QUESTION")); err != nil {
		t.Fatal(err)
	}
	sa := &countingSummarizer{}
	if _, err := boundaryAgent("a", hs, &compactLoopModel{rounds: 60}, sa).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("A 的长任务")); err != nil {
		t.Fatal(err)
	}
	all, _ := agent_v2.LoadAllHistory(ctx, hs, key)
	if countBoundaries(all) == 0 {
		t.Fatal("fixture 没搭对：a 没留下压缩边界")
	}
	// b 再跑一次：它的 prior 里要有自己上次的问题，不能有 a 的摘要
	mb2 := &compactLoopModel{rounds: 0}
	if _, err := boundaryAgent("b", hs, mb2, &countingSummarizer{}).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("b 的第二问")); err != nil {
		t.Fatal(err)
	}
	in := inputText(mb2.inputs[0])
	if !strings.Contains(in, "B-ONLY-QUESTION") {
		t.Fatalf("b 在 a 的边界处停下了，丢了自己上次的问题：\n%.300s", in)
	}
	if strings.Contains(in, "SUMMARY-S") {
		t.Fatalf("b 的视图里出现了 a 的摘要：\n%.300s", in)
	}
}

// TestCompactionBoundary_CorruptBoundaryIgnored：替代历史解不出来的边界当它不存在，接着往前读——
// 不能因为一条坏记录让 prior 变空。
func TestCompactionBoundary_CorruptBoundaryIgnored(t *testing.T) {
	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-bad")
	key := agent_v2.HistoryKey{Owner: "t", ConvID: "conv-bad"}
	hs := agent_v2.NewInMemoryHistoryStore()
	mk := func(m *schema.Message) *schema.Message {
		m.Annotations = map[string]any{agent_v2.HistoryAgentAnnotation: "coder"}
		return m
	}
	bad := mk(schema.UserMessage("坏掉的边界"))
	bad.Annotations[agent_v2.HistoryCompactionAnnotation] = "{not json"
	bad.Annotations["crew_history_compaction_mode"] = "0"
	_ = hs.Append(ctx, key, []*schema.Message{
		mk(schema.UserMessage("EARLY-QUESTION")), mk(schema.AssistantMessage("EARLY-ANSWER")), bad,
	})
	m := &compactLoopModel{rounds: 0}
	if _, err := boundaryAgent("coder", hs, m, &countingSummarizer{}).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("新问题")); err != nil {
		t.Fatal(err)
	}
	if in := inputText(m.inputs[0]); !strings.Contains(in, "EARLY-QUESTION") || strings.Contains(in, "坏掉的边界") {
		t.Fatalf("坏边界之前的记录应照常读回、坏边界本身不进 prompt：\n%.300s", in)
	}
}

// TestCompactionBoundary_NotWrittenWhenHookAltersMessages：OnModelStart 钩子改了这一轮的消息列表时不写
// 压缩边界——压缩是在钩子改写之后的那份上做的，写进去的替代历史会带着钩子插入 / 改写的内容，下一次
// Run 当成历史重放。
func TestCompactionBoundary_NotWrittenWhenHookAltersMessages(t *testing.T) {
	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-hook")
	key := agent_v2.HistoryKey{Owner: "t", ConvID: "conv-hook"}
	hs := agent_v2.NewInMemoryHistoryStore()
	s := &countingSummarizer{}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("coder"), agent_v2.WithModel(&compactLoopModel{rounds: 60}),
		agent_v2.WithToolkit(grepToolkit()), agent_v2.WithMaxRounds(100),
		agent_v2.WithHistory(hs, "t"), agent_v2.WithMaxHistoryPairs(0), agent_v2.WithHooks(&prefixRewriteHook{}),
		agent_v2.WithAutoCompact(smallWindow(s)))
	if _, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("改代码 原始")); err != nil {
		t.Fatal(err)
	}
	if s.count() == 0 {
		t.Fatal("fixture 没搭对：一次摘要都没发生")
	}
	all, _ := agent_v2.LoadAllHistory(ctx, hs, key)
	if n := countBoundaries(all); n != 0 {
		t.Fatalf("钩子每轮改写消息，却写了 %d 条压缩边界——钩子改写的内容会被当成历史重放", n)
	}
}

// 同一成员的 B 在 A 摘要完成、主模型仍阻塞时提交。原始历史及 C 的首次送模都应按追加顺序保留 B。
func TestCompactionBoundary_ConcurrentAppend(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	ctx = compose_v2.WithThreadIDCtx(ctx, "overlap")
	key := agent_v2.HistoryKey{Owner: "t", ConvID: "overlap"}
	hs := agent_v2.NewInMemoryHistoryStore()
	seedCompactionHistory(t, hs, key)
	build := func(m model.ChatModel, extra ...agent_v2.Option) *agent_v2.BasicAgent {
		opts := []agent_v2.Option{agent_v2.WithName("coder"), agent_v2.WithModel(m), agent_v2.WithHistory(hs, "t"), agent_v2.WithMaxHistoryPairs(0)}
		return agent_v2.NewBasicAgent(append(opts, extra...)...)
	}
	ready, resume, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	summary := &countingSummarizer{}
	var runErr error
	go func() {
		defer close(done)
		m := recoveryInspectModel{inspect: func(ctx context.Context, in []*schema.Message) (*schema.Message, error) {
			if !strings.Contains(inputText(in), "SUMMARY-S1") {
				return nil, fmt.Errorf("模型未收到 A 的摘要")
			}
			close(ready)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-resume:
				return schema.AssistantMessage("A-final"), nil
			}
		}}
		_, runErr = build(m, agent_v2.WithAutoCompact(smallWindow(summary))).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("A-input"))
	}()
	defer func() { cancel(); <-done }()
	select {
	case <-ready:
	case <-done:
		t.Fatalf("A 在摘要后屏障前退出: %v", runErr)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	run := func(question, answer string) {
		t.Helper()
		m := recoveryInspectModel{inspect: func(context.Context, []*schema.Message) (*schema.Message, error) {
			return schema.AssistantMessage(answer), nil
		}}
		if _, err := build(m).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage(question)); err != nil {
			t.Fatal(err)
		}
	}
	run("B-question", "B-answer")
	close(resume)
	<-done
	if runErr != nil {
		t.Fatal(runErr)
	}
	raw, err := agent_v2.LoadAllHistory(ctx, hs, key)
	if err != nil {
		t.Fatal(err)
	}
	c := &compactLoopModel{}
	if _, err := build(c).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("C-input")); err != nil {
		t.Fatal(err)
	}
	for _, view := range []struct {
		name string
		msgs []*schema.Message
	}{{"历史", raw}, {"C 的首次请求", c.inputs[0]}} {
		var got []string
		for _, msg := range view.msgs {
			switch msg.Content {
			case "B-question", "B-answer", "A-final":
				got = append(got, msg.Content)
			}
		}
		if strings.Join(got, ",") != "B-question,B-answer,A-final" {
			t.Fatalf("%s 保留的并发问答顺序 = %v", view.name, got)
		}
	}
}

func seedCompactionHistory(t *testing.T, hs agent_v2.HistoryStore, key agent_v2.HistoryKey) {
	t.Helper()
	var seed []*schema.Message
	for i := 0; i < 12; i++ {
		for _, msg := range []*schema.Message{schema.UserMessage(fmt.Sprint(i)), schema.AssistantMessage(strings.Repeat("earlier words ", 400))} {
			msg.Annotations = map[string]any{agent_v2.HistoryAgentAnnotation: "coder"}
			seed = append(seed, msg)
		}
	}
	if err := hs.Append(t.Context(), key, seed); err != nil {
		t.Fatal(err)
	}
}

func TestCompactionBoundary_ResumeAfterOtherWriter(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	ctx = compose_v2.WithThreadIDCtx(ctx, "resume-with-gap")
	key := agent_v2.HistoryKey{Owner: "t", ConvID: "resume-with-gap"}
	hs := agent_v2.NewInMemoryHistoryStore()
	seedCompactionHistory(t, hs, key)
	build := func(m *compactLoopModel, sum *countingSummarizer) *agent_v2.BasicAgent {
		return agent_v2.NewBasicAgent(agent_v2.WithName("coder"), agent_v2.WithModel(m), agent_v2.WithHistory(hs, "t"),
			agent_v2.WithMaxHistoryPairs(0), agent_v2.WithAutoCompact(smallWindow(sum)), agent_v2.WithHooks(askOnModelEnd{}))
	}
	cp := compose_v2.NewInMemoryCheckpointer()
	sess := agent_v2.NewRunSession()
	modelA, sum := &compactLoopModel{}, &countingSummarizer{}
	_, err := build(modelA, sum).Run(compose_v2.WithCheckpointerCtx(ctx, cp), sess, schema.UserMessage("A"))
	it, ok := compose_v2.IsInterrupt(err)
	if !ok || len(it.AllQueries()) != 1 || sum.count() != 1 || !strings.Contains(inputText(modelA.inputs[0]), "SUMMARY-S1") {
		t.Fatalf("A 尚未在摘要之后挂起: %v", err)
	}
	saved, err := cp.LoadNS(ctx, sess.RunID, "")
	if err != nil {
		t.Fatal(err)
	}
	wire, err := compose_v2.MarshalCheckpoint(saved)
	if err != nil {
		t.Fatal(err)
	}
	state, err := compose_v2.UnmarshalCheckpoint(wire)
	if err != nil {
		t.Fatal(err)
	}
	fresh := compose_v2.NewInMemoryCheckpointer()
	if err := fresh.SaveNS(ctx, sess.RunID, "", state); err != nil {
		t.Fatal(err)
	}
	b := schema.AssistantMessage("B-during-pause")
	b.Annotations = map[string]any{agent_v2.HistoryAgentAnnotation: "coder"}
	if err := hs.Append(ctx, key, []*schema.Message{b}); err != nil {
		t.Fatal(err)
	}
	replayed := &compactLoopModel{}
	events, err := build(replayed, &countingSummarizer{}).Resume(compose_v2.WithCheckpointerCtx(ctx, fresh), sess.RunID,
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{it.AllQueries()[0].InterruptID: schema.UserMessage("可以")}))
	if err != nil || len(replayed.inputs) != 0 || agent_v2.FinalMessage(events) == nil {
		t.Fatalf("续跑应复用已完成的主模型答复: calls=%d err=%v", len(replayed.inputs), err)
	}
	c := &compactLoopModel{}
	reader := agent_v2.NewBasicAgent(agent_v2.WithName("coder"), agent_v2.WithModel(c), agent_v2.WithHistory(hs, "t"), agent_v2.WithMaxHistoryPairs(0))
	if _, err := reader.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("C")); err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(inputText(c.inputs[0]), b.Content); got != 1 {
		t.Fatalf("续跑后 C 看到 B 的次数 = %d", got)
	}
}

func TestCompactionBoundary_LegacyAndCopiedRows(t *testing.T) {
	ctx := compose_v2.WithThreadIDCtx(t.Context(), "copied")
	key := agent_v2.HistoryKey{Owner: "t", ConvID: "copied"}
	hs := agent_v2.NewInMemoryHistoryStore()
	seedCompactionHistory(t, hs, key)
	if _, err := boundaryAgent("coder", hs, &compactLoopModel{}, &countingSummarizer{}).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("A")); err != nil {
		t.Fatal(err)
	}
	page, err := hs.Load(ctx, key, agent_v2.HistoryQuery{})
	if err != nil || len(page.VerifiedBoundaries) != 1 {
		t.Fatalf("应恰有一个已确认边界: %v %v", page.VerifiedBoundaries, err)
	}
	var boundary *schema.Message
	for i := range page.VerifiedBoundaries {
		boundary = page.Messages[i]
	}
	b := schema.AssistantMessage("B-after-snapshot")
	b.Annotations = map[string]any{agent_v2.HistoryAgentAnnotation: "coder"}
	// 旧版和从同一会话复制的边界均有完整且可解码的替代历史，只有物理位置的证明能区分它们。
	if err := hs.Append(ctx, key, []*schema.Message{b, b, boundary}); err != nil {
		t.Fatal(err)
	}
	c := &compactLoopModel{}
	reader := agent_v2.NewBasicAgent(agent_v2.WithName("coder"), agent_v2.WithModel(c), agent_v2.WithHistory(hs, "t"), agent_v2.WithMaxHistoryPairs(0))
	if _, err := reader.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("C")); err != nil {
		t.Fatal(err)
	}
	in := inputText(c.inputs[0])
	if strings.Count(in, b.Content) != 2 || strings.Count(in, "SUMMARY-S1") != 1 {
		t.Fatal("C 应看到原边界一次以及两条同 ID 的后续记录")
	}
}

// 包装器仅接管普通追加时，嵌入的底层条件写方法不能绕过它。
type filteredBoundaryStore struct {
	*agent_v2.InMemoryHistoryStore
	writes int
}

func (s *filteredBoundaryStore) Append(ctx context.Context, key agent_v2.HistoryKey, msgs []*schema.Message) error {
	s.writes++
	return nil // 此包装器按策略丢弃整批。
}

func TestCompactionBoundary_RespectsAppendDecorator(t *testing.T) {
	hs := &filteredBoundaryStore{InMemoryHistoryStore: agent_v2.NewInMemoryHistoryStore()}
	ctx := compose_v2.WithThreadIDCtx(t.Context(), "decorated")
	if _, err := boundaryAgent("coder", hs, &compactLoopModel{rounds: 30}, &countingSummarizer{}).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("private")); err != nil {
		t.Fatal(err)
	}
	page, err := hs.Load(ctx, agent_v2.HistoryKey{Owner: "t", ConvID: "decorated"}, agent_v2.HistoryQuery{})
	if err != nil || hs.writes == 0 || len(page.Messages) != 0 {
		t.Fatalf("绕过包装器的写入策略: calls=%d rows=%d err=%v", hs.writes, len(page.Messages), err)
	}
}

// 用已提交但回执丢失的第一批模拟不确定写入；后续答复仍落库且不能重复首批或发布候选边界。
type lostHistoryReceipt struct {
	*agent_v2.InMemoryHistoryStore
	attempts int
}

func (s *lostHistoryReceipt) HistoryAppendOwner() agent_v2.HistoryStore { return s }
func (s *lostHistoryReceipt) AppendAfter(ctx context.Context, k agent_v2.HistoryKey, after agent_v2.HistoryCursor, msgs []*schema.Message, boundary int) (agent_v2.HistoryCursor, bool, error) {
	s.attempts++
	tail, ok, err := s.InMemoryHistoryStore.AppendAfter(ctx, k, after, msgs, boundary)
	if err != nil || !ok {
		return tail, ok, err
	}
	return "", false, fmt.Errorf("测试丢失提交回执")
}

func TestCompactionBoundary_UncertainWrite(t *testing.T) {
	hs := &lostHistoryReceipt{InMemoryHistoryStore: agent_v2.NewInMemoryHistoryStore()}
	ctx := compose_v2.WithThreadIDCtx(t.Context(), "uncertain")
	key := agent_v2.HistoryKey{Owner: "t", ConvID: "uncertain"}
	seedCompactionHistory(t, hs, key)
	s := &countingSummarizer{}
	if _, err := boundaryAgent("coder", hs, &compactLoopModel{}, s).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("once-only")); err != nil {
		t.Fatal(err)
	}
	rows, err := agent_v2.LoadAllHistory(ctx, hs, key)
	if err != nil {
		t.Fatal(err)
	}
	if s.count() != 1 || hs.attempts != 1 || countBoundaries(rows) != 0 || strings.Count(inputText(rows), "once-only") != 1 || rows[len(rows)-1].Content != "完成" {
		t.Fatalf("不确定写入后摘要=%d 条件追加=%d 边界=%d", s.count(), hs.attempts, countBoundaries(rows))
	}
}

func TestCompactionBoundary_NestedWriter(t *testing.T) {
	ctx := agent_v2.WithHandoffHistoryModeCtx(compose_v2.WithThreadIDCtx(t.Context(), "nested"), agent_v2.HandoffHistoryRaw)
	hs := agent_v2.NewInMemoryHistoryStore()
	seedCompactionHistory(t, hs, agent_v2.HistoryKey{Owner: "t", ConvID: "nested"})
	child := agent_v2.NewBasicAgent(agent_v2.WithName("child"), agent_v2.WithModel(recoveryInspectModel{inspect: func(context.Context, []*schema.Message) (*schema.Message, error) {
		return schema.AssistantMessage("child-result"), nil
	}}))
	parent := recoveryInspectModel{inspect: func(ctx context.Context, in []*schema.Message) (*schema.Message, error) {
		if !strings.Contains(inputText(in), "SUMMARY-S1") {
			return nil, fmt.Errorf("父模型未收到摘要")
		}
		if _, err := agent_v2.RunSubAgent(ctx, "child", child, schema.UserMessage("child-input")); err != nil {
			return nil, err
		}
		return schema.AssistantMessage("parent-final"), nil
	}}
	if _, err := agent_v2.NewBasicAgent(agent_v2.WithName("coder"), agent_v2.WithModel(parent), agent_v2.WithHistory(hs, "t"), agent_v2.WithMaxHistoryPairs(0), agent_v2.WithAutoCompact(smallWindow(&countingSummarizer{}))).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("parent")); err != nil {
		t.Fatal(err)
	}
	c := &compactLoopModel{}
	reader := agent_v2.NewBasicAgent(agent_v2.WithName("coder"), agent_v2.WithModel(c), agent_v2.WithHistory(hs, "t"), agent_v2.WithMaxHistoryPairs(0))
	if _, err := reader.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("C")); err != nil {
		t.Fatal(err)
	}
	in := inputText(c.inputs[0])
	for _, want := range []string{"child-input", "child-result", "parent-final"} {
		if strings.Count(in, want) != 1 {
			t.Fatalf("共享写入器的下一次读取丢失或重复了 %q", want)
		}
	}
}

func TestCompactionBoundary_LateCallerHistory(t *testing.T) {
	for _, mode := range []agent_v2.HandoffHistoryMode{agent_v2.HandoffHistorySelfOnly, agent_v2.HandoffHistoryRaw, agent_v2.HandoffHistoryRewrite} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			ctx := agent_v2.WithHandoffHistoryModeCtx(compose_v2.WithThreadIDCtx(t.Context(), "late-caller"), mode)
			key := agent_v2.HistoryKey{Owner: "t", ConvID: "late-caller"}
			hs := agent_v2.NewInMemoryHistoryStore()
			old := schema.UserMessage("already-folded-caller")
			old.Annotations = map[string]any{agent_v2.CallerHistoryAnnotation: true, agent_v2.HistoryAgentAnnotation: "crew_caller_history"}
			if err := hs.Append(ctx, key, []*schema.Message{old}); err != nil {
				t.Fatal(err)
			}
			seedCompactionHistory(t, hs, key)
			if _, err := boundaryAgent("coder", hs, &compactLoopModel{}, &countingSummarizer{}).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("A")); err != nil {
				t.Fatal(err)
			}
			p, err := hs.Load(ctx, key, agent_v2.HistoryQuery{})
			if err != nil || len(p.VerifiedBoundaries) != 1 {
				t.Fatalf("缺少已确认边界: %v %v", p.VerifiedBoundaries, err)
			}
			msg := schema.UserMessage("late-caller-history")
			msg.Annotations = map[string]any{agent_v2.CallerHistoryAnnotation: true, agent_v2.HistoryAgentAnnotation: "crew_caller_history"}
			if err := hs.Append(ctx, key, []*schema.Message{old, msg, msg}); err != nil {
				t.Fatal(err)
			}
			c := &compactLoopModel{}
			reader := agent_v2.NewBasicAgent(agent_v2.WithName("coder"), agent_v2.WithModel(c), agent_v2.WithHistory(hs, "t"), agent_v2.WithMaxHistoryPairs(0))
			if _, err := reader.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("C")); err != nil {
				t.Fatal(err)
			}
			in := inputText(c.inputs[0])
			if strings.Contains(in, old.Content) || strings.Count(in, msg.Content) != 1 || !strings.Contains(in, "SUMMARY-S1") || strings.Index(in, msg.Content) > strings.Index(in, "SUMMARY-S1") {
				t.Fatal("晚到的调用方前文应去重后排在摘要之前")
			}
		})
	}
}

// 有损的 prior 即便在本轮产出了有效摘要，也不能替代整场会话的历史。
func TestCompactionBoundary_IncompletePrior(t *testing.T) {
	for _, tc := range []struct {
		name   string
		option agent_v2.Option
	}{
		{"轮数裁剪", agent_v2.WithMaxHistoryPairs(3)},
		{"自定义历史压缩", agent_v2.WithHistoryCompactor(func(_ context.Context, prior []*schema.Message, _ int) ([]*schema.Message, error) {
			return prior[2:], nil
		})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := compose_v2.WithThreadIDCtx(t.Context(), "partial")
			key := agent_v2.HistoryKey{Owner: "t", ConvID: "partial"}
			hs := agent_v2.NewInMemoryHistoryStore()
			seedCompactionHistory(t, hs, key)
			sum := &countingSummarizer{}
			cfg := smallWindow(sum)
			cfg.ModelContextWindow, cfg.SummaryMaxTokens, cfg.SummaryBufferTokens = 1200, 200, 200
			modelA := &compactLoopModel{}
			a := agent_v2.NewBasicAgent(agent_v2.WithName("coder"), agent_v2.WithHistory(hs, "t"), agent_v2.WithModel(modelA), agent_v2.WithAutoCompact(cfg), tc.option)
			if _, err := a.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("A")); err != nil {
				t.Fatal(err)
			}
			p, err := hs.Load(ctx, key, agent_v2.HistoryQuery{})
			if err != nil || sum.count() != 1 || !strings.Contains(inputText(modelA.inputs[0]), "SUMMARY-S1") || countBoundaries(p.Messages) != 0 {
				t.Fatalf("未完整读取历史的摘要被发布或未生成: summaries=%d boundaries=%d err=%v", sum.count(), countBoundaries(p.Messages), err)
			}
			c := &compactLoopModel{}
			reader := agent_v2.NewBasicAgent(agent_v2.WithName("coder"), agent_v2.WithModel(c), agent_v2.WithHistory(hs, "t"), agent_v2.WithMaxHistoryPairs(0))
			if _, err := reader.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("C")); err != nil {
				t.Fatal(err)
			}
			for _, m := range c.inputs[0] {
				if m.Role == schema.RoleUser && m.Content == "0" {
					return
				}
			}
			t.Fatal("C 没有收到最早一轮原文")
		})
	}
}
