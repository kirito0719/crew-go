package agent_v2

import (
	"fmt"
	"slices"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

func TestHistoryWindowKeepsInputOrderAndVerifiedBoundary(t *testing.T) {
	ctx := compose_v2.WithThreadIDCtx(t.Context(), "verified-order")
	key := HistoryKey{Owner: "tenant", ConvID: "verified-order"}
	stamp := func(role schema.MessageRole, body, task, scope string) *schema.Message {
		m := &schema.Message{Role: role, Content: body}
		m = stampHistoryAgent(m, "same-member", "same-member", task)
		m.Annotations[historyScopeAnnotation] = scope
		return m
	}
	contents := func(xs []*schema.Message) []string {
		vv := []string{}
		for _, x := range xs {
			vv = append(vv, x.Content)
		}
		return vv
	}
	prior := func(t *testing.T, st *InMemoryHistoryStore, n int) []string {
		t.Helper()
		ms, ok := historyPrior(ctx, &historyConfig{store: st, owner: "tenant"}, "same-member", HandoffHistorySelfOnly, n)
		if !ok {
			t.Fatal("read unavailable")
		}
		return contents(truncatePriorPreservingMarkers(ms, 2))
	}
	filler := func() []*schema.Message {
		var f []*schema.Message
		for j := 0; j < 77; j++ {
			f = append(f, stamp(schema.RoleAssistant, fmt.Sprintf("page-filler-%d", j), "other-task", "other-scope"))
		}
		return f
	}
	t.Run("same-task interleaved predecessors", func(t *testing.T) {
		s := NewInMemoryHistoryStore()
		a := stamp(schema.RoleUser, "INPUT_A", "T", "A")
		b := stamp(schema.RoleUser, "INPUT_B", "T", "B")
		rows := append([]*schema.Message{a, b}, filler()...)
		rows = append(rows, stamp(schema.RoleAssistant, "OUTPUT_B", "T", "B"), stamp(schema.RoleAssistant, "OUTPUT_A", "T", "A"))
		if e := s.Append(ctx, key, rows); e != nil {
			t.Fatal(e)
		}
		full, bounded := prior(t, s, 0), prior(t, s, 2)
		if !slices.Equal(full, bounded) {
			t.Fatal("分页颠倒两次执行的原始输入顺序")
		}
	})
	t.Run("verified summarization barrier", func(t *testing.T) {
		s := NewInMemoryHistoryStore()
		before := stamp(schema.RoleUser, "ORIGINAL_ALREADY_SUMMARIZED", "T", "A")
		cur, ok, e := s.AppendAfter(ctx, key, "", []*schema.Message{before}, -1)
		if e != nil || !ok {
			t.Fatal(e)
		}
		summary := schema.UserMessage("SUMMARIZED_HEAD_ONLY")
		summary.Annotations = map[string]any{AutoCompactSummaryAnnotation: true}
		b := buildCompactionBoundary(&compactionMemo{Head: []*schema.Message{summary}}, nil, HandoffHistorySelfOnly)
		b = stampHistoryAgent(b, "same-member", "same-member", "T")
		cur, ok, e = s.AppendAfter(ctx, key, cur, []*schema.Message{b}, 0)
		if e != nil || !ok {
			t.Fatal(e)
		}
		tail := append(filler(), stamp(schema.RoleAssistant, "AFTER_BOUNDARY_RESULT", "T", "A"), stamp(schema.RoleAssistant, "AFTER_BOUNDARY_FINAL", "T", "A"))
		if _, ok, e = s.AppendAfter(ctx, key, cur, tail, -1); e != nil || !ok {
			t.Fatal(e)
		}
		full, bounded := prior(t, s, 0), prior(t, s, 2)
		if !slices.Equal(full, []string{summary.Content, "AFTER_BOUNDARY_RESULT", "AFTER_BOUNDARY_FINAL"}) {
			t.Fatalf("全读必须从已验证的摘要开始，实际 %v", full)
		}
		if !slices.Equal(full, bounded) {
			t.Fatalf("分页应保留摘要，排除已被摘要替代的原始输入，实际 %v", bounded)
		}
	})
	t.Run("recovered input stays in visible tail", func(t *testing.T) {
		store := NewInMemoryHistoryStore()
		summary := schema.UserMessage("历史摘要")
		summary.Annotations = map[string]any{AutoCompactSummaryAnnotation: true}
		before := stamp(schema.RoleAssistant, "摘要保留的旧记录", "T", "A")
		head := []*schema.Message{summary, before}
		cursor, ok, err := store.AppendAfter(ctx, key, "", []*schema.Message{before}, -1)
		if err != nil || !ok {
			t.Fatalf("建立原始历史失败，实际 %v", err)
		}
		boundary := buildCompactionBoundary(&compactionMemo{Head: head}, nil, HandoffHistorySelfOnly)
		boundary = stampHistoryAgent(boundary, "same-member", "same-member", "T")
		cursor, ok, err = store.AppendAfter(ctx, key, cursor, []*schema.Message{boundary}, 0)
		if err != nil || !ok {
			t.Fatalf("发布摘要边界失败，实际 %v", err)
		}
		tail := stamp(schema.RoleAssistant, "边界后继续执行", "T", "A")
		recovered := stamp(schema.RoleUser, "续跑补入的输入", "T", "A")
		recovered.Annotations[historyRecoveredInputAnnotation] = true
		if _, ok, err = store.AppendAfter(ctx, key, cursor, []*schema.Message{tail, recovered}, -1); err != nil || !ok {
			t.Fatalf("边界后写入失败，实际 %v", err)
		}
		view, ok := historyPrior(ctx, &historyConfig{store: store, owner: key.Owner}, "same-member", HandoffHistorySelfOnly, 0)
		want := []string{summary.Content, before.Content, recovered.Content, tail.Content}
		if !ok || !slices.Equal(contents(view), want) {
			t.Fatalf("迟补输入只在已确认压缩边界之后调整顺序，实际 %v", contents(view))
		}
	})
	t.Run("bounded tail follows invocation order", func(t *testing.T) {
		store := NewInMemoryHistoryStore()
		old := stamp(schema.RoleAssistant, "旧调用已有产出", "T", "A")
		currentInput := stamp(schema.RoleUser, "较新调用的输入", "T", "B")
		currentOutput := stamp(schema.RoleAssistant, "较新调用的产出", "T", "B")
		recovered := stamp(schema.RoleUser, "旧调用迟补的输入", "T", "A")
		recovered.Annotations[historyRecoveredInputAnnotation] = true
		msgs := append([]*schema.Message{old}, filler()...)
		msgs = append(msgs, currentInput, currentOutput, recovered)
		if err := store.Append(ctx, key, msgs); err != nil {
			t.Fatal(err)
		}
		full, bounded := prior(t, store, 0), prior(t, store, 2)
		want := []string{currentInput.Content, currentOutput.Content}
		if !slices.Equal(full, want) || !slices.Equal(bounded, want) {
			t.Fatalf("尾页应按实际调用顺序裁剪，实际分页 %v，全读 %v", bounded, full)
		}
	})
}
