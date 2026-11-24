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
}
