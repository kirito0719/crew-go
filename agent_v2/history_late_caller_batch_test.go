package agent_v2_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

const lateCallerOwner = "crew_caller_history"

type lateCallerReadCounter struct {
	*agent_v2.InMemoryHistoryStore
	lookups    atomic.Int64
	lookupRows atomic.Int64
	legacyRead bool
}

func (s *lateCallerReadCounter) HistoryAppendOwner() agent_v2.HistoryStore { return s }

func (s *lateCallerReadCounter) Load(ctx context.Context, key agent_v2.HistoryKey, q agent_v2.HistoryQuery) (agent_v2.HistoryPage, error) {
	lookup := q.Before != "" && q.After == "" && q.Agent == lateCallerOwner
	if s.legacyRead {
		// 自定义后端只认识旧版的筛选字段，仍按约定报告页边界和 More。
		q = agent_v2.HistoryQuery{Before: q.Before, After: q.After, Limit: q.Limit, MessageID: q.MessageID, Task: q.Task, Agent: q.Agent}
	}
	page, err := s.InMemoryHistoryStore.Load(ctx, key, q)
	if lookup {
		s.lookups.Add(1)
		s.lookupRows.Add(int64(len(page.Messages)))
	}
	return page, err
}

// 旧 ID 已在第一页得到真前文的证明。查同批中缺席的 ID 时，后续页应跳过已证明 ID 的全部副本。
func TestCompactionBoundary_LateCallerCoveredIDStopsPaging(t *testing.T) {
	st := &lateCallerReadCounter{InMemoryHistoryStore: agent_v2.NewInMemoryHistoryStore()}
	ctx := agent_v2.WithHandoffHistoryModeCtx(compose_v2.WithThreadIDCtx(t.Context(), "old-duplicates"), agent_v2.HandoffHistoryRewrite)
	key := agent_v2.HistoryKey{Owner: "t", ConvID: "old-duplicates"}
	old := lateCallerRow("repeated", "old covered content", lateCallerOwner, true)
	copies := make([]*schema.Message, 640)
	for i := range copies {
		copies[i] = old
	}
	if err := st.Append(ctx, key, copies); err != nil {
		t.Fatal(err)
	}
	seedCompactionHistory(t, st, key)
	if _, err := boundaryAgent("coder", st, &compactLoopModel{}, &countingSummarizer{}).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("first")); err != nil {
		t.Fatal(err)
	}
	covered := lateCallerRow(old.ID, "later copy of old content", lateCallerOwner, true)
	fresh := lateCallerRow("new", "unique new content", lateCallerOwner, true)
	if err := st.Append(ctx, key, []*schema.Message{covered, fresh}); err != nil {
		t.Fatal(err)
	}
	st.lookups.Store(0)
	st.lookupRows.Store(0)
	m := &compactLoopModel{}
	reader := agent_v2.NewBasicAgent(agent_v2.WithName("coder"), agent_v2.WithModel(m), agent_v2.WithHistory(st, "t"), agent_v2.WithMaxHistoryPairs(0))
	if _, err := reader.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("next")); err != nil {
		t.Fatal(err)
	}
	input := inputText(m.inputs[0])
	if !strings.Contains(input, "SUMMARY-S1") || strings.Contains(input, covered.Content) || strings.Count(input, fresh.Content) != 1 {
		t.Fatal("模型输入应保留摘要和新的前文，过滤已覆盖的副本")
	}
	if calls, rows := st.lookups.Load(), st.lookupRows.Load(); calls > 2 || rows >= int64(len(copies)) {
		t.Fatalf("一个已找到、一个缺席的 ID 触发 %d 次去重读，向框架返回了 %d 个旧副本", calls, rows)
	}
}

func lateCallerRow(id, content, owner string, marked bool) *schema.Message {
	m := schema.UserMessage(content)
	m.ID = id
	m.Annotations = map[string]any{agent_v2.HistoryAgentAnnotation: owner, agent_v2.CallerHistoryAnnotation: marked}
	return m
}

func TestCompactionBoundary_LateCallerBatch(t *testing.T) {
	for _, tc := range []struct {
		name       string
		mode       agent_v2.HandoffHistoryMode
		legacyRead bool
	}{
		{"self", agent_v2.HandoffHistorySelfOnly, false},
		{"raw", agent_v2.HandoffHistoryRaw, false},
		{"rewrite", agent_v2.HandoffHistoryRewrite, false},
		{"legacy-filter", agent_v2.HandoffHistoryRewrite, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := &lateCallerReadCounter{InMemoryHistoryStore: agent_v2.NewInMemoryHistoryStore(), legacyRead: tc.legacyRead}
			ctx := agent_v2.WithHandoffHistoryModeCtx(compose_v2.WithThreadIDCtx(t.Context(), "late-batch"), tc.mode)
			key := agent_v2.HistoryKey{Owner: "t", ConvID: "late-batch"}
			older := []*schema.Message{
				lateCallerRow("buried", "covered by summary", lateCallerOwner, true),
				lateCallerRow("wrong-marker", "old ordinary message", lateCallerOwner, false),
				lateCallerRow("wrong-owner", "from another member", "other-member", true),
			}
			// 同归属、同 ID 的普通记录占满一页。真正进入旧前文的那条在更早的一页。
			for i := range 80 {
				older = append(older,
					lateCallerRow("buried", "ordinary copy", lateCallerOwner, false),
					lateCallerRow(fmt.Sprintf("unrelated-%d", i), "old unrelated caller", lateCallerOwner, true))
			}
			if err := st.Append(ctx, key, older); err != nil {
				t.Fatal(err)
			}
			seedCompactionHistory(t, st, key)
			if _, err := boundaryAgent("coder", st, &compactLoopModel{}, &countingSummarizer{}).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("first")); err != nil {
				t.Fatal(err)
			}

			late := []*schema.Message{
				lateCallerRow("buried", "after-q17/covered", lateCallerOwner, true),
				lateCallerRow("wrong-marker", "after-q17/marker", lateCallerOwner, true),
				lateCallerRow("wrong-owner", "after-q17/owner", lateCallerOwner, true),
				lateCallerRow("wrong-marker", "after-q17/later-duplicate", lateCallerOwner, true),
				lateCallerRow("", "after-q17/empty-a", lateCallerOwner, true),
				lateCallerRow("", "after-q17/empty-b", lateCallerOwner, true),
			}
			want := []string{"after-q17/marker", "after-q17/owner", "after-q17/empty-a", "after-q17/empty-b"}
			for i := range 130 {
				content := fmt.Sprintf("after-q17/%03d", i)
				late = append(late, lateCallerRow(content, content, lateCallerOwner, true))
				want = append(want, content)
			}
			if err := st.Append(ctx, key, late); err != nil {
				t.Fatal(err)
			}
			st.lookups.Store(0)
			m := &compactLoopModel{}
			reader := agent_v2.NewBasicAgent(agent_v2.WithName("coder"), agent_v2.WithModel(m), agent_v2.WithHistory(st, "t"), agent_v2.WithMaxHistoryPairs(0))
			if _, err := reader.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("next")); err != nil {
				t.Fatal(err)
			}
			var got []string
			summary := false
			for _, msg := range m.inputs[0] {
				if strings.Contains(msg.Content, "SUMMARY-S1") {
					summary = true
				}
				if strings.HasPrefix(msg.Content, "after-q17/") {
					if summary {
						t.Fatalf("摘要后出现调用方前文 %q", msg.Content)
					}
					got = append(got, msg.Content)
				}
			}
			if !summary || !slices.Equal(got, want) {
				t.Fatalf("摘要=%v 前文=%v，期望按最早出现次序得到 %v", summary, got, want)
			}
			if calls := st.lookups.Load(); !tc.legacyRead && (calls == 0 || calls > 8) {
				t.Fatalf("130 条不同 ID 的新前文和一页以上的旧重复记录触发 %d 次去重读", calls)
			}
		})
	}
}
