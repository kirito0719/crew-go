package agent_v2

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"

	"crew-go/compose_v2"
	"crew-go/schema"
	"crew-go/store"
)

func recoveryFixture() []*schema.Message {
	call := schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "call-a", Type: "function", Function: schema.FunctionCall{Name: "dump", Arguments: `{}`}}})
	call.Annotations = map[string]any{compose_v2.RunIDAnnotationKey: "a", ContextRecoveryNoteAnnotation: "查到历史事实"}
	result := schema.ToolResultMessage("call-a", strings.Repeat("旧上下文", 4000)+"needle_47")
	return []*schema.Message{schema.SystemMessage("业务 system 必须保留"), schema.UserMessage("旧问题"), call, result,
		schema.AssistantMessage("previous done"), schema.UserMessage("当前问题")}
}

func TestContextRecoveryArchivePagingAndIsolation(t *testing.T) {
	ctx := compose_v2.WithOwner(compose_v2.WithThreadIDCtx(t.Context(), "thread"), "owner")
	st := store.NewInMemoryStore()
	cfg := &ContextRecoveryConfig{Store: st, MaxTokens: 500, KeepRecentRounds: 1}
	original := recoveryFixture()
	before, _ := json.Marshal(original)
	folded, tail := foldContextRecovery(ctx, "agent", cfg, original)
	if tail < 0 || compose_v2.EstimateMessageTokens(folded) >= compose_v2.EstimateMessageTokens(original) {
		t.Fatal("large closed API round was not folded")
	}
	after, _ := json.Marshal(original)
	if string(before) != string(after) {
		t.Fatal("authoritative messages were mutated")
	}
	var id string
	for _, m := range folded {
		if isRecoveryView(m) {
			id, _ = m.Annotations[ContextRecoveryAnnotation].(string)
			if !strings.Contains(m.Content, "查到历史事实") || !strings.Contains(m.Content, "call-a") {
				t.Fatalf("note/manifest missing: %s", m.Content)
			}
		}
	}
	if id == "" || folded[0] != original[0] || folded[len(folded)-1] != original[len(original)-1] {
		t.Fatal("system or current user changed")
	}
	var restored strings.Builder
	offset := 0
	for {
		res := readContext(ctx, cfg, "agent", map[string]any{"id": id, "offset": offset, "limit": 79})
		var page struct {
			Next int    `json:"next_offset"`
			Text string `json:"text"`
			EOF  bool   `json:"eof"`
		}
		if !res.Success || json.Unmarshal([]byte(fmt.Sprint(res.Output)), &page) != nil || !utf8.ValidString(page.Text) {
			t.Fatalf("invalid page: %+v", res)
		}
		restored.WriteString(page.Text)
		if page.EOF {
			break
		}
		if page.Next <= offset {
			t.Fatal("paging made no progress")
		}
		offset = page.Next
	}
	if !strings.Contains(restored.String(), "needle_47") || !strings.Contains(restored.String(), "call-a") || strings.Contains(restored.String(), ContextRecoveryNoteAnnotation) {
		t.Fatal("archive lost tool pairing or exposed annotations")
	}
	if res := readContext(compose_v2.WithGraphNS(ctx, "round_2"), cfg, "agent", map[string]any{"id": id}); !res.Success {
		t.Fatal("archive moved with the execution namespace instead of its stable history owner")
	}
	for _, other := range []struct {
		ctx   context.Context
		agent string
	}{
		{compose_v2.WithOwner(ctx, "other"), "agent"}, {compose_v2.WithThreadIDCtx(ctx, "other"), "agent"}, {ctx, "other"},
		{WithHistorySlot(ctx, "other"), "agent"},
	} {
		if res := readContext(other.ctx, cfg, other.agent, map[string]any{"id": id}); res.Success {
			t.Fatal("archive escaped its scope")
		}
	}
	res := readContext(ctx, cfg, "agent", map[string]any{"id": id, "query": "needle_47", "limit": 2000})
	if !res.Success || !strings.Contains(fmt.Sprint(res.Output), "needle_47") {
		t.Fatal("original fact cannot be recovered")
	}
	memo := nextCompactionMemo(original, 0, 0, len(original), folded, tail)
	if memo == nil {
		t.Fatal("folding cannot be persisted")
	}
	view, _, _ := memo.apply(append(original, schema.AssistantMessage("new turn")))
	if !recoveryViewsMatch(ctx, "agent", view) || recoveryViewsMatch(ctx, "other", view) {
		t.Fatal("memo scope validation failed")
	}
	for _, m := range view {
		if isRecoveryView(m) && !isProtectedGroup([]*schema.Message{m}) {
			t.Fatal("reactive compaction can drop the recovery handle")
		}
	}
}

type failedArchiveStore struct{ store.Store }

func (s failedArchiveStore) Put(context.Context, store.Namespace, string, any, ...store.PutOption) error {
	return errors.New("write unavailable")
}

func TestContextRecoveryFailedArchiveKeepsFullMessages(t *testing.T) {
	for _, st := range []store.Store{nil, failedArchiveStore{store.NewInMemoryStore()}} {
		msgs := recoveryFixture()
		got, tail := foldContextRecovery(t.Context(), "a", &ContextRecoveryConfig{Store: st, MaxTokens: 50, KeepRecentRounds: 1}, msgs)
		if tail != -1 || len(got) != len(msgs) {
			t.Fatal("unreadable handle was emitted")
		}
		for i := range got {
			if got[i] != msgs[i] {
				t.Fatal("failed archive changed the input")
			}
		}
	}
}

func TestContextRecoveryPendingAndProtectedRoundsStayWhole(t *testing.T) {
	msgs := recoveryFixture()
	// 去掉工具结果，该轮仍待答；不能被折叠成一条笔记。
	msgs = append(msgs[:3:3], msgs[4:]...)
	got, tail := foldContextRecovery(t.Context(), "a", &ContextRecoveryConfig{Store: store.NewInMemoryStore(), MaxTokens: 1, KeepRecentRounds: 1}, msgs)
	if tail != -1 || len(got) != len(msgs) {
		t.Fatal("pending tool call was folded")
	}
	msgs = recoveryFixture()
	msgs[2].Annotations[SkillBodyAnnotation] = true
	got, tail = foldContextRecovery(t.Context(), "a", &ContextRecoveryConfig{Store: store.NewInMemoryStore(), MaxTokens: 1, KeepRecentRounds: 1}, msgs)
	if tail != -1 || len(got) != len(msgs) {
		t.Fatal("protected skill body was folded")
	}
}
