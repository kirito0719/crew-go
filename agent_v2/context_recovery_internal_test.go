package agent_v2

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
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

func TestContextRecoveryKeepsOldGroupsWithUnmatchedToolBlocks(t *testing.T) {
	call := func(ids ...string) *schema.Message {
		calls := make([]schema.ToolCall, 0, len(ids))
		for _, id := range ids {
			calls = append(calls, schema.ToolCall{ID: id, Function: schema.FunctionCall{Name: "lookup", Arguments: `{}`}})
		}
		return schema.AssistantToolCallMessage(calls)
	}
	stamp := func(m *schema.Message, task, agent string) *schema.Message {
		m.Annotations = map[string]any{HistoryTaskAnnotation: task, HistoryAgentAnnotation: agent}
		return m
	}
	for _, tc := range []struct {
		name   string
		blocks []*schema.Message
	}{
		{"one_result_for_two_calls", []*schema.Message{call("reused", "reused"), schema.ToolResultMessage("reused", "only one")}},
		{"result_from_another_task", []*schema.Message{
			stamp(call("shared"), "first-task", "reader"), stamp(schema.ToolResultMessage("shared", "other task"), "second-task", "reader"),
		}},
		{"result_from_another_member", []*schema.Message{
			stamp(call("shared"), "task", "first-member"), stamp(schema.ToolResultMessage("shared", "other member"), "task", "second-member"),
		}},
		{"extra_result_without_a_call", []*schema.Message{
			call("matched"), schema.ToolResultMessage("matched", "expected"), schema.ToolResultMessage("orphan", "unexpected"),
		}},
		{"result_preceding_its_call", []*schema.Message{schema.ToolResultMessage("early", "before call"), call("early")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := store.NewInMemoryStore()
			cfg := &ContextRecoveryConfig{Store: st, MaxTokens: 100, KeepRecentRounds: 1}
			in := append([]*schema.Message{schema.UserMessage(strings.Repeat("仍需保留的旧请求 ", 1000))}, tc.blocks...)
			in = append(in, schema.UserMessage("当前问题"))
			out, tail := foldContextRecovery(t.Context(), "reader", cfg, in)
			if tail != -1 || !slices.Equal(out, in) {
				t.Fatalf("包含未配对工具块的旧组应原样保留，折叠边界 %d，输出长度 %d", tail, len(out))
			}
			items, err := st.Search(t.Context(), recoveryNamespace(t.Context(), "reader"))
			if err != nil || len(items) != 0 {
				t.Fatalf("不完整的组不应写归档，条数 %d，错误 %v", len(items), err)
			}
		})
	}
}

func TestContextRecoveryKeepsToolsPairedAcrossGroupBoundaries(t *testing.T) {
	call := func(run string) *schema.Message {
		m := schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "same", Function: schema.FunctionCall{Name: "lookup", Arguments: `{}`}}})
		m.Annotations = map[string]any{HistoryTaskAnnotation: "a", HistoryAgentAnnotation: "reader", compose_v2.RunIDAnnotationKey: run}
		return m
	}
	result := func(content string) *schema.Message {
		m := schema.ToolResultMessage("same", content)
		m.Annotations = map[string]any{HistoryTaskAnnotation: "a", HistoryAgentAnnotation: "reader"}
		return m
	}
	old, newer := call("old"), call("new")
	firstReply, secondReply := result(strings.Repeat("first evidence ", 1000)), result("second evidence")
	msgs := []*schema.Message{
		schema.UserMessage("first request"), old,
		schema.UserMessage(strings.Repeat("interleaved request ", 500)), newer, firstReply,
		schema.UserMessage("current request"), secondReply,
	}
	pairs := pairToolResults(msgs)
	if len(pairs) != 2 || pairs[0].declaredAt != 1 || pairs[0].resultAt != 4 || pairs[1].declaredAt != 3 || pairs[1].resultAt != 6 {
		t.Fatalf("工具调用未按预期顺序跨组配对： %+v", pairs)
	}
	groups := compose_v2.GroupMessagesByAPIRound(msgs)
	if len(groups) != 3 || !slices.Contains(groups[1], firstReply) || slices.Contains(groups[1], old) {
		t.Fatalf("未构造出配对跨越分组边界的输入： %+v", groups)
	}
	st := store.NewInMemoryStore()
	got, cut := foldContextRecovery(t.Context(), "reader", &ContextRecoveryConfig{Store: st, MaxTokens: 100, KeepRecentRounds: 1}, msgs)
	items, err := st.Search(t.Context(), recoveryNamespace(t.Context(), "reader"))
	if err != nil {
		t.Fatal(err)
	}
	if cut != -1 || !slices.Equal(got, msgs) || len(items) != 0 {
		t.Fatalf("跨组配对应保留原文和两组结果，边界 %d，消息 %d，归档 %d", cut, len(got), len(items))
	}
}
