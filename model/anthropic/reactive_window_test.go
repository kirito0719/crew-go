package anthropic_test

import (
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/anthropics/anthropic-sdk-go/option"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/model/anthropic"
	"crew-go/schema"
)

func TestBasicAgent_ReactiveCompactUsesClaudeCompactionWindow(t *testing.T) {
	for _, tc := range []struct {
		name       string
		streamStop bool
		onlyAnchor bool
	}{
		{name: "http_error"},
		{name: "stream_stop", streamStop: true},
		{name: "nothing_to_trim", onlyAnchor: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const olderActive = "摘要之后仍在窗口里的旧轮"
			const latest = "这一轮仍需要答案"
			const summary = "服务端已保存的摘要"
			const resultID = "summary-lookup"
			var rec *recorder
			recReady := make(chan struct{})
			srv, rec := newServer(t, func(w http.ResponseWriter, r *http.Request, n int) {
				if n == 1 {
					replyJSON(w, http.StatusOK, message([]any{
						map[string]any{"type": "compaction", "content": summary, "encrypted_content": "opaque-summary"},
						map[string]any{"type": "thinking", "thinking": "", "signature": "after-summary"},
						map[string]any{"type": "tool_use", "id": resultID, "name": "lookup", "input": map[string]any{}},
					}, "tool_use", nil))
					return
				}
				<-recReady
				body := canon(t, rec.at(t, n-1).body["messages"])
				if !strings.Contains(body, olderActive) && strings.Contains(body, summary) && strings.Contains(body, latest) && !tc.onlyAnchor {
					replySSE(w, sseFor([]any{map[string]any{"type": "text", "text": "已处理"}}, "end_turn")...)
					return
				}
				if tc.streamStop {
					replySSE(w, sseFor(nil, "model_context_window_exceeded")...)
					return
				}
				replyJSON(w, http.StatusBadRequest, map[string]any{"type": "error", "error": map[string]any{
					"type": "invalid_request_error", "message": "Input is too long: 9200 input tokens (max 8000)",
				}})
			})

			close(recReady)
			llm := newClient(srv, anthropic.WithCompaction(anthropic.Compaction{}))
			ctx := compose_v2.WithThreadIDCtx(t.Context(), "claude-reactive-window")
			anchor, err := llm.Generate(ctx, []*schema.Message{schema.UserMessage("准备摘要快照")})
			if err != nil {
				t.Fatal(err)
			}
			history := agent_v2.NewInMemoryHistoryStore()
			key := agent_v2.HistoryKey{Owner: "tenant", ConvID: "claude-reactive-window"}
			prior := []*schema.Message{
				schema.UserMessage("摘要之前的长轮 " + strings.Repeat("old ", 6500)),
				schema.AssistantMessage("旧轮回复"),
				schema.UserMessage("摘要之前的第二轮"),
				anchor,
				schema.ToolResultMessage(resultID, "已得工具结果"),
			}
			if !tc.onlyAnchor {
				prior = append(prior, schema.UserMessage(olderActive+" "+strings.Repeat("active ", 2000)),
					schema.AssistantMessage("可裁轮的回复"))
			}
			for _, m := range prior {
				if m.Annotations == nil {
					m.Annotations = make(map[string]any)
				}
				m.Annotations[agent_v2.HistoryAgentAnnotation] = "claude-reactive"
			}
			if err := history.Append(ctx, key, prior); err != nil {
				t.Fatal(err)
			}
			agent := agent_v2.NewBasicAgent(
				agent_v2.WithName("claude-reactive"),
				agent_v2.WithSystemPrompt("固定的 agent 前言"),
				agent_v2.WithModel(llm),
				agent_v2.WithHistory(history, key.Owner),
				agent_v2.WithReactiveCompact(&agent_v2.ReactiveCompactConfig{Enabled: true, MaxAttempts: 1}),
			)
			events, err := agent.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage(latest))
			first := rec.at(t, 1).body
			firstWire := canon(t, first["messages"])
			if strings.Contains(firstWire, "摘要之前的长轮") || !strings.Contains(firstWire, latest) ||
				str(first, "messages", 0, "content", 0, "content") != summary ||
				str(first, "messages", 0, "content", 1, "signature") != "after-summary" ||
				str(first, "messages", 1, "content", 0, "tool_use_id") != resultID {
				t.Fatalf("首次实际请求应已从摘要开始并携带配对与本轮输入，实际 %v", first["messages"])
			}
			if tc.onlyAnchor {
				if !agent_v2.IsPromptTooLongError(err) || rec.count() != 2 {
					t.Fatalf("窗口无法继续缩小时应保留原错误，实际请求 %d 次，错误 %v", rec.count()-1, err)
				}
				return
			}
			if !strings.Contains(firstWire, olderActive) {
				t.Fatal("必须在第一次实际请求中发送可裁的旧轮")
			}
			final := agent_v2.FinalMessage(events)
			if err != nil || final == nil || final.Content != "已处理" || rec.count() != 3 {
				t.Fatalf("一次有效重试应成功，实际请求 %d 次，答复 %+v，错误 %v", rec.count()-1, final, err)
			}
			retry := rec.at(t, 2).body
			if canon(t, retry["system"]) != canon(t, first["system"]) ||
				canon(t, at(retry, "messages", 0)) != canon(t, at(first, "messages", 0)) ||
				canon(t, at(retry, "messages", 1, "content", 0)) != canon(t, at(first, "messages", 1, "content", 0)) {
				t.Fatalf("重试应保留 system、摘要、签名及配对工具结果，实际 %+v", retry["messages"])
			}
			if got := canon(t, retry["messages"]); strings.Contains(got, olderActive) || !strings.Contains(got, latest) {
				t.Fatal("应裁摘要后的旧轮并保留本轮消息")
			}
		})
	}
}

func TestGenerate_ContextOverflowIdentifiesOnlyNativeReplay(t *testing.T) {
	const fixed = "手工覆盖的消息"
	overridden := map[string]any{"messages": []any{map[string]any{"role": "user", "content": fixed}}}
	srv, rec := newServer(t, func(w http.ResponseWriter, r *http.Request, n int) {
		if n == 1 {
			replyJSON(w, http.StatusOK, message([]any{
				map[string]any{"type": "compaction", "content": "有效摘要"},
				map[string]any{"type": "text", "text": "原答复"},
			}, "end_turn", nil))
			return
		}
		replyJSON(w, http.StatusBadRequest, map[string]any{"type": "error", "error": map[string]any{
			"type": "invalid_request_error", "message": "Input is too long: 9200 input tokens (max 8000)",
		}})
	})
	ctx := t.Context()
	llm := newClient(srv, anthropic.WithCompaction(anthropic.Compaction{}))
	summary, err := llm.Generate(ctx, []*schema.Message{schema.UserMessage("准备摘要快照")})
	if err != nil {
		t.Fatal(err)
	}
	messages := []*schema.Message{schema.UserMessage("摘要之前"), summary, schema.UserMessage("当前问题")}
	_, err = llm.Generate(ctx, messages)
	var apiErr *model.APIError
	if model.CompactedHistoryStart(err) != summary || !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusBadRequest {
		t.Fatalf("失败请求应返回有效摘要的原消息并保留 API 错误，实际 %v", err)
	}

	configured := newClient(srv, anthropic.WithCompaction(anthropic.Compaction{}), anthropic.WithExtraBody(overridden))
	_, err = configured.Generate(ctx, messages)
	if !agent_v2.IsPromptTooLongError(err) || model.CompactedHistoryStart(err) != nil || str(rec.at(t, 2).body, "messages", 0, "content") != fixed {
		t.Fatalf("客户端配置覆盖 messages 后不能提供本地摘要起点，实际 %v", err)
	}
	_, err = llm.Generate(ctx, messages, model.WithExtraBody(overridden))
	if !agent_v2.IsPromptTooLongError(err) || model.CompactedHistoryStart(err) != nil || str(rec.at(t, 3).body, "messages", 0, "content") != fixed {
		t.Fatalf("单次调用覆盖 messages 后不能提供本地摘要起点，实际 %v", err)
	}
}

func TestGenerate_ContextOverflowWithSDKBodyOverride(t *testing.T) {
	const fixed = "SDK 直接指定的消息"
	messageOverride := []any{map[string]any{"role": "user", "content": fixed}}
	rawBody := []byte(`{"model":"claude-opus-5-5","max_tokens":256,"messages":[{"role":"user","content":"SDK 直接指定的消息"}]}`)
	for _, tc := range []struct {
		name string
		opt  option.RequestOption
	}{
		{name: "json_set", opt: option.WithJSONSet("messages", messageOverride)},
		{name: "body", opt: option.WithRequestBody("application/json", rawBody)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, rec := newServer(t, func(w http.ResponseWriter, r *http.Request, n int) {
				if n == 1 {
					replyJSON(w, http.StatusOK, message([]any{
						map[string]any{"type": "compaction", "content": "本地摘要"},
					}, "end_turn", nil))
					return
				}
				replyJSON(w, http.StatusBadRequest, map[string]any{"type": "error", "error": map[string]any{
					"type": "invalid_request_error", "message": "Input is too long: 9200 input tokens (max 8000)",
				}})
			})
			base := newClient(srv, anthropic.WithCompaction(anthropic.Compaction{}))
			anchor, err := base.Generate(t.Context(), []*schema.Message{schema.UserMessage("准备摘要快照")})
			if err != nil {
				t.Fatal(err)
			}
			llm := newClient(srv, anthropic.WithCompaction(anthropic.Compaction{}), anthropic.WithRequestOptions(tc.opt))
			messages := []*schema.Message{schema.UserMessage("摘要之前"), anchor, schema.UserMessage("当前问题")}
			_, err = llm.Generate(t.Context(), messages)
			if str(rec.at(t, 1).body, "messages", 0, "content") != fixed || !agent_v2.IsPromptTooLongError(err) {
				t.Fatalf("应实发 SDK 指定的消息并识别超窗，实际 %v", err)
			}
			if model.CompactedHistoryStart(err) != nil {
				t.Fatal("SDK 替换整个消息数组后，错误中出现了未发送的摘要")
			}
		})
	}
}
