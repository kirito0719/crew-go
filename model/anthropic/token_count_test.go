package anthropic_test

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	"crew-go/model"
	"crew-go/model/anthropic"
	"crew-go/schema"
)

func TestCountInputTokens_ReusesSignedThinkingAndRequestOptions(t *testing.T) {
	srv, rec := newServer(t, func(w http.ResponseWriter, r *http.Request, n int) {
		switch n {
		case 1:
			replyJSON(w, http.StatusOK, message([]any{
				map[string]any{"type": "thinking", "thinking": "", "signature": "opaque-signature"},
				map[string]any{"type": "tool_use", "id": "lookup-id", "name": "lookup", "input": map[string]any{"id": json.Number("9007199254740993")}},
			}, "tool_use", nil))
		case 2:
			if r.Method != http.MethodPost || r.URL.Path != "/v1/messages/count_tokens" {
				t.Errorf("计数请求 = %s %s", r.Method, r.URL.Path)
			}
			replyJSON(w, http.StatusOK, map[string]any{"input_tokens": 18483})
		default:
			textReply(w, r, n)
		}
	})
	ctx := t.Context()
	client := newClient(srv, anthropic.WithPrefixMismatch(anthropic.PrefixMismatchError),
		anthropic.WithExtraBody(map[string]any{"output_config": map[string]any{"effort": "low"}}))
	start := []*schema.Message{schema.SystemMessage("保留前言"), schema.UserMessage("查订单")}
	tool := &schema.ToolInfo{Name: "lookup", Desc: "查订单", Params: map[string]any{"type": "object"}}
	opts := []model.Option{
		model.WithTools([]*schema.ToolInfo{tool}), model.WithModel("claude-opus-5-5"),
		model.WithMaxTokens(256), model.WithStop([]string{"END"}),
		model.WithExtraBody(map[string]any{"output_config": map[string]any{"effort": "high"}}),
	}
	reply, err := client.Generate(ctx, start, opts...)
	if err != nil {
		t.Fatal(err)
	}
	blob, err := json.Marshal(reply)
	if err != nil {
		t.Fatal(err)
	}
	var stored schema.Message
	if err := json.Unmarshal(blob, &stored); err != nil {
		t.Fatal(err)
	}
	history := append(start, &stored, schema.ToolResultMessage("lookup-id", "done"))
	count, err := client.CountInputTokens(ctx, history, opts...)
	if err != nil || count != 18483 {
		t.Fatalf("服务端输入计数 = %d, %v", count, err)
	}
	if _, err := client.Generate(ctx, history, opts...); err != nil {
		t.Fatal(err)
	}
	countReq, generated := rec.at(t, 1), rec.at(t, 2)
	for _, key := range []string{"model", "system", "messages", "tools", "thinking", "output_config", "cache_control"} {
		if got, want := canon(t, countReq.body[key]), canon(t, generated.body[key]); got != want || got == "null" {
			t.Errorf("计数的 %s = %s，生成请求 = %s", key, got, want)
		}
	}
	for _, key := range []string{"max_tokens", "stop_sequences", "fallbacks", "stream"} {
		if _, present := countReq.body[key]; present {
			t.Errorf("计数请求不需要 %s", key)
		}
	}
	if got := str(countReq.body, "messages", 1, "content", 0, "signature"); got != "opaque-signature" {
		t.Fatalf("恢复的签名 = %q", got)
	}
	if got := str(countReq.body, "messages", 1, "content", 1, "input", "id"); got != "9007199254740993" {
		t.Fatalf("大整数在计数请求中失真，实际 %s", got)
	}
	if got := str(countReq.body, "output_config", "effort"); got != "high" {
		t.Errorf("计数应使用按次合并的 effort，实际 %q", got)
	}
	betas := countReq.header.Get("anthropic-beta")
	if !strings.Contains(betas, "thinking-binding-controls-2026-08-01") || strings.Contains(betas, "server-side-fallback") {
		t.Errorf("计数请求 beta 头 = %q", betas)
	}
}

func TestCountInputTokens_ReportsProviderFailure(t *testing.T) {
	srv, rec := newServer(t, func(w http.ResponseWriter, _ *http.Request, n int) {
		if n == 1 {
			w.Header().Set("Retry-After", "2")
			replyJSON(w, http.StatusTooManyRequests, map[string]any{
				"type": "error", "error": map[string]any{"type": "rate_limit_error", "message": "count limit"},
			})
			return
		}
		replyJSON(w, http.StatusOK, map[string]any{})
	})
	client := newClient(srv)
	input := []*schema.Message{schema.UserMessage("计数")}
	_, err := client.CountInputTokens(t.Context(), input)
	var apiErr *model.APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusTooManyRequests || apiErr.RetryAfter.Seconds() != 2 || rec.count() != 1 {
		t.Fatalf("计数限流应保留重试元数据且只调用端点一次，实际 %v，共 %d 次", err, rec.count())
	}
	if n, err := client.CountInputTokens(t.Context(), input); n != 0 || err == nil {
		t.Fatalf("缺少 input_tokens 的响应应报错，实际 %d, %v", n, err)
	}
}
