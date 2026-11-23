package openai_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/fallback"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/schema"
)

const failedResponseUsage = `{"status":"failed","model":"failed-snapshot","error":{"code":"server_error","message":"temporary"},"usage":{"input_tokens":31,"input_tokens_details":{"cached_tokens":7},"output_tokens":9,"output_tokens_details":{"reasoning_tokens":4},"total_tokens":40}}`

func TestResponsesFailedUsageBothModes(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprint(stream), func(t *testing.T) {
			client := responsesMock(func(*http.Request) (*http.Response, error) {
				body := failedResponseUsage
				if stream {
					body = "event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":" + body + "}\n\n"
				}
				return responsesOK(body), nil
			})
			var err error
			if !stream {
				_, err = client.Generate(t.Context(), []*schema.Message{schema.UserMessage("x")})
			} else {
				sr, openErr := client.Stream(t.Context(), []*schema.Message{schema.UserMessage("x")})
				if openErr != nil {
					t.Fatal(openErr)
				}
				defer sr.Close()
				for err == nil {
					_, err = sr.Recv(t.Context())
				}
			}
			u := model.UsageFromError(err)
			if err == nil || errors.Is(err, io.EOF) || u == nil || u.Usage.InputTokens != 31 || u.Usage.OutputTokens != 9 || u.Usage.ThinkTokens != 4 || u.Usage.CachedInputTokens != 7 || u.Model != "failed-snapshot" {
				t.Fatalf("failure usage = %+v, err=%v", u, err)
			}
		})
	}
}

type failureUsageHook struct {
	hook.NoOpHooks
	failed  int32
	success int32
}

func (h *failureUsageHook) OnModelStart(ctx context.Context, _ *hook.AgentInfo, _ *schema.ModelInput) (context.Context, error) {
	return model.WithFailedUsageObserver(ctx, func(_ context.Context, u model.FailedUsage) { h.failed += u.Usage.InputTokens }), nil
}
func (h *failureUsageHook) OnModelEnd(ctx context.Context, _ *hook.AgentInfo, out *schema.ModelOutput) (context.Context, error) {
	if out.Usage != nil {
		h.success += out.Usage.InputTokens
	}
	return ctx, nil
}

// 用实际 Responses 协议与 BasicAgent 验证，失败尝试不能因重试或换候选成功而消失。
func TestResponsesUsageSurvivesRetriesAndFallback(t *testing.T) {
	for _, mode := range []string{"retry", "fallback", "nested-fallback"} {
		t.Run(mode, func(t *testing.T) {
			calls := 0
			badFn := responsesTransport(func(*http.Request) (*http.Response, error) {
				calls++
				return responsesOK("event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":" + failedResponseUsage + "}\n\n"), nil
			})
			goodFn := responsesTransport(func(*http.Request) (*http.Response, error) {
				calls++
				return responsesOK("event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"model\":\"good\",\"output\":[{\"type\":\"message\",\"content\":[{\"type\":\"output_text\",\"text\":\"done\"}]}],\"usage\":{\"input_tokens\":5,\"output_tokens\":2,\"total_tokens\":7}}}\n\n"), nil
			})
			bad, good := responsesMock(badFn), responsesMock(goodFn)
			var llm model.ChatModel
			if mode == "retry" {
				llm = responsesMock(func(r *http.Request) (*http.Response, error) {
					if calls == 0 {
						return badFn(r)
					}
					return goodFn(r)
				})
			} else {
				llm = fallback.New([]model.ChatModel{bad, good})
				if mode == "nested-fallback" {
					llm = fallback.New([]model.ChatModel{fallback.New([]model.ChatModel{bad}), good})
				}
			}
			h := &failureUsageHook{}
			a := agent_v2.NewBasicAgent(agent_v2.WithName("usage"), agent_v2.WithModel(llm), agent_v2.WithHooks(h), agent_v2.WithModelRetry(compose_v2.SimpleRetry(1)))
			events, err := a.Run(t.Context(), agent_v2.NewRunSession(), schema.UserMessage("x"))
			if err != nil || agent_v2.FinalMessage(events) == nil || agent_v2.FinalMessage(events).Content != "done" || calls != 2 || h.failed != 31 || h.success != 5 {
				t.Fatalf("calls=%d failed=%d success=%d err=%v", calls, h.failed, h.success, err)
			}
		})
	}
}
