package agent_v2_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/model/deepseek"
	"crew-go/model/openai"
	"crew-go/model/qwen"
	"crew-go/schema"
)

func TestThinkingHTTPReplayAcrossRunsAndConsumerClose(t *testing.T) {
	for _, provider := range []struct {
		name, field, enabled, disabled string
		client                         func(string) model.ChatModel
		thinking                       func(bool) model.Option
	}{
		{
			name: "qwen", field: "enable_thinking", enabled: "true", disabled: "false",
			client: func(url string) model.ChatModel {
				return qwen.NewClient("mock", "mock-thinking", openai.WithBaseURL(url), qwen.WithEnableThinking(false))
			},
			thinking: qwen.WithEnableThinkingOption,
		},
		{
			name: "deepseek", field: "thinking", enabled: `{"type":"enabled"}`, disabled: `{"type":"disabled"}`,
			client: func(url string) model.ChatModel {
				return deepseek.NewClient("mock", "mock-thinking", openai.WithBaseURL(url))
			},
			thinking: deepseek.WithThinkingOption,
		},
	} {
		t.Run(provider.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			var calls atomic.Int32
			closed := make(chan struct{})
			const lastRun = 4
			type message struct {
				Role      string `json:"role"`
				Content   string `json:"content"`
				Reasoning string `json:"reasoning_content"`
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				call := int(calls.Add(1))
				if call > lastRun+1 {
					t.Errorf("出现额外的模型请求: %d", call)
					http.Error(w, "unexpected request", http.StatusBadRequest)
					return
				}
				var wire map[string]json.RawMessage
				var messages []message
				if err := json.NewDecoder(r.Body).Decode(&wire); err != nil || json.Unmarshal(wire["messages"], &messages) != nil {
					t.Error("模型桩无法读取生成请求")
					http.Error(w, "invalid request", http.StatusBadRequest)
					return
				}
				wantedOption := provider.enabled
				if call == lastRun {
					wantedOption = provider.disabled
				}
				if string(wire[provider.field]) != wantedOption || string(wire["stream"]) != "true" {
					t.Errorf("第 %d 次请求的 thinking 或 stream 参数错误: %s, %s", call, wire[provider.field], wire["stream"])
					http.Error(w, "wrong options", http.StatusBadRequest)
					return
				}
				last := message{Role: "user", Content: fmt.Sprintf("question-%d", call)}
				want := []message{last}
				if call <= lastRun {
					want = make([]message, 0, 2*call-1)
					for i := 1; i < call; i++ {
						want = append(want,
							message{Role: "user", Content: fmt.Sprintf("question-%d", i)},
							message{Role: "assistant", Content: fmt.Sprintf("answer-%d", i), Reasoning: fmt.Sprintf("reason-%d-continued", i)})
					}
					want = append(want, last)
				}
				dialogue := slices.DeleteFunc(messages, func(msg message) bool { return msg.Role == "system" })
				if !slices.Equal(dialogue, want) {
					t.Errorf("第 %d 次请求的历史不匹配\n得到 %+v\n需要 %+v", call, dialogue, want)
					http.Error(w, "wrong history", http.StatusBadRequest)
					return
				}

				w.Header().Set("Content-Type", "text/event-stream")
				if call != lastRun {
					fmt.Fprintf(w, `data: {"choices":[{"delta":{"role":"assistant","reasoning_content":"reason-%d"}}]}`+"\n\n", call)
				}
				if call > lastRun {
					w.(http.Flusher).Flush()
					select {
					case <-r.Context().Done():
						close(closed)
					case <-ctx.Done():
					}
					return
				}
				if call != lastRun {
					_, _ = io.WriteString(w, `data: {"choices":[{"delta":{"reasoning_content":"-continued"}}]}`+"\n\n")
				}
				fmt.Fprintf(w, `data: {"choices":[{"delta":{"role":"assistant","content":"answer-%d"},"finish_reason":"stop"}]}`+
					"\n\ndata: [DONE]\n\n", call)
			}))
			t.Cleanup(server.Close)
			m := provider.client(server.URL)
			ag := agent_v2.NewBasicAgent(agent_v2.WithName("thinking"), agent_v2.WithModel(m),
				agent_v2.WithHistory(agent_v2.NewInMemoryHistoryStore(), "owner"))
			conv := compose_v2.WithThreadIDCtx(ctx, "thinking-history")
			for i := 1; i <= lastRun; i++ {
				enabled := i != lastRun
				events, err := ag.Run(conv, agent_v2.NewRunSession(), schema.UserMessage(fmt.Sprintf("question-%d", i)),
					agent_v2.WithRunModelOptions(provider.thinking(enabled)))
				if err != nil {
					t.Fatalf("第 %d 轮: %v", i, err)
				}
				reasoning := ""
				if enabled {
					reasoning = fmt.Sprintf("reason-%d-continued", i)
				}
				if reply := agent_v2.FinalMessage(events); reply == nil ||
					reply.Content != fmt.Sprintf("answer-%d", i) || reply.ReasoningContent != reasoning {
					t.Fatalf("第 %d 轮的聚合答复错误: %+v", i, reply)
				}
			}

			reader, err := m.Stream(ctx, []*schema.Message{schema.UserMessage("question-5")}, provider.thinking(true))
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			if chunk, err := reader.Recv(ctx); err != nil || chunk == nil || chunk.ReasoningContent != "reason-5" {
				t.Fatalf("关闭前应收到只有推理的增量: %+v, %v", chunk, err)
			}
			reader.Close()
			select {
			case <-closed:
				if ctx.Err() != nil || calls.Load() != lastRun+1 {
					t.Fatalf("消费者关闭应独立结束请求，调用数=%d ctx=%v", calls.Load(), ctx.Err())
				}
			case <-time.After(time.Second):
				t.Fatal("消费者在推理阶段关闭后，模型桩未收到取消")
			}
		})
	}
}
