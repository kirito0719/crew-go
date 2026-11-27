package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/a2a"
	"crew-go/a2a/server"
	"crew-go/agent_v2"
	"crew-go/model/openai"
	"crew-go/model/qwen"
	"crew-go/schema"

	a2atypes "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
)

func TestLocalQwenA2AStreamAndResume(t *testing.T) {
	const (
		prompt   = "给我点穿搭建议"
		question = "你最喜欢什么颜色？"
		answer   = "蓝色"
		callID   = "call_color"
		prefix   = "推荐颜色："
	)
	var calls atomic.Int32
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		call := calls.Add(1)
		var req struct {
			Model    string
			Stream   bool
			Messages []struct {
				Role       string
				Content    string
				ToolCallID string            `json:"tool_call_id"`
				ToolCalls  []schema.ToolCall `json:"tool_calls"`
			}
			Tools []struct {
				Function struct{ Name string }
			}
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil ||
			r.Method != http.MethodPost || r.URL.Path != "/compatible-mode/v1/chat/completions" ||
			req.Model != "qwen-local" || !req.Stream || len(req.Messages) == 0 {
			t.Errorf("第 %d 次 Qwen 请求不符合预期，path=%q request=%+v err=%v", call, r.URL.Path, req, err)
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		last := req.Messages[len(req.Messages)-1]
		var frames []string
		switch call {
		case 1:
			if last.Role != "user" || last.Content != prompt || len(req.Tools) != 1 ||
				req.Tools[0].Function.Name != agent_v2.AskUserToolName {
				t.Errorf("首次请求缺少原消息或 ask_user 工具，实际 %+v", req)
				http.Error(w, "missing question input", http.StatusBadRequest)
				return
			}
			frames = []string{
				`{"choices":[{"delta":{"role":"assistant","tool_calls":[{"index":0,"id":"call_color","type":"function","function":{"name":"ask_user","arguments":"{\"question\":"}}]}}]}`,
				fmt.Sprintf(`{"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":%q}}]},"finish_reason":"tool_calls"}]}`, fmt.Sprintf("%q}", question)),
			}
		case 2:
			if len(req.Messages) < 2 || last.Role != "tool" || last.ToolCallID != callID || last.Content != answer {
				t.Errorf("续跑请求未携带人类答复和原调用 ID，实际 %+v", req.Messages)
				http.Error(w, "missing tool result", http.StatusBadRequest)
				return
			}
			assistant := req.Messages[len(req.Messages)-2]
			if assistant.Role != "assistant" || len(assistant.ToolCalls) != 1 || assistant.ToolCalls[0].ID != callID ||
				assistant.ToolCalls[0].Function.Arguments != fmt.Sprintf(`{"question":%q}`, question) {
				t.Errorf("续跑请求丢失了原始工具声明，实际 %+v", assistant)
				http.Error(w, "missing tool call", http.StatusBadRequest)
				return
			}
			frames = []string{
				fmt.Sprintf(`{"choices":[{"delta":{"role":"assistant","content":%q}}]}`, prefix),
				fmt.Sprintf(`{"choices":[{"delta":{"content":%q},"finish_reason":"stop"}]}`, last.Content),
			}
		default:
			t.Errorf("模型收到第 %d 次额外请求", call)
			http.Error(w, "unexpected additional request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		if _, err := io.WriteString(w, "data: "+strings.Join(append(frames, "[DONE]"), "\n\ndata: ")+"\n\n"); err != nil {
			t.Errorf("写入本地模型 SSE 失败：%v", err)
		}
	}))
	t.Cleanup(provider.Close)

	agent := agent_v2.NewBasicAgent(agent_v2.WithName("local-qwen"),
		agent_v2.WithModel(qwen.NewClient("local-only", "qwen-local", openai.WithBaseURL(provider.URL+"/compatible-mode/v1"))),
		agent_v2.WithAskUser())
	host := httptest.NewUnstartedServer(nil)
	host.Config.Handler = server.Handler(agent, "http://"+host.Listener.Addr().String(),
		server.WithOwnerResolver(func(context.Context) string { return "local" }))
	host.Start()
	t.Cleanup(host.Close)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	client, err := a2aclient.NewFromCard(ctx, server.Card(agent, host.URL))
	if err != nil {
		t.Fatal(err)
	}
	type turn struct {
		info      a2atypes.TaskInfo
		status    a2atypes.TaskStatus
		artifacts map[a2atypes.ArtifactID]string
		updates   int
	}
	stream := func(msg *a2atypes.Message) turn {
		t.Helper()
		out := turn{artifacts: make(map[a2atypes.ArtifactID]string)}
		for event, err := range client.SendStreamingMessage(ctx, &a2atypes.SendMessageRequest{Message: msg}) {
			if err != nil {
				t.Fatal(err)
			}
			switch event := event.(type) {
			case *a2atypes.Task:
				out.info, out.status = a2atypes.TaskInfo{TaskID: event.ID, ContextID: event.ContextID}, event.Status
			case *a2atypes.TaskStatusUpdateEvent:
				out.info, out.status = a2atypes.TaskInfo{TaskID: event.TaskID, ContextID: event.ContextID}, event.Status
			case *a2atypes.TaskArtifactUpdateEvent:
				out.updates++
				if !event.Append {
					out.artifacts[event.Artifact.ID] = ""
				}
				for _, part := range event.Artifact.Parts {
					out.artifacts[event.Artifact.ID] += part.Text()
				}
			}
		}
		return out
	}
	first := stream(a2atypes.NewMessage(a2atypes.MessageRoleUser, a2atypes.NewTextPart(prompt)))
	if first.status.State != a2atypes.TaskStateInputRequired || first.status.Message == nil ||
		first.info.TaskID == "" || first.info.ContextID == "" || calls.Load() != 1 {
		t.Fatalf("首轮应挂起同一个 A2A 任务等待补答，实际 %+v 模型调用=%d", first, calls.Load())
	}
	pending := a2a.PendingQuestionsFromParts(first.status.Message.Parts)
	if len(pending) != 1 || pending[0].InterruptID == "" || pending[0].Question != question {
		t.Fatalf("没有收到模型生成的实际待答问题，实际 %+v", pending)
	}
	second := stream(a2atypes.NewMessageForTask(a2atypes.MessageRoleUser, first.info,
		a2a.AnswerPart(pending[0].InterruptID, nil, answer)))
	if second.info != first.info || second.status.State != a2atypes.TaskStateCompleted ||
		len(second.artifacts) != 1 || second.updates < 2 || calls.Load() != 2 {
		t.Fatalf("续跑应完成原任务并推送多片 artifact，实际 %+v 模型调用=%d", second, calls.Load())
	}
	for _, text := range second.artifacts {
		if text != prefix+answer {
			t.Errorf("聚合的 artifact = %q，需要 %q", text, prefix+answer)
		}
	}
}
