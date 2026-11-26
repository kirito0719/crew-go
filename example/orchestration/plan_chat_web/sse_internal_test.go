package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/observability"
	"crew-go/schema"
)

type burstModel int

func (m burstModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

func (burstModel) Generate(context.Context, []*schema.Message, ...model.Option) (*schema.Message, error) {
	return nil, errors.New("expected streamed response")
}

func (m burstModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	reader, send := schema.NewStreamReader[*schema.Message](int(m))
	for range int(m) {
		send(schema.AssistantMessage("字"))
	}
	schema.CloseSend(reader)
	return reader, nil
}

func TestPlanWebSSECompletionSurvivesProgressBackpressure(t *testing.T) {
	for _, tc := range []struct {
		status string
		event  string
		pause  bool
	}{
		{status: statusSuccess, event: "final"},
		{status: statusInterrupted, event: "interrupted", pause: true},
	} {
		t.Run(tc.status, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 4*time.Second)
			defer cancel()
			sess := agent_v2.NewRunSession()
			cp := compose_v2.NewInMemoryCheckpointer()
			ctx = compose_v2.WithCheckpointerCtx(compose_v2.WithSessionIDCtx(ctx, sess.RunID), cp)
			sink := newEventSink()
			obs := observability.New()
			hooks := obs.AgentHooks(&hookAdapter{sink: sink}, &interruptHook{
				requested: func(context.Context) (bool, error) { return tc.pause, nil },
			})
			orch := obs.OrchHooks(&orchAdapter{sink: sink})
			var status, pendingID string
			const chunks = 512

			// 模拟代理暂停读取，整次模型调用期间 SSE writer 不消费队列。
			runOrResume(ctx, burstModel(chunks), &runSlot{sess: sess, cp: cp, mode: "normal"}, "start",
				false, sink, orch, hooks, func(gotStatus, gotID string, _ int) error {
					status, pendingID = gotStatus, gotID
					return nil
				})
			sink.close()
			if status != tc.status || sink.dropCount.Load() == 0 {
				t.Fatalf("模型运行未复现队列背压，status=%q drops=%d", status, sink.dropCount.Load())
			}

			recorder := httptest.NewRecorder()
			writeSSE(recorder, httptest.NewRequestWithContext(ctx, http.MethodGet, "/api/chat?q=start", nil), sink)
			var events []uiEvent
			var chunksSeen int
			for line := range strings.SplitSeq(recorder.Body.String(), "\n") {
				if payload, ok := strings.CutPrefix(line, "data: "); ok {
					var event uiEvent
					if err := json.Unmarshal([]byte(payload), &event); err != nil {
						t.Fatal(err)
					}
					if event.Type == "node_event" && event.Data["type"] == "chunk" {
						chunksSeen++
					}
					events = append(events, event)
				}
			}
			if chunksSeen == 0 || chunksSeen >= chunks {
				t.Fatalf("应只有部分流式增量通过，收到 %d/%d", chunksSeen, chunks)
			}
			if len(events) < 2 {
				t.Fatalf("事件不足，无法得到请求终态: %+v", events)
			}
			result, done := events[len(events)-2], events[len(events)-1]
			if result.Type != tc.event || done.Type != "done" || done.Data["status"] != tc.status || done.Data["session_id"] != sess.RunID {
				t.Fatalf("流末尾缺少有序终态，最后两条为 %q 和 %q，状态为 %v", result.Type, done.Type, done.Data["status"])
			}
			if tc.pause {
				query, _ := result.Data["query"].(map[string]any)
				checkpoint, err := cp.LoadNS(ctx, sess.RunID, "")
				if pendingID == "" || query["interrupt_id"] != pendingID || err != nil || !resumableCheckpoint(checkpoint) {
					t.Fatalf("挂起事件应携带存档对应的待答 ID，query=%v pending=%q checkpointErr=%v", query, pendingID, err)
				}
			} else if result.Data["content"] != strings.Repeat("字", chunks) {
				t.Fatal("最终答复缺少被丢弃的增量")
			}
		})
	}
}
