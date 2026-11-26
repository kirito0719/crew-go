package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// uiEvent 是推送到浏览器的单条事件。Type 固定小写 snake_case，Data 内部字段
// 由各 event kind 自定义（见 hooks.go / main.go 的 push 调用点）。
type uiEvent struct {
	Type string         `json:"type"`
	TS   int64          `json:"ts"`
	Data map[string]any `json:"data,omitempty"`
}

// eventSink 汇集本次请求的 SSE 事件。进度队列满时允许丢弃，避免钩子等待网络。
// 请求收尾事件单独保留，writer 排空进度后再发送。
type eventSink struct {
	ch         chan uiEvent
	closed     chan struct{}
	once       sync.Once
	dropCount  atomic.Int64
	completion []uiEvent
}

func newEventSink() *eventSink {
	return &eventSink{
		ch:     make(chan uiEvent, 256),
		closed: make(chan struct{}),
	}
}

func newUIEvent(kind string, data map[string]any) uiEvent {
	return uiEvent{Type: kind, TS: time.Now().UnixMilli(), Data: data}
}

// send 非阻塞投递进度事件。队列满时累计丢弃数量。
func (s *eventSink) send(kind string, data map[string]any) bool {
	ev := newUIEvent(kind, data)
	select {
	case <-s.closed:
		return false
	case s.ch <- ev:
		return true
	default:
		s.dropCount.Add(1)
		return false
	}
}

// complete 由执行协程在结束时调用一次。可选结果先于 done，内容由 close 发布给 writer。
func (s *eventSink) complete(kind string, data, done map[string]any) {
	s.completion = make([]uiEvent, 0, 2)
	if kind != "" {
		s.completion = append(s.completion, newUIEvent(kind, data))
	}
	s.completion = append(s.completion, newUIEvent("done", done))
}

// close 由执行协程在退出时调用，通知 SSE writer 排出剩余事件。
func (s *eventSink) close() {
	s.once.Do(func() { close(s.closed) })
}

// writeSSE 把事件持续写入 http response，直到 ctx 取消或 sink 关闭。
// 单 writer 单 response，调用方负责保证不被并发调用。
//
// **iter-5-N HIGH-3**: 每 5s 检 dropCount, 非 0 时 log warn — 业务排查 SSE 事件丢失
// 时 grep stderr "plan_chat_web SSE backpressure drop" 立即定位.
func writeSSE(w http.ResponseWriter, r *http.Request, sink *eventSink) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no") // 防 nginx 缓冲
	// 立即刷出响应头，让前端 EventSource.onopen 马上触发
	flusher.Flush()

	ctx := r.Context()
	// 心跳，防中间件因为长时间没数据误判连接超时。demo 粒度 15s 够用。
	ping := time.NewTicker(15 * time.Second)
	defer ping.Stop()

	// iter-5-N HIGH-3: 周期 log warn drop 计数
	dropWarn := time.NewTicker(5 * time.Second)
	defer dropWarn.Stop()
	var lastDropCount int64

	for {
		select {
		case <-ctx.Done():
			return
		case <-sink.closed:
			// 先排空进度，避免浏览器处理 done 后还收到前一阶段的事件。
			for {
				select {
				case ev := <-sink.ch:
					writeOne(w, flusher, ev)
				default:
					for _, ev := range sink.completion {
						writeOne(w, flusher, ev)
					}
					if curDrop := sink.dropCount.Load(); curDrop > lastDropCount {
						log.Printf("[plan_chat_web] SSE backpressure drop final=%d (since session start) — 浏览器慢消费 / 网络慢", curDrop)
					}
					return
				}
			}
		case ev := <-sink.ch:
			writeOne(w, flusher, ev)
		case <-ping.C:
			fmt.Fprint(w, ": ping\n\n")
			flusher.Flush()
		case <-dropWarn.C:
			if curDrop := sink.dropCount.Load(); curDrop > lastDropCount {
				log.Printf("[plan_chat_web] SSE backpressure drop +%d (total=%d) — 浏览器慢消费 / 网络慢; 容量=256",
					curDrop-lastDropCount, curDrop)
				lastDropCount = curDrop
			}
		}
	}
}

func writeOne(w http.ResponseWriter, f http.Flusher, ev uiEvent) {
	payload, err := json.Marshal(ev)
	if err != nil {
		// 编码错误不对外抛，写个 server-level 错误事件以便前端看得到
		payload, _ = json.Marshal(uiEvent{Type: "error", TS: ev.TS, Data: map[string]any{"message": "marshal: " + err.Error()}})
	}
	// SSE 协议：每条事件以 "data: <json>\n\n" 结尾。type 字段由 JSON 自身携带，
	// 不用 SSE 原生的 event: 字段——前端统一 onmessage 就能拿到。
	fmt.Fprintf(w, "data: %s\n\n", payload)
	f.Flush()
}
