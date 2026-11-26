package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/observability"
	agentRedis "crew-go/redis"
	"crew-go/schema"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
)

type completionRecoveryModel struct {
	calls       atomic.Int32
	toolEffects atomic.Int32
	started     chan struct{}
	proceed     chan struct{}
}

func (m *completionRecoveryModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) {
	return m, nil
}

func (m *completionRecoveryModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}

func (m *completionRecoveryModel) Generate(ctx context.Context, messages []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	switch m.calls.Add(1) {
	case 1:
		if m.started != nil {
			close(m.started)
			select {
			case <-m.proceed:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return schema.AssistantToolCallMessage([]schema.ToolCall{{
			ID: "write", Type: "function",
			Function: schema.FunctionCall{Name: "ask_writer", Arguments: `{"input":"write once"}`},
		}}), nil
	case 2:
		for _, message := range messages {
			if message.Role == schema.RoleUser && strings.Contains(message.Content, "write once") {
				m.toolEffects.Add(1)
				return schema.AssistantMessage("writer result"), nil
			}
		}
		return nil, errors.New("writer did not receive its input")
	case 3:
		for _, message := range messages {
			if message.Role == schema.RoleTool && strings.Contains(message.Content, "writer result") {
				return schema.AssistantMessage("finished once"), nil
			}
		}
		return nil, errors.New("chat entry did not receive the tool result")
	default:
		return nil, errors.New("completed task executed again")
	}
}

// completionWriteFault 在可核对的清档边界注入拒写或确认丢失。
type completionWriteFault struct {
	sessionStore
	cp         compose_v2.Checkpointer
	afterPurge bool
	loseAck    bool
	triggered  atomic.Bool
}

func (s *completionWriteFault) update(ctx context.Context, key sessionKey, edit func(*sessionRecord) (*sessionRecord, error)) error {
	var loseAck bool
	err := s.sessionStore.update(ctx, key, func(current *sessionRecord) (*sessionRecord, error) {
		next, err := edit(current)
		if err != nil || next == nil || next.Status != statusSuccess || s.triggered.Load() {
			return next, err
		}
		hasCheckpoint, err := s.cp.ExistsNS(ctx, key.id, "")
		if err != nil {
			return nil, err
		}
		if s.afterPurge == !hasCheckpoint && s.triggered.CompareAndSwap(false, true) {
			if s.loseAck {
				loseAck = true
				return next, nil
			}
			return nil, errors.New("injected session write failure")
		}
		return next, nil
	})
	if err == nil && loseAck {
		return errors.New("injected lost session write acknowledgement")
	}
	return err
}

func TestPlanWebCompletionRecoveryAcrossReplicas(t *testing.T) {
	for _, tc := range []struct {
		name       string
		afterPurge bool
		loseAck    bool
		resumed    bool
	}{
		{name: "after-checkpoint-cleanup", afterPurge: true},
		{name: "before-success-commit"},
		{name: "success-commit-acknowledgement-lost", loseAck: true},
		{name: "resume-success-commit-rejected", resumed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const ttl = time.Minute
			ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
			defer cancel()
			mini := miniredis.RunT(t)
			model := &completionRecoveryModel{}
			if tc.resumed {
				model.started, model.proceed = make(chan struct{}), make(chan struct{})
			}
			newReplica := func() *chatServer {
				redis := goredis.NewClient(&goredis.Options{Addr: mini.Addr()})
				t.Cleanup(func() { _ = redis.Close() })
				return &chatServer{model: model, obs: observability.New(),
					sessions: &redisSessions{client: redis, ttl: ttl},
					cp:       agentRedis.NewCheckpointer(redis, agentRedis.WithCheckpointerPrefix("crew-plan-web")),
					durable:  true, requestTimeout: 5 * time.Second}
			}
			owner := rand.Text()
			request := func(server *chatServer, method, path string) *httptest.ResponseRecorder {
				req := httptest.NewRequestWithContext(ctx, method, path, nil)
				req.AddCookie(&http.Cookie{Name: ownerCookie, Value: owner})
				resp := httptest.NewRecorder()
				server.routes().ServeHTTP(resp, req)
				return resp
			}
			first := newReplica()
			fault := &completionWriteFault{
				sessionStore: first.sessions, cp: first.cp, afterPurge: tc.afterPurge, loseAck: tc.loseAck,
			}
			first.sessions = fault
			var stream *httptest.ResponseRecorder
			const start = "/api/chat?q=complete"
			if tc.resumed {
				var startedRequest *httptest.ResponseRecorder
				done := make(chan struct{})
				go func() {
					defer close(done)
					startedRequest = request(first, http.MethodGet, start)
				}()
				defer func() { cancel(); <-done }()
				select {
				case <-model.started:
				case <-done:
					t.Fatalf("模型启动前请求退出: %s", startedRequest.Body.String())
				case <-ctx.Done():
					t.Fatal("初次请求未进入模型")
				}
				rows, err := first.sessions.list(ctx, owner)
				if err != nil || len(rows) != 1 {
					t.Fatalf("未找到正在执行的任务: %+v, %v", rows, err)
				}
				id := rows[0].ID
				if got := request(first, http.MethodPost, "/api/interrupt?session_id="+id); got.Code != http.StatusOK {
					t.Fatalf("暂停请求失败: %s", got.Body.String())
				}
				close(model.proceed)
				<-done
				if !strings.Contains(startedRequest.Body.String(), `"status":"interrupted"`) || model.toolEffects.Load() != 0 {
					t.Fatalf("初次运行应停在工具派发前: %s", startedRequest.Body.String())
				}
				stream = request(first, http.MethodGet, "/api/chat?session_id="+id+"&action=continue")
			} else {
				stream = request(first, http.MethodGet, start)
			}
			if stream.Code != http.StatusOK || model.calls.Load() != 3 || model.toolEffects.Load() != 1 {
				t.Fatalf("没有先走完实际模型和工具调用，HTTP=%d 调用=%d 工具=%d: %s",
					stream.Code, model.calls.Load(), model.toolEffects.Load(), stream.Body.String())
			}
			if !fault.triggered.Load() {
				t.Fatal("未命中预定的元数据写入故障边界")
			}

			second := newReplica()
			rows, err := second.sessions.list(t.Context(), owner)
			if err != nil || len(rows) != 1 || rows[0].ID == "" {
				t.Fatalf("无法定位故障后的会话: %+v, %v", rows, err)
			}
			id := rows[0].ID
			detail := "/api/sessions/" + id
			resume := "/api/chat?session_id=" + id
			saved, err := second.cp.LoadNS(t.Context(), id, "")
			if err != nil {
				t.Fatal(err)
			}
			if !tc.afterPurge && !tc.loseAck {
				if rows[0].Status != statusRunning || !saved.HasResumableWork() {
					t.Fatalf("终态写入失败时应保留原进度，状态=%q 存档=%v", rows[0].Status, saved != nil)
				}
				if tc.resumed && saved.Status != compose_v2.CPStatusResuming {
					t.Fatalf("应保留此次续跑已领取的存档，实际为 %v", saved.Status)
				}
			}
			if (saved == nil) != tc.afterPurge {
				t.Fatalf("清理前后应保留对应存档，清理后=%v 有档=%v", tc.afterPurge, saved != nil)
			}
			mini.FastForward(2 * ttl)
			if tc.resumed {
				got := request(second, http.MethodGet, resume+"&action=abort")
				if got.Code != http.StatusGone || !strings.Contains(got.Body.String(), "status=success") {
					t.Fatalf("放弃请求应先看见已完成凭据，HTTP=%d: %s", got.Code, got.Body.String())
				}
			}
			response := request(second, http.MethodGet, detail)
			var completed sessionRecord
			if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &completed) != nil ||
				completed.Status != statusSuccess || completed.HistorySize == 0 {
				t.Fatalf("新实例未读到持久终态，HTTP=%d: %s", response.Code, response.Body.String())
			}
			if slots, err := second.cp.LoadAllInSession(t.Context(), id); err != nil || len(slots) != 0 {
				t.Fatalf("核对成功终态后应清空存档，剩余=%d 错误=%v", len(slots), err)
			}
			for _, action := range []string{"continue", "abort"} {
				got := request(second, http.MethodGet, resume+"&action="+action)
				if got.Code != http.StatusGone || !strings.Contains(got.Body.String(), fmt.Sprintf("status=%s", statusSuccess)) {
					t.Fatalf("已成功任务的 %s 请求应返回原终态，HTTP=%d: %s", action, got.Code, got.Body.String())
				}
			}
			if model.calls.Load() != 3 || model.toolEffects.Load() != 1 {
				t.Fatalf("恢复已完成任务时重复执行，调用=%d 工具=%d", model.calls.Load(), model.toolEffects.Load())
			}
			mini.FastForward(2 * ttl)
			if got := request(second, http.MethodGet, detail); got.Code != http.StatusNotFound {
				t.Fatalf("补清完成后会话仍未到期，HTTP=%d: %s", got.Code, got.Body.String())
			}
		})
	}
}
