package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/observability"
)

const ownerCookie = "crew_plan_owner"
const sessionFinishTimeout = 5 * time.Second

var errSessionMissing = errors.New("session not found")

type finishedSession struct{ status string }

func (e finishedSession) Error() string { return "session already completed (status=" + e.status + ")" }

type chatServer struct {
	model          model.ChatModel
	obs            *observability.Observability
	sessions       sessionStore
	cp             compose_v2.Checkpointer
	requestTimeout time.Duration
	secureCookie   bool
	durable        bool
	active         sync.Map
}

func (s *chatServer) routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		s.owner(w, r, true)
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(indexHTML) // 客户端断开时没有后续响应可写。
	})
	mux.HandleFunc("GET /api/chat", s.chat)
	mux.HandleFunc("POST /api/interrupt", s.interrupt)
	mux.HandleFunc("GET /api/sessions", s.listSessions)
	mux.HandleFunc("GET /api/sessions/{id}", s.getSession)
	mux.HandleFunc("DELETE /api/sessions/{id}", s.deleteSession)
	return mux
}

func (s *chatServer) owner(w http.ResponseWriter, r *http.Request, create bool) string {
	if c, err := r.Cookie(ownerCookie); err == nil && validOwner(c.Value) {
		return c.Value
	}
	if !create {
		return ""
	}
	owner := rand.Text()
	http.SetCookie(w, &http.Cookie{Name: ownerCookie, Value: owner, Path: "/", HttpOnly: true,
		SameSite: http.SameSiteStrictMode, Secure: r.TLS != nil || s.secureCookie})
	return owner
}

func validOwner(owner string) bool {
	if len(owner) < 26 || len(owner) > 64 {
		return false
	}
	for _, ch := range owner {
		if !(ch >= 'A' && ch <= 'Z' || ch >= '2' && ch <= '7') {
			return false
		}
	}
	return true
}

func (s *chatServer) taskContext(ctx context.Context, owner string) context.Context {
	ctx = compose_v2.WithOwner(compose_v2.WithCheckpointerCtx(ctx, s.cp), owner)
	if s.durable {
		ctx = compose_v2.WithDurabilityCtx(ctx, compose_v2.DurabilitySync)
	}
	return ctx
}

// acquire 先挡同一实例的重叠 HTTP 操作，再向 Checkpointer 取得跨副本许可。
// 运行与终态元数据写入共用同一许可。删除与中止使用放弃许可。
func (s *chatServer) acquire(ctx context.Context, id string, abandon bool) (context.Context, func(), error) {
	if _, active := s.active.LoadOrStore(id, struct{}{}); active {
		return ctx, nil, compose_v2.ErrTaskBusy
	}
	var abandoned []string
	runID := id
	if abandon {
		runID, abandoned = "", []string{id}
	}
	ctx, activity, err := compose_v2.BeginTaskActivity(ctx, runID, abandoned)
	if err != nil {
		s.active.Delete(id)
		return ctx, nil, err
	}
	return ctx, sync.OnceFunc(func() {
		activity.Release()
		s.active.Delete(id)
	}), nil
}

func (s *chatServer) chat(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	q := r.URL.Query()
	id, input := strings.TrimSpace(q.Get("session_id")), strings.TrimSpace(q.Get("q"))
	resuming := id != ""
	if !resuming && input == "" {
		http.Error(w, "missing q (new session) or session_id (resume)", http.StatusBadRequest)
		return
	}
	key := sessionKey{owner: s.owner(w, r, !resuming), id: id}
	sess := agent_v2.NewRunSession()
	if resuming {
		if _, err := s.load(r.Context(), key); err != nil {
			writeSessionError(w, err)
			return
		}
		sess.RunID = id
	} else {
		key.id = sess.RunID
	}
	mode := strings.TrimSpace(q.Get("mode"))
	action := parseResumeAction(strings.TrimSpace(q.Get("action")))
	ctx := s.taskContext(r.Context(), key.owner)
	if s.requestTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.requestTimeout)
		defer cancel()
	}
	ctx, release, err := s.acquire(ctx, key.id, resuming && action == agent_v2.ResumeActionAbort)
	if err != nil {
		writeSessionError(w, err)
		return
	}
	defer release()
	ctx = compose_v2.WithSessionIDCtx(ctx, sess.RunID)
	ctx, err = compose_v2.WithDeferredRootCheckpointCleanup(ctx, sess.RunID)
	if err != nil {
		writeSessionError(w, err)
		return
	}
	var record sessionRecord
	var verifyResume bool
	err = s.sessions.update(ctx, key, func(current *sessionRecord) (*sessionRecord, error) {
		now := time.Now().UTC()
		if resuming {
			if current == nil {
				return nil, errSessionMissing
			}
			if current.terminal() {
				record = *current
				return nil, finishedSession{current.Status}
			}
			// 已取得独占许可，遗留 running 表示前一次未提交终态。核对期间先保留旧记录。
			verifyResume = current.needsRecoveryCheck()
			if verifyResume {
				record = *current
				return current, nil
			}
		} else {
			if current != nil {
				return nil, compose_v2.ErrTaskBusy
			}
			current = &sessionRecord{ID: key.id, Mode: "normal", CreatedAt: now, Ephemeral: q.Get("ephemeral") == "true"}
		}
		if mode == "normal" || mode == "plan" {
			current.Mode = mode
		}
		current.Status, current.LastActivity, current.PauseRequested = statusRunning, now, false
		record = *current
		return current, nil
	})
	if err != nil {
		if record.CleanupPending {
			cleanupCtx, cancel := context.WithTimeout(ctx, sessionFinishTimeout)
			defer cancel()
			if _, cleanupErr := s.finishSuccessCleanup(cleanupCtx, key); cleanupErr != nil {
				log.Printf("[plan_chat_web] 补清已完成会话存档: %v", cleanupErr)
			}
		}
		writeSessionError(w, err)
		return
	}
	if verifyResume {
		verifyCtx, cancel := context.WithTimeout(ctx, sessionFinishTimeout)
		defer cancel()
		size, complete, err := loadSessionSuccess(verifyCtx, s.cp, key.id)
		if err != nil {
			writeSessionError(w, err)
			return
		}
		if complete {
			_, err = s.commitSuccess(verifyCtx, key, size)
			if err == nil {
				err = finishedSession{statusSuccess}
			}
			writeSessionError(w, err)
			return
		}
	}

	slot := &runSlot{sess: sess, cp: s.cp, mode: record.Mode, resumeAction: action,
		resumeText: strings.TrimSpace(q.Get("text")), lastInterruptID: record.InterruptID,
		verifyResume: verifyResume, historySize: record.HistorySize}
	sink := newEventSink()
	intHook := &interruptHook{requested: func(ctx context.Context) (bool, error) {
		current, err := s.sessions.get(ctx, key)
		return current != nil && current.PauseRequested, err
	}}
	hooks := s.obs.AgentHooks(&hookAdapter{sink: sink}, intHook)
	orch := s.obs.OrchHooks(&orchAdapter{sink: sink})
	ctx, span := s.obs.Tracer().Start(ctx, "plan_chat_web "+r.URL.Path)
	defer span.End()
	if traceID := observability.TraceIDFromContext(ctx); traceID != "" {
		sink.send("trace_started", map[string]any{"session_id": sess.RunID, "trace_id": traceID})
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer sink.close()
		defer release()
		runOrResume(ctx, s.model, slot, input, resuming, sink, orch, hooks, func(status, pendingID string, historySize int) error {
			finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sessionFinishTimeout)
			defer cancel()
			return s.finish(finishCtx, key, status, pendingID, historySize)
		})
	}()
	writeSSE(w, r, sink)
	<-done
}

func (s *chatServer) finish(ctx context.Context, key sessionKey, status, pendingID string, historySize int) error {
	if err := compose_v2.CheckCheckpointTaskLease(ctx); err != nil {
		return err
	}
	if status == statusSuccess {
		if err := saveSessionSuccess(ctx, s.cp, key.id, historySize); err != nil {
			return fmt.Errorf("save session success: %w", err)
		}
		_, err := s.commitSuccess(ctx, key, historySize)
		return err
	}
	if (sessionRecord{Status: status}).terminal() {
		if err := s.cleanupCheckpoint(ctx, key.id); err != nil {
			return err
		}
	}
	_, err := s.recordFinish(ctx, key, status, pendingID, historySize)
	return err
}

func (s *chatServer) recordFinish(ctx context.Context, key sessionKey, status, pendingID string, historySize int) (*sessionRecord, error) {
	var recorded *sessionRecord
	err := s.sessions.update(ctx, key, func(current *sessionRecord) (*sessionRecord, error) {
		if current == nil {
			return nil, errSessionMissing
		}
		if current.terminal() {
			return nil, finishedSession{current.Status}
		}
		current.Status, current.LastActivity = status, time.Now().UTC()
		current.InterruptID, current.HistorySize, current.PauseRequested = pendingID, historySize, false
		current.CleanupPending = status == statusSuccess
		if current.mayExpire() && current.Ephemeral {
			return nil, nil
		}
		recorded = current
		return current, nil
	})
	return recorded, err
}

// commitSuccess 先提交可查询的终态，再尝试清理进度和完成凭据。
func (s *chatServer) commitSuccess(ctx context.Context, key sessionKey, historySize int) (*sessionRecord, error) {
	recorded, err := s.recordFinish(ctx, key, statusSuccess, "", historySize)
	if err != nil {
		return nil, err
	}
	settled, err := s.finishSuccessCleanup(ctx, key)
	if err != nil {
		log.Printf("[plan_chat_web] 成功会话的清档待补: %v", err)
		return recorded, nil
	}
	return settled, nil
}

// finishSuccessCleanup 补做已提交成功会话的清档。调用方持有任务许可。
func (s *chatServer) finishSuccessCleanup(ctx context.Context, key sessionKey) (*sessionRecord, error) {
	if err := compose_v2.CheckCheckpointTaskLease(ctx); err != nil {
		return nil, err
	}
	if err := s.cleanupCheckpoint(ctx, key.id); err != nil {
		return nil, err
	}
	var settled *sessionRecord
	err := s.sessions.update(ctx, key, func(current *sessionRecord) (*sessionRecord, error) {
		if current == nil {
			return nil, errSessionMissing
		}
		if current.Ephemeral {
			return nil, nil
		}
		current.CleanupPending = false
		current.LastActivity = time.Now().UTC()
		settled = current
		return current, nil
	})
	return settled, err
}

// reconcileSuccessCleanup 处理进程重启或清档暂时失败留下的记录。
func (s *chatServer) reconcileSuccessCleanup(ctx context.Context, key sessionKey) (*sessionRecord, error) {
	ctx, cancel := context.WithTimeout(ctx, sessionFinishTimeout)
	defer cancel()
	ctx, release, err := s.acquire(s.taskContext(ctx, key.owner), key.id, true)
	if err != nil {
		return nil, err
	}
	defer release()
	record, err := s.load(ctx, key)
	if err != nil {
		return record, err
	}
	if record.CleanupPending {
		return s.finishSuccessCleanup(ctx, key)
	}
	if record.needsRecoveryCheck() {
		size, complete, err := loadSessionSuccess(ctx, s.cp, key.id)
		if err != nil || !complete {
			return record, err
		}
		return s.commitSuccess(ctx, key, size)
	}
	return record, nil
}

func (s *chatServer) interrupt(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(r.URL.Query().Get("session_id"))
	if id == "" {
		http.Error(w, "missing session_id", http.StatusBadRequest)
		return
	}
	key := sessionKey{owner: s.owner(w, r, false), id: id}
	err := s.sessions.update(r.Context(), key, func(current *sessionRecord) (*sessionRecord, error) {
		if current == nil {
			return nil, errSessionMissing
		}
		if current.terminal() {
			return nil, finishedSession{current.Status}
		}
		if current.Status == statusRunning {
			current.PauseRequested = true
		}
		return current, nil
	})
	if err != nil {
		writeSessionError(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *chatServer) load(ctx context.Context, key sessionKey) (*sessionRecord, error) {
	if key.owner == "" {
		return nil, errSessionMissing
	}
	record, err := s.sessions.get(ctx, key)
	if err == nil && record == nil {
		err = errSessionMissing
	}
	return record, err
}

func (s *chatServer) listSessions(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit := 20
	if value := q.Get("limit"); value != "" {
		n, err := strconv.Atoi(value)
		if err != nil || n < 1 || n > 100 {
			http.Error(w, "limit must be between 1 and 100", http.StatusBadRequest)
			return
		}
		limit = n
	}
	owner := s.owner(w, r, false)
	records := make([]sessionRecord, 0)
	if owner != "" {
		all, err := s.sessions.list(r.Context(), owner)
		if err != nil {
			writeSessionError(w, err)
			return
		}
		for _, record := range all {
			if status := q.Get("status"); status == "" || status == record.Status {
				records = append(records, record)
			}
		}
	}
	sort.Slice(records, func(i, j int) bool {
		if records[i].CreatedAt.Equal(records[j].CreatedAt) {
			return records[i].ID < records[j].ID
		}
		return records[i].CreatedAt.After(records[j].CreatedAt)
	})
	if len(records) > limit {
		records = records[:limit]
	}
	writeSessionJSON(w, records)
}

func (s *chatServer) getSession(w http.ResponseWriter, r *http.Request) {
	key := sessionKey{owner: s.owner(w, r, false), id: r.PathValue("id")}
	record, err := s.load(r.Context(), key)
	if err != nil {
		writeSessionError(w, err)
		return
	}
	needsCleanup := record.CleanupPending
	if record.needsRecoveryCheck() {
		hasSuccess, err := s.cp.ExistsNS(r.Context(), key.id, successCheckpointNS)
		if err != nil {
			log.Printf("[plan_chat_web] 查询会话的成功凭据: %v", err)
		}
		needsCleanup = hasSuccess
	}
	if needsCleanup {
		settled, err := s.reconcileSuccessCleanup(r.Context(), key)
		switch {
		case errors.Is(err, errSessionMissing) || err == nil && settled == nil:
			writeSessionError(w, errSessionMissing)
			return
		case err == nil:
			record = settled
		case !errors.Is(err, compose_v2.ErrTaskBusy):
			log.Printf("[plan_chat_web] 查询时补清会话存档: %v", err)
		}
	}
	writeSessionJSON(w, record)
}

func (s *chatServer) deleteSession(w http.ResponseWriter, r *http.Request) {
	key := sessionKey{owner: s.owner(w, r, false), id: r.PathValue("id")}
	if _, err := s.load(r.Context(), key); err != nil {
		writeSessionError(w, err)
		return
	}
	ctx, release, err := s.acquire(s.taskContext(r.Context(), key.owner), key.id, true)
	if err != nil {
		writeSessionError(w, err)
		return
	}
	defer release()
	if _, err := s.load(ctx, key); err != nil {
		writeSessionError(w, err)
		return
	}
	if err := s.cleanupCheckpoint(ctx, key.id); err != nil {
		writeSessionError(w, err)
		return
	}
	if err := s.sessions.update(ctx, key, func(_ *sessionRecord) (*sessionRecord, error) { return nil, nil }); err != nil {
		writeSessionError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *chatServer) cleanupCheckpoint(ctx context.Context, id string) error {
	if _, err := s.cp.DeleteSubtree(ctx, id, ""); err != nil {
		return fmt.Errorf("clean up session checkpoint: %w", err)
	}
	s.obs.CleanupSession(id)
	return nil
}

func writeSessionJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value) // 客户端断开后不能再写错误响应。
}

func writeSessionError(w http.ResponseWriter, err error) {
	w.Header().Set("Cache-Control", "no-store")
	var finished finishedSession
	switch {
	case errors.Is(err, errSessionMissing):
		http.Error(w, errSessionMissing.Error(), http.StatusNotFound)
	case errors.Is(err, compose_v2.ErrTaskBusy):
		http.Error(w, "session has an active operation", http.StatusConflict)
	case errors.As(err, &finished):
		http.Error(w, finished.Error(), http.StatusGone)
	default:
		log.Printf("[plan_chat_web] 会话操作失败: %v", err)
		http.Error(w, "session state unavailable", http.StatusServiceUnavailable)
	}
}
