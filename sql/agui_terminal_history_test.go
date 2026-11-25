package sql_test

import (
	"context"
	dbsql "database/sql"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/agui"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
	crewsql "crew-go/sql"
)

type exitAfterAGUIInterrupt struct {
	hook.NoOpHooks
	id string
}

func (h *exitAfterAGUIInterrupt) OnAgentEnd(ctx context.Context, _ *hook.AgentInfo, _ *hook.AgentOutput) context.Context {
	if interrupted, ok := compose_v2.IsInterrupt(agent_v2.GetEndError(ctx)); ok {
		h.id = interrupted.AllQueries()[0].InterruptID
		runtime.Goexit()
	}
	return ctx
}

func TestSQLiteAGUITerminalHistoryRecoversWithoutRequest(t *testing.T) {
	path := filepath.Join(t.TempDir(), "terminal-history.db")
	f := setupAGUI(t, crewsql.SQLite{}, func() *dbsql.DB {
		db, err := dbsql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(5000)&_txlock=immediate")
		if err != nil {
			t.Fatal(err)
		}
		db.SetMaxOpenConns(1)
		return db
	})
	oldHistory := crewsql.NewHistoryStore(f.dbA, crewsql.SQLite{}, crewsql.WithHistoryTable("terminal_history"))
	if err := oldHistory.Setup(t.Context()); err != nil {
		t.Fatal(err)
	}
	key := agent_v2.HistoryKey{Owner: "old-history-owner", ConvID: "shared"}
	sink := &sharedReplacementHistory{HistoryStore: oldHistory}
	sink.fail.Store(true)
	var executed, started atomic.Int32
	makeAgent := func(h hook.Hooks) agent_v2.Agent {
		started.Add(1)
		return sharedApprovalAgent(sink, func() { executed.Add(1) }, agent_v2.WithHooks(h),
			agent_v2.WithHistory(sink, key.Owner, agent_v2.WithHistoryOwnerFunc(func(context.Context) string { return key.Owner })))
	}
	exit := &exitAfterAGUIInterrupt{}
	var sid string
	store := &failingCancelSessionStore{AGUISessionStore: f.a, before: func(_ context.Context, _ string, rec *agui.SessionRecord, _ string) error {
		if rec != nil {
			sid = rec.TaskID
		}
		return nil
	}}
	first := sharedHandler(func(h hook.Hooks) agent_v2.Agent {
		return makeAgent(hook.NewChainHooks(h, exit))
	}, store, agui.WithWorkflowVersions("old", nil))

	req := aguiRequest(t, "owner", http.MethodPost, map[string]any{
		"threadId": "shared", "runId": "abnormal", "messages": []map[string]any{{"role": "user", "content": "转账"}},
	})
	resp := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		defer close(done)
		first.ServeHTTP(resp, req)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("异常执行未结束")
	}
	if exit.id == "" || sid == "" || !strings.Contains(resp.Body.String(), `"code":"agent_error"`) {
		t.Fatalf("需要先挂起审批再异常退出，task=%q interrupt=%q response=%s", sid, exit.id, resp.Body)
	}
	load := func() []*schema.Message {
		t.Helper()
		msgs, err := agent_v2.LoadAllHistory(t.Context(), oldHistory, key)
		if err != nil {
			t.Fatal(err)
		}
		return msgs
	}
	toolCalls := 0
	for _, msg := range load() {
		for _, call := range schema.GetToolCalls(msg) {
			if call.ID == "transfer" {
				toolCalls++
			}
		}
		if schema.GetToolResultID(msg) == "transfer" {
			t.Fatal("注入历史故障时不应有工具结果")
		}
	}
	if toolCalls != 1 {
		t.Fatalf("需要有一条实际落库的工具调用，实际 %d", toolCalls)
	}
	checkCancelCheckpoint(t, f.b, sid, false)
	stored, err := f.b.LoadSession(t.Context(), store.key)
	if err != nil || stored == nil || !stored.Closing || stored.TaskID != sid || stored.Cancellation == nil ||
		!stored.Cancellation.Terminal || !stored.Cancellation.CheckpointsDeleted || stored.Cancellation.HistoryKey == nil || *stored.Cancellation.HistoryKey != key {
		t.Fatalf("历史失败后应保留已清档进度和原历史分区，实际 %+v 错误 %v", stored, err)
	}
	late := aguiPost(t, first, "owner", aguiAnswer(exit.id, "resolved", true))
	if late.Code != http.StatusConflict || !strings.Contains(late.Body.String(), "cleanup_pending") {
		t.Fatalf("迟到审批应被拒绝，实际 %d %s", late.Code, late.Body)
	}

	sink.fail.Store(false)
	var currentFactory, resolverCalls atomic.Int32
	restarted := agui.Handler(func(h hook.Hooks) agent_v2.Agent {
		currentFactory.Add(1)
		return makeAgent(h)
	}, agui.WithSessionStore(f.b), agui.WithOwnerResolver(func(context.Context) string { return "owner" }),
		agui.WithWorkflowVersions("new", map[string]agui.AgentFactory{"old": makeAgent}),
		agui.WithCancellationHistory(func(context.Context, agent_v2.HistoryKey) (agent_v2.HistoryStore, error) {
			resolverCalls.Add(1)
			return agent_v2.NewInMemoryHistoryStore(), nil
		}))
	defer restarted.Close()
	waitIdleCancel(t, func() (bool, error) {
		record, err := f.b.LoadSession(t.Context(), store.key)
		return record == nil, err
	})
	results := 0
	for _, msg := range load() {
		if schema.GetToolResultID(msg) == "transfer" {
			results++
			if msg.Annotations[agent_v2.HistoryTaskAnnotation] != sid {
				t.Fatalf("结果应属于原任务，实际 %+v", msg.Annotations)
			}
		}
	}
	if results != 1 || executed.Load() != 0 || started.Load() < 2 || currentFactory.Load() != 0 || resolverCalls.Load() != 0 {
		t.Fatalf("后台应使用旧版本的静态历史且只补一个结果，结果=%d 工具=%d 工厂=%d 新版本=%d 解析器=%d",
			results, executed.Load(), started.Load(), currentFactory.Load(), resolverCalls.Load())
	}
}
