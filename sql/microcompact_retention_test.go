package sql_test

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
	crewsql "crew-go/sql"
	"crew-go/store"
)

func TestSQLiteMicroCompactOffloadRetention(t *testing.T) {
	const table = "crew_tool_retention"
	path := filepath.Join(t.TempDir(), "offload.sqlite")
	db := openBackgroundSQLite(t, path)
	t.Cleanup(func() { _ = db.Close() }) // 写入阶段可能已关闭。
	st := newTTLTestStore(t, db, crewsql.SQLite{}, table)
	if err := st.Setup(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctx := store.WithContext(compose_v2.WithSessionIDCtx(t.Context(), "inspection-task"), st)
	permanent := agent_v2.MicroCompactConfig{Enabled: true, OffloadEnabled: true, KeepRecent: 1}
	retained := permanent
	retained.OffloadTTL = time.Hour

	large := strings.Repeat("现场记录\n", 950)
	pair := func(id, result string) []*schema.Message {
		call := schema.ToolCall{ID: id, Function: schema.FunctionCall{Name: "Read", Arguments: `{}`}}
		msg := schema.ToolResultMessage(id, result)
		msg.Annotations = map[string]any{"sample": json.Number("9007199254740993")}
		return []*schema.Message{schema.AssistantToolCallMessage([]schema.ToolCall{call}), msg}
	}
	original := append(pair("old", large), pair("recent", "新记录")...)
	keep := agent_v2.MicroCompactRun(ctx, original, &permanent)
	fold := agent_v2.MicroCompactRun(ctx, original, &retained)
	oldKey, _ := keep[1].Annotations[agent_v2.MicroCompactOffloadKeyAnnotation].(string)
	newKey, _ := fold[1].Annotations[agent_v2.MicroCompactOffloadKeyAnnotation].(string)
	if oldKey == "" || newKey == "" || oldKey == newKey || schema.GetToolResultID(fold[1]) != "old" || fold[3].Content != "新记录" {
		t.Fatalf("原引用 %q，限期引用 %q，消息配对 %q，近端内容 %q", oldKey, newKey, schema.GetToolResultID(fold[1]), fold[3].Content)
	}
	if saved := compose_v2.EstimateMessageTokens(original) - compose_v2.EstimateMessageTokens(fold); saved < 1000 {
		t.Fatalf("工具原文转存只省下 %d token", saved)
	}
	var live, indefinite int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*), SUM(CASE WHEN expires_at_ms IS NULL THEN 1 ELSE 0 END) FROM "+table).
		Scan(&live, &indefinite); err != nil || live != 2 || indefinite != 1 {
		t.Fatalf("临时、永久各应存一份，得到 %d / %d，错误 %v", live, indefinite, err)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	coldDB := openBackgroundSQLite(t, path)
	t.Cleanup(func() { _ = coldDB.Close() }) // 释放恢复进程的测试连接。
	cold := newTTLTestStore(t, coldDB, crewsql.SQLite{}, table)
	ctx = store.WithContext(ctx, cold)
	loaded, err := retained.LoadOffloadedToolResult(ctx, newKey)
	if err != nil || loaded == nil || loaded.Content != large || schema.GetToolResultID(loaded) != "old" ||
		loaded.Annotations["sample"] != json.Number("9007199254740993") {
		t.Fatalf("冷恢复找不到完整工具消息，错误 %v", err)
	}
	foreign := compose_v2.WithSessionIDCtx(ctx, "second-inspection")
	if item, err := retained.LoadOffloadedToolResult(foreign, newKey); err != nil || item != nil {
		t.Fatalf("另一任务不应读到转存，错误 %v", err)
	}
	ttlExec(t, coldDB, "UPDATE "+table+" SET expires_at_ms = -1 WHERE data_key = ?", newKey)
	if item, err := retained.LoadOffloadedToolResult(ctx, newKey); err != nil || item != nil {
		t.Fatalf("到期条目应不可读，错误 %v", err)
	}
	if item, err := retained.LoadOffloadedToolResult(ctx, oldKey); err != nil || item == nil || item.Content != large {
		t.Fatalf("原有永久引用应仍可读，错误 %v", err)
	}

	memory := store.WithContext(ctx, store.NewInMemoryStore())
	fallback := agent_v2.MicroCompactRun(memory, original, &retained)
	if _, exists := fallback[1].Annotations[agent_v2.MicroCompactOffloadKeyAnnotation]; exists || fallback[1].Content == large {
		t.Fatal("拒绝 TTL 的 Store 应只生成普通占位符")
	}
}
