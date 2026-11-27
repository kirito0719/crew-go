package sql_test

import (
	"context"
	"io"
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
	crewsql "crew-go/sql"
	"crew-go/store"
	"crew-go/tool"
)

type toolHistoryWriteFault struct {
	agent_v2.HistoryTailAppender
	commit bool
	broken atomic.Bool
}

func (f *toolHistoryWriteFault) HistoryAppendOwner() agent_v2.HistoryStore { return f }

func (f *toolHistoryWriteFault) AppendAfter(ctx context.Context, key agent_v2.HistoryKey, after agent_v2.HistoryCursor, messages []*schema.Message, boundary int) (agent_v2.HistoryCursor, bool, error) {
	if slices.ContainsFunc(messages, func(m *schema.Message) bool { return m != nil && m.Role == schema.RoleTool }) && f.broken.CompareAndSwap(false, true) {
		if f.commit {
			_, ok, err := f.HistoryTailAppender.AppendAfter(ctx, key, after, messages, boundary)
			if err != nil || !ok {
				return "", ok, err
			}
		}
		return "", false, io.ErrUnexpectedEOF
	}
	return f.HistoryTailAppender.AppendAfter(ctx, key, after, messages, boundary)
}

func (f *toolHistoryWriteFault) Load(ctx context.Context, key agent_v2.HistoryKey, q agent_v2.HistoryQuery) (agent_v2.HistoryPage, error) {
	if f.broken.Load() {
		return agent_v2.HistoryPage{}, io.ErrClosedPipe
	}
	return f.HistoryTailAppender.Load(ctx, key, q)
}

type recoverToolHistoryModel struct {
	requests [][]*schema.Message
}

func (m *recoverToolHistoryModel) Generate(_ context.Context, messages []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.requests = append(m.requests, append([]*schema.Message(nil), messages...))
	if len(m.requests) == 1 {
		return schema.AssistantToolCallMessage([]schema.ToolCall{{
			ID: "call-review", Function: schema.FunctionCall{Name: "read_status", Arguments: "{}"},
		}}), nil
	}
	return schema.AssistantMessage("检查完成"), nil
}

func (m *recoverToolHistoryModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}

func (m *recoverToolHistoryModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) {
	return m, nil
}

func TestSQLiteRecoverableHistoryRestoresToolResultWithoutReexecuting(t *testing.T) {
	for _, tc := range []struct {
		name   string
		commit bool
	}{{"failed_before_commit", false}, {"lost_commit_ack", true}} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "recover-history.sqlite")
			first := openBackgroundSQLite(t, path)
			t.Cleanup(func() { _ = first.Close() }) // 冷恢复会显式关闭首个连接。
			base := crewsql.NewHistoryStore(first, crewsql.SQLite{})
			journal := crewsql.NewStore(first, crewsql.SQLite{})
			ctx, cancel := context.WithTimeout(compose_v2.WithThreadIDCtx(t.Context(), "validation"), 5*time.Second)
			defer cancel()
			if err := base.Setup(ctx); err != nil {
				t.Fatal(err)
			}
			if err := journal.Setup(ctx); err != nil {
				t.Fatal(err)
			}
			fault := &toolHistoryWriteFault{HistoryTailAppender: base, commit: tc.commit}
			key := agent_v2.HistoryKey{Owner: "tenant", ConvID: "validation"}
			m := &recoverToolHistoryModel{}
			var toolCalls atomic.Int32
			tk := tool.NewToolkit()
			tk.Register(tool.NewFuncTool("read_status", "读取状态", map[string]any{"type": "object"}, func(context.Context, map[string]any) (*tool.Result, error) {
				toolCalls.Add(1)
				return tool.Success("READY-17"), nil
			}))
			newAgent := func(history agent_v2.HistoryStore) *agent_v2.BasicAgent {
				return agent_v2.NewBasicAgent(agent_v2.WithName("reviewer"), agent_v2.WithModel(m),
					agent_v2.WithToolkit(tk), agent_v2.WithHistory(history, key.Owner))
			}
			before := newAgent(agent_v2.NewRecoverableHistoryStore(fault, journal, "review-history"))
			if _, err := before.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("请检查状态")); err != nil || !fault.broken.Load() || toolCalls.Load() != 1 {
				t.Fatalf("首次任务应执行工具并留下待写历史，调用 %d，故障 %v，错误 %v", toolCalls.Load(), fault.broken.Load(), err)
			}
			resultCount := func(messages []*schema.Message) int {
				total := 0
				for _, message := range messages {
					if message != nil && message.Role == schema.RoleTool && schema.GetToolResultID(message) == "call-review" && message.Content == "READY-17" {
						total++
					}
				}
				return total
			}
			page, err := base.Load(ctx, key, agent_v2.HistoryQuery{})
			if err != nil || (resultCount(page.Messages) == 1) != tc.commit {
				t.Fatalf("故障期间的实库次数 %d，错误 %v", resultCount(page.Messages), err)
			}
			if err := first.Close(); err != nil {
				t.Fatal(err)
			}

			second := openBackgroundSQLite(t, path)
			defer second.Close()
			cold := agent_v2.NewRecoverableHistoryStore(crewsql.NewHistoryStore(second, crewsql.SQLite{}), crewsql.NewStore(second, crewsql.SQLite{}), "review-history")
			if _, err = newAgent(cold).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("用之前的结果")); err != nil {
				t.Fatal(err)
			}
			if len(m.requests) != 3 || resultCount(m.requests[2]) != 1 || toolCalls.Load() != 1 {
				t.Fatalf("冷恢复应只把原结果送模一次，模型调用数 %d，工具实际调用数 %d", len(m.requests), toolCalls.Load())
			}
			page, err = cold.Load(ctx, key, agent_v2.HistoryQuery{})
			if err != nil || resultCount(page.Messages) != 1 {
				t.Fatalf("历史应只有一份结果，实际 %d，错误 %v", resultCount(page.Messages), err)
			}
		})
	}
}

func TestSQLiteRecoverableHistoryKeepsTriggerRows(t *testing.T) {
	base, db := newSQLiteHistory(t)
	ctx := t.Context()
	key := agent_v2.HistoryKey{Owner: "tenant", ConvID: "trigger"}
	if _, err := db.ExecContext(ctx, `CREATE TRIGGER history_followup AFTER INSERT ON crew_history
WHEN NEW.msg_id='source' BEGIN
  INSERT INTO crew_history(owner,conv_id,payload,msg_id,task,agent)
  VALUES (NEW.owner, NEW.conv_id, '{"role":"assistant","content":"trigger reply"}', 'reply', '', '');
END`); err != nil {
		t.Fatal(err)
	}
	hs := agent_v2.NewRecoverableHistoryStore(base, store.NewInMemoryStore(), "history")
	msg := schema.UserMessage("trigger input")
	msg.ID = "source"
	if err := hs.Append(ctx, key, []*schema.Message{msg}); err != nil {
		t.Fatal(err)
	}
	page, err := hs.Load(ctx, key, agent_v2.HistoryQuery{})
	if err != nil || len(page.Messages) != 2 || page.Messages[0].Content != msg.Content ||
		page.Messages[1].Content != "trigger reply" || len(page.VerifiedBoundaries) != 0 {
		t.Fatalf("recoverable append lost trigger rows: %+v err=%v", page, err)
	}
}
