package sql_test

import (
	"context"
	dbsql "database/sql"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/agent_v2/background"
	"crew-go/compose_v2"
	"crew-go/schema"
	crewsql "crew-go/sql"
)

type ambiguousProgressStore struct {
	*crewsql.BackgroundTaskStore
	lost atomic.Bool
}

func (s *ambiguousProgressStore) UpdateBackgroundTask(ctx context.Context, task *background.Task, clear bool) (bool, error) {
	saved, err := s.BackgroundTaskStore.UpdateBackgroundTask(ctx, task, clear)
	if saved && err == nil && task.Status == background.Running && task.Progress != nil && s.lost.CompareAndSwap(false, true) {
		return false, io.ErrUnexpectedEOF
	}
	return saved, err
}

func checkBackgroundProgress(t *testing.T, dialect crewsql.Dialect, open func() *dbsql.DB) {
	t.Helper()
	a, b := openBackgroundStore(t, dialect, open)
	ctx, stop := context.WithTimeout(t.Context(), 15*time.Second)
	defer stop()
	stage1 := background.Progress{Version: 1, Stage: "已核验", Evidence: []string{"inspection/accepted"}}
	stage2 := background.Progress{Version: 2, Stage: "报告已生成", Evidence: []string{"inspection/report"}}
	var attempts atomic.Int32
	var reportAfterExit func() error
	build := func(_ context.Context, _ string) (agent_v2.Agent, error) {
		out := compose_v2.LastValue[*schema.Message]("phase")
		graph := compose_v2.NewBuilder().AddNode("inspect", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			if attempts.Add(1) == 1 {
				lateCtx := context.WithoutCancel(ctx)
				reportAfterExit = func() error { return background.ReportProgress(lateCtx, stage2) }
				if err := background.ReportProgress(ctx, stage1); !errors.Is(err, io.ErrUnexpectedEOF) {
					return nil, fmt.Errorf("应模拟阶段写入确认丢失，实际 %v", err)
				}
			}
			if err := background.ReportProgress(ctx, stage1); err != nil {
				return nil, err
			}
			conflict := background.Progress{Version: stage1.Version, Stage: "旧版本的新内容"}
			if err := background.ReportProgress(ctx, conflict); !errors.Is(err, background.ErrProgressConflict) {
				return nil, fmt.Errorf("同版本只能提交相同的证据，实际 %v", err)
			}
			if err := background.ReportProgress(agent_v2.DetachTask(ctx), stage2); !errors.Is(err, background.ErrProgressUnavailable) {
				return nil, fmt.Errorf("独立任务不应使用外层后台任务上报资格，实际 %v", err)
			}
			if err := background.ReportProgress(compose_v2.WithOwner(ctx, "other"), stage2); !errors.Is(err, background.ErrProgressUnavailable) {
				return nil, fmt.Errorf("更换主体后不能写原任务的进度，实际 %v", err)
			}
			answer, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "是否发布"})
			if err != nil {
				return nil, err
			}
			if err := background.ReportProgress(ctx, stage2); err != nil {
				return nil, err
			}
			if err := background.ReportProgress(ctx, stage1); err != nil {
				return nil, err
			}
			return []compose_v2.ChannelWrite{compose_v2.Write(out, schema.AssistantMessage(answer.Content))}, nil
		}).AddEdge(compose_v2.StartNode, "inspect").AddEdge("inspect", compose_v2.EndNode)
		return backgroundGraph("inspection", out, graph)
	}
	cfg := background.Config{Workers: map[string]background.Factory{"inspection@v1": build}}
	manager := backgroundManager(t, &ambiguousProgressStore{BackgroundTaskStore: a}, cfg)
	parent := background.Parent{Owner: "tenant", TaskID: "parent", ThreadID: "case"}
	task, err := manager.Submit(ctx, parent, "inspection", "inspection@v1", schema.UserMessage("核验"))
	if err != nil || task == nil || task.Progress != nil {
		t.Fatalf("新任务应没有阶段记录，实际 %+v，错误 %v", task, err)
	}
	if err := manager.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if reportAfterExit == nil {
		t.Fatal("首次工作进程没有启动")
	}
	if err := reportAfterExit(); !errors.Is(err, background.ErrProgressUnavailable) {
		t.Fatalf("第一次执行已交出上报资格，实际 %v", err)
	}
	peer := backgroundManager(t, b, cfg)
	waiting, err := peer.Get(ctx, parent, task.ID)
	if err != nil || waiting == nil || waiting.Status != background.Waiting || waiting.Revision != task.Revision+3 ||
		len(waiting.Pending) != 1 || waiting.Progress == nil || waiting.Progress.Version != 1 ||
		waiting.Progress.Stage != stage1.Stage || !slices.Equal(waiting.Progress.Evidence, stage1.Evidence) {
		t.Fatalf("跨连接应读到一次上报及挂起，实际 %+v，错误 %v", waiting, err)
	}
	foreign := parent
	foreign.Owner = "other"
	if got, err := peer.Get(ctx, foreign, task.ID); got != nil || !errors.Is(err, background.ErrNotFound) {
		t.Fatalf("另一租户读到阶段，实际 %+v，错误 %v", got, err)
	}
	waiting.Progress.Evidence[0] = "修改读取快照"
	if again, err := peer.Get(ctx, parent, task.ID); err != nil || again.Progress.Evidence[0] != stage1.Evidence[0] {
		t.Fatalf("修改读取快照污染已存证据，实际 %+v，错误 %v", again, err)
	}
	accepted, err := peer.Answer(ctx, parent, task.ID, map[string]string{waiting.Pending[0].InterruptID: "确认"})
	if err != nil {
		t.Fatal(err)
	}
	if err := peer.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	finished, err := manager.Get(ctx, parent, task.ID)
	if err != nil || finished == nil || finished.Status != background.Succeeded || finished.Result == nil ||
		finished.Result.Content != "确认" || finished.Revision != accepted.Revision+2 || finished.Progress == nil ||
		finished.Progress.Version != 2 || finished.Progress.Stage != stage2.Stage ||
		!slices.Equal(finished.Progress.Evidence, stage2.Evidence) || attempts.Load() != 2 {
		t.Fatalf("另一个执行者续跑后应只有一次新阶段和终态，实际 %+v，执行次数 %d，错误 %v", finished, attempts.Load(), err)
	}
}
