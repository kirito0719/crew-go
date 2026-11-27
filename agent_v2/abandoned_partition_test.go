package agent_v2_test

import (
	"context"
	"errors"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

type abandonPartitionDelete struct {
	compose_v2.Checkpointer
	task     string
	fail     bool
	attempts int
}

func (c *abandonPartitionDelete) DeleteSubtree(ctx context.Context, sid, ns string, condition ...compose_v2.CheckpointDeleteCondition) (int32, error) {
	if sid == c.task {
		c.attempts++
		if c.fail {
			return 0, errAbandonDelete
		}
	}
	return c.Checkpointer.DeleteSubtree(ctx, sid, ns, condition...)
}

func TestWithAbandonedTasks_CrossPartitionDeletesBeforeClosingHistory(t *testing.T) {
	t.Run("按任务路由", func(t *testing.T) { testCrossPartitionAbandon(t, false) })
	t.Run("新任务分区已绑定", func(t *testing.T) { testCrossPartitionAbandon(t, true) })
}

func testCrossPartitionAbandon(t *testing.T, bound bool) {
	t.Helper()
	f := newAbandonFixture(t, nil)
	old := f.pending.RunID
	source := &abandonPartitionDelete{Checkpointer: f.cp, task: old, fail: true}
	destination := compose_v2.NewInMemoryCheckpointer()
	router := &routedGraphCheckpointer{
		Checkpointer: destination,
		other:        source,
		useOther:     func(ctx context.Context) bool { return compose_v2.GetSessionID(ctx) == old },
	}
	ctx := compose_v2.WithCheckpointerCtx(f.ctx, router)
	if bound {
		ctx = compose_v2.WithBoundCheckpointPartitionCtx(ctx, destination)
	}
	if err := source.SaveNS(ctx, old, "child", &compose_v2.Checkpoint{GraphID: "child"}); err != nil {
		t.Fatal(err)
	}
	if err := destination.SaveNS(ctx, old, "", &compose_v2.Checkpoint{GraphID: "another-partition"}); err != nil {
		t.Fatal(err)
	}
	run := func() {
		t.Helper()
		events, err := f.newAgent().Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("查余额"), agent_v2.WithAbandonedTasks(old))
		if reply := agent_v2.FinalMessage(events); err != nil || reply == nil || reply.Content != "好的" {
			t.Fatalf("新任务应正常结束，实际 reply=%v err=%v", reply, err)
		}
	}
	check := func(wantSlots, wantResults int) {
		t.Helper()
		slots, err := source.LoadAllInSession(ctx, old)
		if err != nil || len(slots) != wantSlots {
			t.Errorf("旧任务应有 %d 份存档，实际 %d 份 err=%v", wantSlots, len(slots), err)
		}
		other, err := destination.LoadNS(ctx, old, "")
		if err != nil || other == nil || other.GraphID != "another-partition" {
			t.Errorf("另一分区的同名槽应保留，实际 cp=%v err=%v", other, err)
		}
		results := 0
		for _, msg := range f.history(t) {
			if schema.GetToolResultID(msg) == "c1" && msg.Annotations[agent_v2.HistoryTaskAnnotation] == old {
				results++
			}
		}
		if results != wantResults || f.executed.Load() != 0 {
			t.Errorf("旧任务应有 %d 条收尾结果，实际 %d 条，工具执行 %d 次", wantResults, results, f.executed.Load())
		}
	}
	check(2, 0)
	run()
	check(2, 0)
	if source.attempts != 1 {
		t.Errorf("第一次应尝试清理原分区，实际 %d 次", source.attempts)
	}

	source.fail = false
	run()
	check(0, 1)
	run()
	check(0, 1)
}

type fixedAbandonCheckpointer struct {
	compose_v2.Checkpointer
	labels []string
}

func TestWithAbandonedTasks_OpaqueFixedPartition(t *testing.T) {
	f := newAbandonFixture(t, nil)
	memory := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(f.ctx, fixedAbandonCheckpointer{Checkpointer: memory, labels: []string{"本地"}})
	session := agent_v2.NewRunSession()
	old := session.RunID
	if _, err := f.newAgent().Run(ctx, session, schema.UserMessage("转账")); !compose_v2.LeavesResumableCheckpoint(err) {
		t.Fatalf("旧任务应挂起，实际 %v", err)
	}
	if cp, err := memory.LoadNS(ctx, old, ""); err != nil || cp == nil || !cp.HasResumableWork() {
		t.Fatalf("应有真实挂起存档，实际 cp=%v err=%v", cp, err)
	}
	session.NextTask()
	if _, err := f.newAgent().Run(ctx, session, schema.UserMessage("查余额"), agent_v2.WithAbandonedTasks(old)); err != nil {
		t.Fatal(err)
	}
	if cp, err := memory.LoadNS(ctx, old, ""); err != nil || cp != nil {
		t.Errorf("同一个不可比较的固定后端应完成删档，实际 cp=%v err=%v", cp, err)
	}
	if abandonIndexOf(f.history(t), func(msg *schema.Message) bool {
		return schema.GetToolResultID(msg) == "c1" && msg.Annotations[agent_v2.HistoryTaskAnnotation] == old
	}) < 0 {
		t.Error("应在确认原档删除后补写历史")
	}
}

func TestWithAbandonedTasks_PreservesAnIndependentCheckpointCopy(t *testing.T) {
	f := newAbandonFixture(t, nil)
	memory := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(f.ctx, fixedAbandonCheckpointer{Checkpointer: memory, labels: []string{"本地"}})
	session := agent_v2.NewRunSession()
	old := session.RunID
	if _, err := f.newAgent().Run(ctx, session, schema.UserMessage("转账")); !compose_v2.LeavesResumableCheckpoint(err) {
		t.Fatalf("旧任务应挂起，实际 %v", err)
	}
	cp, err := memory.LoadNS(ctx, old, "")
	if err != nil || cp == nil || !cp.HasResumableWork() {
		t.Fatalf("应有真实挂起存档，实际 cp=%v err=%v", cp, err)
	}
	other := compose_v2.NewInMemoryCheckpointer()
	if err := other.SaveNS(ctx, old, "", cp); err != nil {
		t.Fatal(err)
	}
	otherCtx := compose_v2.WithCheckpointerCtx(ctx, fixedAbandonCheckpointer{Checkpointer: other, labels: []string{"本地"}})
	session.NextTask()
	if _, err := f.newAgent().Run(otherCtx, session, schema.UserMessage("查余额"), agent_v2.WithAbandonedTasks(old)); err != nil {
		t.Fatal(err)
	}
	for _, store := range []*compose_v2.InMemoryCheckpointer{memory, other} {
		if cp, err := store.LoadNS(ctx, old, ""); err != nil || cp == nil || !cp.HasResumableWork() {
			t.Errorf("无法确认分区时应保留原档及另一分区的副本，实际 cp=%v err=%v", cp, err)
		}
	}
	if abandonIndexOf(f.history(t), func(msg *schema.Message) bool {
		return schema.GetToolResultID(msg) == "c1" && msg.Annotations[agent_v2.HistoryTaskAnnotation] == old
	}) >= 0 {
		t.Error("原档仍可续时应保留待答历史")
	}
}

type abandonHistoryFault struct {
	agent_v2.HistoryStore
	task     string
	attempts int
}

func (s *abandonHistoryFault) Append(ctx context.Context, key agent_v2.HistoryKey, msgs []*schema.Message) error {
	for _, msg := range msgs {
		if schema.GetToolResultID(msg) == "c1" && msg.Annotations[agent_v2.HistoryTaskAnnotation] == s.task {
			s.attempts++
			if s.attempts == 1 {
				return errors.New("cannot store abandoned history")
			}
		}
	}
	return s.HistoryStore.Append(ctx, key, msgs)
}

func TestWithAbandonedTasks_HistoryRetryKeepsTheConfirmedPartition(t *testing.T) {
	f := newAbandonFixture(t, nil)
	old := f.pending.RunID
	history := &abandonHistoryFault{HistoryStore: f.hs, task: old}
	f.hs = history
	run := func(ctx context.Context) {
		t.Helper()
		f.pending.NextTask()
		if _, err := f.newAgent().Run(ctx, f.pending, schema.UserMessage("查余额"), agent_v2.WithAbandonedTasks(old)); err != nil {
			t.Fatal(err)
		}
	}
	other := compose_v2.NewInMemoryCheckpointer()
	if err := other.SaveNS(f.ctx, old, "", &compose_v2.Checkpoint{GraphID: "independent-partition"}); err != nil {
		t.Fatal(err)
	}
	run(f.ctx)
	if cp, err := f.cp.LoadNS(f.ctx, old, ""); err != nil || cp != nil || history.attempts != 1 {
		t.Fatalf("首次应删原档，历史写入失败，实际 cp=%v err=%v append=%d", cp, err, history.attempts)
	}
	run(compose_v2.WithCheckpointerCtx(f.ctx, other))
	if cp, err := other.LoadNS(f.ctx, old, ""); err != nil || cp == nil || cp.GraphID != "independent-partition" || history.attempts != 1 {
		t.Errorf("重试应保留另一分区的同名档及待补写历史，实际 cp=%v err=%v append=%d", cp, err, history.attempts)
	}
	run(f.ctx)
	run(f.ctx)
	if history.attempts != 2 || abandonIndexOf(f.history(t), func(msg *schema.Message) bool {
		return schema.GetToolResultID(msg) == "c1" && msg.Annotations[agent_v2.HistoryTaskAnnotation] == old
	}) < 0 {
		t.Errorf("原分区重试后应补写一次，实际尝试 %d 次", history.attempts)
	}
}

func TestWithAbandonedTasks_PreservesHistoryWhenRecordedPartitionDiffers(t *testing.T) {
	f := newAbandonFixture(t, nil)
	old := f.pending.RunID
	session := agent_v2.NewSharedChildSession(f.pending)
	session.NextTask()
	other := compose_v2.NewInMemoryCheckpointer()
	router := &routedGraphCheckpointer{Checkpointer: other, useOther: func(context.Context) bool { return false }}
	ctx := compose_v2.WithCheckpointerCtx(f.ctx, router)
	if err := other.SaveNS(ctx, old, "", &compose_v2.Checkpoint{GraphID: "another-partition"}); err != nil {
		t.Fatal(err)
	}
	events, err := f.newAgent().Run(ctx, session, schema.UserMessage("查余额"), agent_v2.WithAbandonedTasks(old))
	if reply := agent_v2.FinalMessage(events); err != nil || reply == nil || reply.Content != "好的" {
		t.Fatalf("新任务应正常结束，实际 reply=%v err=%v", reply, err)
	}
	pending, err := f.cp.ListPending(ctx, old)
	if err != nil || len(pending) != 1 {
		t.Fatalf("分区不符时应保留实际挂起档，实际 pending=%v err=%v", pending, err)
	}
	if cp, err := other.LoadNS(ctx, old, ""); err != nil || cp == nil || cp.GraphID != "another-partition" {
		t.Errorf("分区不符时应保留同名槽，实际 cp=%v err=%v", cp, err)
	}
	if abandonIndexOf(f.history(t), func(msg *schema.Message) bool { return schema.GetToolResultID(msg) == "c1" }) >= 0 {
		t.Error("原分区的档仍在时应保留未应答的历史")
	}
}

type retiredAbandonRoute struct {
	compose_v2.Checkpointer
	oldID   string
	lookups int
}

func (r *retiredAbandonRoute) ResolveCheckpointPartition(ctx context.Context) (compose_v2.Checkpointer, error) {
	if compose_v2.GetSessionID(ctx) == r.oldID {
		r.lookups++
		return nil, errors.New("checkpoint route has been retired")
	}
	return r.Checkpointer, nil
}

func TestWithSettledAbandonedTask_DoesNotResolveRetiredPartition(t *testing.T) {
	f := newAbandonFixture(t, nil)
	old := f.pending.RunID
	if _, err := f.newAgent().Resume(f.ctx, old, agent_v2.WithResumeAction(agent_v2.ResumeActionAbort)); err != nil {
		t.Fatal(err)
	}
	router := &retiredAbandonRoute{Checkpointer: compose_v2.NewInMemoryCheckpointer(), oldID: old}
	ctx := compose_v2.WithCheckpointerCtx(f.ctx, router)
	events, err := f.newAgent().Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("查余额"), agent_v2.WithSettledAbandonedTask(old))
	if reply := agent_v2.FinalMessage(events); err != nil || reply == nil || reply.Content != "好的" || router.lookups != 0 {
		t.Errorf("已收尾任务的路由退役后，新任务应正常完成，实际 reply=%v err=%v 旧分区解析=%d", reply, err, router.lookups)
	}
}
