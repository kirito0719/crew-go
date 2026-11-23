package compose_v2_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/compose_v2"
	"crew-go/schema"
)

type restoreClaimStore struct {
	*compose_v2.InMemoryCheckpointer
	reject       bool
	claimedEpoch string
}

func (s *restoreClaimStore) ClaimResume(ctx context.Context, sid, ns string, expected, source *compose_v2.Checkpoint) (*compose_v2.Checkpoint, error) {
	if s.reject {
		return nil, errors.New("claim rejected")
	}
	claimed, err := s.InMemoryCheckpointer.ClaimResume(ctx, sid, ns, expected, source)
	if err == nil {
		s.claimedEpoch = claimed.Epoch
	}
	return claimed, err
}

func TestRestoreSourceOnlyReportsAdmittedSnapshot(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	store := &restoreClaimStore{InMemoryCheckpointer: compose_v2.NewInMemoryCheckpointer()}
	calls := &graphConflictCalls{question: "可以继续吗？"}
	g := buildGraphConflictFixture(t, calls, []compose_v2.NodeID{"safe"}, "safe", func(b *compose_v2.GraphBuilder) { b.WithName("restore-source") })
	const sid = "admission"
	var sources []*compose_v2.Checkpoint
	run := func(graph *compose_v2.Graph, opts ...compose_v2.RunOption) error {
		base := []compose_v2.RunOption{compose_v2.WithCheckpointer(store), compose_v2.WithSessionID(sid),
			compose_v2.WithRestoreSource(func(source *compose_v2.Checkpoint) {
				if store.claimedEpoch == "" || calls.ask != 1 || calls.safe != 0 {
					t.Errorf("回调时领取代号=%q 提问执行=%d 后续执行=%d", store.claimedEpoch, calls.ask, calls.safe)
				}
				sources = append(sources, source)
			})}
		return compose_v2.Run(ctx, graph, compose_v2.NewChannelManager(), append(base, opts...)...)
	}
	id, answer := graphConflictAnswer(t, run(g), calls.question)
	original, _ := graphConflictCheckpoint(t, ctx, store, sid, "")
	if original == nil || original.Epoch == "" || len(sources) != 0 {
		t.Fatalf("首跑挂起应产生原档且没有任何恢复通知，source=%v", sources)
	}
	if err := run(g.WithCheckpointRevision("different"), answer); !errors.Is(err, compose_v2.ErrGraphIDConflict) || len(sources) != 0 {
		t.Fatalf("拒绝图版本时 err=%v 回调次数=%d", err, len(sources))
	}
	store.reject = true
	if err := run(g, answer); !errors.Is(err, compose_v2.ErrCheckpointNotResumable) || len(sources) != 0 {
		t.Fatalf("领取失败时 err=%v 回调次数=%d", err, len(sources))
	}
	store.reject = false
	if err := run(g, answer); err != nil {
		t.Fatal(err)
	}
	if len(sources) != 1 || sources[0] == nil || sources[0].Epoch != original.Epoch || sources[0].Epoch == store.claimedEpoch ||
		len(sources[0].PendingInterrupts) != 1 || sources[0].PendingInterrupts[0].InterruptID != id || calls.safe != 1 || calls.answer != "approve-original" {
		t.Fatalf("实际恢复来源=%v 领取代号=%q 后续节点=%d 答案=%q", sources, store.claimedEpoch, calls.safe, calls.answer)
	}
}

func TestRestoreSourceReportsExplicitChannelRestoreWithoutFrontier(t *testing.T) {
	ch := compose_v2.LastValue[string]("output")
	b := compose_v2.NewBuilder().WithName("restore-completed-output")
	compose_v2.WithChannel(b, ch)
	runs := 0
	b.AddNode("write", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		runs++
		return []compose_v2.ChannelWrite{compose_v2.Write(ch, "原结果")}, nil
	}).AddEdge(compose_v2.StartNode, "write").AddEdge("write", compose_v2.EndNode)
	g := rsCompile(t, b)
	cm := compose_v2.NewChannelManager()
	if err := compose_v2.Run(t.Context(), g, cm); err != nil {
		t.Fatal(err)
	}
	saved, err := cm.SnapshotForGraph(g)
	if err != nil || saved == nil || len(saved.CurrentFrontier) != 0 {
		t.Fatalf("完成后应能导出空 frontier 的快照，err=%v", err)
	}
	restored := compose_v2.NewChannelManager()
	var sources []*compose_v2.Checkpoint
	err = compose_v2.Run(t.Context(), g, restored, compose_v2.WithResumeFrom(saved),
		compose_v2.WithRestoreSource(func(cp *compose_v2.Checkpoint) {
			sources = append(sources, cp)
			panic("观察者失败")
		}))
	got, ok := compose_v2.Get(restored.Channels(), ch)
	if err != nil || runs != 1 || !ok || got != "原结果" || len(sources) != 1 || sources[0] == nil ||
		string(sources[0].Channels["output"].Data) != `"原结果"` {
		t.Fatalf("导入完成快照后的执行次数=%d 结果=%q 来源=%v err=%v", runs, got, sources, err)
	}
}

func TestRestoreSourceIsLocalToConcurrentNestedRuns(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	store := compose_v2.NewInMemoryCheckpointer()
	childBuilder := compose_v2.NewBuilder().WithName("child")
	childBuilder.AddNode("ask", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: compose_v2.GetGraphNS(ctx)})
		return nil, err
	}).AddEdge(compose_v2.StartNode, "ask").AddEdge("ask", compose_v2.EndNode)
	child := rsCompile(t, childBuilder)
	parent := compose_v2.NewBuilder().WithName("parent")
	parent.AddSubgraphNode("automatic", child).AddEdge(compose_v2.StartNode, "automatic").AddEdge("automatic", compose_v2.EndNode)

	type observed struct{ scope, ns, epoch string }
	reports := make(chan observed, 8)
	both := make(chan struct{})
	var ready atomic.Int32
	for _, name := range []string{"left", "right"} {
		parent.AddNode(compose_v2.NodeID(name), func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return nil, compose_v2.Run(ctx, child, compose_v2.NewChannelManager(), compose_v2.WithRunNS(name),
				compose_v2.WithRestoreSource(func(cp *compose_v2.Checkpoint) {
					reports <- observed{name, cp.NS, cp.Epoch}
					if ready.Add(1) == 2 {
						close(both)
					}
					select {
					case <-both:
					case <-ctx.Done():
					}
				}))
		}).AddEdge(compose_v2.StartNode, compose_v2.NodeID(name)).AddEdge(compose_v2.NodeID(name), compose_v2.EndNode)
	}
	g := rsCompile(t, parent)
	const sid = "nested"
	opts := []compose_v2.RunOption{compose_v2.WithSessionID(sid), compose_v2.WithCheckpointer(store),
		compose_v2.WithRestoreSource(func(cp *compose_v2.Checkpoint) { reports <- observed{"", cp.NS, cp.Epoch} })}
	run := func(extra ...compose_v2.RunOption) error {
		return compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), append(opts, extra...)...)
	}
	interrupted, ok := compose_v2.IsInterrupt(run())
	if !ok || len(interrupted.AllQueries()) != 3 || len(reports) != 0 {
		t.Fatalf("父图和子图的首次运行，挂起=%v 回调=%d", interrupted, len(reports))
	}
	answers := make(map[string]*schema.Message)
	for _, q := range interrupted.AllQueries() {
		answers[q.InterruptID] = schema.UserMessage(q.Question)
	}
	expected := make(map[string]string)
	for _, ns := range []string{"", "left", "right"} {
		saved, _ := graphConflictCheckpoint(t, ctx, store, sid, ns)
		if saved == nil || saved.Epoch == "" {
			t.Fatalf("缺少 %q 原档", ns)
		}
		expected[ns] = saved.Epoch
	}
	if err := run(compose_v2.WithResumeAnswers(answers)); err != nil {
		t.Fatal(err)
	}
	close(reports)
	for report := range reports {
		if report.ns != report.scope || expected[report.scope] != report.epoch {
			t.Fatalf("观察者 %q 收到 %q 的存档 %q，剩余期望 %v", report.scope, report.ns, report.epoch, expected)
		}
		delete(expected, report.scope)
	}
	if ready.Load() != 2 || len(expected) != 0 {
		t.Fatalf("并发进入回调的子图=%d，未收到来源 %v", ready.Load(), expected)
	}
}
