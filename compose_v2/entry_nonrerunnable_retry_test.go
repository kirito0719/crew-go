package compose_v2_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"crew-go/compose_v2"
)

func nonrerunnableEntryGraph(t *testing.T, ch *compose_v2.Channel[string], decide func(context.Context, string) error, opts ...compose_v2.CondOption) *compose_v2.Graph {
	t.Helper()
	b := compose_v2.NewBuilder().WithName("inspection")
	compose_v2.WithChannel(b, ch)
	b.AddNode("accept", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) { return nil, nil })
	b.AddConditionalEdge(compose_v2.StartNode, func(ctx context.Context, channels compose_v2.Channels) (compose_v2.NodeID, error) {
		value, _ := compose_v2.Get(channels, ch)
		return "accept", decide(ctx, value)
	}, opts...).AddEdge("accept", compose_v2.EndNode)
	return rsCompile(t, b)
}

func checkEntryRetryArchive(t *testing.T, ctx context.Context, cp *compose_v2.InMemoryCheckpointer, sid string, frontier compose_v2.NodeID) {
	t.Helper()
	archives, err := cp.LoadAllInSession(ctx, sid)
	if err != nil || len(archives) != 1 || archives[0].NS != "" {
		t.Fatalf("应只保留父存档，实际 %+v err=%v", archives, err)
	}
	root := archives[0].Checkpoint
	if root == nil {
		t.Fatal("父存档为空")
	}
	if root.Status != compose_v2.CPStatusPending || len(root.CurrentFrontier) != 1 || root.CurrentFrontier[0].NodeID != frontier {
		t.Fatalf("父存档应挂起在 %q，实际 status=%s frontier=%v", frontier, root.Status, root.CurrentFrontier)
	}
	pending, err := cp.ListPending(ctx, sid)
	if err != nil || len(pending) != 1 || pending[0].NS != "" {
		t.Fatalf("待恢复列表应仅包含父图，实际 %v err=%v", pending, err)
	}
}

func TestNonrerunnableSubgraphEntryKeepsCommittedProducer(t *testing.T) {
	rejected := errors.New("需要人工复核")
	for _, tc := range []struct {
		name                         string
		failure                      error
		redo, cancelCaller, terminal bool
		nextGeneration               int
	}{
		{name: "入口否决后重审", failure: rejected, redo: true, nextGeneration: 1},
		{name: "允许重审时入口取消", failure: context.Canceled, redo: true, cancelCaller: true},
		{name: "默认策略入口否决", failure: rejected, terminal: true},
		{name: "默认策略调用方取消", failure: context.Canceled, cancelCaller: true},
		{name: "默认策略调用方取消但判官返回业务错", failure: rejected, cancelCaller: true},
		{name: "默认策略软取消", failure: compose_v2.ErrSoftCanceled},
		{name: "默认策略后端超时且调用方仍活跃", failure: context.DeadlineExceeded, terminal: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, stop := context.WithTimeout(t.Context(), 5*time.Second)
			defer stop()
			initial, cancel := context.WithCancel(ctx)
			defer cancel()
			ref := compose_v2.LastValue[string]("receipt")
			checks := []string{}
			var generations []int
			var edgeOpts []compose_v2.CondOption
			if tc.redo {
				edgeOpts = append(edgeOpts, compose_v2.WithCondFailPolicy(compose_v2.CondFailRerunFrom))
			}
			child := nonrerunnableEntryGraph(t, ref, func(ctx context.Context, value string) error {
				checks = append(checks, value)
				generations = append(generations, compose_v2.RerunGeneration(ctx))
				if len(checks) != 1 {
					return nil
				}
				if tc.cancelCaller {
					cancel()
				}
				return tc.failure
			}, edgeOpts...)
			writes := 0
			b := compose_v2.NewBuilder().WithName("committed-producer")
			compose_v2.WithChannel(b, ref)
			b.AddNode("write", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
				writes++
				return []compose_v2.ChannelWrite{compose_v2.Write(ref, fmt.Sprintf("receipt-%d", writes))}, nil
			}).AddSubgraphNode("inspection", child, compose_v2.WithImport(ref)).
				AddEdge(compose_v2.StartNode, "write").AddEdge("write", "inspection").AddEdge("inspection", compose_v2.EndNode)
			g := rsCompile(t, b)
			cp := compose_v2.NewInMemoryCheckpointer()
			opts := []compose_v2.RunOption{compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID("committed")}
			err := compose_v2.Run(initial, g, compose_v2.NewChannelManager(), opts...)
			if !errors.Is(err, tc.failure) && !(tc.cancelCaller && errors.Is(err, context.Canceled)) {
				t.Fatalf("第一次执行 err=%v，期望 %v", err, tc.failure)
			}
			if tc.terminal {
				archives, loadErr := cp.LoadAllInSession(ctx, "committed")
				if !errors.Is(err, compose_v2.ErrRunAborted) || loadErr != nil || len(archives) != 0 || writes != 1 || !slices.Equal(checks, []string{"receipt-1"}) {
					t.Fatalf("默认策略明确否决应终止并清档，写入=%d 判断=%v 剩余档=%d err=%v load=%v", writes, checks, len(archives), err, loadErr)
				}
				return
			}
			if errors.Is(err, compose_v2.ErrRunAborted) {
				t.Fatalf("可恢复的判断结果被标成终局，err=%v", err)
			}
			checkEntryRetryArchive(t, ctx, cp, "committed", "inspection")
			if err := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), opts...); err != nil {
				t.Fatal(err)
			}
			if writes != 1 || !slices.Equal(checks, []string{"receipt-1", "receipt-1"}) || !slices.Equal(generations, []int{0, tc.nextGeneration}) {
				t.Fatalf("上游写入=%d 子图入口输入=%v 代际=%v", writes, checks, generations)
			}
			if remaining, err := cp.LoadAllInSession(ctx, "committed"); err != nil || len(remaining) != 0 {
				t.Fatalf("成功后剩余存档=%v err=%v", remaining, err)
			}
		})
	}
}

func TestNonrerunnableRecordSurvivesEntryRetryAndCancellation(t *testing.T) {
	for _, stopped := range []bool{false, true} {
		t.Run(map[bool]string{false: "重试耗尽", true: "重试中取消"}[stopped], func(t *testing.T) {
			ctx, stop := context.WithTimeout(t.Context(), 5*time.Second)
			defer stop()
			initial, cancel := context.WithCancel(ctx)
			defer cancel()
			cp := compose_v2.NewInMemoryCheckpointer()
			value := compose_v2.LastValue[string]("inspection-input")
			var inputs []string
			resuming := false
			rejected := errors.New("要求复核")
			child := nonrerunnableEntryGraph(t, value, func(ctx context.Context, s string) error {
				inputs = append(inputs, s)
				if resuming {
					return nil
				}
				if stopped && len(inputs) == 2 {
					cancel()
					return ctx.Err()
				}
				return rejected
			}, compose_v2.WithCondFailPolicy(compose_v2.CondFailRerunFrom))
			var generations []int
			writes, drafts := 0, 0
			b := compose_v2.NewBuilder().WithName("retry-owner")
			b.AddNode("host", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
				_, err := compose_v2.RunWithRetry(ctx, compose_v2.SimpleRetry(2), func(ctx context.Context) (struct{}, error) {
					generations = append(generations, compose_v2.RerunGeneration(ctx))
					receipt, ok := compose_v2.GetStepResult[string](ctx, "external-write")
					if !ok {
						writes++
						receipt = fmt.Sprintf("order-%d", writes)
						if err := compose_v2.SaveStepResult(ctx, "external-write", receipt); err != nil {
							return struct{}{}, err
						}
					}
					draft, err := erDraftOnce(ctx, &drafts, "draft")
					if err != nil {
						return struct{}{}, err
					}
					return struct{}{}, compose_v2.Run(ctx, child, compose_v2.NewChannelManager(),
						compose_v2.WithRunNS("judge"), compose_v2.WithInitialWrites(compose_v2.Write(value, receipt+"/"+draft)))
				})
				return nil, err
			}).AddEdge(compose_v2.StartNode, "host").AddEdge("host", compose_v2.EndNode)
			g := rsCompile(t, b)
			opts := []compose_v2.RunOption{compose_v2.WithSessionID("entry-retry"), compose_v2.WithCheckpointer(cp)}
			err := compose_v2.Run(initial, g, compose_v2.NewChannelManager(), opts...)
			var exhausted *compose_v2.RetriesExhaustedError
			if stopped {
				if !errors.Is(err, context.Canceled) || errors.As(err, &exhausted) {
					t.Fatalf("第二次尝试取消后应立即退出，实际 %v", err)
				}
			} else if !errors.Is(err, rejected) || !errors.As(err, &exhausted) {
				t.Fatalf("应保留重试耗尽与原始否决，实际 %v", err)
			}
			checkEntryRetryArchive(t, ctx, cp, "entry-retry", "host")
			resuming = true
			if err := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), opts...); err != nil {
				t.Fatal(err)
			}
			wantGen := []int{0, 0, 0, 1}
			wantValues := []string{"order-1/draft-v1", "order-1/draft-v1", "order-1/draft-v1", "order-1/draft-v2"}
			wantDrafts := 2
			if stopped {
				wantGen, wantValues, wantDrafts = []int{0, 0, 0}, wantValues[:3], 1
			}
			if writes != 1 || drafts != wantDrafts || !slices.Equal(generations, wantGen) || !slices.Equal(inputs, wantValues) {
				t.Fatalf("普通写入=%d 草稿=%d 尝试代际=%v 入口收到=%v", writes, drafts, generations, inputs)
			}
			if remaining, err := cp.LoadAllInSession(ctx, "entry-retry"); err != nil || len(remaining) != 0 {
				t.Fatalf("成功后剩余存档=%v err=%v", remaining, err)
			}
		})
	}
}

func TestNonrerunnableWriteAtCanceledMemberOutEdge(t *testing.T) {
	ctx, stop := context.WithTimeout(t.Context(), 5*time.Second)
	defer stop()
	initial, cancel := context.WithCancel(ctx)
	defer cancel()
	ref := compose_v2.LastValue[string]("receipt")
	b := compose_v2.NewBuilder().WithName("order")
	compose_v2.WithChannel(b, ref)
	writes := 0
	var checked []string
	b.AddNode("charge", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		receipt, ok := compose_v2.GetStepResult[string](ctx, "charge")
		if !ok {
			writes++
			receipt = fmt.Sprintf("charge-%d", writes)
			if err := compose_v2.SaveStepResult(ctx, "charge", receipt); err != nil {
				return nil, err
			}
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(ref, receipt)}, nil
	}).AddEdge(compose_v2.StartNode, "charge")
	b.AddConditionalEdge("charge", func(ctx context.Context, chs compose_v2.Channels) (compose_v2.NodeID, error) {
		value, _ := compose_v2.Get(chs, ref)
		checked = append(checked, value)
		if len(checked) == 1 {
			cancel()
			return "", ctx.Err()
		}
		return compose_v2.EndNode, nil
	})
	g := rsCompile(t, b)
	cp := compose_v2.NewInMemoryCheckpointer()
	opts := []compose_v2.RunOption{compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID("charge")}
	if err := compose_v2.Run(initial, g, compose_v2.NewChannelManager(), opts...); !errors.Is(err, context.Canceled) || errors.Is(err, compose_v2.ErrRunAborted) {
		t.Fatalf("默认条件边在判断期间被取消，实际 %v", err)
	}
	checkEntryRetryArchive(t, ctx, cp, "charge", "charge")
	restored := compose_v2.NewChannelManager()
	if err := compose_v2.Run(ctx, g, restored, opts...); err != nil {
		t.Fatal(err)
	}
	if final, ok := compose_v2.Get(restored.Channels(), ref); !ok || final != "charge-1" || writes != 1 || !slices.Equal(checked, []string{"charge-1", "charge-1"}) {
		t.Fatalf("最终回执=%q 实际写入=%d 条件边收到=%v", final, writes, checked)
	}
}
