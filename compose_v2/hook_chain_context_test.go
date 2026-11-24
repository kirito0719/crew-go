package compose_v2_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"crew-go/compose_v2"
)

type graphChainStart struct {
	compose_v2.NoOpOrchestrationHooks
	fn func(context.Context) (context.Context, error)
}

func (h graphChainStart) OnGraphStart(ctx context.Context, _ compose_v2.Channels) (context.Context, error) {
	return h.fn(ctx)
}

func TestGraphHookChainRestoresCurrentExecution(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	deadline, _ := ctx.Deadline()
	ctx = compose_v2.WithOwner(compose_v2.WithSessionIDCtx(ctx, "current-task"), "current-owner")
	foreign := compose_v2.WithOwner(compose_v2.WithSessionIDCtx(context.Background(), "other-task"), "other-owner")
	called := false
	graph, err := compose_v2.NewBuilder().
		AddNode("check", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			called = true
			if owner := compose_v2.GetOwner(ctx); owner != "worker-owner" {
				return nil, fmt.Errorf("node owner %q", owner)
			}
			return nil, nil
		}).
		AddEdge(compose_v2.StartNode, "check").
		AddEdge("check", compose_v2.EndNode).Compile()
	if err != nil {
		t.Fatal(err)
	}
	hooks := compose_v2.NewChainOrchHooks(
		graphChainStart{fn: func(context.Context) (context.Context, error) { return foreign, nil }},
		graphChainStart{fn: func(ctx context.Context) (context.Context, error) {
			gotDeadline, _ := ctx.Deadline()
			if owner, sid := compose_v2.GetOwner(ctx), compose_v2.GetSessionID(ctx); owner != "current-owner" || sid != "current-task" || !gotDeadline.Equal(deadline) {
				return ctx, fmt.Errorf("hook got owner %q, sid %q, deadline %v", owner, sid, gotDeadline)
			}
			return compose_v2.WithOwner(ctx, "worker-owner"), nil
		}},
	)
	if err := compose_v2.Run(ctx, graph, compose_v2.NewChannelManager(), compose_v2.WithHooks(hooks)); err != nil || !called {
		t.Fatalf("图应继续执行，实际 called=%v err=%v", called, err)
	}
}
