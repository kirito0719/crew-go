package agent_v2_test

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
)

type chainHookValue struct{}

type chainAgentStart struct {
	hook.NoOpHooks
	fn func(context.Context) (context.Context, error)
}

func (h chainAgentStart) OnAgentStart(ctx context.Context, _ *hook.AgentInfo, _ *hook.AgentInput) (context.Context, error) {
	return h.fn(ctx)
}

func TestAgentHookChainScopesEachCallback(t *testing.T) {
	m := &ownerSpyModel{}
	first := &staleAgentStart{stale: true}
	var observed []string
	after := chainAgentStart{fn: func(ctx context.Context) (context.Context, error) {
		owner := compose_v2.GetOwner(ctx)
		observed = append(observed, owner)
		if ctx.Value(chainHookValue{}) != "current" {
			return ctx, fmt.Errorf("missing current request value")
		}
		if _, ok := ctx.Deadline(); !ok {
			return ctx, fmt.Errorf("missing current deadline")
		}
		return compose_v2.WithOwner(ctx, owner+"-worker"), nil
	}}
	agent := agent_v2.NewBasicAgent(agent_v2.WithName("account"), agent_v2.WithModel(m),
		agent_v2.WithHooks(hook.NewChainHooks(first, hook.NewChainHooks(after))))
	for _, owner := range []string{"alice", "bob"} {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		ctx = compose_v2.WithOwner(context.WithValue(ctx, chainHookValue{}, "current"), owner)
		_, err := agent.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("query"))
		cancel()
		if err != nil {
			t.Fatal(err)
		}
	}
	if !slices.Equal(observed, []string{"alice", "bob"}) ||
		!slices.Equal(m.owners, []string{"alice-worker", "bob-worker"}) {
		t.Fatalf("回调应看到本请求身份，模型沿用合法的派生身份。回调 %v，模型 %v", observed, m.owners)
	}
}
