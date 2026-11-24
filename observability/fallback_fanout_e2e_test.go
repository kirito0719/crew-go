package observability_test

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/fallback"
	"crew-go/model"
	"crew-go/observability"
	"crew-go/schema"
)

func TestFallbackSwitchEmitsNodeSpanAndCounter(t *testing.T) {
	h := newHarness(t, observability.WithContentRecording(true))
	primary := &scriptModel{fail: &model.APIError{StatusCode: 503}}
	backup := &scriptModel{}
	m := fallback.New([]model.ChatModel{primary, backup}, fallback.WithPerModelRetry(fallback.PerModelRetry{}), fallback.WithBreaker(1, time.Minute))
	ctx, opts := h.obs.Install(t.Context(), agent_v2.WithName("router"), agent_v2.WithModel(m))
	ag := agent_v2.NewBasicAgent(opts...)
	for _, input := range []string{"第一次", "第二次"} {
		events, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage(input))
		if err != nil || agent_v2.FinalMessage(events).GetContent() != "最终答复："+input {
			t.Fatalf("备用模型应交付本轮答复，err=%v events=%v", err, events)
		}
	}
	if primary.calls.Load() != 1 || backup.calls.Load() != 2 {
		t.Fatalf("跳闸后应直接走备用，主=%d 备=%d", primary.calls.Load(), backup.calls.Load())
	}
	modelSpans := h.spansNamed("node model")
	if len(modelSpans) != 2 {
		t.Fatalf("两次请求应有两个模型节点 span，实际 %v", names(h.spans()))
	}
	var switches, trips int
	for _, span := range modelSpans {
		for _, ev := range span.Events() {
			switch ev.Name {
			case "crew." + fallback.FailoverEventType:
				switches++
				hasPath := slices.ContainsFunc(ev.Attributes, func(kv attribute.KeyValue) bool {
					return kv.Key == "content" && kv.Value.AsString() == "0->1"
				})
				if !hasPath {
					t.Errorf("切换事件缺候选路径，实际 %v", ev.Attributes)
				}
			case "crew." + fallback.MemberTrippedEventType:
				trips++
			}
		}
	}
	rm := h.collect(t)
	gotSwitches := sumInt(t, rm, observability.MetricFailovers, map[string]string{
		string(observability.AttrFailoverReason): "server_error", string(observability.AttrFailoverFrom): "0"})
	gotTrips := sumInt(t, rm, observability.MetricMemberTrips, map[string]string{
		string(observability.AttrTrippedReason): "server_error", string(observability.AttrTrippedMember): "0"})
	if switches != 1 || trips != 1 || gotSwitches != 1 || gotTrips != 1 ||
		sumInt(t, rm, observability.MetricFailovers, nil) != 1 || sumInt(t, rm, observability.MetricMemberTrips, nil) != 1 {
		t.Fatalf("span 上切换=%d 跳闸=%d，计数器切换=%d 跳闸=%d", switches, trips, gotSwitches, gotTrips)
	}
	h.assertAllEnded(t)
}

func TestFanoutInstanceIDsMatchBothStackedProviders(t *testing.T) {
	a, b := newHarness(t), newHarness(t)
	ids := compose_v2.Append[string]("instance_ids")
	build := compose_v2.NewBuilder()
	compose_v2.WithChannel(build, ids)
	build.AddNode("spread", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		compose_v2.SendFrom(ctx, compose_v2.SendDirective{Target: "fan"}, compose_v2.SendDirective{Target: "fan"})
		return nil, nil
	}).AddNode("fan", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return []compose_v2.ChannelWrite{compose_v2.Write(ids, []string{compose_v2.GetCurrentInstanceID(ctx)})}, nil
	}).AddEdge(compose_v2.StartNode, "spread").AddEdge("fan", compose_v2.EndNode)
	g, err := build.Compile()
	if err != nil {
		t.Fatal(err)
	}
	ag := agent_v2.NewGraphAgent(g, agent_v2.WithGraphAgentName("fanout"),
		agent_v2.WithGraphAgentHooks(a.obs.AgentHooks(a.obs.AgentHooks(b.obs.AgentHooks()))),
		agent_v2.WithGraphInputWriter(func(*schema.Message) []compose_v2.ChannelWrite { return nil }),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			ran, _ := compose_v2.Get(chs, ids)
			return schema.AssistantMessage(strings.Join(ran, ","))
		}))
	ctx := agent_v2.WithCtxOrchHooks(t.Context(), a.obs.OrchHooks(a.obs.OrchHooks(b.obs.OrchHooks())))
	events, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("并行处理"))
	if err != nil {
		t.Fatal(err)
	}
	ran := strings.Split(agent_v2.FinalMessage(events).GetContent(), ",")
	slices.Sort(ran)
	if len(ran) != 2 || ran[0] == "" || ran[0] == ran[1] {
		t.Fatalf("引擎应执行两个不同实例，实际 %v", ran)
	}
	for _, h := range []*harness{a, b} {
		var observed []string
		for _, span := range h.spansNamed("node fan") {
			observed = append(observed, strAttr(t, span, observability.AttrNodeInstance))
		}
		slices.Sort(observed)
		if !slices.Equal(observed, ran) {
			t.Fatalf("当前 provider 每个实例应恰好一个 span，执行=%v span=%v", ran, observed)
		}
		h.assertAllEnded(t)
	}
}
