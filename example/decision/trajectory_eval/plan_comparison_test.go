package main

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

type planningLedger struct {
	tools  []string
	models int
	in     int
	out    int
}

// taskPlanner 根据相同的工具证据选择下一步，校验真实 NewLLMPlanner 提供的步骤回执。
type taskPlanner struct {
	ledger *planningLedger
	tools  []*schema.ToolInfo
}

func (p taskPlanner) WithTools(tools []*schema.ToolInfo) (model.ChatModel, error) {
	p.tools = tools
	return p, nil
}

func (p taskPlanner) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}

func (p taskPlanner) Generate(_ context.Context, messages []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	input := messages[len(messages)-1].Content
	name, step := "plan", "primary"
	var result string
	switch {
	case strings.Contains(input, "→ status=ready"):
		name, result = "respond", "status=ready"
	case strings.Contains(input, "→ status=missing"):
		name, result = "respond", "status=missing"
	case strings.Contains(input, "→ status=try-secondary"):
		step = "secondary"
	case !strings.Contains(input, "目标：check inventory"):
		return nil, fmt.Errorf("unexpected planning input: %s", input)
	}
	if !slices.ContainsFunc(p.tools, func(info *schema.ToolInfo) bool { return info.Name == name }) {
		return nil, fmt.Errorf("planner tool %q is unavailable", name)
	}
	var args any
	if name == "plan" {
		refs := []int{}
		if step == "secondary" {
			refs = []int{0}
		}
		args = map[string]any{"steps": []map[string]any{{"description": step, "depends_on": refs}}}
	} else {
		args = map[string]string{"answer": result}
	}
	data, err := json.Marshal(args)
	if err != nil {
		return nil, err
	}
	reply := schema.AssistantToolCallMessage([]schema.ToolCall{
		{ID: name, Function: schema.FunctionCall{Name: name, Arguments: string(data)}},
	})
	p.ledger.models++
	p.ledger.in += compose_v2.EstimateMessageTokens(messages)
	p.ledger.out += compose_v2.EstimateMessageTokens([]*schema.Message{reply})
	return reply, nil
}

func TestPlanAndFixedGraphCompareSameTask(t *testing.T) {
	for _, tc := range []struct {
		name, primary, secondary, final string
		steps                           []string
	}{
		{"primary_hit", "status=ready", "status=missing", "status=ready", []string{"primary"}},
		{"secondary_hit", "status=try-secondary", "status=ready", "status=ready", []string{"primary", "secondary"}},
		{"no_inventory", "status=try-secondary", "status=missing", "status=missing", []string{"primary", "secondary"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, strategy := range []string{"fixed", "plan"} {
				t.Run(strategy, func(t *testing.T) {
					metrics := &planningLedger{}
					lookup := tool.NewFuncTool("inventory", "查库存状态", nil,
						func(_ context.Context, args map[string]any) (*tool.Result, error) {
							source, _ := args["source"].(string)
							metrics.tools = append(metrics.tools, source)
							switch source {
							case "primary":
								return tool.Success(tc.primary), nil
							case "secondary":
								return tool.Success(tc.secondary), nil
							default:
								return nil, fmt.Errorf("unknown inventory source %q", source)
							}
						})
					agent := inventoryComparisonAgent(t, strategy, metrics, lookup)
					ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
					defer cancel()
					start := time.Now()
					events, err := agent.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("check inventory"))
					elapsed := time.Since(start)
					final := agent_v2.FinalMessage(events)
					if err != nil || final == nil || final.Content != tc.final || !slices.Equal(metrics.tools, tc.steps) {
						t.Fatalf("最终产出 %v，工具 %v，错误 %v", final, metrics.tools, err)
					}
					wantModels := 0
					if strategy == "plan" {
						wantModels = len(tc.steps) + 1
					}
					if metrics.models != wantModels || (wantModels != 0 && (metrics.in == 0 || metrics.out == 0)) {
						t.Fatalf("调用次数和估算 token 不符，实际 %+v", metrics)
					}
					t.Logf("correct=1 tools=%d model_calls=%d estimated_input_tokens=%d estimated_output_tokens=%d elapsed_us=%d",
						len(metrics.tools), metrics.models, metrics.in, metrics.out, elapsed.Microseconds())
				})
			}
		})
	}
}

func inventoryComparisonAgent(t *testing.T, strategy string, metrics *planningLedger, lookup tool.Tool) agent_v2.Agent {
	t.Helper()
	input := compose_v2.LastValue[string]("source")
	output := compose_v2.LastValue[*schema.Message]("inventory")
	g := compose_v2.NewBuilder().WithName("inventory_" + strategy)
	compose_v2.WithChannel(g, input)
	compose_v2.WithChannel(g, output)
	lookupNode := func(source func(compose_v2.Channels) string) compose_v2.NodeFunc {
		return func(ctx context.Context, ch compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			result, err := lookup.Call(ctx, map[string]any{"source": source(ch)})
			if err != nil {
				return nil, err
			}
			text, ok := result.Output.(string)
			if !result.Success || !ok {
				return nil, fmt.Errorf("inventory lookup produced no status")
			}
			return []compose_v2.ChannelWrite{compose_v2.Write(output, schema.AssistantMessage(text))}, nil
		}
	}
	if strategy == "fixed" {
		g.AddNode("primary", lookupNode(func(compose_v2.Channels) string { return "primary" }))
		g.AddNode("secondary", lookupNode(func(compose_v2.Channels) string { return "secondary" }))
		g.AddEdge(compose_v2.StartNode, "primary")
		g.AddConditionalEdge("primary", func(_ context.Context, ch compose_v2.Channels) (compose_v2.NodeID, error) {
			if compose_v2.MustGet(ch, output).Content == "status=try-secondary" {
				return "secondary", nil
			}
			return compose_v2.EndNode, nil
		})
		g.AddEdge("secondary", compose_v2.EndNode)
	} else {
		g.AddNode("read", lookupNode(func(ch compose_v2.Channels) string { return compose_v2.MustGet(ch, input) }))
		g.AddEdge(compose_v2.StartNode, "read").AddEdge("read", compose_v2.EndNode)
	}
	graph, err := g.Compile()
	if err != nil {
		t.Fatal(err)
	}
	worker := agent_v2.NewGraphAgent(graph, agent_v2.WithGraphAgentName("inventory_"+strategy),
		agent_v2.WithGraphInputWriter(func(m *schema.Message) []compose_v2.ChannelWrite {
			return []compose_v2.ChannelWrite{compose_v2.Write(input, m.Content)}
		}),
		agent_v2.WithGraphOutputReader(func(ch compose_v2.Channels) *schema.Message {
			return compose_v2.MustGet(ch, output)
		}))
	if strategy == "fixed" {
		return worker
	}
	return agent_v2.NewPlanAgent(agent_v2.NewLLMPlanner(taskPlanner{ledger: metrics}),
		agent_v2.WithExecInputBuilder(func(_ context.Context, _ string, done []agent_v2.ExecutedStep, step agent_v2.PlanStep, refs []int) (*schema.Message, error) {
			if step.Description == "secondary" && (len(done) != 1 || done[0].Result != "status=try-secondary" || !slices.Equal(refs, []int{0})) {
				return nil, fmt.Errorf("secondary step did not receive the primary evidence")
			}
			return schema.UserMessage(step.Description), nil
		})).SetExecutor(worker)
}
