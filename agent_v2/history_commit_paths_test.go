package agent_v2_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
)

func TestHistoryCommit_AcceptedRerunnableLeafWithOwnHistoryIsVisibleNextRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	hs := agent_v2.NewInMemoryHistoryStore()
	key := agent_v2.HistoryKey{Owner: "owner", ConvID: "independent-child-history"}
	ctx = compose_v2.WithThreadIDCtx(ctx, key.ConvID)
	ctx = compose_v2.WithCheckpointerCtx(ctx, compose_v2.NewInMemoryCheckpointer())
	const accepted = "ACCEPTED-LEAF-PREVIOUS"
	m := &promptRecorder{resp: []*schema.Message{schema.AssistantMessage(accepted), schema.AssistantMessage("next-answer")}}
	leaf := agent_v2.NewBasicAgent(agent_v2.WithName("leaf"), agent_v2.WithModel(m), agent_v2.WithHistory(hs, key.Owner))
	parent := rerunHost(t, func(ctx context.Context, in *schema.Message) (string, error) {
		events, err := agent_v2.RunSubAgent(ctx, "leaf", leaf, in, agent_v2.WithRerunnable())
		return nestedFinal(events), err
	})
	if events, err := parent.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("previous-question")); err != nil || nestedFinal(events) != accepted {
		t.Fatalf("parent must accept the first run: final=%q err=%v", nestedFinal(events), err)
	}
	raw, err := agent_v2.LoadAllHistory(ctx, hs, key)
	if err != nil || !strings.Contains(dumpHist(raw), accepted) {
		t.Fatalf("the accepted response must have been physically appended: raw=%s err=%v", dumpHist(raw), err)
	}
	if events, err := parent.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("next-question")); err != nil || nestedFinal(events) != "next-answer" {
		t.Fatalf("parent must complete the next run: final=%q err=%v", nestedFinal(events), err)
	}
	if view := dumpHist(m.call(1)); !strings.Contains(view, accepted) {
		t.Errorf("the leaf's actual next request lost its previously accepted response: %s", view)
	}
}

func TestHistoryCommit_AcceptedRerunnablePredecessorVisibleToNextCommittedNode(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	hs := agent_v2.NewInMemoryHistoryStore()
	ctx = agent_v2.WithHistoryCtx(compose_v2.WithThreadIDCtx(ctx, "same-run-read"), hs, "owner")
	ctx = compose_v2.WithCheckpointerCtx(ctx, compose_v2.NewInMemoryCheckpointer())
	const accepted = "ACCEPTED-PREDECESSOR"
	firstModel := &promptRecorder{resp: []*schema.Message{schema.AssistantMessage(accepted)}}
	nextModel := &promptRecorder{resp: []*schema.Message{schema.AssistantMessage("finished")}}
	first := agent_v2.NewBasicAgent(agent_v2.WithName("producer"), agent_v2.WithModel(firstModel))
	next := agent_v2.NewBasicAgent(agent_v2.WithName("reader"), agent_v2.WithModel(nextModel))
	out := compose_v2.LastValue[*schema.Message]("result")
	g := compose_v2.NewBuilder().WithName("read-committed-predecessor")
	compose_v2.WithChannel(g, out)
	g.AddNode("produce", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		es, err := agent_v2.RunSubAgent(ctx, "producer", first, schema.UserMessage("make suggestion"), agent_v2.WithRerunnable())
		if err != nil || nestedFinal(es) != accepted {
			t.Errorf("first node must produce the accepted value: got=%v err=%v", nestedFinal(es), err)
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(out, agent_v2.FinalMessage(es))}, err
	})
	g.AddConditionalEdge("produce", func(_ context.Context, ch compose_v2.Channels) (compose_v2.NodeID, error) {
		if got := compose_v2.MustGet(ch, out); got == nil || got.Content != accepted {
			t.Errorf("condition must inspect and accept the exact output: %v", got)
		}
		return "read", nil
	}, compose_v2.WithCondFailPolicy(compose_v2.CondFailRerunFrom))
	g.AddNode("read", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		reader := historyViewAgent{Agent: next, mode: agent_v2.HandoffHistoryRaw}
		es, err := agent_v2.RunSubAgent(ctx, "reader", reader, schema.UserMessage("use shared history"))
		return []compose_v2.ChannelWrite{compose_v2.Write(out, agent_v2.FinalMessage(es))}, err
	})
	g.AddEdge(compose_v2.StartNode, "produce").AddEdge("read", compose_v2.EndNode)
	compiled, err := g.Compile()
	if err != nil {
		t.Fatal(err)
	}
	parent := agent_v2.NewGraphAgent(compiled, agent_v2.WithGraphAgentName("parent"),
		agent_v2.WithGraphInputWriter(func(*schema.Message) []compose_v2.ChannelWrite { return nil }),
		agent_v2.WithGraphOutputReader(func(ch compose_v2.Channels) *schema.Message { return compose_v2.MustGet(ch, out) }))
	if events, err := parent.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("root request")); err != nil || nestedFinal(events) != "finished" {
		t.Fatalf("two-step graph should finish: final=%q err=%v", nestedFinal(events), err)
	}
	if view := dumpHist(nextModel.call(0)); !strings.Contains(view, accepted) {
		t.Errorf("the next node's shared history lost its already accepted predecessor: %s", view)
	}
}

func TestHistoryCommit_ColdResumeKeepsAcceptedNestedOutputSingle(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	hs := agent_v2.NewInMemoryHistoryStore()
	key := agent_v2.HistoryKey{Owner: "owner", ConvID: "cold-accepted-replay"}
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx = agent_v2.WithHistoryCtx(compose_v2.WithThreadIDCtx(ctx, key.ConvID), hs, key.Owner)
	ctx = compose_v2.WithCheckpointerCtx(ctx, cper)
	const accepted = "ALREADY-STORED-RESPONSE"
	m := &promptRecorder{resp: []*schema.Message{schema.AssistantMessage(accepted)}}
	leaf := agent_v2.NewBasicAgent(agent_v2.WithName("leaf"), agent_v2.WithModel(m))
	parent := mkEchoGraph(t, "wait-after-leaf", func(ctx context.Context, in *schema.Message) (*schema.Message, error) {
		es, err := agent_v2.RunSubAgent(ctx, "leaf", leaf, in, agent_v2.WithRerunnable())
		if err != nil {
			return nil, err
		}
		if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "accept the leaf reply?"}); err != nil {
			return nil, err
		}
		return agent_v2.FinalMessage(es), nil
	})
	session := agent_v2.NewRunSession()
	_, err := parent.Run(ctx, session, schema.UserMessage("question"))
	id := partitionQuestionID(t, err)
	count := func() int {
		t.Helper()
		msgs, err := agent_v2.LoadAllHistory(ctx, hs, key)
		if err != nil {
			t.Fatal(err)
		}
		n := 0
		for _, msg := range msgs {
			if msg.Role == schema.RoleAssistant && msg.Content == accepted {
				n++
			}
		}
		return n
	}
	if n := count(); n != 1 {
		t.Fatalf("child response must already be persisted before parent resumes, saw %d copies", n)
	}
	ctx = compose_v2.WithCheckpointerCtx(ctx, reopenDirectChildStore(t, ctx, cper, session.RunID))
	events, err := parent.Resume(ctx, session.RunID, agent_v2.WithResumeUserInputs(map[string]*schema.Message{id: schema.UserMessage("yes")}))
	if err != nil || nestedFinal(events) != accepted || m.call(1) != nil {
		t.Fatalf("cold completion should accept the already produced result once: final=%q err=%v executions=%v", nestedFinal(events), err, m.call(1))
	}
	if n := count(); n != 1 {
		t.Errorf("cold parent replay appended its already persisted child response again, got %d copies", n)
	}
}

func TestHistoryCommit_ColdChildCompletionRequiresBothRerunnableAncestors(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	hs := agent_v2.NewInMemoryHistoryStore()
	const conv = "cold-nested-ancestor"
	ctx = agent_v2.WithHistoryCtx(compose_v2.WithThreadIDCtx(ctx, conv), hs, "owner")
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx = compose_v2.WithCheckpointerCtx(ctx, cper)
	m := &promptRecorder{resp: []*schema.Message{schema.AssistantMessage("BAD-STALE-ANCESTOR"), schema.AssistantMessage("passed"), schema.AssistantMessage("subsequent")}}
	leaf := agent_v2.NewBasicAgent(agent_v2.WithName("model"), agent_v2.WithModel(m))
	child := mkEchoGraph(t, "candidate", func(ctx context.Context, in *schema.Message) (*schema.Message, error) {
		result, err := agent_v2.RunSubAgent(ctx, "model", leaf, in, agent_v2.WithRerunnable())
		if err != nil {
			return nil, err
		}
		if in.Content != "follow-up" {
			if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "candidate ready?"}); err != nil {
				return nil, err
			}
		}
		return agent_v2.FinalMessage(result), nil
	})
	parent := rerunHost(t, func(ctx context.Context, in *schema.Message) (string, error) {
		events, err := agent_v2.RunSubAgent(ctx, "candidate", child, in, agent_v2.WithRerunnable())
		return nestedFinal(events), err
	})
	sess := agent_v2.NewRunSession()
	_, err := parent.Run(ctx, sess, schema.UserMessage("draft"))
	firstID := partitionQuestionID(t, err)
	childNS := directPendingNS(t, ctx, cper, sess.RunID, "candidate#")
	coldReopen := func() {
		cper = reopenDirectChildStore(t, ctx, cper, sess.RunID)
		ctx = compose_v2.WithCheckpointerCtx(ctx, cper)
	}
	coldReopen()
	es, err := child.Resume(compose_v2.WithGraphNS(ctx, childNS), sess.RunID, agent_v2.WithResumeUserInputs(map[string]*schema.Message{firstID: schema.UserMessage("ready")}))
	if err != nil || nestedFinal(es) != "BAD-STALE-ANCESTOR" || m.call(1) != nil {
		t.Fatalf("direct child completion must preserve the first child output, reply=%q err=%v", nestedFinal(es), err)
	}
	coldReopen()
	if es, err = parent.Resume(ctx, sess.RunID); err == nil || compose_v2.IsControlFlowSignal(err) {
		t.Fatalf("outer condition must reject the independent completion, reply=%q err=%v", nestedFinal(es), err)
	}
	coldReopen()
	_, err = parent.Resume(ctx, sess.RunID)
	secondID := partitionQuestionID(t, err)
	if firstID == secondID || strings.Contains(dumpHist(m.call(1)), "BAD-STALE-ANCESTOR") {
		t.Errorf("redo should get a novel question and exclude the rejected descendant: same-question=%t prior=%s", firstID == secondID, dumpHist(m.call(1)))
	}
	es, err = parent.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(map[string]*schema.Message{secondID: schema.UserMessage("ready2")}))
	if err != nil || nestedFinal(es) != "passed" {
		t.Fatalf("new ancestry should complete, final=%q err=%v", nestedFinal(es), err)
	}
	if es, err = parent.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("follow-up")); err != nil || nestedFinal(es) != "subsequent" {
		t.Fatalf("follow-up should succeed: final=%q err=%v", nestedFinal(es), err)
	}
	if view := dumpHist(m.call(2)); !strings.Contains(view, "passed") || strings.Contains(view, "BAD-STALE-ANCESTOR") {
		t.Errorf("only the rerunnable descendant accepted by both ancestors should reach the next task: %s", view)
	}
	raw, err := agent_v2.LoadAllHistory(ctx, hs, agent_v2.HistoryKey{Owner: "owner", ConvID: conv})
	if err != nil || !strings.Contains(dumpHist(raw), "BAD-STALE-ANCESTOR") {
		t.Errorf("raw audit must retain the original declined child result: raw=%s err=%v", dumpHist(raw), err)
	}
}

type rejectFirstHistoryTrial struct {
	agent_v2.Agent
	runs int
}

func (a *rejectFirstHistoryTrial) Run(ctx context.Context, sess *agent_v2.RunSession, input *schema.Message, opts ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	a.runs++
	events, err := a.Agent.Run(ctx, sess, input, opts...)
	if err == nil && a.runs == 1 {
		return events, fmt.Errorf("first child run was invalid before returning a successful result")
	}
	return events, err
}

func TestHistoryCommit_NodeRetryMustNotPromoteSupersededRerunnableRows(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	hs := agent_v2.NewInMemoryHistoryStore()
	const conv = "retry-selected-rerunnable"
	ctx = agent_v2.WithHistoryCtx(compose_v2.WithThreadIDCtx(ctx, conv), hs, "owner")
	const rejected, accepted = "INVALID-PHYSICAL-ATTEMPT", "VALID-PHYSICAL-ATTEMPT"
	m := &promptRecorder{resp: []*schema.Message{schema.AssistantMessage(rejected), schema.AssistantMessage(accepted), schema.AssistantMessage("next")}}
	leaf := &rejectFirstHistoryTrial{Agent: agent_v2.NewBasicAgent(agent_v2.WithName("leaf"), agent_v2.WithModel(m))}
	in := compose_v2.LastValue[*schema.Message]("in")
	out := compose_v2.LastValue[*schema.Message]("out")
	b := compose_v2.NewBuilder().WithName("retry-rejected-child")
	compose_v2.WithChannel(b, in)
	compose_v2.WithChannel(b, out)
	b.AddNode("dispatch", func(ctx context.Context, ch compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		events, err := agent_v2.RunSubAgent(ctx, "leaf", leaf, compose_v2.MustGet(ch, in), agent_v2.WithRerunnable())
		if err != nil {
			return nil, err
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(out, agent_v2.FinalMessage(events))}, nil
	}, compose_v2.WithNodeRetry(compose_v2.SimpleRetry(1)))
	b.AddEdge(compose_v2.StartNode, "dispatch").AddEdge("dispatch", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	parent := agent_v2.NewGraphAgent(g, agent_v2.WithGraphAgentName("parent"),
		agent_v2.WithGraphInputWriter(func(msg *schema.Message) []compose_v2.ChannelWrite {
			return []compose_v2.ChannelWrite{compose_v2.Write(in, msg)}
		}),
		agent_v2.WithGraphOutputReader(func(ch compose_v2.Channels) *schema.Message { return compose_v2.MustGet(ch, out) }))
	if events, err := parent.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("question")); err != nil || nestedFinal(events) != accepted || leaf.runs != 2 {
		t.Fatalf("root should accept the second physical trial only, final=%q err=%v runs=%d", nestedFinal(events), err, leaf.runs)
	}
	if events, err := parent.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("follow-up")); err != nil || nestedFinal(events) != "next" {
		t.Fatalf("follow-up should finish, final=%q err=%v", nestedFinal(events), err)
	}
	if view := dumpHist(m.call(2)); strings.Contains(view, rejected) || !strings.Contains(view, accepted) {
		t.Errorf("the independently failed physical trial must stay off the next task's model view: %s", view)
	}
}

func TestHistoryCommit_ComposeRootCommitsRerunnableLeafHistory(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	const accepted = "ACCEPTED-COMPOSE-LEAF-PREVIOUS"
	ctx = compose_v2.WithThreadIDCtx(ctx, "compose-leaf-history")
	hs := agent_v2.NewInMemoryHistoryStore()
	cp := compose_v2.NewInMemoryCheckpointer()
	m := &promptRecorder{resp: []*schema.Message{schema.AssistantMessage(accepted), schema.AssistantMessage("follow-up")}}
	leaf := agent_v2.NewBasicAgent(agent_v2.WithName("leaf"), agent_v2.WithModel(m), agent_v2.WithHistory(hs, "owner"))
	in := compose_v2.LastValue[*schema.Message]("input")
	out := compose_v2.LastValue[*schema.Message]("output")
	b := compose_v2.NewBuilder().WithName("zz_compose_owner")
	compose_v2.WithChannel(b, in)
	compose_v2.WithChannel(b, out)
	b.AddNode("dispatch", func(ctx context.Context, ch compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		events, err := agent_v2.RunSubAgent(ctx, "leaf", leaf, compose_v2.MustGet(ch, in), agent_v2.WithRerunnable())
		return []compose_v2.ChannelWrite{compose_v2.Write(out, agent_v2.FinalMessage(events))}, err
	})
	b.AddEdge(compose_v2.StartNode, "dispatch").AddEdge("dispatch", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	for i, expected := range []string{accepted, "follow-up"} {
		cm := compose_v2.NewChannelManager()
		err = compose_v2.Run(ctx, g, cm, compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID(fmt.Sprintf("task-%d", i)),
			compose_v2.WithInitialWrites(compose_v2.Write(in, schema.UserMessage(fmt.Sprintf("question %d", i)))))
		if err != nil || compose_v2.MustGet(cm.Channels(), out).Content != expected {
			t.Fatalf("accepted root output %d should be %q, err=%v", i, expected, err)
		}
	}
	if view := dumpHist(m.call(1)); !strings.Contains(view, accepted) {
		t.Errorf("the bare compose root should commit the accepted nested answer before the next task's request: %s", view)
	}
}

func TestHistoryCommit_ColdReplayRebindsHistory(t *testing.T) {
	for _, shape := range []string{"direct_child", "static_subtree", "bound_dynamic_subgraph"} {
		t.Run(shape, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			const accepted = "ACCEPTED-PRIVATE-LEAF"
			key := agent_v2.HistoryKey{Owner: "owner", ConvID: "private-cold"}
			hs := agent_v2.NewInMemoryHistoryStore()
			cp := compose_v2.NewInMemoryCheckpointer()
			ctx = compose_v2.WithCheckpointerCtx(compose_v2.WithThreadIDCtx(ctx, key.ConvID), cp)
			if shape == "bound_dynamic_subgraph" {
				ctx = agent_v2.WithHistoryCtx(ctx, hs, key.Owner)
			}
			m := &promptRecorder{resp: []*schema.Message{schema.AssistantMessage(accepted), schema.AssistantMessage("next")}}
			build := func() *agent_v2.GraphAgent {
				var producer agent_v2.Agent = agent_v2.NewBasicAgent(agent_v2.WithName("leaf"), agent_v2.WithModel(m), agent_v2.WithHistory(hs, key.Owner))
				switch shape {
				case "static_subtree":
					producer = agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("leaf-sequence")).AddStep("leaf", producer)
				case "bound_dynamic_subgraph":
					leaf := producer
					producer = mkEchoGraph(t, "leaf-host", func(ctx context.Context, in *schema.Message) (*schema.Message, error) {
						es, err := agent_v2.RunSubAgent(ctx, "leaf", leaf, in, agent_v2.WithRerunnable())
						return agent_v2.FinalMessage(es), err
					})
				}
				return mkEchoGraph(t, "pause-after-private", func(ctx context.Context, in *schema.Message) (*schema.Message, error) {
					es, err := agent_v2.RunSubAgent(ctx, "producer", producer, in, agent_v2.WithRerunnable())
					if err != nil {
						return nil, err
					}
					if in.Content != "next-question" {
						if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "use accepted response?"}); err != nil {
							return nil, err
						}
					}
					return agent_v2.FinalMessage(es), nil
				})
			}
			sess := agent_v2.NewRunSession()
			_, err := build().Run(ctx, sess, schema.UserMessage("question"))
			id := partitionQuestionID(t, err)
			raw, err := agent_v2.LoadAllHistory(ctx, hs, key)
			if err != nil || !strings.Contains(dumpHist(raw), accepted) {
				t.Fatalf("the leaf's private history must already include the answer: raw=%s err=%v", dumpHist(raw), err)
			}
			ctx = compose_v2.WithCheckpointerCtx(ctx, reopenDirectChildStore(t, ctx, cp, sess.RunID))
			es, err := build().Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(map[string]*schema.Message{id: schema.UserMessage("yes")}))
			if nestedFinal(es) != accepted || err != nil || m.call(1) != nil {
				t.Fatalf("reconstructed root should use the exact replayed answer: final=%q err=%v replay=%v", nestedFinal(es), err, m.call(1))
			}
			es, err = build().Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("next-question"))
			if nestedFinal(es) != "next" || err != nil {
				t.Fatalf("next task must succeed: final=%q err=%v", nestedFinal(es), err)
			}
			if view := dumpHist(m.call(1)); !strings.Contains(view, accepted) {
				t.Errorf("cold replay did not commit the already accepted private history: %s", view)
			}
		})
	}
}

func TestHistoryCommit_NestedRerunnablePredecessorVisibleInsideParentGraph(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	hs := agent_v2.NewInMemoryHistoryStore()
	ctx = agent_v2.WithHistoryCtx(compose_v2.WithThreadIDCtx(ctx, "same-run-read"), hs, "owner")
	ctx = compose_v2.WithCheckpointerCtx(ctx, compose_v2.NewInMemoryCheckpointer())
	const accepted = "ACCEPTED-PREDECESSOR"
	firstModel := &promptRecorder{resp: []*schema.Message{schema.AssistantMessage(accepted)}}
	nextModel := &promptRecorder{resp: []*schema.Message{schema.AssistantMessage("finished")}}
	first := agent_v2.NewBasicAgent(agent_v2.WithName("producer"), agent_v2.WithModel(firstModel))
	next := agent_v2.NewBasicAgent(agent_v2.WithName("reader"), agent_v2.WithModel(nextModel))
	out := compose_v2.LastValue[*schema.Message]("result")
	g := compose_v2.NewBuilder().WithName("read-committed-predecessor")
	compose_v2.WithChannel(g, out)
	g.AddNode("produce", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		es, err := agent_v2.RunSubAgent(ctx, "producer", first, schema.UserMessage("make suggestion"), agent_v2.WithRerunnable())
		if err != nil || nestedFinal(es) != accepted {
			t.Errorf("first node must produce the accepted value: got=%v err=%v", nestedFinal(es), err)
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(out, agent_v2.FinalMessage(es))}, err
	})
	g.AddConditionalEdge("produce", func(_ context.Context, ch compose_v2.Channels) (compose_v2.NodeID, error) {
		if got := compose_v2.MustGet(ch, out); got == nil || got.Content != accepted {
			t.Errorf("condition must inspect and accept the exact output: %v", got)
		}
		return "read", nil
	}, compose_v2.WithCondFailPolicy(compose_v2.CondFailRerunFrom))
	g.AddNode("read", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		reader := historyViewAgent{Agent: next, mode: agent_v2.HandoffHistoryRaw}
		es, err := agent_v2.RunSubAgent(ctx, "reader", reader, schema.UserMessage("use shared history"))
		return []compose_v2.ChannelWrite{compose_v2.Write(out, agent_v2.FinalMessage(es))}, err
	})
	g.AddEdge(compose_v2.StartNode, "produce").AddEdge("read", compose_v2.EndNode)
	compiled, err := g.Compile()
	if err != nil {
		t.Fatal(err)
	}
	parent := agent_v2.NewGraphAgent(compiled, agent_v2.WithGraphAgentName("parent"),
		agent_v2.WithGraphInputWriter(func(*schema.Message) []compose_v2.ChannelWrite { return nil }),
		agent_v2.WithGraphOutputReader(func(ch compose_v2.Channels) *schema.Message { return compose_v2.MustGet(ch, out) }))
	root := mkEchoGraph(t, "host-reader", func(ctx context.Context, in *schema.Message) (*schema.Message, error) {
		events, err := agent_v2.RunSubAgent(ctx, "graph", parent, in, agent_v2.WithRerunnable())
		return agent_v2.FinalMessage(events), err
	})
	if events, err := root.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("root request")); err != nil || nestedFinal(events) != "finished" {
		t.Fatalf("two-step graph should finish: final=%q err=%v", nestedFinal(events), err)
	}
	if view := dumpHist(nextModel.call(0)); !strings.Contains(view, accepted) {
		t.Errorf("the next node's shared history lost its already accepted predecessor: %s", view)
	}
}

type beforeGraphHistoryHook struct {
	hook.NoOpHooks
	child agent_v2.Agent
}

func (h beforeGraphHistoryHook) OnAgentStart(ctx context.Context, _ *hook.AgentInfo, input *hook.AgentInput) (context.Context, error) {
	_, err := agent_v2.RunSubAgent(ctx, "lookup", h.child, input.Messages[len(input.Messages)-1], agent_v2.WithRerunnable())
	return ctx, err
}

func TestHistoryCommit_RerunnableHookBeforeGraphKeepsHistory(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	hs := agent_v2.NewInMemoryHistoryStore()
	ctx = agent_v2.WithHistoryCtx(compose_v2.WithThreadIDCtx(ctx, "rerunnable-hook-history"), hs, "owner")
	const accepted = "HELPER-ACCEPTED-IN-START-HOOK"
	childModel := &promptRecorder{resp: []*schema.Message{schema.AssistantMessage(accepted), schema.AssistantMessage("follow up")}}
	child := agent_v2.NewBasicAgent(agent_v2.WithName("lookup"), agent_v2.WithModel(childModel))
	parentModel := &promptRecorder{resp: []*schema.Message{schema.AssistantMessage("parent completed"), schema.AssistantMessage("parent completed again")}}
	parent := agent_v2.NewBasicAgent(agent_v2.WithName("root"), agent_v2.WithModel(parentModel), agent_v2.WithHooks(beforeGraphHistoryHook{child: child}))
	for i, expected := range []string{"parent completed", "parent completed again"} {
		es, err := parent.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage(fmt.Sprintf("question %d", i)))
		if nestedFinal(es) != expected || err != nil {
			t.Fatalf("parent must accept hook completion: final=%q err=%v", nestedFinal(es), err)
		}
	}
	if view := dumpHist(childModel.call(1)); !strings.Contains(view, accepted) {
		t.Errorf("next invocation lost a successful helper message from the prior hook: %s", view)
	}
}

func TestHistoryCommit_NestedOrdinaryPredecessorVisibleInsideRerunnableParentGraph(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	hs := agent_v2.NewInMemoryHistoryStore()
	ctx = agent_v2.WithHistoryCtx(compose_v2.WithThreadIDCtx(ctx, "same-run-read"), hs, "owner")
	ctx = compose_v2.WithCheckpointerCtx(ctx, compose_v2.NewInMemoryCheckpointer())
	const accepted = "ACCEPTED-PREDECESSOR"
	firstModel := &promptRecorder{resp: []*schema.Message{schema.AssistantMessage(accepted)}}
	nextModel := &promptRecorder{resp: []*schema.Message{schema.AssistantMessage("finished")}}
	first := agent_v2.NewBasicAgent(agent_v2.WithName("producer"), agent_v2.WithModel(firstModel))
	next := agent_v2.NewBasicAgent(agent_v2.WithName("reader"), agent_v2.WithModel(nextModel))
	out := compose_v2.LastValue[*schema.Message]("result")
	g := compose_v2.NewBuilder().WithName("read-committed-predecessor")
	compose_v2.WithChannel(g, out)
	g.AddNode("produce", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		es, err := agent_v2.RunSubAgent(ctx, "producer", first, schema.UserMessage("make suggestion"))
		if err != nil || nestedFinal(es) != accepted {
			t.Errorf("first node must produce the accepted value: got=%v err=%v", nestedFinal(es), err)
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(out, agent_v2.FinalMessage(es))}, err
	})
	g.AddConditionalEdge("produce", func(_ context.Context, ch compose_v2.Channels) (compose_v2.NodeID, error) {
		if got := compose_v2.MustGet(ch, out); got == nil || got.Content != accepted {
			t.Errorf("condition must inspect and accept the exact output: %v", got)
		}
		return "read", nil
	}, compose_v2.WithCondFailPolicy(compose_v2.CondFailRerunFrom))
	g.AddNode("read", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		reader := historyViewAgent{Agent: next, mode: agent_v2.HandoffHistoryRaw}
		es, err := agent_v2.RunSubAgent(ctx, "reader", reader, schema.UserMessage("use shared history"))
		return []compose_v2.ChannelWrite{compose_v2.Write(out, agent_v2.FinalMessage(es))}, err
	})
	g.AddEdge(compose_v2.StartNode, "produce").AddEdge("read", compose_v2.EndNode)
	compiled, err := g.Compile()
	if err != nil {
		t.Fatal(err)
	}
	parent := agent_v2.NewGraphAgent(compiled, agent_v2.WithGraphAgentName("parent"),
		agent_v2.WithGraphInputWriter(func(*schema.Message) []compose_v2.ChannelWrite { return nil }),
		agent_v2.WithGraphOutputReader(func(ch compose_v2.Channels) *schema.Message { return compose_v2.MustGet(ch, out) }))
	root := mkEchoGraph(t, "host-reader", func(ctx context.Context, in *schema.Message) (*schema.Message, error) {
		events, err := agent_v2.RunSubAgent(ctx, "graph", parent, in, agent_v2.WithRerunnable())
		return agent_v2.FinalMessage(events), err
	})
	if events, err := root.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("root request")); err != nil || nestedFinal(events) != "finished" {
		t.Fatalf("two-step graph should finish: final=%q err=%v", nestedFinal(events), err)
	}
	if view := dumpHist(nextModel.call(0)); !strings.Contains(view, accepted) {
		t.Errorf("the next node's shared history lost its already accepted predecessor: %s", view)
	}
}

func TestHistoryCommit_InsideRerunnableAncestorExcludesRejectedDescendant(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	hs := agent_v2.NewInMemoryHistoryStore()
	ctx = agent_v2.WithHistoryCtx(compose_v2.WithThreadIDCtx(ctx, "same-run-read"), hs, "owner")
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx = compose_v2.WithCheckpointerCtx(ctx, cper)
	const accepted = "ACCEPTED-PREDECESSOR"
	const rejected = "BAD-LOCAL-PREDECESSOR"
	firstModel := &promptRecorder{resp: []*schema.Message{schema.AssistantMessage(rejected), schema.AssistantMessage(accepted)}}
	nextModel := &promptRecorder{resp: []*schema.Message{schema.AssistantMessage("finished")}}
	first := agent_v2.NewBasicAgent(agent_v2.WithName("producer"), agent_v2.WithModel(firstModel))
	next := agent_v2.NewBasicAgent(agent_v2.WithName("reader"), agent_v2.WithModel(nextModel))
	out := compose_v2.LastValue[*schema.Message]("result")
	g := compose_v2.NewBuilder().WithName("read-committed-predecessor")
	compose_v2.WithChannel(g, out)
	g.AddNode("produce", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		es, err := agent_v2.RunSubAgent(ctx, "producer", first, schema.UserMessage("make suggestion"), agent_v2.WithRerunnable())
		if err != nil {
			t.Errorf("model must produce a candidate, got error %v", err)
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(out, agent_v2.FinalMessage(es))}, err
	})
	g.AddConditionalEdge("produce", func(_ context.Context, ch compose_v2.Channels) (compose_v2.NodeID, error) {
		if got := compose_v2.MustGet(ch, out); got == nil || got.Content != accepted {
			return "", fmt.Errorf("condition rejected the prior candidate: %v", got)
		}
		return "read", nil
	}, compose_v2.WithCondFailPolicy(compose_v2.CondFailRerunFrom))
	g.AddNode("read", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		reader := historyViewAgent{Agent: next, mode: agent_v2.HandoffHistoryRaw}
		es, err := agent_v2.RunSubAgent(ctx, "reader", reader, schema.UserMessage("use shared history"))
		return []compose_v2.ChannelWrite{compose_v2.Write(out, agent_v2.FinalMessage(es))}, err
	})
	g.AddEdge(compose_v2.StartNode, "produce").AddEdge("read", compose_v2.EndNode)
	compiled, err := g.Compile()
	if err != nil {
		t.Fatal(err)
	}
	parent := agent_v2.NewGraphAgent(compiled, agent_v2.WithGraphAgentName("parent"),
		agent_v2.WithGraphInputWriter(func(*schema.Message) []compose_v2.ChannelWrite { return nil }),
		agent_v2.WithGraphOutputReader(func(ch compose_v2.Channels) *schema.Message {
			m, _ := compose_v2.Get(ch, out)
			return m
		}))
	root := mkEchoGraph(t, "host-reader", func(ctx context.Context, in *schema.Message) (*schema.Message, error) {
		events, err := agent_v2.RunSubAgent(ctx, "graph", parent, in, agent_v2.WithRerunnable())
		return agent_v2.FinalMessage(events), err
	})
	session := agent_v2.NewRunSession()
	if es, err := root.Run(ctx, session, schema.UserMessage("root request")); err == nil || !strings.Contains(err.Error(), "condition rejected the prior candidate") || nextModel.call(0) != nil {
		t.Fatalf("first inner attempt must be rejected before downstream model: final=%q err=%v", nestedFinal(es), err)
	}
	ctx = compose_v2.WithCheckpointerCtx(ctx, reopenDirectChildStore(t, ctx, cper, session.RunID))
	if events, err := root.Resume(ctx, session.RunID); err != nil || nestedFinal(events) != "finished" {
		t.Fatalf("two-step graph should finish: final=%q err=%v", nestedFinal(events), err)
	}
	if view := dumpHist(nextModel.call(0)); !strings.Contains(view, accepted) || strings.Contains(view, rejected) {
		t.Errorf("the next node should see the accepted local attempt only: %s", view)
	}
}

func TestHistoryCommit_NestedHandoffReadsAcceptedHopInsideRerunnableAncestor(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	hs := agent_v2.NewInMemoryHistoryStore()
	ctx = agent_v2.WithHistoryCtx(compose_v2.WithThreadIDCtx(ctx, "nested-handoff-history"), hs, "owner")
	const accepted = "FRONTDESK-ACCEPTED-BEFORE-RERUNNABLE-ANCESTOR"
	transfer := transferToolCall("transfer", "billing")
	transfer.Content = accepted
	from := agent_v2.NewBasicAgent(agent_v2.WithName("frontdesk"), agent_v2.WithModel(newStubModel([]*schema.Message{transfer})),
		agent_v2.WithHandoffTransfer(), agent_v2.WithMaxRounds(5))
	toModel := &captureModel{resp: schema.AssistantMessage("billing done")}
	to := agent_v2.NewBasicAgent(agent_v2.WithName("billing"), agent_v2.WithModel(toModel))
	handoff := agent_v2.NewHandoffAgent(agent_v2.WithHandoffName("support"), agent_v2.WithHandoffInitial("frontdesk")).
		AddAgent("frontdesk", from).AddAgent("billing", to)
	root := mkEchoGraph(t, "host-handoff", func(ctx context.Context, in *schema.Message) (*schema.Message, error) {
		es, err := agent_v2.RunSubAgent(ctx, "handoff", handoff, in, agent_v2.WithRerunnable())
		return agent_v2.FinalMessage(es), err
	})
	es, err := root.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("找账单"))
	if nestedFinal(es) != "billing done" || err != nil {
		t.Fatalf("must reach the billing agent and complete: final=%q err=%v", nestedFinal(es), err)
	}
	if !toModel.sawContains(accepted) {
		t.Errorf("billing's actual preamble lost history from the preceding committed handoff: %v", toModel.seen)
	}
}
