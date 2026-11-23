package agent_v2_test

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

// 相同 ID 可能来自两个生产者，也可能随输入或子图结果透传。分别观测各节点实际收到的输入和历史归属。
func TestHistoryOutputOwnership(t *testing.T) {
	const firstText = "固定回复"
	const editedText = "重新组织的回复"
	shared := schema.AssistantMessage(firstText)
	shared.Annotations = map[string]any{"route_target": "legal"}
	newReply := func(*schema.Message) *schema.Message { return schema.AssistantMessage(firstText) }
	sharedReply := func(*schema.Message) *schema.Message { return shared }
	sameID := func(*schema.Message) *schema.Message {
		m := schema.AssistantMessage(firstText)
		m.ID = shared.ID
		return m
	}
	returnInput := func(in *schema.Message) *schema.Message { return in }
	copyInput := func(in *schema.Message) *schema.Message {
		m := *in
		return &m
	}
	changeRoute := func(in *schema.Message) *schema.Message {
		m := *in
		m.Annotations = maps.Clone(in.Annotations)
		m.Annotations["route_target"] = "finance"
		return &m
	}
	editInput := func(in *schema.Message) *schema.Message {
		m := schema.AssistantMessage(editedText)
		m.ID = in.ID
		return m
	}
	both := []string{"s1/" + firstText, "s2/" + firstText}
	for _, tc := range []struct {
		name          string
		forwardInput  bool
		runDescendant bool
		reply         func(*schema.Message) *schema.Message
		want          []string
	}{
		{"独立回复新ID", false, false, newReply, both},
		{"独立回复复用对象", false, false, sharedReply, both},
		{"独立回复同ID新对象", false, false, sameID, both},
		{"另一子调用已写无关记录", false, true, sharedReply, both},
		{"透传输入", true, false, returnInput, []string{"s1/" + firstText}},
		{"透传输入的同ID副本", true, false, copyInput, []string{"s1/" + firstText}},
		{"沿用输入ID和正文但改变路由", true, false, changeRoute, both},
		{"沿用输入ID但改写正文", true, false, editInput, []string{"s1/" + firstText, "s2/" + editedText}},
		{"沿用独立用户输入的ID生成回复", false, false, editInput, []string{"s1/" + firstText, "s2/" + editedText}},
	} {
		for _, cold := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/冷续跑=%t", tc.name, cold), func(t *testing.T) {
				hs := agent_v2.NewInMemoryHistoryStore()
				firstCalls, secondCalls := 0, 0
				var produced *schema.Message
				first := mkEchoGraph(t, "first", func(_ context.Context, in *schema.Message) (*schema.Message, error) {
					if in.Content != "第一项请求" {
						return nil, fmt.Errorf("第一项请求收到 %q", in.Content)
					}
					firstCalls++
					return shared, nil
				})
				helper := mkEchoGraph(t, "helper", func(context.Context, *schema.Message) (*schema.Message, error) {
					return schema.AssistantMessage("另一项结果"), nil
				})
				second := mkEchoGraph(t, "second", func(ctx context.Context, in *schema.Message) (*schema.Message, error) {
					if tc.forwardInput {
						if in.ID != shared.ID || in.Content != firstText {
							return nil, fmt.Errorf("透传输入错误: %v", in)
						}
					} else if in.Content != "第二项独立请求" || in.ID == shared.ID {
						return nil, fmt.Errorf("第二项独立请求收到 %v", in)
					}
					if cold {
						ack, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "提交第二份回复？"})
						if err != nil {
							return nil, err
						}
						if ack.Content != "提交" {
							return nil, fmt.Errorf("补答为 %q", ack.Content)
						}
					}
					if tc.runDescendant {
						if _, err := agent_v2.RunSubAgent(ctx, "helper", helper, schema.UserMessage("另行检查")); err != nil {
							return nil, err
						}
					}
					secondCalls++
					produced = tc.reply(in)
					return produced, nil
				})
				parent := mkEchoGraph(t, "parent", func(ctx context.Context, _ *schema.Message) (*schema.Message, error) {
					ev, err := agent_v2.RunSubAgent(ctx, "s1", first, schema.UserMessage("第一项请求"))
					if err != nil {
						return nil, err
					}
					next := schema.UserMessage("第二项独立请求")
					if tc.forwardInput {
						next = agent_v2.FinalMessage(ev)
					}
					ev, err = agent_v2.RunSubAgent(ctx, "s2", second, next)
					return agent_v2.FinalMessage(ev), err
				})
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				ctx = agent_v2.WithHistoryCtx(compose_v2.WithThreadIDCtx(ctx, "conversation"), hs, "t")
				cp := compose_v2.NewInMemoryCheckpointer()
				ctx = compose_v2.WithCheckpointerCtx(ctx, cp)
				sess := agent_v2.NewRunSession()
				ev, err := parent.Run(ctx, sess, schema.UserMessage("开始"))
				if cold {
					var answer agent_v2.ResumeOption
					ctx, answer = coldOwnershipAnswer(t, ctx, cp, sess.RunID, err, 2)
					ev, err = parent.Resume(ctx, sess.RunID, answer)
				}
				if err != nil {
					t.Fatal(err)
				}
				if firstCalls != 1 || secondCalls != 1 || produced == nil {
					t.Fatalf("两个成员应各产出一次并完成，实际 %d / %d", firstCalls, secondCalls)
				}
				if final := agent_v2.FinalMessage(ev); final == nil || final.ID != produced.ID || final.Content != produced.Content {
					t.Fatalf("父图返回 %v，期望第二个成员的回复 %v", final, produced)
				}
				msgs := scopeHist(t, hs, ctx, "conversation")
				var got []string
				for _, m := range msgs {
					if m.Role == schema.RoleAssistant && (m.Content == firstText || m.Content == editedText) {
						got = append(got, fmt.Sprintf("%v/%s", m.Annotations[agent_v2.HistoryAgentAnnotation], m.Content))
						if m.Annotations[agent_v2.HistoryAgentAnnotation] == "s2" && m.Annotations["route_target"] != produced.Annotations["route_target"] {
							t.Fatalf("历史中的转交目标 %v，实际产出的目标 %v", m.Annotations["route_target"], produced.Annotations["route_target"])
						}
					}
				}
				if !slices.Equal(got, tc.want) {
					t.Fatalf("回复归属 %v，期望 %v：%s", got, tc.want, scopeDump(msgs))
				}
			})
		}
	}
}

func TestHistoryOutputOwnershipAcrossDetachedTask(t *testing.T) {
	for _, forwarded := range []bool{false, true} {
		t.Run(fmt.Sprintf("转交脱离任务的回复=%t", forwarded), func(t *testing.T) {
			hs := agent_v2.NewInMemoryHistoryStore()
			ctx := agent_v2.WithHistoryCtx(compose_v2.WithThreadIDCtx(t.Context(), "conversation"), hs, "t")
			template := schema.AssistantMessage("共用答复")
			detached := mkEchoGraph(t, "detached", func(_ context.Context, in *schema.Message) (*schema.Message, error) {
				if in.Content != "独立任务" {
					return nil, fmt.Errorf("独立任务收到 %q", in.Content)
				}
				return template, nil
			})
			receiverCalls := 0
			receiver := mkEchoGraph(t, "receiver", func(_ context.Context, in *schema.Message) (*schema.Message, error) {
				receiverCalls++
				if forwarded {
					if in.ID != template.ID || in.Content != template.Content {
						return nil, fmt.Errorf("转交的回复为 %v", in)
					}
					return in, nil
				}
				if in.Content != "独立输入" {
					return nil, fmt.Errorf("期望独立输入，收到 %q", in.Content)
				}
				return template, nil
			})
			parent := mkEchoGraph(t, "parent", func(ctx context.Context, _ *schema.Message) (*schema.Message, error) {
				dctx := compose_v2.WithThreadIDCtx(agent_v2.DetachTask(ctx), "conversation")
				first, err := detached.Run(dctx, agent_v2.NewRunSession(), schema.UserMessage("独立任务"))
				if err != nil {
					return nil, err
				}
				input := schema.UserMessage("独立输入")
				if forwarded {
					input = agent_v2.FinalMessage(first)
				}
				events, err := agent_v2.RunSubAgent(ctx, "member", receiver, input)
				return agent_v2.FinalMessage(events), err
			})
			events, err := parent.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("开始"))
			if err != nil || receiverCalls != 1 || agent_v2.FinalMessage(events) == nil {
				t.Fatalf("成员运行次数 %d，错误 %v", receiverCalls, err)
			}
			owners := storedOwnership(t, hs, ctx, template.ID)
			want := []string{"detached", "member"}
			if forwarded {
				want = []string{"detached"}
			}
			if !slices.Equal(owners, want) {
				t.Fatalf("脱离任务和当前成员的回复归属 %v，期望 %v", owners, want)
			}
		})
	}
}

func TestHistoryOutputOwnershipRepeatedSlotAfterUpgrade(t *testing.T) {
	for _, tc := range []struct {
		name        string
		inside      bool
		forwardLeaf bool
	}{
		{"两次之间挂起", false, false},
		{"第二次内部挂起后独立产出", true, false},
		{"第二次内部挂起后透传自己已完成的叶回复", true, true},
	} {
		for _, legacy := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/旧历史=%t", tc.name, legacy), func(t *testing.T) {
				hs := agent_v2.NewInMemoryHistoryStore()
				cp := compose_v2.NewInMemoryCheckpointer()
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				ctx = agent_v2.WithHistoryCtx(compose_v2.WithThreadIDCtx(ctx, "conversation"), hs, "t")
				ctx = compose_v2.WithCheckpointerCtx(ctx, cp)
				shared := schema.AssistantMessage("两个请求的回复相同")
				leafCalls, workerCalls := 0, 0
				leaf := mkEchoGraph(t, "leaf", func(context.Context, *schema.Message) (*schema.Message, error) {
					leafCalls++
					return shared, nil
				})
				worker := mkEchoGraph(t, "worker", func(ctx context.Context, in *schema.Message) (*schema.Message, error) {
					if in.Content != "一号" && in.Content != "二号" {
						return nil, fmt.Errorf("工作节点收到 %q", in.Content)
					}
					reply := shared
					if in.Content == "一号" || tc.forwardLeaf {
						// 第二次若也委派，挂起前先完成本次 leaf。恢复阶段应复用该节点的子调用完成记录。
						ev, err := agent_v2.RunSubAgent(ctx, "leaf", leaf, in)
						if err != nil {
							return nil, err
						}
						reply = agent_v2.FinalMessage(ev)
					}
					if in.Content == "二号" && tc.inside {
						if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "提交二号的回复？"}); err != nil {
							return nil, err
						}
					}
					workerCalls++
					return reply, nil
				})
				wrapped := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("wrapped")).AddStep("body", worker)
				review := mkEchoGraph(t, "review", func(ctx context.Context, _ *schema.Message) (*schema.Message, error) {
					return compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "批准下一项？"})
				})
				parent := mkEchoGraph(t, "parent", func(ctx context.Context, _ *schema.Message) (*schema.Message, error) {
					if _, err := agent_v2.RunSubAgent(ctx, "slot", wrapped, schema.UserMessage("一号")); err != nil {
						return nil, err
					}
					if !tc.inside {
						if _, err := agent_v2.RunSubAgent(ctx, "review", review, schema.UserMessage("请批准")); err != nil {
							return nil, err
						}
					}
					ev, err := agent_v2.RunSubAgent(ctx, "slot", wrapped, schema.UserMessage("二号"))
					return agent_v2.FinalMessage(ev), err
				})
				session := agent_v2.NewRunSession()
				_, err := parent.Run(ctx, session, schema.UserMessage("开始"))
				wantScopes := 2
				if tc.inside {
					wantScopes = 3
				}
				ctx, answer := coldOwnershipAnswer(t, ctx, cp, session.RunID, err, wantScopes)
				wantOwners := []string{"slot/body/leaf", "slot/body"}
				wantLeafCalls := 1
				if tc.forwardLeaf {
					wantOwners[1] = "slot/body/leaf"
					wantLeafCalls = 2
				}
				if leafCalls != wantLeafCalls || workerCalls != 1 {
					t.Fatalf("挂起时叶执行 %d 次、成员已完成 %d 次", leafCalls, workerCalls)
				}
				if legacy {
					// 升级前的历史行带任务和归属，但没有调用范围。对照组读本版原样落库的行。
					rows := scopeHist(t, hs, ctx, "conversation")
					for _, m := range rows {
						delete(m.Annotations, "crew_history_scopes")
					}
					hs = agent_v2.NewInMemoryHistoryStore()
					if err := hs.Append(ctx, agent_v2.HistoryKey{Owner: "t", ConvID: "conversation"}, rows); err != nil {
						t.Fatal(err)
					}
					ctx = agent_v2.WithHistoryCtx(ctx, hs, "t")
				}
				ev, err := parent.Resume(ctx, session.RunID, answer)
				if err != nil || agent_v2.FinalMessage(ev) == nil || agent_v2.FinalMessage(ev).ID != shared.ID || leafCalls != wantLeafCalls || workerCalls != 2 {
					t.Fatalf("续跑错误 %v，叶执行 %d 次、成员执行 %d 次", err, leafCalls, workerCalls)
				}
				if got := storedOwnership(t, hs, ctx, shared.ID); !slices.Equal(got, wantOwners) {
					t.Fatalf("重复调用同一位置的回复归属 %v，期望 %v", got, wantOwners)
				}
			})
		}
	}
}

func TestHistoryOutputOwnershipDirectChildResume(t *testing.T) {
	for _, direct := range []bool{false, true} {
		t.Run(fmt.Sprintf("直接续子图=%t", direct), func(t *testing.T) {
			hs := agent_v2.NewInMemoryHistoryStore()
			cp := compose_v2.NewInMemoryCheckpointer()
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			ctx = agent_v2.WithHistoryCtx(compose_v2.WithThreadIDCtx(ctx, "conversation"), hs, "t")
			ctx = compose_v2.WithCheckpointerCtx(ctx, cp)
			shared := schema.AssistantMessage("叶节点的回复")
			calls := 0
			leaf := mkEchoGraph(t, "leaf", func(_ context.Context, in *schema.Message) (*schema.Message, error) {
				if in.Content != "开始" {
					return nil, fmt.Errorf("叶节点收到 %q", in.Content)
				}
				calls++
				return shared, nil
			})
			input := compose_v2.LastValue[*schema.Message]("ownership-input")
			out := compose_v2.LastValue[*schema.Message]("ownership-output")
			b := compose_v2.NewBuilder().WithName("child")
			compose_v2.WithChannel(b, input)
			compose_v2.WithChannel(b, out)
			b.AddNode("produce", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
				in, _ := compose_v2.Get(chs, input)
				ev, err := agent_v2.RunSubAgent(ctx, "leaf", leaf, in)
				if err != nil {
					return nil, err
				}
				return []compose_v2.ChannelWrite{compose_v2.Write(out, agent_v2.FinalMessage(ev))}, nil
			})
			b.AddNode("approve", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
				_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "接受这份回复？"})
				return nil, err
			})
			b.AddEdge(compose_v2.StartNode, "produce")
			b.AddEdge("produce", "approve")
			b.AddEdge("approve", compose_v2.EndNode)
			graph, err := b.Compile()
			if err != nil {
				t.Fatal(err)
			}
			child := agent_v2.NewGraphAgent(graph, agent_v2.WithGraphAgentName("child"),
				agent_v2.WithGraphInputWriter(func(in *schema.Message) []compose_v2.ChannelWrite {
					return []compose_v2.ChannelWrite{compose_v2.Write(input, in)}
				}),
				agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
					msg, _ := compose_v2.Get(chs, out)
					return msg
				}),
			)
			parent := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("parent")).AddStep("step", child)
			session := agent_v2.NewRunSession()
			_, err = parent.Run(ctx, session, schema.UserMessage("开始"))
			ctx, answer := coldOwnershipAnswer(t, ctx, cp, session.RunID, err, 2)
			var ev []*agent_v2.AgentEvent
			if direct {
				ev, err = child.Resume(compose_v2.WithGraphNS(ctx, "step"), session.RunID, answer)
			} else {
				ev, err = parent.Resume(ctx, session.RunID, answer)
			}
			if err != nil || agent_v2.FinalMessage(ev) == nil || agent_v2.FinalMessage(ev).ID != shared.ID || calls != 1 {
				t.Fatalf("续跑错误 %v，叶执行 %d 次", err, calls)
			}
			if got := storedOwnership(t, hs, ctx, shared.ID); !slices.Equal(got, []string{"step/leaf"}) {
				t.Fatalf("已完成节点的回复归属 %v", got)
			}
		})
	}
}

func coldOwnershipAnswer(t *testing.T, ctx context.Context, cp *compose_v2.InMemoryCheckpointer, runID string, err error, wantScopes int) (context.Context, agent_v2.ResumeOption) {
	t.Helper()
	interrupt, ok := compose_v2.IsInterrupt(err)
	if !ok || len(interrupt.AllQueries()) != 1 {
		t.Fatalf("预期一条挂起提问，实际 %v", err)
	}
	archives, err := cp.LoadAllInSession(ctx, runID)
	if err != nil || len(archives) != wantScopes {
		t.Fatalf("预期 %d 份存档，实际 %d 份，错误 %v", wantScopes, len(archives), err)
	}
	fresh := compose_v2.NewInMemoryCheckpointer()
	for _, archive := range archives {
		wire, err := compose_v2.MarshalCheckpoint(archive.Checkpoint)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := compose_v2.UnmarshalCheckpoint(wire)
		if err != nil {
			t.Fatal(err)
		}
		if err := fresh.SaveNS(ctx, runID, archive.NS, decoded); err != nil {
			t.Fatal(err)
		}
	}
	return compose_v2.WithCheckpointerCtx(ctx, fresh), agent_v2.WithResumeUserInputs(map[string]*schema.Message{
		interrupt.AllQueries()[0].InterruptID: schema.UserMessage("提交"),
	})
}

func storedOwnership(t *testing.T, hs agent_v2.HistoryStore, ctx context.Context, id string) []string {
	t.Helper()
	var owners []string
	for _, m := range scopeHist(t, hs, ctx, "conversation") {
		if m.Role == schema.RoleAssistant && m.ID == id {
			owners = append(owners, fmt.Sprint(m.Annotations[agent_v2.HistoryAgentAnnotation]))
		}
	}
	return owners
}
