package agent_v2_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
	"crew-go/tool"
)

type nestedInputFaultStore struct {
	agent_v2.HistoryStore
	target     string
	failOnce   atomic.Bool
	failLookup atomic.Bool
	commit     bool
}

func (s *nestedInputFaultStore) Append(ctx context.Context, key agent_v2.HistoryKey, msgs []*schema.Message) error {
	for _, message := range msgs {
		if message.Content == s.target && message.Role == schema.RoleUser && s.failOnce.CompareAndSwap(true, false) {
			if s.commit {
				if err := s.HistoryStore.Append(ctx, key, msgs); err != nil {
					return err
				}
			}
			return io.ErrUnexpectedEOF
		}
	}
	return s.HistoryStore.Append(ctx, key, msgs)
}

func (s *nestedInputFaultStore) Load(ctx context.Context, key agent_v2.HistoryKey, query agent_v2.HistoryQuery) (agent_v2.HistoryPage, error) {
	if query.Agent == "worker" && query.MessageID != "" && s.failLookup.CompareAndSwap(true, false) {
		return agent_v2.HistoryPage{}, io.ErrUnexpectedEOF
	}
	return s.HistoryStore.Load(ctx, key, query)
}

func TestNestedHistoryRecoversOriginalInputAfterWriteFailure(t *testing.T) {
	for _, scenario := range []struct {
		name       string
		commit     bool
		failLookup bool
		wantBefore int
	}{
		{name: "append_failed", wantBefore: 0},
		{name: "commit_ack_lost", commit: true, wantBefore: 1},
		{name: "read_unavailable_after_commit", commit: true, failLookup: true, wantBefore: 1},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			hs := &nestedInputFaultStore{HistoryStore: agent_v2.NewInMemoryHistoryStore(), target: "核验 1", commit: scenario.commit}
			hs.failOnce.Store(true)
			hs.failLookup.Store(scenario.failLookup)
			cp := compose_v2.NewInMemoryCheckpointer()
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			ctx = agent_v2.WithHistoryCtx(compose_v2.WithThreadIDCtx(compose_v2.WithCheckpointerCtx(ctx, cp), "recover"), hs, "t")

			callee := hoGraphHost(t, "worker", "work", func(ctx context.Context, input string) (string, error) {
				for _, question := range []string{"确认商品？", "确认数量？"} {
					if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: question}); err != nil {
						return "", err
					}
				}
				return "已完成 " + input, nil
			})
			var attempts atomic.Int32
			parent := hoGraphHost(t, "caller", "call", func(ctx context.Context, _ string) (string, error) {
				in := schema.UserMessage(fmt.Sprintf("核验 %d", attempts.Add(1)))
				in.Annotations = map[string]any{"reference": json.Number("9007199254740993")}
				events, err := agent_v2.RunSubAgent(ctx, "worker", callee, in, agent_v2.WithCallKey("verification"))
				return nestedFinal(events), err
			})
			sess := agent_v2.NewRunSession()
			_, err := parent.Run(ctx, sess, schema.UserMessage("用户原问题"))
			if ie, ok := compose_v2.IsInterrupt(err); !ok || len(ie.AllQueries()) != 1 || hs.failOnce.Load() {
				t.Fatalf("预期首次挂起时注入历史写入错误，实际 %v", err)
			}
			if n := hoCountOwner(loadHist(t, hs, ctx, "recover"), "worker", schema.RoleUser, "核验 1"); n != scenario.wantBefore {
				t.Fatalf("续跑前输入数 %d，预期 %d", n, scenario.wantBefore)
			}
			var resumed []*agent_v2.AgentEvent
			answers := map[string]*schema.Message{}
			for _, expected := range []string{"确认商品？", "确认数量？"} {
				interrupt, ok := compose_v2.IsInterrupt(err)
				if !ok || len(interrupt.AllQueries()) != 1 || interrupt.AllQueries()[0].Question != expected {
					t.Fatalf("预期收回原调用的提问 %q，实际 %v", expected, err)
				}
				cp = reopenDirectChildStore(t, ctx, cp, sess.RunID)
				ctx = compose_v2.WithCheckpointerCtx(ctx, cp)
				answers[interrupt.AllQueries()[0].InterruptID] = schema.UserMessage("确认")
				resumed, err = parent.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(answers))
			}
			if output := nestedFinal(resumed); err != nil || output != "已完成 核验 1" {
				t.Fatalf("原调用应通过两次补答完成，实际结果=%q 错误=%v", output, err)
			}
			if hs.failLookup.Load() {
				t.Fatal("续跑应实际回查原始历史输入")
			}
			msgs := loadHist(t, hs, ctx, "recover")
			original := hoCountOwner(msgs, "worker", schema.RoleUser, "核验 1")
			final := hoCountOwner(msgs, "worker", schema.RoleAssistant, "已完成 核验 1")
			var replacements int
			for _, text := range []string{"核验 2", "核验 3"} {
				replacements += hoCountOwner(msgs, "worker", schema.RoleUser, text)
			}
			if original != 1 || final != 1 || replacements != 0 || attempts.Load() != 3 {
				t.Fatalf("应各保留原输入和产出一次，实际输入=%d 产出=%d 重造输入=%d 执行入口=%d",
					original, final, replacements, attempts.Load())
			}
			for _, msg := range msgs {
				if msg.Content == "核验 1" && msg.Annotations[agent_v2.HistoryAgentAnnotation] == "worker" &&
					msg.Annotations["reference"] != json.Number("9007199254740993") {
					t.Fatalf("冷续跑应保留原标注的数字精度，实际 %v", msg.Annotations["reference"])
				}
			}
			if saved, err := cp.LoadNS(ctx, sess.RunID, ""); err != nil || saved != nil {
				t.Fatalf("任务完成后应清理根存档，实际 %v，错误 %v", saved, err)
			}
		})
	}
}

func TestNestedHistoryRecoversRedactedInput(t *testing.T) {
	for _, mode := range []string{"committed", "failed", "ack-lost"} {
		t.Run(mode, func(t *testing.T) {
			hs := &nestedInputFaultStore{HistoryStore: agent_v2.NewInMemoryHistoryStore(), target: "MASKED-1", commit: mode == "ack-lost"}
			hs.failOnce.Store(mode != "committed")
			key := agent_v2.HistoryKey{Owner: "t", ConvID: "redaction"}
			cp := compose_v2.NewInMemoryCheckpointer()
			raw, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			ctx := agent_v2.WithHistoryCtx(compose_v2.WithThreadIDCtx(compose_v2.WithCheckpointerCtx(raw, cp), key.ConvID), hs, key.Owner)
			kit := tool.NewToolkit()
			var side atomic.Int32
			kit.Register(tool.NewFuncTool("approve", "approval", map[string]any{"type": "object"}, func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
				a, e := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "approve safe operation?"})
				if e != nil {
					return nil, e
				}
				if a.Content != "ok" {
					return nil, fmt.Errorf("wrong answer")
				}
				side.Add(1)
				return tool.Success("approved"), nil
			}))
			rec := &promptRecorder{resp: []*schema.Message{oneToolCall("approve"), schema.AssistantMessage("SAFE-RECEIPT"), schema.AssistantMessage("NEXT")}}
			worker := agent_v2.NewBasicAgent(agent_v2.WithName("agent"), agent_v2.WithModel(rec), agent_v2.WithToolkit(kit), agent_v2.WithHooks(&maskingHooks{from: "SECRET-", to: "MASKED-"}))
			var attempts atomic.Int32
			parent := hoGraphHost(t, "parent", "call", func(ctx context.Context, _ string) (string, error) {
				in := schema.UserMessage(fmt.Sprintf("SECRET-%d", attempts.Add(1)))
				ev, e := agent_v2.RunSubAgent(ctx, "worker", worker, in, agent_v2.WithCallKey("fixed"))
				return nestedFinal(ev), e
			})
			sess := agent_v2.NewRunSession()
			_, e := parent.Run(ctx, sess, schema.UserMessage("public"))
			interrupt, ok := compose_v2.IsInterrupt(e)
			if !ok || len(interrupt.AllQueries()) != 1 {
				t.Fatalf("预期脱敏后的 BasicAgent 等待工具审批，实际 %v", e)
			}
			id := interrupt.AllQueries()[0].InterruptID
			if hs.failOnce.Load() || (!containsMsg(rec.call(0), "MASKED-1") || containsMsg(rec.call(0), "SECRET-")) {
				t.Fatalf("fixture: drop=%v first model=%v", hs.failOnce.Load(), contentsOfTest(rec.call(0)))
			}
			cp = reopenDirectChildStore(t, ctx, cp, sess.RunID)
			ctx = compose_v2.WithCheckpointerCtx(ctx, cp)
			ev, e := parent.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(map[string]*schema.Message{id: schema.UserMessage("ok")}))
			if e != nil || nestedFinal(ev) != "SAFE-RECEIPT" || side.Load() != 1 || attempts.Load() != 2 {
				t.Fatalf("business receipt=%q e=%v writes=%d, entries=%d", nestedFinal(ev), e, side.Load(), attempts.Load())
			}
			msgs, e := agent_v2.LoadAllHistory(ctx, hs, key)
			if e != nil {
				t.Fatal(e)
			}
			maskCount := hoCountOwner(msgs, "worker", schema.RoleUser, "MASKED-1")
			secret := containsMsg(msgs, "SECRET-")
			alt := containsMsg(msgs, "MASKED-2")
			if maskCount != 1 || secret || alt {
				t.Fatal("member-owned history should recover sole redacted input once")
			}

			followUp := hoGraphHost(t, "followup", "call", func(ctx context.Context, _ string) (string, error) {
				ev, err := agent_v2.RunSubAgent(ctx, "worker", worker, schema.UserMessage("后续问题"))
				return nestedFinal(ev), err
			})
			if events, err := followUp.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("查看后续")); err != nil || nestedFinal(events) != "NEXT" {
				t.Fatalf("后续任务应得到模型回复，实际 %q，错误 %v", nestedFinal(events), err)
			}
			inputAt, declarationAt := -1, -1
			for i, message := range rec.call(2) {
				if message.Role == schema.RoleUser && message.Content == "MASKED-1" {
					inputAt = i
				}
				if len(schema.GetToolCalls(message)) != 0 {
					declarationAt = i
				}
			}
			if inputAt < 0 || declarationAt <= inputAt {
				t.Fatalf("送模历史应先交付恢复的输入，再交付由它发起的工具调用，实际输入位置=%d，工具位置=%d", inputAt, declarationAt)
			}

			rewrite := &promptRecorder{resp: []*schema.Message{schema.AssistantMessage("SHARED")}}
			reader := agent_v2.NewBasicAgent(agent_v2.WithName("reader"), agent_v2.WithModel(rewrite))
			shared := hoGraphHost(t, "shared", "call", func(ctx context.Context, _ string) (string, error) {
				candidate := historyViewAgent{Agent: reader, mode: agent_v2.HandoffHistoryRewrite}
				ev, err := agent_v2.RunSubAgent(ctx, "reader", candidate, schema.UserMessage("共享视图"))
				return nestedFinal(ev), err
			})
			if events, err := shared.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("接手会话")); err != nil || nestedFinal(events) != "SHARED" {
				t.Fatalf("共享历史应得到模型回复，实际 %q，错误 %v", nestedFinal(events), err)
			}
			inputAt, declarationAt = -1, -1
			for i, message := range rewrite.call(0) {
				if message.Content == "MASKED-1" {
					inputAt = i
				}
				if strings.Contains(message.Content, "called tool: `approve`") {
					declarationAt = i
				}
			}
			if inputAt < 0 || declarationAt <= inputAt {
				t.Fatalf("改写视图应保留输入和工具因果顺序，实际输入位置=%d，工具位置=%d", inputAt, declarationAt)
			}
		})
	}
}
