package agent_v2_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
	"crew-go/store"
	"crew-go/tool"
)

// 子图完成的工具回执归本次子调用。经父图补答和按 namespace 单独补答，都应只执行一次。
func TestDirectChildCompletionThenParentResume(t *testing.T) {
	for _, tc := range []struct {
		name             string
		direct           bool
		repeatAnswer     bool
		durability       compose_v2.Durability
		abortAfterDirect bool
	}{
		{name: "只续父图"},
		{name: "续完子图后续父图", direct: true},
		{name: "续完子图后再递原答案", direct: true, repeatAnswer: true},
		{name: "完成子图后再取消和续该子图", direct: true, abortAfterDirect: true},
		{name: "同步逐步存档续父图", direct: true, durability: compose_v2.DurabilitySync},
		{name: "异步逐步存档续父图", direct: true, durability: compose_v2.DurabilityAsync},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			cp := compose_v2.NewInMemoryCheckpointer()
			hs := agent_v2.NewInMemoryHistoryStore()
			ctx = agent_v2.WithHistoryCtx(compose_v2.WithThreadIDCtx(ctx, "direct-child"), hs, "t")
			ctx = compose_v2.WithCheckpointerCtx(ctx, cp)
			if tc.durability != "" {
				ctx = compose_v2.WithDurabilityCtx(ctx, tc.durability)
			}
			var prepared, written atomic.Int32
			toolRun := func(ctx context.Context, name string, calls *atomic.Int32, input *schema.Message) (*schema.Message, error) {
				tk := tool.NewToolkit()
				tk.Register(tool.NewFuncTool(name, "登记回执", map[string]any{"type": "object"}, func(context.Context, map[string]any) (*tool.Result, error) {
					r := tool.Success(fmt.Sprintf("%s-%d", name, calls.Add(1)))
					r.Metadata = map[string]any{tool.ReturnDirectMetaKey: true}
					return r, nil
				}))
				// 每次进入工具成员都构造同一份模型脚本，若框架重做业务，则实际工具会再写一次。
				worker := agent_v2.NewBasicAgent(agent_v2.WithName(name), agent_v2.WithModel(newStubModel([]*schema.Message{oneToolCall(name)})), agent_v2.WithToolkit(tk))
				events, err := agent_v2.RunSubAgent(ctx, name, worker, input)
				return agent_v2.FinalMessage(events), err
			}
			child := mkEchoGraph(t, "directChild", func(ctx context.Context, in *schema.Message) (*schema.Message, error) {
				first, err := toolRun(ctx, "prepare", &prepared, in)
				if err != nil {
					return nil, err
				}
				answer, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "确认提交？"})
				if err != nil {
					return nil, err
				}
				second, err := toolRun(ctx, "apply", &written, answer)
				if err != nil {
					return nil, err
				}
				agent_v2.GetSession(ctx).AddEvent(agent_v2.NewOutputEvent("回执", schema.AssistantMessage(second.Content)))
				return schema.AssistantMessage(first.Content + "/" + answer.Content + "/" + second.Content), nil
			})
			parent := mkEchoGraph(t, "directParent", func(ctx context.Context, in *schema.Message) (*schema.Message, error) {
				ev, err := agent_v2.RunSubAgent(ctx, "child", child, in)
				if err != nil {
					return nil, err
				}
				final := agent_v2.FinalMessage(ev)
				if final == nil {
					return nil, fmt.Errorf("子图缺少回复")
				}
				var receipts []string
				for _, event := range ev {
					if event != nil && event.AgentName == "回执" && event.Output != nil && event.Output.Message != nil {
						receipts = append(receipts, event.Output.Message.Content)
					}
				}
				if !slices.Equal(receipts, []string{"apply-1"}) {
					return nil, fmt.Errorf("子图交回的中间事件为 %v", receipts)
				}
				return schema.AssistantMessage("父图收到 " + final.Content), nil
			})
			sess := agent_v2.NewRunSession()
			_, err := parent.Run(ctx, sess, schema.UserMessage("工单"))
			qid := partitionQuestionID(t, err)
			if prepared.Load() != 1 || written.Load() != 0 {
				t.Fatalf("挂起时工具次数 prepare=%d apply=%d", prepared.Load(), written.Load())
			}
			cp = reopenBackgroundCheckpoints(t, ctx, cp, sess.RunID)
			ctx = compose_v2.WithCheckpointerCtx(ctx, cp)
			answer := agent_v2.WithResumeUserInputs(map[string]*schema.Message{qid: schema.UserMessage("同意")})
			wantChild := "prepare-1/同意/apply-1"
			if tc.direct {
				entries, err := cp.ListPending(ctx, sess.RunID)
				if err != nil {
					t.Fatal(err)
				}
				var ns string
				for _, e := range entries {
					if strings.HasPrefix(e.NS, "child#") && !strings.Contains(e.NS, "/") {
						ns = e.NS
					}
				}
				if ns == "" {
					t.Fatal("未找到挂起的子图 namespace")
				}
				ev, err := child.Resume(compose_v2.WithGraphNS(ctx, ns), sess.RunID, answer)
				if err != nil || agent_v2.FinalMessage(ev) == nil || agent_v2.FinalMessage(ev).Content != wantChild {
					t.Fatalf("单续子图得到 %v，错误 %v", agent_v2.FinalMessage(ev), err)
				}
				saved, err := cp.LoadNS(ctx, sess.RunID, "")
				if err != nil || saved == nil || !saved.HasResumableWork() {
					t.Fatalf("父图应仍可恢复，错误 %v", err)
				}
				if prepared.Load() != 1 || written.Load() != 1 {
					t.Fatalf("单续子图后工具次数 prepare=%d apply=%d", prepared.Load(), written.Load())
				}
				cp = reopenDirectChildStore(t, ctx, cp, sess.RunID)
				ctx = compose_v2.WithCheckpointerCtx(ctx, cp)
				completed, err := cp.LoadNS(ctx, sess.RunID, ns)
				if err != nil || completed == nil || completed.Status != compose_v2.CPStatusCompleted || completed.HasResumableWork() {
					t.Fatalf("已完成的子调用没有可续工作，存档 %+v，错误 %v", completed, err)
				}
				if tc.abortAfterDirect {
					target := compose_v2.WithGraphNS(ctx, ns)
					before := partitionCheckpointBytes(t, ctx, cp, sess.RunID, ns)
					if _, err := child.Resume(target, sess.RunID, agent_v2.WithResumeAction(agent_v2.ResumeActionAbort)); err != nil {
						t.Fatalf("已完成的子图没有待取消的执行，实际 %v", err)
					}
					if _, err := child.Resume(target, sess.RunID, answer); !errors.Is(err, compose_v2.ErrNoPendingCheckpoint) {
						t.Fatalf("已完成的子图再次恢复应无可续工作，实际 %v", err)
					}
					if !bytes.Equal(before, partitionCheckpointBytes(t, ctx, cp, sess.RunID, ns)) || prepared.Load() != 1 || written.Load() != 1 {
						t.Fatalf("子图取消或重复恢复改动了父图回执，工具次数 %d/%d", prepared.Load(), written.Load())
					}
				}
			}
			var opts []agent_v2.ResumeOption
			if !tc.direct || tc.repeatAnswer {
				opts = append(opts, answer)
			}
			ev, err := parent.Resume(ctx, sess.RunID, opts...)
			final := agent_v2.FinalMessage(ev)
			if err != nil || final == nil || final.Content != "父图收到 "+wantChild || prepared.Load() != 1 || written.Load() != 1 {
				t.Fatalf("父图回复 %v，错误 %v，工具次数 prepare=%d apply=%d", final, err, prepared.Load(), written.Load())
			}
			entries, err := cp.LoadAllInSession(ctx, sess.RunID)
			if err != nil || len(entries) != 0 {
				t.Fatalf("父图完成后剩余 %d 份存档，错误 %v", len(entries), err)
			}
			var owners []string
			for _, msg := range scopeHist(t, hs, ctx, "direct-child") {
				if msg.Content == wantChild {
					owners = append(owners, fmt.Sprint(msg.Annotations[agent_v2.HistoryAgentAnnotation]))
				}
			}
			if !slices.Equal(owners, []string{"child"}) {
				t.Fatalf("子图最终回复归属 %v，预期只有 child", owners)
			}
		})
	}
}

func reopenDirectChildStore(t *testing.T, ctx context.Context, old *compose_v2.InMemoryCheckpointer, sid string) *compose_v2.InMemoryCheckpointer {
	t.Helper()
	rows, err := old.LoadAllInSession(ctx, sid)
	if err != nil || len(rows) == 0 {
		t.Fatalf("没有可重新打开的任务存档，错误 %v", err)
	}
	fresh := compose_v2.NewInMemoryCheckpointer()
	for _, row := range rows {
		wire, err := compose_v2.MarshalCheckpoint(row.Checkpoint)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := compose_v2.UnmarshalCheckpoint(wire)
		if err != nil {
			t.Fatal(err)
		}
		if err := fresh.SaveNS(ctx, sid, row.NS, decoded); err != nil {
			t.Fatal(err)
		}
	}
	return fresh
}

var directCompletionSignal = compose_v2.NewTaskCommitSignalKind("test.direct-child-completion", func(ctx context.Context, s compose_v2.Signal) error {
	return store.FromContext(ctx).Put(ctx, store.NS("commits"), s.Dest, s.Value)
})

// 同一子图在独立的非空根 namespace 完成时可以提交。作为成员时，挂起前后的两个信号都等父图批准。
func TestDirectChildCompletionSignalAuthority(t *testing.T) {
	for _, tc := range []struct {
		name       string
		standalone bool
		reject     bool
	}{
		{"独立非空根", true, false},
		{"父图接受", false, false},
		{"父图末端否决", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			st := store.NewInMemoryStore()
			cp := compose_v2.NewInMemoryCheckpointer()
			ctx = compose_v2.WithCheckpointerCtx(store.WithContext(ctx, st), cp)
			// 这个独立根的外形也像派生的成员位置，是否有父调用只能按实际归属判断。
			const root = "workflow/tenant#17"
			rootCtx := compose_v2.WithGraphNS(ctx, root)
			emit := func(ctx context.Context, phase string) error {
				if !compose_v2.EmitSignal(ctx, directCompletionSignal, compose_v2.Signal{Dest: phase, Value: "已批准"}) {
					return fmt.Errorf("提交信号 %q 未进入节点", phase)
				}
				return nil
			}
			child := newTwoNodeAgent(t, "signal-child", func(ctx context.Context, input string) (string, error) {
				return input, emit(ctx, "before")
			}, func(ctx context.Context, _ string, first string) (string, error) {
				ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "提交？"})
				if err != nil {
					return "", err
				}
				return first + "/" + ans.Content, emit(ctx, "after")
			})
			refusal := errors.New("父节点否决")
			parent := mkEchoGraph(t, "signal-parent", func(ctx context.Context, in *schema.Message) (*schema.Message, error) {
				ev, err := agent_v2.RunSubAgent(ctx, "child", child, in)
				if err != nil {
					return nil, err
				}
				if tc.reject {
					return nil, refusal
				}
				return agent_v2.FinalMessage(ev), nil
			})
			top := agent_v2.Agent(parent)
			if tc.standalone {
				top = child
			}
			session := agent_v2.NewRunSession()
			_, err := top.Run(rootCtx, session, schema.UserMessage("批准"))
			id := partitionQuestionID(t, err)
			var childNS string
			rows, err := cp.ListPending(ctx, session.RunID)
			if err != nil {
				t.Fatal(err)
			}
			if tc.standalone {
				childNS = root
			} else {
				for _, r := range rows {
					if strings.HasPrefix(r.NS, root+"/child#") {
						childNS = r.NS
					}
				}
			}
			if childNS == "" {
				t.Fatal("未找到子图存档")
			}
			check := func(stage string, want bool) {
				t.Helper()
				for _, phase := range []string{"before", "after"} {
					item, err := st.Get(ctx, store.NS("commits"), phase)
					if err != nil || (item != nil) != want || (item != nil && item.Value != "已批准") {
						t.Errorf("%s 的 %s 信号产生记录 %+v，预期提交=%t，错误 %v", stage, phase, item, want, err)
					}
				}
			}
			check("挂起", false)
			ev, err := child.Resume(compose_v2.WithGraphNS(ctx, childNS), session.RunID, agent_v2.WithResumeUserInputs(map[string]*schema.Message{id: schema.UserMessage("确认")}))
			if err != nil || agent_v2.FinalMessage(ev) == nil || agent_v2.FinalMessage(ev).Content != "批准/确认" {
				t.Fatalf("子图错误 %v，结果 %v", err, agent_v2.FinalMessage(ev))
			}
			check("直接续子图", tc.standalone)
			if tc.standalone {
				return
			}
			cp = reopenDirectChildStore(t, ctx, cp, session.RunID)
			rootCtx = compose_v2.WithCheckpointerCtx(rootCtx, cp)
			ev, err = parent.Resume(rootCtx, session.RunID)
			if tc.reject {
				if !errors.Is(err, refusal) {
					t.Errorf("父图应否决恢复结果，实际 %v", err)
				}
			} else if err != nil || agent_v2.FinalMessage(ev) == nil || agent_v2.FinalMessage(ev).Content != "批准/确认" {
				t.Errorf("父图错误 %v，结果 %v", err, agent_v2.FinalMessage(ev))
			}
			check("父图结算", !tc.reject)
		})
	}
}

func TestDirectChildCompletionSiblingIsolation(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(fmt.Sprintf("兄弟部分提交=%t", partial), func(t *testing.T) { testDirectChildCompletionSiblings(t, partial) })
	}
}

func testDirectChildCompletionSiblings(t *testing.T, partial bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	cp := compose_v2.NewInMemoryCheckpointer()
	ctx = compose_v2.WithCheckpointerCtx(ctx, cp)
	var writes [2]atomic.Int32
	var failed atomic.Bool
	transient := errors.New("右支保存失败一次")
	child := mkEchoGraph(t, "shared-child", func(ctx context.Context, in *schema.Message) (*schema.Message, error) {
		answer, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "批准 " + in.Content + "？"})
		if err != nil {
			return nil, err
		}
		i := slices.Index([]string{"left", "right"}, in.Content)
		if i < 0 {
			return nil, fmt.Errorf("未知工作项 %q", in.Content)
		}
		return schema.AssistantMessage(fmt.Sprintf("%s/%s/%d", in.Content, answer.Content, writes[i].Add(1))), nil
	})
	member := func(reg string) *agent_v2.GraphAgent {
		return mkEchoGraph(t, "member-"+reg, func(ctx context.Context, _ *schema.Message) (*schema.Message, error) {
			ev, err := agent_v2.RunSubAgent(ctx, "child", child, schema.UserMessage(reg))
			if err == nil && partial && reg == "right" && failed.CompareAndSwap(false, true) {
				return nil, transient
			}
			return agent_v2.FinalMessage(ev), err
		})
	}
	p := agent_v2.NewParallelAgent(agent_v2.WithParallelName("siblings")).AddBranch("left", member("left")).AddBranch("right", member("right"))
	parent := mkEchoGraph(t, "isolation-parent", func(ctx context.Context, input *schema.Message) (*schema.Message, error) {
		ev, err := agent_v2.RunSubAgent(ctx, "siblings", p, input)
		return agent_v2.FinalMessage(ev), err
	})
	sess := agent_v2.NewRunSession()
	_, err := parent.Run(ctx, sess, schema.UserMessage("同时处理"))
	pause, ok := compose_v2.IsInterrupt(err)
	if !ok || len(pause.AllQueries()) != 2 {
		t.Fatalf("预期两个挂起问题，实际 %v", err)
	}
	pending, err := cp.ListPending(ctx, sess.RunID)
	if err != nil {
		t.Fatal(err)
	}
	var leftNS, leftID, rightNS, rightID string
	for _, e := range pending {
		for _, side := range []string{"left", "right"} {
			if !strings.Contains(e.NS, "/"+side+"/child#") || len(e.Checkpoint.PendingInterrupts) != 1 {
				continue
			}
			q := e.Checkpoint.PendingInterrupts[0]
			if q.Question != "批准 "+side+"？" {
				t.Fatalf("子任务 %s 的问题为 %q", side, q.Question)
			}
			if side == "left" {
				leftNS, leftID = e.NS, q.InterruptID
			} else {
				rightNS, rightID = e.NS, q.InterruptID
			}
		}
	}
	if leftNS == "" || rightNS == "" || leftID == rightID {
		t.Fatalf("两份子图地址错误 %q, %q；问题 %q, %q", leftNS, rightNS, leftID, rightID)
	}
	cp = reopenDirectChildStore(t, ctx, cp, sess.RunID)
	ctx = compose_v2.WithCheckpointerCtx(ctx, cp)
	ev, err := child.Resume(compose_v2.WithGraphNS(ctx, leftNS), sess.RunID, agent_v2.WithResumeUserInputs(map[string]*schema.Message{leftID: schema.UserMessage("甲")}))
	if err != nil || agent_v2.FinalMessage(ev) == nil || agent_v2.FinalMessage(ev).Content != "left/甲/1" || writes[1].Load() != 0 {
		t.Fatalf("直续左侧结果 %v，错误 %v，右写入 %d", agent_v2.FinalMessage(ev), err, writes[1].Load())
	}
	right, err := cp.LoadNS(ctx, sess.RunID, rightNS)
	if err != nil || right == nil || len(right.PendingInterrupts) != 1 || right.PendingInterrupts[0].InterruptID != rightID {
		t.Fatalf("右边的挂起问题被改变，错误 %v", err)
	}
	cp = reopenDirectChildStore(t, ctx, cp, sess.RunID)
	ctx = compose_v2.WithCheckpointerCtx(ctx, cp)
	ev, err = parent.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(map[string]*schema.Message{rightID: schema.UserMessage("乙")}))
	if partial {
		if !errors.Is(err, transient) || writes[0].Load() != 1 || writes[1].Load() != 1 {
			t.Fatalf("父图应留下部分完成超步，实际错误 %v、左右写入 %d/%d", err, writes[0].Load(), writes[1].Load())
		}
		cp = reopenDirectChildStore(t, ctx, cp, sess.RunID)
		ctx = compose_v2.WithCheckpointerCtx(ctx, cp)
		ev, err = parent.Resume(ctx, sess.RunID)
	}
	final := agent_v2.FinalMessage(ev)
	if err != nil || final == nil || !strings.Contains(final.Content, "left/甲/1") || !strings.Contains(final.Content, "right/乙/1") || writes[0].Load() != 1 || writes[1].Load() != 1 {
		t.Fatalf("父图汇总 %v，错误 %v，左右实际写入 %d/%d", final, err, writes[0].Load(), writes[1].Load())
	}
}

func TestDirectChildCompletionRerunGeneration(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	cp := compose_v2.NewInMemoryCheckpointer()
	ctx = compose_v2.WithCheckpointerCtx(ctx, cp)
	var charges, plans atomic.Int32
	charger := newSingleNodeAgent(t, "charger", "charge", nil, func(_ context.Context, _ string) (string, error) { charges.Add(1); return "已扣款", nil })
	planner := newSingleNodeAgent(t, "planner", "decide", nil, func(ctx context.Context, _ string) (string, error) {
		answer, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "计划内容？"})
		if err != nil {
			return "", err
		}
		plans.Add(1)
		return answer.Content, nil
	})
	parent := planHost(t, charger, planner, nil, agent_v2.WithRerunnable())
	sess := agent_v2.NewRunSession()
	_, err := parent.Run(ctx, sess, schema.UserMessage("工单"))
	first := partitionQuestionID(t, err)
	ns := directPendingNS(t, ctx, cp, sess.RunID, "planner#")
	approve := func(id, text string) agent_v2.ResumeOption {
		return agent_v2.WithResumeUserInputs(map[string]*schema.Message{id: schema.UserMessage(text)})
	}
	events, err := planner.Resume(compose_v2.WithGraphNS(ctx, ns), sess.RunID, approve(first, "BAD-plan"))
	if err != nil || nestedFinal(events) != "BAD-plan" {
		t.Fatalf("直接恢复计划 %v，错误 %v", nestedFinal(events), err)
	}
	_, err = parent.Resume(ctx, sess.RunID)
	if err == nil || !strings.Contains(err.Error(), "plan 不合法") || charges.Load() != 1 || plans.Load() != 1 {
		t.Fatalf("首次结果应被父边否决，实际 %v，扣款/计划 %d/%d", err, charges.Load(), plans.Load())
	}
	cp = reopenDirectChildStore(t, ctx, cp, sess.RunID)
	ctx = compose_v2.WithCheckpointerCtx(ctx, cp)
	_, err = parent.Resume(ctx, sess.RunID)
	second := partitionQuestionID(t, err)
	if first == second {
		t.Fatal("父边要求重算后的新计划应使用新提问")
	}
	events, err = parent.Resume(ctx, sess.RunID, approve(second, "plan-ok"))
	if err != nil || nestedFinal(events) != "plan-ok" || charges.Load() != 1 || plans.Load() != 2 {
		t.Fatalf("第二代恢复 %v，错误 %v，扣款/计划 %d/%d", nestedFinal(events), err, charges.Load(), plans.Load())
	}
}
func directPendingNS(t *testing.T, ctx context.Context, cp compose_v2.Checkpointer, sid, prefix string) string {
	t.Helper()
	rows, err := cp.ListPending(ctx, sid)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range rows {
		if strings.HasPrefix(e.NS, prefix) && !strings.Contains(strings.TrimPrefix(e.NS, prefix), "/") {
			return e.NS
		}
	}
	t.Fatalf("未找到前缀为 %q 的挂起子图", prefix)
	return ""
}

func TestDirectChildCompletionCannotEncodeResult(t *testing.T) {
	for _, durability := range []compose_v2.Durability{compose_v2.DurabilityExit, compose_v2.DurabilitySync, compose_v2.DurabilityAsync} {
		t.Run(string(durability), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			cp := compose_v2.NewInMemoryCheckpointer()
			ctx = compose_v2.WithDurabilityCtx(compose_v2.WithCheckpointerCtx(ctx, cp), durability)
			var effects atomic.Int32
			child := mkEchoGraph(t, "unencodable", func(ctx context.Context, _ *schema.Message) (*schema.Message, error) {
				ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "写入？"})
				if err != nil {
					return nil, err
				}
				effects.Add(1)
				progress := schema.AssistantMessage("导出回执时将失败")
				progress.Annotations = map[string]any{"handle": make(chan struct{})}
				agent_v2.GetSession(ctx).AddEvent(agent_v2.NewOutputEvent("进度", progress))
				return schema.AssistantMessage(ans.Content), nil
			})
			parent := mkEchoGraph(t, "guard-parent", func(ctx context.Context, in *schema.Message) (*schema.Message, error) {
				ev, err := agent_v2.RunSubAgent(ctx, "slot", child, in)
				return agent_v2.FinalMessage(ev), err
			})
			sess := agent_v2.NewRunSession()
			_, err := parent.Run(ctx, sess, schema.UserMessage("请求"))
			qid := partitionQuestionID(t, err)
			ns := directPendingNS(t, ctx, cp, sess.RunID, "slot#")
			answer := agent_v2.WithResumeUserInputs(map[string]*schema.Message{qid: schema.UserMessage("批准")})
			_, err = child.Resume(compose_v2.WithGraphNS(ctx, ns), sess.RunID, answer)
			if !errors.Is(err, compose_v2.ErrCheckpointResumeUnconfirmed) || effects.Load() != 1 {
				t.Fatalf("预期工具已执行一次但无法确认交接，实际错误 %v、副作用 %d", err, effects.Load())
			}
			cp = reopenDirectChildStore(t, ctx, cp, sess.RunID)
			ctx = compose_v2.WithCheckpointerCtx(ctx, cp)
			_, childErr := child.Resume(compose_v2.WithGraphNS(ctx, ns), sess.RunID, answer)
			_, parentErr := parent.Resume(ctx, sess.RunID, answer)
			if !errors.Is(childErr, compose_v2.ErrCheckpointResumeUnconfirmed) || !errors.Is(parentErr, compose_v2.ErrCheckpointResumeUnconfirmed) || effects.Load() != 1 {
				t.Fatalf("未确认交接后禁止重做，子错误 %v、父错误 %v、副作用 %d", childErr, parentErr, effects.Load())
			}
		})
	}
}

func TestDirectChildCompletionLegacyCaller(t *testing.T) {
	for _, nested := range []bool{false, true} {
		t.Run(fmt.Sprintf("旧档属于嵌套调用=%t", nested), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			cp := compose_v2.NewInMemoryCheckpointer()
			const root = "tenant/flow#17"
			c := compose_v2.WithCheckpointerCtx(compose_v2.WithGraphNS(ctx, root), cp)
			var writes atomic.Int32
			child := mkEchoGraph(t, "pre-upgrade", func(ctx context.Context, in *schema.Message) (*schema.Message, error) {
				answer, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "确认旧任务？"})
				if err != nil {
					return nil, err
				}
				writes.Add(1)
				return schema.AssistantMessage(in.Content + "/" + answer.Content), nil
			})
			parent := mkEchoGraph(t, "pre-upgrade-parent", func(ctx context.Context, in *schema.Message) (*schema.Message, error) {
				es, err := agent_v2.RunSubAgent(ctx, "member", child, in)
				return agent_v2.FinalMessage(es), err
			})
			entry := agent_v2.Agent(child)
			if nested {
				entry = parent
			}
			sess := agent_v2.NewRunSession()
			_, err := entry.Run(c, sess, schema.UserMessage("工单"))
			qid := partitionQuestionID(t, err)
			ns := root
			if nested {
				ns = directPendingNS(t, c, cp, sess.RunID, root+"/member#")
			}
			rows, err := cp.LoadAllInSession(c, sess.RunID)
			if err != nil {
				t.Fatal(err)
			}
			cp = compose_v2.NewInMemoryCheckpointer()
			for _, row := range rows {
				// v24 存档有任务、图身份、原提问及父子 namespace，没有 v25 新增的调用方字段。
				row.Checkpoint.Caller = nil
				row.Checkpoint.SchemaVer = 24
				wire, err := compose_v2.MarshalCheckpoint(row.Checkpoint)
				if err != nil {
					t.Fatal(err)
				}
				saved, err := compose_v2.UnmarshalCheckpoint(wire)
				if err != nil {
					t.Fatal(err)
				}
				if saved.Caller != nil || saved.SchemaVer != 24 {
					t.Fatalf("旧档构造失真：%+v", saved)
				}
				if err := cp.SaveNS(ctx, sess.RunID, row.NS, saved); err != nil {
					t.Fatal(err)
				}
			}
			c = compose_v2.WithCheckpointerCtx(c, cp)
			answer := agent_v2.WithResumeUserInputs(map[string]*schema.Message{qid: schema.UserMessage("同意")})
			beforeRoot := partitionCheckpointBytes(t, c, cp, sess.RunID, root)
			beforeChild := partitionCheckpointBytes(t, c, cp, sess.RunID, ns)
			es, err := child.Resume(compose_v2.WithGraphNS(c, ns), sess.RunID, answer)
			if nested {
				if err == nil || writes.Load() != 0 {
					t.Fatalf("无法交回的旧子档应在副作用前拒绝直续，错误 %v、写入 %d", err, writes.Load())
				}
				if !bytes.Equal(beforeRoot, partitionCheckpointBytes(t, c, cp, sess.RunID, root)) || !bytes.Equal(beforeChild, partitionCheckpointBytes(t, c, cp, sess.RunID, ns)) {
					t.Fatal("拒绝直续时改写了旧父档或子档")
				}
				es, err = parent.Resume(c, sess.RunID, answer)
			}
			if err != nil || agent_v2.FinalMessage(es) == nil || agent_v2.FinalMessage(es).Content != "工单/同意" || writes.Load() != 1 {
				t.Fatalf("兼容入口恢复结果 %v，错误 %v，写入 %d", agent_v2.FinalMessage(es), err, writes.Load())
			}
		})
	}
}

type directCompletionPartitionHook struct {
	compose_v2.NoOpOrchestrationHooks
}

func (directCompletionPartitionHook) OnGraphStart(ctx context.Context, _ compose_v2.Channels) (context.Context, error) {
	if strings.HasPrefix(compose_v2.GetGraphNS(ctx), "child#") {
		return context.WithValue(ctx, checkpointPartitionKey{}, true), nil
	}
	return ctx, nil
}

func TestDirectChildCompletionLateSelectedPartition(t *testing.T) {
	for _, direct := range []bool{false, true} {
		t.Run(fmt.Sprintf("先直接续子图=%t", direct), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			parentStore := compose_v2.NewInMemoryCheckpointer()
			childStore := compose_v2.NewInMemoryCheckpointer()
			router := &routedGraphCheckpointer{Checkpointer: parentStore, other: childStore, useOther: func(ctx context.Context) bool {
				child, _ := ctx.Value(checkpointPartitionKey{}).(bool)
				return child
			}}
			root := compose_v2.WithOrchHooks(compose_v2.WithBoundCheckpointerCtx(ctx, router), directCompletionPartitionHook{})
			var writes atomic.Int32
			child := mkEchoGraph(t, "routed-child", func(ctx context.Context, in *schema.Message) (*schema.Message, error) {
				ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "审批 " + in.Content + "？"})
				if err != nil {
					return nil, err
				}
				writes.Add(1)
				return schema.AssistantMessage(ans.Content), nil
			})
			parent := mkEchoGraph(t, "routed-parent", func(ctx context.Context, in *schema.Message) (*schema.Message, error) {
				ev, err := agent_v2.RunSubAgent(ctx, "child", child, in)
				return agent_v2.FinalMessage(ev), err
			})
			sess := agent_v2.NewRunSession()
			_, err := parent.Run(root, sess, schema.UserMessage("工单"))
			id := partitionQuestionID(t, err)
			pending, err := parentStore.ListPending(ctx, sess.RunID)
			if err != nil || len(pending) != 1 || pending[0].NS != "" {
				t.Fatalf("父分区应只含挂起根，实际 %+v，错误 %v", pending, err)
			}
			ns := directPendingNS(t, ctx, childStore, sess.RunID, "child#")
			answer := agent_v2.WithResumeUserInputs(map[string]*schema.Message{id: schema.UserMessage("同意")})
			var parentOpts []agent_v2.ResumeOption
			if direct {
				target := compose_v2.WithGraphNS(context.WithValue(root, checkpointPartitionKey{}, true), ns)
				ev, err := child.Resume(target, sess.RunID, answer)
				if err != nil || agent_v2.FinalMessage(ev) == nil || agent_v2.FinalMessage(ev).Content != "同意" || writes.Load() != 1 {
					t.Fatalf("单续子分区结果 %v，错误 %v，写入 %d", agent_v2.FinalMessage(ev), err, writes.Load())
				}
			} else {
				parentOpts = append(parentOpts, answer)
			}
			ev, err := parent.Resume(root, sess.RunID, parentOpts...)
			if err != nil || agent_v2.FinalMessage(ev) == nil || agent_v2.FinalMessage(ev).Content != "同意" || writes.Load() != 1 {
				t.Fatalf("父分区领取子分区回复 %v，错误 %v，写入 %d", agent_v2.FinalMessage(ev), err, writes.Load())
			}
			for _, st := range []compose_v2.Checkpointer{parentStore, childStore} {
				rows, err := st.LoadAllInSession(ctx, sess.RunID)
				if err != nil || len(rows) != 0 {
					t.Fatalf("任务完成后仍有 %d 份分区存档，错误 %v", len(rows), err)
				}
			}
		})
	}
}
