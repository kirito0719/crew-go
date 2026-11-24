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

type fanoutSaveFailure struct {
	*compose_v2.InMemoryCheckpointer
	foreign *compose_v2.Checkpoint
	once    atomic.Bool
	mode    string
}

func (s *fanoutSaveFailure) SaveNS(ctx context.Context, sid, ns string, cp *compose_v2.Checkpoint) error {
	if ns == "blocked" && s.once.CompareAndSwap(false, true) {
		if s.mode == "io" {
			return errBackendDown
		}
		if err := s.InMemoryCheckpointer.SaveNS(ctx, sid, ns, s.foreign); err != nil {
			return err
		}
	}
	return s.InMemoryCheckpointer.SaveNS(ctx, sid, ns, cp)
}
func fanoutReportedQuestions(err error) []*compose_v2.AskQuery {
	if ie, ok := compose_v2.IsInterrupt(err); ok {
		return ie.AllQueries()
	}
	return nil
}
func TestSiblingQuestionsRemainRecoverableAfterCheckpointOwnershipConflict(t *testing.T) {
	for _, mode := range []string{"io", "collision"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 4*time.Second)
			defer cancel()
			sid := "fanout-conflict-" + mode
			capture := compose_v2.NewInMemoryCheckpointer()
			peer := compose_v2.NewBuilder().WithName("foreign-owner")
			peer.AddNode("ask", npAsk("foreign"))
			peer.AddEdge(compose_v2.StartNode, "ask")
			peer.AddEdge("ask", compose_v2.EndNode)
			other := npCompile(t, peer)
			drive := func(g *compose_v2.Graph, cp compose_v2.Checkpointer, ns string, answers map[string]*schema.Message) error {
				opts := []compose_v2.RunOption{compose_v2.WithSessionID(sid), compose_v2.WithCheckpointer(cp), compose_v2.WithRunNS(ns)}
				if answers != nil {
					opts = append(opts, compose_v2.WithResumeAnswers(answers))
				}
				return compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), opts...)
			}
			external := fanoutReportedQuestions(drive(other, capture, "blocked", nil))
			if len(external) != 1 {
				t.Fatal("expected existing external graph question")
			}
			protected, err := capture.LoadNS(ctx, sid, "blocked")
			if err != nil || protected == nil {
				t.Fatal(err)
			}
			cp := &fanoutSaveFailure{InMemoryCheckpointer: compose_v2.NewInMemoryCheckpointer(), mode: mode, foreign: protected}
			child := compose_v2.NewBuilder().WithName("local-nested")
			child.AddNode("ask", npAsk("failed-write-branch"))
			child.AddEdge(compose_v2.StartNode, "ask")
			child.AddEdge("ask", compose_v2.EndNode)
			nested := npCompile(t, child)
			parent := compose_v2.NewBuilder().WithName("parent")
			parent.AddNode("start", npAsk("enter"))
			parent.AddEdge(compose_v2.StartNode, "start")
			parent.AddNode("host", func(c context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
				return nil, compose_v2.Run(c, nested, compose_v2.NewChannelManager(), compose_v2.WithRunNS("blocked"))
			})
			parent.AddEdge("start", "host")
			parent.AddEdge("host", compose_v2.EndNode)
			var answered [2]atomic.Int32
			for i, name := range []string{"healthy-A", "healthy-B"} {
				parent.AddNode(compose_v2.NodeID(name), func(c context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
					a, e := compose_v2.Ask(c, &compose_v2.AskQuery{Question: name})
					if e != nil {
						return nil, e
					}
					if a.GetContent() != "yes-"+name {
						return nil, errors.New("wrong answer")
					}
					answered[i].Add(1)
					return nil, nil
				})
				parent.AddEdge("start", compose_v2.NodeID(name))
				parent.AddEdge(compose_v2.NodeID(name), compose_v2.EndNode)
			}
			g := npCompile(t, parent)
			gate := fanoutReportedQuestions(drive(g, cp, "", nil))
			if len(gate) != 1 || gate[0].Question != "enter" {
				t.Fatal(gate)
			}
			err = drive(g, cp, "", map[string]*schema.Message{gate[0].InterruptID: schema.UserMessage("go")})
			if len(fanoutReportedQuestions(err)) != 0 || !errors.Is(err, compose_v2.ErrCheckpointNotPersisted) {
				t.Fatalf("first err=%v", err)
			}
			root, e := cp.LoadNS(ctx, sid, "")
			if mode == "io" {
				if !errors.Is(err, compose_v2.ErrRunAborted) || root != nil || e != nil {
					t.Fatalf("lost save: err=%v root=%+v load=%v", err, root, e)
				}
				rows, e := cp.ListPending(ctx, sid)
				if len(rows) != 0 || e != nil {
					t.Fatal(rows, e)
				}
				return
			}
			if !errors.Is(err, compose_v2.ErrGraphIDConflict) || errors.Is(err, compose_v2.ErrRunAborted) || e != nil || root == nil || len(root.PendingInterrupts) != 2 {
				t.Fatalf("root=%+v e=%v err=%v", root, e, err)
			}
			answers := map[string]*schema.Message{}
			for _, q := range root.PendingInterrupts {
				if q.Question != "healthy-A" && q.Question != "healthy-B" {
					t.Fatal(q.Question)
				}
				answers[q.InterruptID] = schema.UserMessage("yes-" + q.Question)
			}
			err = drive(g, cp, "", answers)
			if !errors.Is(err, compose_v2.ErrGraphIDConflict) || answered[0].Load() != 1 || answered[1].Load() != 1 {
				t.Fatalf("approved siblings while separately blocked: A=%d B=%d err=%v", answered[0].Load(), answered[1].Load(), err)
			}
			claimed, loadErr := cp.LoadNS(ctx, sid, "blocked")
			if loadErr != nil || claimed == nil || claimed.GraphID != protected.GraphID || len(claimed.PendingInterrupts) != 1 ||
				claimed.PendingInterrupts[0].InterruptID != external[0].InterruptID {
				t.Fatalf("另一张图的原提问应保留，实际 %+v，错误 %v", claimed, loadErr)
			}
			err = drive(other, cp, "blocked", map[string]*schema.Message{external[0].InterruptID: schema.UserMessage("foreign owner's answer")})
			if err != nil {
				t.Fatalf("foreign slot: %v", err)
			}
			err = drive(g, cp, "", nil)
			rest := fanoutReportedQuestions(err)
			if len(rest) != 1 || rest[0].Question != "failed-write-branch" {
				t.Fatalf("remaining err=%v", err)
			}
			err = drive(g, cp, "", map[string]*schema.Message{rest[0].InterruptID: schema.UserMessage("own answer")})
			if err != nil || answered[0].Load() != 1 || answered[1].Load() != 1 {
				t.Fatalf("end err=%v A=%d B=%d", err, answered[0].Load(), answered[1].Load())
			}
			if rows, err := cp.LoadAllInSession(ctx, sid); err != nil || len(rows) != 0 {
				t.Fatalf("最后一个分支完成后，应清理本任务存档，剩余 %d，错误 %v", len(rows), err)
			}
		})
	}
}
