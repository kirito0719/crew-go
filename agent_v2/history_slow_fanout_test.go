package agent_v2_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

// 三个叶子都读到同一份旧快照，再让写入器提交。共享视图在首次输入的 ID 查询之后汇合；
// SelfOnly 直接按分支归属读 prior。底层保存真实消息与索引。
type slowFanoutHistory struct {
	agent_v2.HistoryStore
	slots       map[string]bool
	inputID     string
	shared      bool
	reads       *fanoutBarrier
	writing     chan struct{}
	allowWrite  chan struct{}
	firstAppend sync.Once
}

func (s *slowFanoutHistory) Load(ctx context.Context, key agent_v2.HistoryKey, q agent_v2.HistoryQuery) (agent_v2.HistoryPage, error) {
	page, err := s.HistoryStore.Load(ctx, key, q)
	slot := agent_v2.HistorySlot(ctx)
	if err == nil && s.slots[slot] &&
		(s.shared && q.MessageID == s.inputID || !s.shared && q.MessageID == "" && q.Agent == slot) {
		err = s.reads.wait(ctx)
	}
	return page, err
}

func (s *slowFanoutHistory) Append(ctx context.Context, key agent_v2.HistoryKey, msgs []*schema.Message) error {
	s.firstAppend.Do(func() { close(s.writing) })
	select {
	case <-s.allowWrite:
		return s.HistoryStore.Append(ctx, key, msgs)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestHistShape_SlowStore_ScopedAndSharedInputs(t *testing.T) {
	for _, tc := range []struct {
		name, slotFormat string
		build            func(func(int) agent_v2.Agent) agent_v2.Agent
		wantInputSlots   int
		explicitShared   bool
	}{
		{"B_内层成员各读自己", "par/br%d/s1", shapeHandoffOverParallelSeq, 3, false},
		{"B_叶子显式共享", "par/br%d/s1", shapeHandoffOverParallelSeq, 1, true},
		{"C_直接候选共享", "br%d/w", shapeParallelOverHandoff, 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			inputs := []*schema.Message{schema.UserMessage("之前的问题"), schema.UserMessage("现在的问题")}
			store := &slowFanoutHistory{HistoryStore: agent_v2.NewInMemoryHistoryStore(), slots: make(map[string]bool),
				inputID: inputs[0].ID, shared: tc.wantInputSlots == 1, reads: newFanoutBarrier(3),
				writing: make(chan struct{}), allowWrite: make(chan struct{})}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			ctx = compose_v2.WithThreadIDCtx(ctx, tc.name)
			if tc.explicitShared {
				// 此形态的业务外壳只实现 Agent，历史配置从本次请求传入。
				ctx = agent_v2.WithHistoryCtx(ctx, store, "tenant")
			}
			key := agent_v2.HistoryKey{Owner: "tenant", ConvID: tc.name}
			var models [3]*promptRecorder
			workflow := tc.build(func(n int) agent_v2.Agent {
				store.slots[fmt.Sprintf(tc.slotFormat, n)] = true
				m := &promptRecorder{resp: []*schema.Message{
					schema.AssistantMessage(fmt.Sprintf("首次答复%d", n)),
					schema.AssistantMessage(fmt.Sprintf("再次答复%d", n)),
				}}
				models[n-1] = m
				leaf := agent_v2.NewBasicAgent(agent_v2.WithName(fmt.Sprintf("worker%d", n)),
					agent_v2.WithModel(m), agent_v2.WithHistory(store, key.Owner))
				if tc.explicitShared {
					return historyViewAgent{Agent: leaf, mode: agent_v2.HandoffHistoryRaw}
				}
				return leaf
			})

			sessions := []*agent_v2.RunSession{agent_v2.NewRunSession(), agent_v2.NewRunSession()}
			runDone := make(chan struct{})
			var runErr error
			go func() {
				defer close(runDone)
				_, runErr = workflow.Run(ctx, sessions[0], inputs[0])
			}()
			release := sync.OnceFunc(func() { close(store.allowWrite) })
			defer func() {
				release()
				cancel()
				<-runDone
			}()
			select {
			case <-store.writing:
			case <-runDone:
				t.Fatalf("首次执行未抵达写入屏障，err=%v", runErr)
			case <-ctx.Done():
				t.Fatal("三个分支未在超时前抵达慢后端")
			}
			select {
			case <-store.reads.ready:
			default:
				t.Fatal("首次写入前应有三个分支拿到旧快照")
			}
			pending, err := store.HistoryStore.Load(ctx, key, agent_v2.HistoryQuery{})
			if err != nil || len(pending.Messages) != 0 {
				t.Fatalf("慢写期间应尚无持久记录，got=%v err=%v", pending.Messages, err)
			}
			release()
			<-runDone
			if runErr != nil {
				t.Fatal(runErr)
			}
			if _, err := workflow.Run(ctx, sessions[1], inputs[1]); err != nil {
				t.Fatal(err)
			}
			rows, err := agent_v2.LoadAllHistory(ctx, store.HistoryStore, key)
			if err != nil {
				t.Fatal(err)
			}
			for turn, input := range inputs {
				owners := make(map[string]int)
				for _, row := range rows {
					if row.Role != schema.RoleUser || row.Content != input.Content {
						continue
					}
					owner := historyOwnerOf(row)
					if !store.slots[owner] || row.ID != input.ID || row.Annotations[agent_v2.HistoryTaskAnnotation] != sessions[turn].RunID {
						t.Fatalf("用户记录的归属、消息或任务身份有误，got=%v", row)
					}
					owners[owner]++
				}
				if len(owners) != tc.wantInputSlots || countUserWithContent(rows, input.Content) != tc.wantInputSlots {
					t.Fatalf("第 %d 轮输入应在 %d 条流各写一次，实际 %v\n%s", turn+1, tc.wantInputSlots, owners, histDump(rows))
				}
			}
			for i, m := range models {
				if countUserWithContent(m.call(0), inputs[0].Content) != 1 || countUserWithContent(m.call(1), inputs[0].Content) != 1 ||
					countUserWithContent(m.call(1), inputs[1].Content) != 1 {
					t.Fatalf("分支 %d 应在各轮看到完整且不重复的问题\n%s", i+1, m.dump())
				}
				for j, previous := range models {
					want := i == j || store.shared
					reply := previous.resp[0].Content
					if containsMsg(m.call(1), reply) != want {
						t.Fatalf("分支 %d 第二轮对 %q 的可见性应为 %t\n%s", i+1, reply, want, m.dump())
					}
				}
				for turn, reply := range m.resp {
					found := 0
					for _, row := range rows {
						if row.Role == schema.RoleAssistant && row.Content == reply.Content {
							found++
							if historyOwnerOf(row) != fmt.Sprintf(tc.slotFormat, i+1) || row.Annotations[agent_v2.HistoryTaskAnnotation] != sessions[turn].RunID {
								t.Fatalf("答复 %q 归属错误，实际 %v", reply.Content, row.Annotations)
							}
						}
					}
					if found != 1 {
						t.Fatalf("分支 %d 第 %d 轮答复应写入一次，实际 %d\n%s", i+1, turn+1, found, histDump(rows))
					}
				}
			}
		})
	}
}
