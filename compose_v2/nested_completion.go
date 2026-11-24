package compose_v2

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// NestedCaller 记录嵌套 agent 的原调用位置和任务提交域。独立续跑子图时沿用这份归属，
// 完成后将事件保留在子槽，直到发起调用的父节点接收结果。
type NestedCaller struct {
	Address      string          `json:"address"`
	Kind         string          `json:"kind"`
	HistoryTrail string          `json:"history_trail,omitempty"`
	HistorySlot  string          `json:"history_slot,omitempty"`
	RootNS       string          `json:"root_ns,omitempty"`
	RootGraph    string          `json:"root_graph"`
	Thread       string          `json:"thread,omitempty"`
	Owner        string          `json:"owner,omitempty"`
	Result       json.RawMessage `json:"result,omitempty"`
}

type nestedCallKey struct{}

func (nestedCallKey) ExecutionState() {}

// NestedCompletion 连接一次独立 Resume 的图出口与 agent 最终事件。由该次调用独占。
type NestedCompletion struct {
	sid, ns string
	caller  NestedCaller
	signals signalScope
	save    func(json.RawMessage) error
	replay  func(context.Context, Checkpointer, *Checkpoint, bool) (bool, error)
}

func (cp *Checkpoint) hasNestedResult() bool {
	return cp != nil && cp.Status == CPStatusCompleted
}

// nestedTransfer 随父节点的执行进度存档。即使子槽的清理返回结果不明，父图也已持有完整回执。
type nestedTransfer struct {
	Source  *Checkpoint `json:"source"`
	Cleared bool        `json:"cleared,omitempty"`
}

func init() {
	RegisterValueType[nestedTransfer]("compose_v2.nestedTransfer")
}

// EnterNestedAgent 记录交接位置。父图续跑遇到同一调用的完成回执时，先解码事件，再接管结果和副作用。
func EnterNestedAgent(ctx context.Context, sid, kind, historySlot, historyTrail string, receive func(json.RawMessage) error) (context.Context, error) {
	acc := nodeStateAccessorFrom(ctx)
	ns := GetGraphNS(ctx)
	root := taskCommitBase(ctx)
	store := GetCheckpointer(ctx)
	if root == nil || acc == nil || acc.handover == nil || acc.handover.ns != ns || store == nil || IsCheckpointingDisabled(ctx) {
		return ctx, nil
	}
	caller := NestedCaller{
		Address: SignalRecipient(ctx, "agent-result/"+kind), Kind: kind,
		HistorySlot: historySlot, HistoryTrail: historyTrail, RootNS: GetGraphNS(root.base),
		RootGraph: root.graphID, Thread: GetThreadID(root.base), Owner: GetOwner(ctx),
	}
	h := &NestedCompletion{sid: sid, ns: ns, caller: caller}
	outer := ctx
	ctx = context.WithValue(ctx, nestedCallKey{}, h)
	if !hasResumeAdmission(ctx, sid, ns, acc.cm.TaskID()) {
		return ctx, nil
	}
	key := "compose_v2.nested-result:" + caller.Address
	memo, _ := GetStepResult[nestedTransfer](outer, key)
	h.replay = func(selected context.Context, partition Checkpointer, current *Checkpoint, finalPartition bool) (bool, error) {
		source := current
		if memo.Source != nil {
			if !memo.Cleared && !finalPartition && current == nil {
				return false, nil
			}
			if current != nil && !sameStoredCheckpoint(memo.Source, current) {
				return false, RejectCheckpointIdentity(current, "nested result changed during transfer")
			}
			source = memo.Source
		}
		if !source.hasNestedResult() {
			return false, nil
		}
		saved := source.Caller
		if saved == nil || len(saved.Result) == 0 {
			return false, RejectCheckpointIdentity(source, "completed nested checkpoint lacks its result")
		}
		if source.TaskID != acc.cm.TaskID() || saved.Address != caller.Address || saved.Owner != GetOwner(selected) ||
			saved.Kind != caller.Kind || saved.RootGraph != caller.RootGraph || saved.RootNS != caller.RootNS || saved.Thread != caller.Thread {
			return false, RejectCheckpointIdentity(source, "nested result belongs to a different caller")
		}
		if err := CheckCheckpointTaskLease(selected); err != nil {
			return false, err
		}
		if receive == nil {
			return false, fmt.Errorf("%w: missing nested result receiver", ErrCheckpointNotResumable)
		}
		if err := receive(saved.Result); err != nil {
			return false, fmt.Errorf("%w: decode nested result for %q: %w", ErrCheckpointNotResumable, ns, err)
		}
		if !memo.Cleared {
			if err := memo.transfer(outer, selected, partition, sid, ns, key, source, current); err != nil {
				return false, err
			}
		}
		EmitSignals(outer, Signals{items: source.Signals})
		for _, id := range source.ConsumedAnswers {
			getAnswerLedger(outer).record(id)
		}
		markStreamsUncertain(outer)
		return true, nil
	}
	if memo.Cleared {
		_, err := h.replay(ctx, nil, nil, false)
		return ctx, err
	}
	partition, err := ResolveCheckpointPartition(ctx, store)
	if err != nil {
		return ctx, err
	}
	current, err := partition.LoadNS(ctx, sid, ns)
	if err != nil {
		return ctx, fmt.Errorf("load nested result for %q: %w", ns, err)
	}
	_, err = h.replay(ctx, partition, current, false)
	return ctx, err
}

// transfer 先把完整回执交给持有持久续跑准入的父节点，才按世代删子槽。
// 删除已生效但回包丢失时，下一次仍能从父节点找到同一份回执。
func (m *nestedTransfer) transfer(outer, selected context.Context, partition Checkpointer, sid, ns, key string, source, current *Checkpoint) error {
	if m.Source == nil {
		m.Source = source
		if err := SaveStepResult(outer, key, *m); err != nil {
			return err
		}
	}
	if current != nil {
		start := time.Now()
		n, err := deleteTransferredResult(selected, partition, sid, ns, source)
		EmitCpCleanupHelper(selected, sid, ns, "success", n, time.Since(start).Milliseconds(), err)
		if err != nil {
			return fmt.Errorf("clear transferred nested result %q: %w", ns, err)
		}
	}
	m.Cleared = true
	return SaveStepResult(outer, key, *m)
}

func deleteTransferredResult(ctx context.Context, partition Checkpointer, sid, ns string, source *Checkpoint) (int32, error) {
	slots, err := loadCheckpointSubtree(ctx, partition, sid, ns)
	if err != nil {
		return 0, err
	}
	matched := false
	for _, slot := range slots {
		if slot.NS == ns {
			matched = sameStoredCheckpoint(source, slot.Checkpoint)
		}
		if checkpointRequiresTaskOwnership(slot.Checkpoint) && slot.Checkpoint.TaskID != source.TaskID {
			return 0, RejectCheckpointIdentity(slot.Checkpoint, "nested result subtree belongs to a different task")
		}
	}
	if !matched {
		return 0, ErrCheckpointStale
	}
	return partition.DeleteSubtree(ctx, sid, ns, CheckpointDeleteCondition{Expected: slots})
}

// RestoreNestedCaller 为从子槽直接发起的 Resume 恢复原任务提交范围与信号接收者。
func RestoreNestedCaller(ctx context.Context, cp *Checkpoint, sid, ns, kind string) (context.Context, *NestedCompletion, error) {
	if cp == nil || cp.Caller == nil {
		return ctx, nil, nil
	}
	caller := *cp.Caller
	if caller.Kind != kind || caller.Owner != GetOwner(ctx) || !IsNSInSubtree(caller.RootNS, ns) || caller.RootNS == ns {
		return ctx, nil, RejectCheckpointIdentity(cp, "nested resume caller does not match the stored task")
	}
	if thread := GetThreadID(ctx); thread != "" && thread != caller.Thread {
		return ctx, nil, RejectCheckpointIdentity(cp, "nested resume thread changed")
	}
	ctx = WithThreadIDCtx(ctx, caller.Thread)
	base := WithGraphNS(withSessionID(ctx, sid), caller.RootNS)
	ctx = context.WithValue(ctx, taskCommitContextKey{}, &taskCommitScope{base: base, graphID: caller.RootGraph})
	h := &NestedCompletion{sid: sid, ns: ns, caller: caller}
	ctx = context.WithValue(withSignalScope(ctx, &h.signals), nestedCallKey{}, h)
	return ctx, h, nil
}

// Complete 保存 agent 成功返回的完整事件。编码或落盘失败保留已准入世代，阻止副作用再次执行。
func (h *NestedCompletion) Complete(events any) error {
	if h == nil {
		return nil
	}
	if h.save == nil {
		return fmt.Errorf("%w: nested result for %q has no claimed graph", ErrCheckpointResumeUnconfirmed, h.ns)
	}
	result, err := json.Marshal(events)
	if err == nil {
		err = h.save(result)
	}
	if err != nil {
		return fmt.Errorf("%w: save nested result for %q: %w", ErrCheckpointResumeUnconfirmed, h.ns, err)
	}
	return nil
}

func (h *NestedCompletion) awaitAgentResult(ctx context.Context, cfg *runOptions, cm *ChannelManager, graphID string) {
	epoch := cm.CpEpochBaseline()
	h.save = func(result json.RawMessage) error {
		caller := h.caller
		caller.Result = result
		cp := &Checkpoint{Status: CPStatusCompleted, SchemaVer: CheckpointSchemaVer, GraphID: graphID, Caller: &caller}
		start := time.Now()
		err := exitStorageOp(ctx, exitOpWrite, "save nested result", func(c context.Context) error {
			if err := CheckCheckpointTaskLease(c); err != nil {
				return err
			}
			current, err := cfg.checkpointer.LoadNS(c, cfg.sessionID, cfg.graphNS)
			if err != nil {
				return err
			}
			if current == nil || current.Status != CPStatusResuming || current.Epoch != epoch || current.GraphID != graphID || current.TaskID != cm.TaskID() {
				return ErrCheckpointStale
			}
			cp.ConsumedAnswers = cm.consumedAnswersSnapshot()
			cp.Signals = h.signals.drain()
			start, err = writeCp(c, cfg, cm, cp)
			return err
		})
		emitCpSaveEvent(ctx, cfg, cp, start, err)
		return err
	}
}
