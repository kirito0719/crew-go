package agent_v2

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"

	"crew-go/compose_v2"
	"crew-go/schema"
)

const historyCommitAnnotation = "crew_history_commit"

var historyCommitSignal = compose_v2.NewRootNodeCommitSignalKind("agent_v2.history.commit", commitHistoryAttempt)

type historyCommitTaskKey struct{}

type historyCommitTarget struct {
	key    HistoryKey
	store  HistoryStore
	writer *historyWriter
}

type historyCommitTask struct {
	id      string
	mu      sync.Mutex
	targets map[string][]historyCommitTarget
}

func historyCommitTaskFrom(ctx context.Context) *historyCommitTask {
	task, _ := compose_v2.TaskCommitLocal(ctx, historyCommitTaskKey{}, func() any {
		return &historyCommitTask{id: compose_v2.GetSessionID(compose_v2.TaskCommitContext(ctx))}
	}).(*historyCommitTask)
	return task
}

func (t *historyCommitTask) remember(attempt string, target historyCommitTarget) {
	if t == nil || attempt == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if slices.ContainsFunc(t.targets[attempt], func(known historyCommitTarget) bool {
		return known.key == target.key && sameHistoryStore(known.store, target.store)
	}) {
		return
	}
	if t.targets == nil {
		t.targets = make(map[string][]historyCommitTarget)
	}
	t.targets[attempt] = append(t.targets[attempt], target)
}

func (t *historyCommitTask) destinations(attempt string) []historyCommitTarget {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return slices.Clone(t.targets[attempt])
}

func rememberHistoryCommitTargets(ctx context.Context, w *historyWriter) {
	attempts := historyAttempts(w.task, historyScopeTrail(ctx))
	if len(attempts) == 0 {
		return
	}
	task := historyCommitTaskFrom(ctx)
	for _, attempt := range attempts {
		task.remember(attempt, historyCommitTarget{key: w.key, store: w.store, writer: w})
	}
}

func historyAttemptPrefix(task string) string {
	return "!" + identityHash(task) + ":"
}

// markHistoryAttempt 将待提交范围放入随嵌套存档保存的历史路径。任务维度隔离脱离执行的调用。
func markHistoryAttempt(ctx, parent context.Context, call nestedCall) (context.Context, string) {
	if historySuppressed(ctx) || !compose_v2.WithinNodeScope(parent) {
		return ctx, ""
	}
	task := compose_v2.GetSessionID(ctx)
	receipt := historyReceiptFrom(ctx)
	if receipt == nil || task == "" || !call.rerunnable && len(historyAttempts(task, receipt.trail)) == 0 {
		return ctx, ""
	}
	attempt, _ := compose_v2.GetStepResult[string](parent, historyAttemptStep(call))
	if attempt == "" {
		attempt = historyAttemptPrefix(task) + newUUID()
	}
	receipt = &historyReceipt{parent: receipt.parent, trail: receipt.trail + attempt + "/"}
	return context.WithValue(ctx, historyReceiptCtxKey{}, receipt), attempt
}

func historyAttemptStep(call nestedCall) string {
	return "agent_v2.history_attempt:" + identityHash(call.identity)
}

// saveHistoryAttemptOnPause 延续实际落档的尝试。重试时从头执行的调用会获得新的历史身份。
func saveHistoryAttemptOnPause(ctx, subCtx context.Context, call nestedCall, attempt string, runErr error) {
	if attempt == "" || !compose_v2.LeavesResumableCheckpoint(runErr) {
		return
	}
	cp, err := compose_v2.ResolveCheckpointPartition(subCtx, compose_v2.GetCheckpointer(subCtx))
	if err != nil || isFrameworkNil(cp) {
		return
	}
	sid := compose_v2.GetSessionID(subCtx)
	saved, err := cp.LoadNS(subCtx, sid, call.ns)
	if err != nil || !saved.HasResumableWork() || saved.Caller == nil || !strings.Contains(saved.Caller.HistoryTrail, attempt+"/") {
		return
	}
	opts := []compose_v2.StepOption{compose_v2.WithStreams(nil)}
	if call.rerunnable {
		opts = append(opts, compose_v2.Rerunnable())
	}
	if err := compose_v2.SaveStepResult(ctx, historyAttemptStep(call), attempt, opts...); err != nil {
		slog.WarnContext(ctx, "agent_v2: 保存待续历史尝试失败", "ns", call.ns, "err", err)
	}
}

func stageHistoryCommit(ctx context.Context, attempt string) {
	if attempt == "" {
		return
	}
	task := historyCommitTaskFrom(ctx)
	if w := historyWriterFrom(ctx); w != nil {
		task.remember(attempt, historyCommitTarget{key: w.key, store: w.store, writer: w})
	}
	var keys []HistoryKey
	for _, target := range task.destinations(attempt) {
		if !slices.Contains(keys, target.key) {
			keys = append(keys, target.key)
		}
	}
	if len(keys) != 0 {
		wire, _ := json.Marshal(keys) // 键只含字符串。
		compose_v2.EmitSignal(ctx, historyCommitSignal, compose_v2.Signal{Dest: attempt, Value: string(wire)})
	}
}

// rebindHistoryCommit 在重放直接子调用时恢复它声明的历史后端。
func rebindHistoryCommit(ctx context.Context, sub Agent, signals compose_v2.Signals) {
	if historyWriterFrom(ctx) != nil {
		return
	}
	commits := signals.OfKind(historyCommitSignal)
	if len(commits) == 0 {
		return
	}
	hc := historyCfgFromCtx(ctx)
	if hc == nil {
		var conflict *historyConflict
		hc, conflict = borrowHistoryConfig([]Agent{sub})
		if conflict != nil {
			return
		}
	}
	if hc == nil || isFrameworkNil(hc.store) {
		return
	}
	key, ok := historyKeyFrom(ctx, hc)
	if !ok {
		return
	}
	task := historyCommitTaskFrom(ctx)
	for _, signal := range commits {
		var keys []HistoryKey
		if json.Unmarshal([]byte(signal.Value), &keys) == nil && slices.Contains(keys, key) {
			task.remember(signal.Dest, historyCommitTarget{key: key, store: hc.store})
		}
	}
}

func historyCommitID(attempt string) string {
	return "crew-history-commit:" + attempt
}

func sameHistoryCommitRecord(a, b *schema.Message) bool {
	attempt, ok := b.Annotations[historyCommitAnnotation].(string)
	task, _ := b.Annotations[HistoryTaskAnnotation].(string)
	return ok && attempt != "" && a.ID == b.ID && a.Role == schema.RoleSystem && b.Role == schema.RoleSystem &&
		task != "" && a.Annotations[historyCommitAnnotation] == attempt && a.Annotations[HistoryTaskAnnotation] == task
}

func commitHistoryAttempt(ctx context.Context, signal compose_v2.Signal) error {
	task := historyCommitTaskFrom(ctx)
	if task == nil || !strings.HasPrefix(signal.Dest, historyAttemptPrefix(task.id)) {
		return errors.New("history commit task changed")
	}
	var keys []HistoryKey
	if err := json.Unmarshal([]byte(signal.Value), &keys); err != nil {
		return fmt.Errorf("decode history commit: %w", err)
	}
	marker := schema.SystemMessage("")
	marker.ID = historyCommitID(signal.Dest)
	marker.Annotations = map[string]any{
		historyCommitAnnotation: signal.Dest,
		HistoryTaskAnnotation:   task.id,
	}
	if w := historyWriterFrom(ctx); w != nil && slices.Contains(keys, w.key) {
		task.remember(signal.Dest, historyCommitTarget{key: w.key, store: w.store, writer: w})
	}
	var result error
	for _, target := range task.destinations(signal.Dest) {
		if !slices.Contains(keys, target.key) {
			continue
		}
		if target.writer != nil {
			target.writer.waitDrained(ctx)
		}
		if err := appendHistoryCommit(ctx, target, marker, signal.Dest, task.id); err != nil {
			result = errors.Join(result, err)
		}
	}
	return result
}

func appendHistoryCommit(ctx context.Context, target historyCommitTarget, marker *schema.Message, attempt, task string) error {
	query := HistoryQuery{MessageID: marker.ID, Task: task, Limit: 1}
	reader := target.store
	recoverable, canRecover := target.store.(*RecoverableHistoryStore)
	if canRecover {
		// 让待写库接住提交记录，即使历史库暂时无法查证。
		reader = recoverable.history
	}
	page, err := reader.Load(ctx, target.key, query)
	if err == nil && slices.ContainsFunc(page.Messages, func(m *schema.Message) bool {
		return m != nil && query.Matches(m) && m.Annotations[historyCommitAnnotation] == attempt
	}) {
		return nil
	}
	if err != nil && !canRecover {
		return err
	}
	return target.store.Append(ctx, target.key, []*schema.Message{marker})
}

func historyAttempts(task, trail string) []string {
	if task == "" || !strings.Contains(trail, "!") {
		return nil
	}
	prefix := historyAttemptPrefix(task)
	var attempts []string
	for part := range strings.SplitSeq(trail, "/") {
		if strings.HasPrefix(part, prefix) {
			attempts = append(attempts, part)
		}
	}
	return attempts
}

// LoadCommittedHistory 读取适合作为另一会话前文的已提交记录。
// 原始审计记录使用 LoadAllHistory。复制前文时移除已结算的尝试标识。
func LoadCommittedHistory(ctx context.Context, hs HistoryStore, key HistoryKey) ([]*schema.Message, error) {
	msgs, err := LoadAllHistory(ctx, hs, key)
	if err != nil {
		return nil, err
	}
	accepted := make(map[string]string)
	for _, m := range msgs {
		if m == nil || m.Role != schema.RoleSystem {
			continue
		}
		attempt, _ := m.Annotations[historyCommitAnnotation].(string)
		task, _ := m.Annotations[HistoryTaskAnnotation].(string)
		if attempt != "" && task != "" && m.ID == historyCommitID(attempt) && strings.HasPrefix(attempt, historyAttemptPrefix(task)) {
			accepted[attempt] = task
		}
	}
	committed := make([]*schema.Message, 0, len(msgs))
	for _, m := range msgs {
		if m == nil || m.Annotations[historyCommitAnnotation] != nil {
			continue
		}
		task, _ := m.Annotations[HistoryTaskAnnotation].(string)
		trail, _ := m.Annotations[historyScopeAnnotation].(string)
		attempts := historyAttempts(task, trail)
		if slices.ContainsFunc(attempts, func(attempt string) bool { return accepted[attempt] != task }) {
			continue
		}
		if len(attempts) != 0 {
			copy := *m
			copy.Annotations = maps.Clone(m.Annotations)
			for _, attempt := range attempts {
				trail = strings.ReplaceAll(trail, attempt+"/", "")
			}
			copy.Annotations[historyScopeAnnotation] = trail
			m = &copy
		}
		committed = append(committed, m)
	}
	return committed, nil
}

type historyCommitView struct {
	source   historySource
	checked  map[string]bool
	hasDraft bool
}

// includes 在生成模型历史时检查整条祖先链的提交证明。原始查询仍能读取所有尝试。
func (v *historyCommitView) includes(ctx context.Context, m *schema.Message, local string, deduplicate bool) bool {
	if m == nil || m.Annotations[historyCommitAnnotation] != nil {
		return false
	}
	task, _ := m.Annotations[HistoryTaskAnnotation].(string)
	trail, _ := m.Annotations[historyScopeAnnotation].(string)
	acceptedDescendant := false
	for _, attempt := range slices.Backward(historyAttempts(task, trail)) {
		// 本范围内已接纳的后代可供同范围内后续节点使用，离开该范围仍需外层提交。
		if (deduplicate || acceptedDescendant) && local != "" && task == v.source.task && strings.Contains(local, attempt+"/") {
			continue
		}
		committed, checked := v.checked[attempt]
		if !checked {
			query := HistoryQuery{Task: task, MessageID: historyCommitID(attempt)}
			err := v.source.scanBackward(ctx, query, historyPageMin, true, func(proof *schema.Message, _ HistoryCursor) bool {
				committed = proof.Role == schema.RoleSystem && proof.Annotations[historyCommitAnnotation] == attempt
				return committed
			}, nil)
			if err != nil {
				slog.WarnContext(ctx, "agent_v2: 核对历史尝试提交失败", "key", v.source.key.String(), "err", err)
				committed = false
			}
			if v.checked == nil {
				v.checked = make(map[string]bool)
			}
			v.checked[attempt] = committed
		}
		if committed {
			continue
		}
		if local != "" && task == v.source.task {
			if signal, ok := compose_v2.CommittedRootNodeSignal(ctx, historyCommitSignal, attempt); ok {
				var keys []HistoryKey
				if json.Unmarshal([]byte(signal.Value), &keys) == nil && slices.Contains(keys, v.source.key) {
					acceptedDescendant = true
					// 局部证明不能作为跨轮持久摘要的覆盖依据。
					v.hasDraft = true
					continue
				}
			}
		}
		v.hasDraft = true
		return false
	}
	return true
}
