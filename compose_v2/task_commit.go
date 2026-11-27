package compose_v2

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"sync"
)

// NewTaskCommitSignalKind 注册一类随节点和存档传递的任务提交信号。
// 外层图成功退出时调用 apply。处理器应使用任务入口的配置，并能幂等处理同一值。
// apply 返回的错误只记日志。存档恢复后的成功出口可以处理存档中的信号。
func NewTaskCommitSignalKind(name string, apply func(context.Context, Signal) error) *SignalKind {
	if apply == nil {
		panic("compose_v2: task commit signal requires an apply function")
	}
	return registerSignalKind(name, apply, false)
}

// NewRootNodeCommitSignalKind 注册由根图成员提交后处理的信号。
// 信号仍可随子调用和存档传递。根图恢复时重放已提交信号，apply 必须幂等。
func NewRootNodeCommitSignalKind(name string, apply func(context.Context, Signal) error) *SignalKind {
	if apply == nil {
		panic("compose_v2: root node commit signal requires an apply function")
	}
	return registerSignalKind(name, apply, true)
}

type taskCommitContextKey struct{}

func (taskCommitContextKey) ExecutionState() {}

type committedSignalGraphKey struct{}

func (committedSignalGraphKey) ExecutionState() {}

type committedSignalGraph struct {
	cm     *ChannelManager
	parent *committedSignalGraph
}

func withCommittedSignalGraph(ctx context.Context, cm *ChannelManager) context.Context {
	parent, _ := ctx.Value(committedSignalGraphKey{}).(*committedSignalGraph)
	return context.WithValue(ctx, committedSignalGraphKey{}, &committedSignalGraph{cm: cm, parent: parent})
}

// CommittedRootNodeSignal 读取当前执行链上某层图已提交的信号。它证明该层节点已经接受该信号，
// 调用方仍须核对外层尚未提交的范围。
func CommittedRootNodeSignal(ctx context.Context, kind *SignalKind, dest string) (Signal, bool) {
	scope := taskCommitBase(ctx)
	key := taskRecipientKeyFrom(ctx)
	if kind == nil || !kind.rootNodeCommit || len(key) == 0 || scope == nil {
		return Signal{}, false
	}
	for graph, _ := ctx.Value(committedSignalGraphKey{}).(*committedSignalGraph); graph != nil; graph = graph.parent {
		r, ok := graph.cm.committedSignal(kind.name, dest)
		if ok && r.Seal != "" && hmac.Equal([]byte(r.Seal), []byte(sealCommitSignal(key, scope, r))) {
			return Signal{Dest: r.Dest, Value: r.Value, Origin: r.Origin}, true
		}
	}
	return Signal{}, false
}

type taskCommitScope struct {
	base    context.Context
	graphID string
	locals  sync.Map
}

func taskCommitBase(ctx context.Context) *taskCommitScope {
	base, _ := ctx.Value(taskCommitContextKey{}).(*taskCommitScope)
	return base
}

// TaskCommitLocal 返回当前任务图和根节点提交处理器共享的进程内状态。
// key 使用调用包的私有可比较类型。create 无副作用，可被并发调用。图外返回 nil，状态不随存档保存。
func TaskCommitLocal(ctx context.Context, key any, create func() any) any {
	scope := taskCommitBase(ctx)
	if scope == nil {
		return nil
	}
	if value, ok := scope.locals.Load(key); ok {
		return value
	}
	if create == nil {
		return nil
	}
	value, _ := scope.locals.LoadOrStore(key, create())
	return value
}

// TaskCommitContext 返回任务最外层图进入时的 ctx，供提交信号的写入者核对外层持有的配置。
// 图外调用返回传入的 ctx。若当前调用限制了恢复答复的访问，返回的任务入口 ctx 同样受限。
func TaskCommitContext(ctx context.Context) context.Context {
	if base := taskCommitBase(ctx); base != nil {
		if resumeDeliveryFrom(ctx).addressedOnly {
			return StripResumeContext(WithAddressedResumeOnly(base.base))
		}
		return base.base
	}
	return ctx
}

type taskCommitBarrierKey struct{}

func (taskCommitBarrierKey) ExecutionState() {}

type taskCommitBarrier struct {
	mu    sync.Mutex
	apply func()
}

// WithTaskCommitBarrier 让图的宿主在业务结果生成成功后确认任务提交。用于图退出后还会调用业务代码的入口。
func WithTaskCommitBarrier(ctx context.Context) (context.Context, func()) {
	if WithinNodeScope(ctx) {
		return ctx, func() {}
	}
	barrier := &taskCommitBarrier{}
	return context.WithValue(ctx, taskCommitBarrierKey{}, barrier), func() {
		barrier.mu.Lock()
		apply := barrier.apply
		barrier.apply = nil
		barrier.mu.Unlock()
		if apply != nil {
			apply()
		}
	}
}

func finishTaskCommit(ctx context.Context, cm *ChannelManager) {
	if barrier, _ := ctx.Value(taskCommitBarrierKey{}).(*taskCommitBarrier); barrier != nil {
		barrier.mu.Lock()
		barrier.apply = func() { applyTaskCommitSignals(ctx, cm) }
		barrier.mu.Unlock()
		return
	}
	applyTaskCommitSignals(ctx, cm)
}

func sealCommitSignal(key []byte, scope *taskCommitScope, s SignalRecord) string {
	base := scope.base
	payload, _ := json.Marshal([7]string{s.Kind, s.Dest, s.Value,
		GetSessionID(base), GetGraphNS(base), scope.graphID, GetThreadID(base)}) // 字符串数组总能编码。
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(payload) // HMAC 写入不会失败。
	return hex.EncodeToString(mac.Sum(nil))
}

func applyTaskCommitSignals(ctx context.Context, cm *ChannelManager) {
	if cm == nil {
		return
	}
	applyCommitSignals(ctx, cm.signalsSnapshot(), false)
}

func applyCommitSignals(ctx context.Context, records []SignalRecord, rootNode bool) {
	key := taskRecipientKeyFrom(ctx)
	scope := taskCommitBase(ctx)
	if len(key) == 0 || scope == nil {
		return
	}
	base := scope.base
	if rootNode {
		base = context.WithValue(base, taskCommitContextKey{}, scope)
	}
	for _, record := range records {
		if record.Seal == "" || !hmac.Equal([]byte(record.Seal), []byte(sealCommitSignal(key, scope, record))) {
			continue
		}
		signalKindsMu.Lock()
		reg, ok := signalKinds[record.Kind]
		signalKindsMu.Unlock()
		if !ok || reg.kind.commit == nil || reg.kind.rootNodeCommit != rootNode {
			continue
		}
		signal := Signal{Dest: record.Dest, Value: record.Value, Origin: record.Origin}
		if err := exitStorageOp(base, exitOpWrite, record.Kind, func(c context.Context) error {
			return reg.kind.commit(c, signal)
		}); err != nil {
			slog.WarnContext(base, "compose_v2: 任务提交信号写入失败", "kind", record.Kind, "err", err)
		}
	}
}
