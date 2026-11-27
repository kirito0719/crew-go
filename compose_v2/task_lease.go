package compose_v2

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"sync/atomic"
	"time"
)

// TaskLeaseProvider 是支持多进程协调的 Checkpointer 扩展。租约存储值须可比较，相等的值须指向同一存储域。
type TaskLeaseProvider interface {
	CheckpointTaskLeases() TaskLeaseStore
}

// TaskLeaseStore 按存档存储域和任务 ID 原子协调执行和放弃。租约有效期使用存储端时间，各方法遵守 ctx 取消。
// RenewTaskLease 只为当前未过期且令牌匹配的租约续约并返回 true。过期的持有者不能释放新持有者的租约。
// SaveNS 和 DeleteSubtree 需将 [CheckpointTaskLeaseToken]
// 给出的 token 与有效租约的比较和实际变更放在同一个原子操作中。
type TaskLeaseStore interface {
	TryAcquireTaskLease(ctx context.Context, sid, token string, ttl time.Duration) (bool, error)
	RenewTaskLease(ctx context.Context, sid, token string, ttl time.Duration) (bool, error)
	ReleaseTaskLease(ctx context.Context, sid, token string) error
}

const (
	taskLeaseTTL       = 30 * time.Second
	taskLeaseRenewal   = 10 * time.Second
	taskLeaseIOTimeout = 5 * time.Second
)

type checkpointTaskLeaseKey struct{}

type checkpointTaskLeaseBinding struct {
	parent *checkpointTaskLeaseBinding
	store  TaskLeaseStore
	lease  *checkpointTaskLease
}

type checkpointTaskLease struct {
	store    TaskLeaseStore
	sid      string
	token    string
	stop     context.CancelFunc
	cancel   context.CancelCauseFunc
	done     chan struct{}
	expires  atomic.Pointer[time.Time]
	lost     atomic.Bool
	released atomic.Bool
}

func checkpointTaskLeaseBindings(ctx context.Context) *checkpointTaskLeaseBinding {
	binding, _ := ctx.Value(checkpointTaskLeaseKey{}).(*checkpointTaskLeaseBinding)
	return binding
}

func bindCheckpointTaskLease(ctx context.Context, store TaskLeaseStore, lease *checkpointTaskLease) context.Context {
	return context.WithValue(ctx, checkpointTaskLeaseKey{}, &checkpointTaskLeaseBinding{
		parent: checkpointTaskLeaseBindings(ctx),
		store:  store,
		lease:  lease,
	})
}

func taskLeaseFrom(ctx context.Context, store TaskLeaseStore, sid string) *checkpointTaskLease {
	for binding := checkpointTaskLeaseBindings(ctx); binding != nil; binding = binding.parent {
		if binding.lease.sid == sid && binding.store == store {
			return binding.lease
		}
	}
	return nil
}

// CheckpointTaskLeaseToken 返回该调用的任务租约令牌。即使租约已失效也保留令牌，
// 让存储端能拒绝过期持有者的写档与删档。
func CheckpointTaskLeaseToken(ctx context.Context, store TaskLeaseStore, sid string) string {
	if lease := taskLeaseFrom(ctx, store, sid); lease != nil {
		return lease.token
	}
	return ""
}

// CheckCheckpointTaskLease 在执行边界检查续约失败及保守的本地有效期。
// 存档变更仍由存储端在同一原子操作中校验 token。
func CheckCheckpointTaskLease(ctx context.Context) error {
	binding := checkpointTaskLeaseBindings(ctx)
	if binding == nil {
		return nil
	}
	if cause := context.Cause(ctx); errors.Is(cause, ErrTaskLeaseLost) {
		return cause
	}
	for ; binding != nil; binding = binding.parent {
		lease := binding.lease
		if !lease.released.Load() && (lease.lost.Load() || !time.Now().Before(*lease.expires.Load())) {
			return lease.markLost()
		}
	}
	return nil
}

// WithCheckpointLeases 将这次登记绑定到 cp。用于已在应用锁下做进程内登记、现在可以访问后端的集成层。
// 调用方在成功和失败路径都要 Release。执行与放弃只能绑定一次。
func (a *TaskActivity) WithCheckpointLeases(ctx context.Context, cp Checkpointer) (context.Context, error) {
	return a.acquireCheckpointLeases(ctx, cp, cp, 0)
}

func (a *TaskActivity) acquireCheckpointLeases(ctx context.Context, cp, abandonedConfig Checkpointer, pendingCount int) (context.Context, error) {
	if a.cancel != nil || a.releaseLocal != nil || a.abandonedCheckpoints != nil {
		return ctx, errors.New("compose_v2: task activity already has checkpoint leases")
	}
	ctx, err := a.acquireRunCheckpointLease(ctx, cp)
	if err != nil || len(a.abandoned) == 0 || (isFrameworkNil(cp) && isFrameworkNil(abandonedConfig)) {
		return ctx, err
	}

	a.abandonedCheckpoints = make(map[string]Checkpointer, len(a.abandoned))
	for i, id := range a.abandoned {
		if _, resolved := a.abandonedCheckpoints[id]; resolved {
			continue
		}
		partition := cp
		if i < pendingCount {
			rootCtx := WithGraphNS(withSessionID(ctx, id), "")
			partition, err = ResolveCheckpointPartition(rootCtx, abandonedConfig)
			if err != nil {
				return ctx, fmt.Errorf("resolve abandoned task %q checkpoint: %w", id, err)
			}
		}
		a.abandonedCheckpoints[id] = partition
		store, err := checkpointLeaseStore(partition)
		if err != nil {
			return ctx, err
		}
		if store == nil {
			continue
		}
		if err := CheckCheckpointTaskLease(ctx); err != nil {
			return ctx, err
		}
		if existing := taskLeaseFrom(ctx, store, id); existing != nil && !existing.released.Load() {
			return ctx, &TaskBusyError{TaskID: id, operation: "cannot abandon task"}
		}
		if a.cancel == nil {
			ctx, a.cancel = context.WithCancelCause(ctx)
		}
		next, lease, err := acquireCheckpointTaskLease(ctx, store, id, a.cancel)
		if err != nil {
			return ctx, err
		}
		ctx = next
		a.sharedAbandons = append(a.sharedAbandons, lease)
	}
	return ctx, nil
}

func (a *TaskActivity) acquireRunCheckpointLease(ctx context.Context, cp Checkpointer) (context.Context, error) {
	if a.sid == "" || isFrameworkNil(cp) {
		return ctx, nil
	}
	store, err := checkpointLeaseStore(cp)
	if err != nil {
		return ctx, err
	}
	if store == nil {
		return ctx, a.acquireLocalCheckpointLease(ctx, cp)
	}
	if err := CheckCheckpointTaskLease(ctx); err != nil {
		return ctx, err
	}
	existing := taskLeaseFrom(ctx, store, a.sid)
	if existing != nil && !existing.released.Load() {
		return ctx, nil
	}
	ctx, a.cancel = context.WithCancelCause(ctx)
	outerCtx := ctx
	ctx, a.sharedRun, err = acquireCheckpointExecutionLease(ctx, store, a.sid, a.cancel)
	if err == nil && a.sharedRun != nil {
		err = verifyOuterCheckpointLeases(outerCtx, a.sid)
	}
	return ctx, err
}

func checkpointLeaseStore(cp Checkpointer) (TaskLeaseStore, error) {
	provider, ok := cp.(TaskLeaseProvider)
	if !ok {
		return nil, nil
	}
	store := provider.CheckpointTaskLeases()
	if isFrameworkNil(store) {
		return nil, nil
	}
	if !reflect.ValueOf(store).Comparable() {
		return nil, errors.New("compose_v2: checkpoint task lease store must be comparable")
	}
	return store, nil
}

// ConfirmCheckpointPartition 核对两个已解析的句柄是否指向同一分区。
// 句柄不同但都支持租约时，用 selected 持有的任务令牌在 recorded 验证续约。
func ConfirmCheckpointPartition(ctx context.Context, sid string, selected, recorded Checkpointer) (bool, error) {
	// 反射句柄相同表示同一个装箱引用，也适用于沿同一配置传递的不可比较值。
	// 比较的是引用身份，避免将两个分区中内容相同的快照当成同一个分区。
	if SameCheckpointPartition(selected, recorded) || reflect.ValueOf(selected) == reflect.ValueOf(recorded) {
		return true, nil
	}
	active, err := checkpointLeaseStore(selected)
	if err != nil || active == nil {
		return false, err
	}
	previous, err := checkpointLeaseStore(recorded)
	if err != nil || previous == nil {
		return false, err
	}
	lease := taskLeaseFrom(ctx, active, sid)
	if lease == nil || lease.released.Load() {
		return false, nil
	}
	if err := CheckCheckpointTaskLease(ctx); err != nil {
		return false, err
	}
	ok, err := renewCheckpointTaskLease(ctx, previous, lease)
	if err != nil {
		return false, fmt.Errorf("verify recorded checkpoint lease for task %q: %w", sid, err)
	}
	if !ok {
		return false, nil
	}
	if err := CheckCheckpointTaskLease(ctx); err != nil {
		return false, err
	}
	return true, nil
}

// verifyOuterCheckpointLeases 在新租约成功后检查同一任务的外层持有者。
// 目标可能是刚失租的存储域的另一个句柄，重新申请不能替代外层令牌。
func verifyOuterCheckpointLeases(ctx context.Context, sid string) error {
	for binding := checkpointTaskLeaseBindings(ctx); binding != nil; binding = binding.parent {
		lease := binding.lease
		// 通过最初的存储值验证一次即可，跳过借用这个持有者的别名。
		if lease.sid != sid || binding.store != lease.store || lease.released.Load() {
			continue
		}
		ok, err := renewCheckpointTaskLease(ctx, lease.store, lease)
		if err != nil {
			return fmt.Errorf("verify outer checkpoint lease for task %q: %w", sid, err)
		}
		if !ok {
			return lease.markLost()
		}
	}
	return CheckCheckpointTaskLease(ctx)
}

// acquireCheckpointExecutionLease 允许嵌套执行经不同客户端复用外层的租约。
// 目标后端必须用同一令牌成功续约，才把这个客户端绑定到外层持有者。
func acquireCheckpointExecutionLease(ctx context.Context, store TaskLeaseStore, sid string, cancel context.CancelCauseFunc) (context.Context, *checkpointTaskLease, error) {
	ctx, own, err := acquireCheckpointTaskLease(ctx, store, sid, cancel)
	if !errors.Is(err, ErrTaskBusy) {
		return ctx, own, err
	}
	for binding := checkpointTaskLeaseBindings(ctx); binding != nil; binding = binding.parent {
		lease := binding.lease
		if lease.sid != sid || lease.released.Load() || lease.lost.Load() {
			continue
		}
		ok, renewErr := renewCheckpointTaskLease(ctx, store, lease)
		if renewErr != nil {
			return ctx, nil, fmt.Errorf("verify inherited checkpoint lease for task %q: %w", sid, renewErr)
		}
		if !ok || lease.released.Load() {
			continue
		}
		if err := CheckCheckpointTaskLease(ctx); err != nil {
			return ctx, nil, err
		}
		return bindCheckpointTaskLease(ctx, store, lease), nil, nil
	}
	return ctx, nil, err
}

func renewCheckpointTaskLease(ctx context.Context, store TaskLeaseStore, lease *checkpointTaskLease) (bool, error) {
	ioCtx, stopIO := context.WithTimeout(ctx, taskLeaseIOTimeout)
	defer stopIO()
	return store.RenewTaskLease(ioCtx, lease.sid, lease.token, taskLeaseTTL)
}

func acquireCheckpointTaskLease(ctx context.Context, store TaskLeaseStore, sid string, cancel context.CancelCauseFunc) (context.Context, *checkpointTaskLease, error) {
	token := rand.Text()
	expires := time.Now().Add(taskLeaseTTL)
	ioCtx, stopIO := context.WithTimeout(ctx, taskLeaseIOTimeout)
	defer stopIO()
	ok, err := store.TryAcquireTaskLease(ioCtx, sid, token, taskLeaseTTL)
	if err != nil {
		// 请求可能已在服务端执行，按 token 尽力归还。
		releaseCheckpointTaskLease(store, sid, token)
		return ctx, nil, fmt.Errorf("acquire checkpoint lease for task %q: %w", sid, err)
	}
	if !ok {
		return ctx, nil, &TaskBusyError{TaskID: sid, operation: "cannot acquire checkpoint lease for task"}
	}

	renewCtx, stop := context.WithCancel(context.WithoutCancel(ctx))
	lease := &checkpointTaskLease{store: store, sid: sid, token: token, stop: stop, cancel: cancel, done: make(chan struct{})}
	lease.expires.Store(&expires)
	go lease.renew(renewCtx, taskLeaseRenewal)
	return bindCheckpointTaskLease(ctx, store, lease), lease, nil
}

func (l *checkpointTaskLease) renew(ctx context.Context, interval time.Duration) {
	defer close(l.done)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			expires := time.Now().Add(taskLeaseTTL)
			ok, err := renewCheckpointTaskLease(ctx, l.store, l)
			if ctx.Err() != nil {
				return
			}
			if err != nil || !ok {
				slog.WarnContext(ctx, "compose_v2: 存档任务租约丢失", "sid", l.sid, "err", err)
				l.markLost()
				return
			}
			l.expires.Store(&expires)
		}
	}
}

func (l *checkpointTaskLease) markLost() error {
	err := fmt.Errorf("checkpoint lease for task %q: %w", l.sid, ErrTaskLeaseLost)
	l.lost.Store(true)
	l.cancel(err)
	return err
}

func (l *checkpointTaskLease) release() {
	l.released.Store(true)
	l.stop()
	<-l.done
	releaseCheckpointTaskLease(l.store, l.sid, l.token)
}

func releaseCheckpointTaskLease(store TaskLeaseStore, sid, token string) {
	ctx, cancel := context.WithTimeout(context.Background(), taskLeaseIOTimeout)
	defer cancel()
	if err := store.ReleaseTaskLease(ctx, sid, token); err != nil {
		slog.WarnContext(ctx, "compose_v2: 归还存档任务租约失败", "sid", sid, "err", err)
	}
}
