package background

import (
	"context"
	"errors"
	"slices"
	"strings"
	"unicode/utf8"

	"crew-go/compose_v2"
)

// Progress 是工作进程报告的最新阶段快照。Version 是业务按阶段稳定分配的递增正整数。
// Evidence 保存应用可授权读取的证据引用。
type Progress struct {
	Version  int64    `json:"version"`
	Stage    string   `json:"stage"`
	Evidence []string `json:"evidence,omitempty"`
}

var (
	// ErrProgressUnavailable 表示当前上下文没有运行中任务的上报资格。
	ErrProgressUnavailable = errors.New("background: progress requires an active worker")
	// ErrProgressConflict 表示同一个阶段版本已有不同的阶段或证据。
	ErrProgressConflict = errors.New("background: progress version contains different data")
)

type progressKey struct{}

func (progressKey) ExecutionState() {}

type progressWriter struct {
	manager *Manager
	taskID  string
	owner   string
	done    <-chan struct{}
}

// ReportProgress 保存当前子任务的阶段，由父任务通过 [Manager.Get] 读取。
// 重报相同版本和内容、以及回放更早版本，均成功且无需写入。同版本内容不同返回 [ErrProgressConflict]。
func ReportProgress(ctx context.Context, progress Progress) error {
	w, ok := ctx.Value(progressKey{}).(progressWriter)
	if !ok || compose_v2.GetSessionID(ctx) != w.taskID || compose_v2.GetOwner(ctx) != w.owner {
		return ErrProgressUnavailable
	}
	if progress.Version <= 0 || strings.TrimSpace(progress.Stage) == "" || !utf8.ValidString(progress.Stage) {
		return errors.New("background: progress requires a positive version and a valid stage")
	}
	for _, evidence := range progress.Evidence {
		if !utf8.ValidString(evidence) {
			return errors.New("background: progress evidence must be valid UTF-8")
		}
	}
	progress.Evidence = slices.Clone(progress.Evidence)
	return w.save(ctx, progress)
}

func (w progressWriter) save(ctx context.Context, progress Progress) error {
	leaseStore := w.manager.store.CheckpointTaskLeases()
	if compose_v2.CheckpointTaskLeaseToken(ctx, leaseStore, w.taskID) == "" {
		return ErrProgressUnavailable
	}
	for ctx.Err() == nil {
		select {
		case <-w.done:
			return ErrProgressUnavailable
		default:
		}
		if err := compose_v2.CheckCheckpointTaskLease(ctx); err != nil {
			return err
		}
		task, err := w.manager.store.LoadBackgroundTask(ctx, w.taskID)
		if err != nil {
			return err
		}
		if task == nil || task.Status != Running || task.CancelRequested {
			return ErrProgressUnavailable
		}
		if old := task.Progress; old != nil {
			if old.Version > progress.Version {
				return nil
			}
			if old.Version == progress.Version {
				if old.Stage != progress.Stage || !slices.Equal(old.Evidence, progress.Evidence) {
					return ErrProgressConflict
				}
				return nil
			}
		}
		task.Progress = &progress
		if saved, err := w.manager.save(ctx, task, false); err != nil || saved {
			return err
		}
	}
	return ctx.Err()
}
