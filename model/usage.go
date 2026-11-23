package model

import (
	"context"
	"errors"

	"crew-go/schema"
)

// FailedUsage 保存一次失败请求已由服务端报告的用量。没有报告的 token 不作估算。
// Usage 是累计快照；ThinkTokens 是 OutputTokens 的子集。
type FailedUsage struct {
	Usage    schema.MessageUsage `json:"usage"`
	Provider string              `json:"provider,omitempty"`
	Model    string              `json:"model,omitempty"`
}

type usageError struct {
	err   error
	usage FailedUsage
}

func (e *usageError) Error() string { return e.err.Error() }
func (e *usageError) Unwrap() error { return e.err }

// WithFailureUsage 把失败请求的用量挂到错误链，保留原有错误判据。已有用量时沿用错误中
// 的最终快照，避免把流式累计值相加。err 或 usage 为 nil 时原样返回。
func WithFailureUsage(err error, usage *schema.MessageUsage, provider, name string) error {
	if err == nil || usage == nil || UsageFromError(err) != nil {
		return err
	}
	return &usageError{err: err, usage: FailedUsage{Usage: *usage, Provider: provider, Model: name}}
}

// UsageFromError 读取失败用量的副本；返回 nil 表示服务端没有报告用量。
func UsageFromError(err error) *FailedUsage {
	var e *usageError
	if !errors.As(err, &e) {
		return nil
	}
	u := e.usage
	return &u
}

type failedUsageObserverKey struct{}
type failedUsageScope struct{ active bool }
type failedUsageObserverID struct{ active bool }
type failedUsageObserver struct {
	id      *failedUsageObserverID
	observe func(context.Context, FailedUsage)
}
type failedUsageState struct {
	scope     *failedUsageScope
	observers []*failedUsageObserver
}

func failedUsageStateFrom(ctx context.Context) *failedUsageState {
	state, _ := ctx.Value(failedUsageObserverKey{}).(*failedUsageState)
	return state
}

// WithFailedUsageObserver 添加失败用量观察者。回调按真实失败尝试触发，可能来自并行候选，
// 需自行同步共享状态。成功答复仍由原有 Message.Metadata.Usage / OnModelEnd 统计。
func WithFailedUsageObserver(ctx context.Context, observe func(context.Context, FailedUsage)) context.Context {
	if observe == nil {
		return ctx
	}
	state := failedUsageStateFrom(ctx)
	next := &failedUsageState{scope: &failedUsageScope{active: true}}
	if state != nil {
		next.scope = state.scope
		next.observers = append(next.observers, state.observers...)
	}
	next.observers = append(next.observers, &failedUsageObserver{id: &failedUsageObserverID{active: true}, observe: observe})
	return context.WithValue(ctx, failedUsageObserverKey{}, next)
}

// ResetFailedUsageObserver 为新的逻辑模型调用隔离观察者；避免嵌套 agent 向父调用重复计费。
// BasicAgent 在 OnModelStart 前调用，观察者应在该钩子内安装。
func ResetFailedUsageObserver(ctx context.Context) context.Context {
	return context.WithValue(ctx, failedUsageObserverKey{}, &failedUsageState{scope: &failedUsageScope{active: true}})
}

// KeepFailedUsageObservers 在钩子更换 ctx 时保留本次模型调用的观察者。
// 同一作用域内合并观察者；来自其他调用的观察者不随外来 ctx 混入。
func KeepFailedUsageObservers(from, to context.Context) context.Context {
	if to == nil {
		to = from
	}
	source := failedUsageStateFrom(from)
	if source == nil {
		return to
	}
	target := failedUsageStateFrom(to)
	if target == nil || target.scope != source.scope {
		return context.WithValue(to, failedUsageObserverKey{}, source)
	}
	next := &failedUsageState{scope: source.scope, observers: append([]*failedUsageObserver(nil), source.observers...)}
	for _, observer := range target.observers {
		found := false
		for _, existing := range next.observers {
			if existing == observer {
				found = true
				break
			}
		}
		if !found {
			next.observers = append(next.observers, observer)
		}
	}
	return context.WithValue(to, failedUsageObserverKey{}, next)
}

type reportedUsageError struct {
	err   error
	scope *failedUsageScope
	// 回执只保留身份，避免共享限流门通过错误持有观察者回调和整场会话。
	delivered map[*failedUsageObserverID]bool
}

func (e *reportedUsageError) Error() string { return e.err.Error() }
func (e *reportedUsageError) Unwrap() error { return e.err }

// ReportFailedUsage 为一次实际失败请求登记回执，再向当前作用域未接收过的观察者上报。
// 重试与 fallback 必须传递返回值；错误在父调用或共享限流门中再次出现时不重新计费。
// 同一个原始错误用于另一次真实请求时仍单独计数，无观察者时也登记请求已处理。
func ReportFailedUsage(ctx context.Context, err error) error {
	usage := UsageFromError(err)
	if usage == nil {
		return err
	}
	state := failedUsageStateFrom(ctx)
	var scope *failedUsageScope
	if state != nil {
		scope = state.scope
	}
	var receipt *reportedUsageError
	if errors.As(err, &receipt) && receipt.scope != scope {
		return err
	}
	delivered := map[*failedUsageObserverID]bool{}
	if receipt != nil {
		for observer := range receipt.delivered {
			delivered[observer] = true
		}
	}
	if state != nil {
		for _, observer := range state.observers {
			if delivered[observer.id] {
				continue
			}
			observer.observe(ctx, *usage)
			delivered[observer.id] = true
		}
	}
	return &reportedUsageError{err: err, scope: scope, delivered: delivered}
}
