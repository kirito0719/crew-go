package agent_v2

import (
	"context"
	"reflect"

	"crew-go/schema"
)

// historyCoverage 仅属于一次实际读取 prior 的 BasicAgent 调用。初值在输入入队前设定，之后只由写入器的单个 worker 推进。
// 每批普通记录与自己的历史视图同时增长；其他调用写入同一 key 会导致下一次条件追加失配。
type historyCoverage struct {
	writer *historyWriter
	owner  string
	mode   HandoffHistoryMode
	store  HistoryTailAppender
	tail   HistoryCursor
	valid  bool
}

// HistoryTailAppenderFor 返回当前实例明确承接的条件追加能力。普通后端、nil 或仅嵌入底层能力的包装器返回 nil。
func HistoryTailAppenderFor(store HistoryStore) HistoryTailAppender {
	appender, ok := store.(HistoryTailAppender)
	if !ok || isFrameworkNil(appender) {
		return nil
	}
	owner := appender.HistoryAppendOwner()
	if owner == nil || !reflect.ValueOf(owner).Comparable() || owner != store {
		return nil
	}
	return appender
}

func captureHistoryCoverage(ctx context.Context, cfg *Config) *historyCoverage {
	w := historyWriterFrom(ctx)
	if w == nil || cfg.AutoCompact == nil || !cfg.AutoCompact.Enabled || isFrameworkNil(cfg.AutoCompact.Model) || cfg.HistoryCompactor != nil {
		return nil
	}
	st := HistoryTailAppenderFor(w.store)
	if st == nil {
		return nil
	}
	w.waitDrained(ctx)
	page, err := st.Load(ctx, w.key, HistoryQuery{Limit: 1})
	if err != nil || len(page.Messages) > 1 || page.More && len(page.Messages) == 0 || (len(page.Messages) == 0) != (page.Start == "") {
		return nil
	}
	return &historyCoverage{writer: w, owner: historyOwner(ctx, cfg.Name), mode: GetHandoffHistoryMode(ctx), store: st, tail: page.Start, valid: true}
}

// historyWrite 的 boundary 按盖章和过滤派生消息后的批次定位，-1 表示普通批次。
type historyWrite struct {
	msgs     []*schema.Message
	coverage *historyCoverage
	boundary int
}

func (b historyWrite) append(ctx context.Context, store HistoryStore, key HistoryKey) error {
	c := b.coverage
	if c != nil && c.valid {
		if recoverable, ok := c.store.(*RecoverableHistoryStore); ok {
			tail, matched, err := recoverable.write(ctx, key, b.msgs, &c.tail, b.boundary, true)
			c.valid = err == nil && matched && tail != "" && tail != c.tail
			if c.valid {
				c.tail = tail
			}
			return err
		}
		tail, ok, err := c.store.AppendAfter(ctx, key, c.tail, b.msgs, b.boundary)
		if err == nil && ok {
			c.valid = tail != "" && tail != c.tail // 非空批次必须推进到一个新位置。
			c.tail = tail
			return nil
		}
		c.valid = false
		if err != nil {
			// 结果不确定，重试可能重复原始记录。
			return err
		}
	}
	msgs := b.msgs
	if b.boundary >= 0 {
		msgs = append(msgs[:b.boundary:b.boundary], msgs[b.boundary+1:]...)
	}
	if len(msgs) == 0 {
		return nil
	}
	return store.Append(ctx, key, msgs)
}
