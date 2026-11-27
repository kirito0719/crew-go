package agent_v2

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"crew-go/schema"
	"crew-go/store"
)

const (
	historyWriteReceipt  = "crew_history_write_receipt"
	historyWriteFallback = "crew_history_write_fallback"
)

// ErrHistoryWritePending 表示批次已入待写库。调用方可以恢复对应会话。
var ErrHistoryWritePending = errors.New("history write pending")

// RecoverableHistoryStore 将历史批次保存到待写 Store，再按会话尾游标原子追加。
// 下次读写同一会话时补写。已追加但确认丢失时，使用批次回执识别原记录。
type RecoverableHistoryStore struct {
	history   HistoryTailAppender
	journal   store.Store
	partition string
	clock     atomic.Int64
}

// NewRecoverableHistoryStore 为支持条件追加的历史库启用可恢复写入。
// journal 应跨进程保留数据。partition 标识逻辑历史库，同库客户端须使用同值，不同库须使用不同值。
func NewRecoverableHistoryStore(history HistoryStore, journal store.Store, partition string) *RecoverableHistoryStore {
	target := HistoryTailAppenderFor(history)
	if target == nil || isFrameworkNil(journal) || partition == "" {
		panic("recoverable history requires an atomic history store, a journal, and a partition")
	}
	return &RecoverableHistoryStore{history: target, journal: journal, partition: identityHash(partition)}
}

type pendingHistoryWrite struct {
	Messages []*schema.Message `json:"messages"`
	After    *HistoryCursor    `json:"after,omitempty"`
	Boundary int               `json:"boundary"`
	Fallback bool              `json:"fallback,omitempty"`
}

type historyUnverifiedAppender interface {
	HistoryUnverifiedAppendOwner() HistoryStore
	AppendUnverifiedAfter(context.Context, HistoryKey, HistoryCursor, []*schema.Message) (HistoryCursor, bool, error)
}

func (h *RecoverableHistoryStore) namespace(key HistoryKey) store.Namespace {
	return store.NS("crew-history-writes", h.partition, identityHash(identityString(key.Owner, key.ConvID)))
}

// Append 持久化待写批次后追加历史。失败批次可由后续读写或 Recover 补写。
func (h *RecoverableHistoryStore) Append(ctx context.Context, key HistoryKey, msgs []*schema.Message) error {
	_, _, err := h.write(ctx, key, msgs, nil, -1, false)
	return err
}

// HistoryAppendOwner 返回接管条件追加的包装器。
func (h *RecoverableHistoryStore) HistoryAppendOwner() HistoryStore { return h }

// AppendAfter 按给定尾游标追加，继续保留压缩边界的条件追加语义。
func (h *RecoverableHistoryStore) AppendAfter(ctx context.Context, key HistoryKey, after HistoryCursor, msgs []*schema.Message, boundary int) (HistoryCursor, bool, error) {
	return h.write(ctx, key, msgs, &after, boundary, false)
}

// Load 先处理待写批次，再按查询读取历史库。
func (h *RecoverableHistoryStore) Load(ctx context.Context, key HistoryKey, q HistoryQuery) (HistoryPage, error) {
	if err := h.Recover(ctx, key); err != nil {
		return HistoryPage{}, err
	}
	page, err := h.history.Load(ctx, key, q)
	if err != nil {
		return HistoryPage{}, err
	}
	msgs := slices.Clone(page.Messages)
	for i, m := range msgs {
		if m == nil || m.Annotations[historyWriteReceipt] == nil {
			continue
		}
		copy := *m
		copy.Annotations = maps.Clone(m.Annotations)
		delete(copy.Annotations, historyWriteReceipt)
		delete(copy.Annotations, historyWriteFallback)
		if len(copy.Annotations) == 0 {
			copy.Annotations = nil
		}
		msgs[i] = &copy
	}
	page.Messages = msgs
	return page, nil
}

// Recover 补写指定会话中已经持久化的批次。多个调用者可同时重试同一批次。
func (h *RecoverableHistoryStore) Recover(ctx context.Context, key HistoryKey) error {
	return h.recoverBefore(ctx, h.namespace(key), key, "")
}

func (h *RecoverableHistoryStore) recoverBefore(ctx context.Context, ns store.Namespace, key HistoryKey, before string) error {
	items, err := h.pending(ctx, ns)
	if err != nil {
		return err
	}
	for _, item := range items {
		if before != "" && item.Key >= before {
			break
		}
		batch, err := decodePendingHistory(item)
		if err != nil {
			return err
		}
		if _, _, err := h.deliver(ctx, ns, key, item.Key, batch); err != nil {
			return err
		}
	}
	return nil
}

func (h *RecoverableHistoryStore) pending(ctx context.Context, ns store.Namespace) ([]*store.SearchItem, error) {
	for limit := 100; ; limit *= 2 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		page, err := h.journal.Search(ctx, ns, store.WithLimit(limit))
		if err != nil {
			return nil, fmt.Errorf("list pending history: %w", err)
		}
		if len(page) < limit {
			page = slices.DeleteFunc(slices.Clone(page), func(item *store.SearchItem) bool {
				return item == nil || !slices.Equal(item.Namespace, ns)
			})
			slices.SortFunc(page, func(a, b *store.SearchItem) int { return strings.Compare(a.Key, b.Key) })
			return page, nil
		}
		if limit > math.MaxInt/2 {
			return nil, errors.New("pending history exceeds search capacity")
		}
	}
}

func decodePendingHistory(item *store.SearchItem) (pendingHistoryWrite, error) {
	var batch pendingHistoryWrite
	wire, ok := item.Value.(string)
	if !ok {
		return batch, errors.New("pending history has an invalid payload")
	}
	decoder := json.NewDecoder(strings.NewReader(wire))
	decoder.UseNumber()
	if err := decoder.Decode(&batch); err != nil {
		return batch, fmt.Errorf("decode pending history: %w", err)
	}
	if batch.Boundary < -1 || batch.Boundary >= len(batch.Messages) || !slices.ContainsFunc(batch.Messages, func(m *schema.Message) bool {
		return m != nil && m.Annotations[historyWriteReceipt] == item.Key
	}) {
		return batch, errors.New("pending history has an invalid receipt")
	}
	return batch, nil
}

func (h *RecoverableHistoryStore) write(ctx context.Context, key HistoryKey, msgs []*schema.Message, after *HistoryCursor, boundary int, fallback bool) (HistoryCursor, bool, error) {
	if key.Owner == "" || key.ConvID == "" {
		return "", false, errors.New("history key is incomplete")
	}
	if boundary < -1 || boundary >= len(msgs) {
		return "", false, fmt.Errorf("invalid history boundary %d", boundary)
	}
	if !slices.ContainsFunc(msgs, func(m *schema.Message) bool { return m != nil }) {
		if err := h.Recover(ctx, key); err != nil {
			return "", false, err
		}
		if after != nil {
			return h.history.AppendAfter(ctx, key, *after, msgs, boundary)
		}
		return "", true, h.history.Append(ctx, key, msgs)
	}
	ns := h.namespace(key)
	id := h.nextID()
	batch := pendingHistoryWrite{Messages: make([]*schema.Message, len(msgs)), After: after, Boundary: boundary, Fallback: fallback}
	for i, m := range msgs {
		if m != nil {
			copy := *m
			copy.Annotations = maps.Clone(m.Annotations)
			if copy.Annotations == nil {
				copy.Annotations = make(map[string]any, 1)
			}
			copy.Annotations[historyWriteReceipt] = id
			delete(copy.Annotations, historyWriteFallback)
			batch.Messages[i] = &copy
		}
	}
	wire, err := json.Marshal(batch)
	if err != nil {
		return "", false, fmt.Errorf("encode pending history: %w", err)
	}
	if err := h.journal.Put(ctx, ns, id, string(wire)); err != nil {
		return "", false, fmt.Errorf("save pending history: %w", err)
	}
	if err := h.recoverBefore(ctx, ns, key, id); err != nil {
		return "", false, fmt.Errorf("%w: %w", ErrHistoryWritePending, err)
	}
	tail, ok, err := h.deliver(ctx, ns, key, id, batch)
	if err != nil {
		return "", false, fmt.Errorf("%w: %w", ErrHistoryWritePending, err)
	}
	return tail, ok, nil
}

func (h *RecoverableHistoryStore) nextID() string {
	for {
		last := h.clock.Load()
		now := max(time.Now().UnixNano(), last+1)
		if h.clock.CompareAndSwap(last, now) {
			return fmt.Sprintf("%020d-%s", now, newUUID())
		}
	}
}

func (h *RecoverableHistoryStore) deliver(ctx context.Context, ns store.Namespace, key HistoryKey, id string, batch pendingHistoryWrite) (HistoryCursor, bool, error) {
	result, accepted, err := h.appendOnce(ctx, key, id, batch)
	if err != nil {
		return "", false, err
	}
	if err := h.journal.Delete(ctx, ns, id); err != nil {
		return "", false, fmt.Errorf("confirm pending history: %w", err)
	}
	return result, accepted, nil
}

func (h *RecoverableHistoryStore) appendOnce(ctx context.Context, key HistoryKey, id string, batch pendingHistoryWrite) (HistoryCursor, bool, error) {
	var previous *HistoryCursor
	var previousMatched bool
	for {
		if err := ctx.Err(); err != nil {
			return "", false, err
		}
		tail, err := h.history.Load(ctx, key, HistoryQuery{Limit: 1})
		if err != nil {
			return "", false, fmt.Errorf("load history tail: %w", err)
		}
		at := tail.Start
		if len(tail.Messages) > 1 || (len(tail.Messages) == 0) != (at == "") || tail.More && at == "" {
			return "", false, errHistoryCursorStuck
		}
		if at != "" {
			end, matched, err := h.receiptTail(ctx, key, id, batch, tail)
			if err != nil {
				return "", false, err
			}
			if end != "" {
				return end, matched, nil
			}
		}
		matched := batch.After == nil || *batch.After == at
		if !matched && !batch.Fallback {
			return "", false, nil
		}
		if previous != nil && *previous == at {
			if batch.Fallback && matched && previousMatched {
				// 历史库拒绝生成覆盖证明。原尾游标仍在，同一待写批只追加正文。
				matched = false
			} else if batch.After != nil && !batch.Fallback {
				return "", false, nil
			} else {
				return "", false, errors.New("history append did not advance the matched tail")
			}
		}
		msgs, boundary := batch.Messages, batch.Boundary
		if !matched {
			msgs, boundary = historyFallbackMessages(batch.Messages, batch.Boundary), -1
			if len(msgs) == 0 {
				return at, false, nil
			}
			for i, m := range msgs {
				if m != nil {
					copy := *m
					copy.Annotations = maps.Clone(m.Annotations)
					copy.Annotations[historyWriteFallback] = true
					msgs[i] = &copy
				}
			}
		}
		end, ok, err := h.appendAt(ctx, key, at, msgs, boundary, batch.After == nil || !matched)
		if err != nil {
			return "", false, fmt.Errorf("append pending history: %w", err)
		}
		if ok {
			return end, matched, nil
		}
		previous = &at
		previousMatched = matched
	}
}

func (h *RecoverableHistoryStore) appendAt(ctx context.Context, key HistoryKey, tail HistoryCursor, msgs []*schema.Message, boundary int, plain bool) (HistoryCursor, bool, error) {
	if store, ok := h.history.(historyUnverifiedAppender); ok && plain {
		owner := store.HistoryUnverifiedAppendOwner()
		if owner != nil && reflect.ValueOf(owner).Comparable() && owner == h.history {
			return store.AppendUnverifiedAfter(ctx, key, tail, msgs)
		}
	}
	return h.history.AppendAfter(ctx, key, tail, msgs, boundary)
}

func historyFallbackMessages(msgs []*schema.Message, boundary int) []*schema.Message {
	result := slices.Clone(msgs)
	if boundary >= 0 {
		result = slices.Delete(result, boundary, boundary+1)
	}
	return result
}

func lastHistoryRecord(msgs []*schema.Message) int {
	i := len(msgs) - 1
	for i >= 0 && msgs[i] == nil {
		i--
	}
	return i
}

// receiptTail 找已写批次最后一条真实记录的位置。尾部有 nil 元素时，以原批次长度定位剩余行。
func (h *RecoverableHistoryStore) receiptTail(ctx context.Context, key HistoryKey, id string, batch pendingHistoryWrite, page HistoryPage) (HistoryCursor, bool, error) {
	base := batch.Messages
	fallback := base
	if batch.Fallback && batch.Boundary >= 0 {
		fallback = historyFallbackMessages(base, batch.Boundary)
	}
	last, fallbackLast := lastHistoryRecord(base), lastHistoryRecord(fallback)
	q := HistoryQuery{Limit: 1}
	if fallbackLast >= 0 && base[last].ID == fallback[fallbackLast].ID {
		q.MessageID = base[last].ID
	}
	for {
		if err := ctx.Err(); err != nil {
			return "", false, err
		}
		for _, m := range page.Messages {
			if m != nil && (m.Annotations[historyWriteReceipt] == id || sameHistoryCommitRecord(m, base[last])) {
				matched := m.Annotations[historyWriteFallback] != true
				trailing := len(base) - 1 - last
				if !matched {
					trailing = len(fallback) - 1 - fallbackLast
				}
				end, err := h.advanceReceipt(ctx, key, page.Start, trailing)
				return end, matched, err
			}
		}
		if !page.More {
			return "", false, nil
		}
		if page.Start == "" || page.Start == q.Before {
			return "", false, errHistoryCursorStuck
		}
		q.Before = page.Start
		var err error
		page, err = h.history.Load(ctx, key, q)
		if err != nil {
			return "", false, fmt.Errorf("find history receipt: %w", err)
		}
	}
}

func (h *RecoverableHistoryStore) advanceReceipt(ctx context.Context, key HistoryKey, witness HistoryCursor, trailing int) (HistoryCursor, error) {
	if trailing == 0 {
		return witness, nil
	}
	q := HistoryQuery{After: witness, Limit: 1}
	nearest := make([]HistoryCursor, trailing)
	total := 0
	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		page, err := h.history.Load(ctx, key, q)
		if err != nil {
			return "", fmt.Errorf("find trailing history records: %w", err)
		}
		if len(page.Messages) == 0 || page.Start == "" || page.Start == q.Before {
			return "", errHistoryCursorStuck
		}
		nearest[total%trailing] = page.Start
		total++
		if !page.More {
			if total < trailing {
				return "", errors.New("incomplete history receipt")
			}
			return nearest[total%trailing], nil
		}
		q.Before = page.Start
	}
}
