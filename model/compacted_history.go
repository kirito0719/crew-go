package model

import (
	"errors"
	"weak"

	"crew-go/schema"
)

type compactedHistoryError struct {
	err   error
	start weak.Pointer[schema.Message]
}

func (e *compactedHistoryError) Error() string { return e.err.Error() }
func (e *compactedHistoryError) Unwrap() error { return e.err }

// WithCompactedHistoryStart 记录失败请求中实际启用的服务端摘要。start 指向本次输入中的原消息。
// 此错误仅借用消息身份，缓存错误的调用方不会因此持有历史内容。
func WithCompactedHistoryStart(err error, start *schema.Message) error {
	if err == nil || start == nil {
		return err
	}
	return &compactedHistoryError{err: err, start: weak.Make(start)}
}

// CompactedHistoryStart 返回失败请求中生效的服务端摘要。调用方在本次输入视图中按对象身份核对该消息。
func CompactedHistoryStart(err error) *schema.Message {
	var window *compactedHistoryError
	if errors.As(err, &window) {
		return window.start.Value()
	}
	return nil
}
