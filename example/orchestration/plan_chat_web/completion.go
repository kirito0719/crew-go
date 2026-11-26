package main

import (
	"context"
	"crypto/rand"
	"errors"
	"time"

	"crew-go/compose_v2"
)

const successCheckpointNS = "__plan_web_success"

var successHistorySize = compose_v2.LastValue[int]("history_size")

func newSuccessCheckpointManager() (*compose_v2.ChannelManager, error) {
	cm := compose_v2.NewChannelManager()
	return cm, compose_v2.RegisterChannel(cm, successHistorySize)
}

// saveSessionSuccess 在 Agent 确认成功后单独写下会话终态的凭据，根图进度留给清档。
func saveSessionSuccess(ctx context.Context, cp compose_v2.Checkpointer, sid string, historySize int) error {
	cm, err := newSuccessCheckpointManager()
	if err != nil {
		return err
	}
	if err := cm.ApplyAll([]compose_v2.ChannelWrite{compose_v2.Write(successHistorySize, historySize)}); err != nil {
		return err
	}
	receipt, err := cm.Snapshot()
	if err != nil {
		return err
	}
	receipt.GraphID = successCheckpointNS
	receipt.CreatedAt = time.Now().UnixNano()
	receipt.Epoch = rand.Text()
	return cp.SaveNS(ctx, sid, successCheckpointNS, receipt)
}

func loadSessionSuccess(ctx context.Context, cp compose_v2.Checkpointer, sid string) (int, bool, error) {
	receipt, err := cp.LoadNS(ctx, sid, successCheckpointNS)
	if err != nil || receipt == nil {
		return 0, false, err
	}
	if receipt.ID != sid || receipt.NS != successCheckpointNS || receipt.GraphID != successCheckpointNS || receipt.HasResumableWork() {
		return 0, false, errors.New("invalid session success checkpoint")
	}
	cm, err := newSuccessCheckpointManager()
	if err != nil {
		return 0, false, err
	}
	if err := cm.Restore(receipt); err != nil {
		return 0, false, err
	}
	historySize, ok := compose_v2.Get(cm.Channels(), successHistorySize)
	if !ok {
		return 0, false, errors.New("missing history size in session success checkpoint")
	}
	return historySize, true, nil
}
