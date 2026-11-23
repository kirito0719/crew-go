package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"crew-go/fallback"
	"crew-go/model"
	"crew-go/schema"
)

type billedFailure struct{}

func (billedFailure) Generate(context.Context, []*schema.Message, ...model.Option) (*schema.Message, error) {
	return nil, model.WithFailureUsage(errors.New("provider failure"), &schema.MessageUsage{InputTokens: 11, OutputTokens: 7, ThinkTokens: 3, TotalTokens: 18}, "mock", "failed")
}
func (billedFailure) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}
func (m billedFailure) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

func TestRecordsIncludeFailedAttemptAndRealRounds(t *testing.T) {
	c := fixture{Name: "help", Input: "help", Expect: expectations{Route: "help", Outcome: "success", FinalContains: "use lookup"}}
	llm := fallback.New([]model.ChatModel{billedFailure{}, mockModel{}})
	o, err := runFixture(t.Context(), c, llm)
	if err != nil || o.InputTokens != 11 || o.OutputTokens != 7 || len(o.Rounds) != 1 || len(o.Rounds[0].FailedUsage) != 1 || o.Rounds[0].Output == nil || len(o.Rounds[0].Input) == 0 {
		t.Fatalf("missing actual trajectory: %+v, %v", o, err)
	}
	var buf bytes.Buffer
	if err := writeTrialRecord(&buf, c, 2, evaluationSettings{Mode: "mock", Model: "model", DatasetSHA256: "dataset", InputPrice: 3, OutputPrice: 9}, time.Millisecond, o, nil); err != nil {
		t.Fatal(err)
	}
	var got struct {
		Version       int          `json:"version"`
		Trial         int          `json:"trial"`
		Passed        bool         `json:"passed"`
		EstimatedCost float64      `json:"estimated_cost"`
		Observed      observations `json:"observed"`
	}
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.Version != 1 || got.Trial != 2 || !got.Passed || got.Observed.InputTokens != 11 || got.EstimatedCost != 0.000096 || len(got.Observed.Rounds) != 1 {
		t.Fatalf("record=%s", buf.String())
	}

	o, runErr := runFixture(t.Context(), c, billedFailure{})
	if runErr == nil || o.Outcome != "error" || o.InputTokens != 11 || len(o.Rounds) != 1 || o.Rounds[0].Error == "" {
		t.Fatalf("failed round lost: %+v, %v", o, runErr)
	}
	buf.Reset()
	if err := writeTrialRecord(&buf, c, 1, evaluationSettings{Mode: "mock", Model: "failed", DatasetSHA256: "dataset"}, time.Millisecond, o, runErr); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), `"passed":false`) || !strings.Contains(buf.String(), "provider failure") || !strings.Contains(buf.String(), `"input_tokens":11`) {
		t.Fatalf("failed record=%s", buf.String())
	}
}

func TestRecordKeepsToolArgumentsAndInterrupt(t *testing.T) {
	cases, err := loadFixtures(strings.NewReader(examples))
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range cases {
		o, err := runFixture(t.Context(), c, mockModel{})
		if err != nil {
			t.Fatal(err)
		}
		if c.Expect.Route == "lookup" {
			if len(o.Rounds) == 0 || len(o.Rounds[0].Tools) != 1 || o.Rounds[0].Tools[0].Name != lookupTool || len(o.Rounds[0].Preamble) == 0 {
				t.Fatalf("bound tools or preamble missing: %+v", o.Rounds)
			}
		}
		var b bytes.Buffer
		if err := writeTrialRecord(&b, c, 1, evaluationSettings{Mode: "mock", DatasetSHA256: "dataset"}, time.Millisecond, o, nil); err != nil {
			t.Fatal(err)
		}
		var got struct {
			Observed observations `json:"observed"`
		}
		if err := json.Unmarshal(b.Bytes(), &got); err != nil {
			t.Fatal(err)
		}
		if err := compare(c.Expect, got.Observed); err != nil {
			t.Fatalf("recorded trajectory differs: %v", err)
		}
	}
}

type failedRecordWriter struct{}

func (failedRecordWriter) Write([]byte) (int, error) { return 0, errors.New("disk full") }

func TestRecordWriteFailurePropagates(t *testing.T) {
	if err := writeTrialRecord(failedRecordWriter{}, fixture{}, 1, evaluationSettings{Mode: "mock", DatasetSHA256: "dataset"}, 0, observations{}, nil); err == nil || !strings.Contains(err.Error(), "disk full") {
		t.Fatal("record loss was ignored")
	}
}
