package main

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"slices"
	"strings"
	"time"

	"crew-go/agent_v2"
	"crew-go/example/envloader"
	"crew-go/model"
	"crew-go/model/openai"
	"crew-go/model/qwen"
)

//go:embed cases.jsonl
var examples string

type fixture struct {
	Name    string       `json:"name"`
	Input   string       `json:"input"`
	Answers []answer     `json:"answers"`
	Expect  expectations `json:"expect"`
}

type answer struct {
	Tool  string `json:"tool"`
	Value string `json:"value"`
}

type invocation struct {
	Name string         `json:"name"`
	Args map[string]any `json:"args"`
}

type expectations struct {
	Route         string       `json:"route"`
	Outcome       string       `json:"outcome"`
	Calls         []invocation `json:"calls"`
	Approvals     []string     `json:"approvals"`
	FinalContains string       `json:"finalContains"`
}

func loadFixtures(reader io.Reader) ([]fixture, error) {
	d := json.NewDecoder(reader)
	d.DisallowUnknownFields()
	var cases []fixture
	names := map[string]bool{}
	for {
		var next fixture
		if err := d.Decode(&next); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return nil, fmt.Errorf("decode case %d: %w", len(cases)+1, err)
		}
		if next.Name == "" || next.Input == "" || names[next.Name] || next.Expect.Route == "" {
			return nil, fmt.Errorf("case %d needs unique name, input and expected route", len(cases)+1)
		}
		if next.Expect.Outcome != "success" && next.Expect.Outcome != "interrupt" {
			return nil, fmt.Errorf("case %q needs outcome success or interrupt", next.Name)
		}
		for _, a := range next.Answers {
			if _, ok := agent_v2.NormalizeApprovalAnswer(a.Value); a.Tool == "" || !ok {
				return nil, fmt.Errorf("case %q has invalid approval answer for %q", next.Name, a.Tool)
			}
		}
		names[next.Name] = true
		cases = append(cases, next)
	}
	if len(cases) == 0 {
		return nil, errors.New("evaluation dataset is empty")
	}
	return cases, nil
}

func fixtureModel(mode, name string) (model.ChatModel, error) {
	switch mode {
	case "mock":
		return mockModel{}, nil
	case "qwen":
		envloader.Load()
		if os.Getenv("QWEN_API_KEY") == "" {
			return nil, errors.New("qwen evaluation requires QWEN_API_KEY")
		}
		return qwen.NewTestClient(name), nil
	case "openai":
		envloader.Load()
		key := os.Getenv("OPENAI_API_KEY")
		if key == "" {
			return nil, errors.New("openai evaluation requires OPENAI_API_KEY")
		}
		if name == "" {
			name = "gpt-5.4-mini"
		}
		return openai.NewResponsesClient(key, name), nil
	default:
		return nil, fmt.Errorf("unknown model mode %q", mode)
	}
}

func main() {
	path := flag.String("cases", "", "JSONL 数据集，留空使用内置用例")
	mode := flag.String("mode", "mock", "模型模式：mock、qwen、openai")
	name := flag.String("model", "", "真机模型名，留空使用 provider 默认模型")
	repeats := flag.Int("runs", 1, "每条用例重复次数")
	timeout := flag.Duration("timeout", 90*time.Second, "单条用例的最长执行时间")
	inputPrice := flag.Float64("input-price", 0, "每百万输入 token 单价")
	outputPrice := flag.Float64("output-price", 0, "每百万输出 token 单价")
	recordsPath := flag.String("records", "", "逐次试验与逐轮轨迹 JSONL 输出路径")
	flag.Parse()
	if *repeats <= 0 || *timeout <= 0 || *inputPrice < 0 || *outputPrice < 0 {
		log.Fatal("runs and timeout must be positive, and prices must be non-negative")
	}

	data := examples
	if *path != "" {
		bytes, err := os.ReadFile(*path)
		if err != nil {
			log.Fatal(err)
		}
		data = string(bytes)
	}
	cases, err := loadFixtures(strings.NewReader(data))
	if err != nil {
		log.Fatal(err)
	}
	llm, err := fixtureModel(*mode, *name)
	if err != nil {
		log.Fatal(err)
	}

	var records *os.File
	if *recordsPath != "" {
		records, err = os.OpenFile(*recordsPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
		if err != nil {
			log.Fatal(err)
		}
	}
	recordModel := *name
	if recordModel == "" {
		if named, ok := llm.(model.ModelNamer); ok {
			recordModel = named.ModelName()
		}
	}
	digest := sha256.Sum256([]byte(data))
	settings := evaluationSettings{Mode: *mode, Model: recordModel, DatasetSHA256: hex.EncodeToString(digest[:]), Runs: *repeats,
		TimeoutMS: timeout.Milliseconds(), InputPrice: *inputPrice, OutputPrice: *outputPrice}
	var anyPassed, allPassed, trialPassed, inTokens, outTokens int
	var elapsed []time.Duration
	for _, c := range cases {
		passed := 0
		for trial := 1; trial <= *repeats; trial++ {
			ctx, cancel := context.WithTimeout(context.Background(), *timeout)
			start := time.Now()
			o, err := runFixture(ctx, c, llm)
			cancel()
			duration := time.Since(start)
			elapsed = append(elapsed, duration)
			if records != nil {
				if writeErr := writeTrialRecord(records, c, trial, settings, duration, o, err); writeErr != nil {
					log.Fatal(writeErr)
				}
			}
			inTokens += o.InputTokens
			outTokens += o.OutputTokens
			if err != nil {
				fmt.Printf("FAIL %s [%d] %v\n", c.Name, trial, err)
				continue
			}
			passed++
		}
		fmt.Printf("%s %d/%d\n", c.Name, passed, *repeats)
		trialPassed += passed
		if passed > 0 {
			anyPassed++
		}
		if passed == *repeats {
			allPassed++
		}
	}
	if records != nil {
		if err := records.Close(); err != nil {
			log.Fatal(err)
		}
	}
	slices.Sort(elapsed)
	p95 := elapsed[int(math.Ceil(float64(len(elapsed))*0.95))-1]
	fmt.Printf("trials=%d/%d pass@%d=%d/%d pass^%d=%d/%d p95=%s input_tokens=%d output_tokens=%d",
		trialPassed, len(elapsed), *repeats, anyPassed, len(cases), *repeats, allPassed, len(cases), p95.Round(time.Millisecond), inTokens, outTokens)
	if *inputPrice != 0 || *outputPrice != 0 {
		fmt.Printf(" estimated_cost=%.6f", (float64(inTokens)**inputPrice+float64(outTokens)**outputPrice)/1_000_000)
	}
	fmt.Println()
	if trialPassed != len(elapsed) {
		os.Exit(1)
	}
}

type evaluationSettings struct {
	Mode          string  `json:"mode"`
	Model         string  `json:"model"`
	DatasetSHA256 string  `json:"dataset_sha256"`
	Runs          int     `json:"runs"`
	TimeoutMS     int64   `json:"timeout_ms"`
	InputPrice    float64 `json:"input_price_per_million"`
	OutputPrice   float64 `json:"output_price_per_million"`
}

// 每完成一次试验即写一行，失败和中断也保留已经发生的轨迹与用量。
func writeTrialRecord(w io.Writer, c fixture, trial int, config evaluationSettings, elapsed time.Duration, o observations, runErr error) error {
	record := struct {
		Version       int                `json:"version"`
		Case          string             `json:"case"`
		Trial         int                `json:"trial"`
		Config        evaluationSettings `json:"config"`
		Input         string             `json:"input"`
		Expect        expectations       `json:"expect"`
		Answers       []answer           `json:"answers"`
		Passed        bool               `json:"passed"`
		ElapsedMS     float64            `json:"elapsed_ms"`
		EstimatedCost float64            `json:"estimated_cost"`
		Error         string             `json:"error,omitempty"`
		Observed      observations       `json:"observed"`
	}{Version: 1, Case: c.Name, Trial: trial, Config: config,
		Input: c.Input, Expect: c.Expect, Answers: c.Answers, Passed: runErr == nil,
		ElapsedMS:     float64(elapsed) / float64(time.Millisecond),
		EstimatedCost: (float64(o.InputTokens)*config.InputPrice + float64(o.OutputTokens)*config.OutputPrice) / 1_000_000, Observed: o}
	if runErr != nil {
		record.Error = runErr.Error()
	}
	if err := json.NewEncoder(w).Encode(record); err != nil {
		return fmt.Errorf("write trial record: %w", err)
	}
	return nil
}
