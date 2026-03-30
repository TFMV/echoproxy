package proxy_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/TFMV/echoproxy/internal/diff"
	"github.com/TFMV/echoproxy/internal/httpx"
	"github.com/TFMV/echoproxy/internal/log"
	"github.com/TFMV/echoproxy/internal/proxy"
	"github.com/TFMV/echoproxy/internal/replay"
)

func TestRecordingBasic(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Custom", "value")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"message":"hello"}`))
	}))
	defer upstream.Close()

	logFile := filepath.Join(t.TempDir(), "test.jsonl")
	logger, err := log.NewJSONLLogger(logFile)
	if err != nil {
		t.Fatalf("create logger: %v", err)
	}

	p := proxy.New("127.0.0.1:0", upstream.URL,
		proxy.WithLogger(logger),
		proxy.WithRecord(true),
	)

	go p.Serve()
	defer p.Shutdown(context.Background())

	time.Sleep(100 * time.Millisecond)

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get("http://" + p.ListenAddr() + "/test?foo=bar")
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	if string(body) != `{"message":"hello"}` {
		t.Errorf("expected body %q, got %q", `{"message":"hello"}`, string(body))
	}

	if resp.Header.Get("X-Custom") != "value" {
		t.Errorf("expected header X-Custom: value, got %s", resp.Header.Get("X-Custom"))
	}

	time.Sleep(200 * time.Millisecond)

	logger.Sync()
	logger.Close()

	recorded, err := replay.LoadLogFile(logFile)
	if err != nil {
		t.Fatalf("load log: %v", err)
	}

	if len(recorded) != 1 {
		data, _ := os.ReadFile(logFile)
		t.Fatalf("expected 1 recorded request, got %d. file content: %s", len(recorded), string(data))
	}

	req := recorded[0]
	if req.Request.Method != "GET" {
		t.Errorf("expected method GET, got %s", req.Request.Method)
	}

	if !strings.Contains(req.Request.URL, "/test") {
		t.Errorf("expected URL to contain /test, got %s", req.Request.URL)
	}

	if req.Response.StatusCode != 200 {
		t.Errorf("expected status 200, got %d", req.Response.StatusCode)
	}

	if string(req.Response.Body) != `{"message":"hello"}` {
		t.Errorf("expected body %q, got %q", `{"message":"hello"}`, string(req.Response.Body))
	}
}

func TestRecordingLargeBody(t *testing.T) {
	largeBody := bytes.Repeat([]byte("x"), 1<<20)

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
		w.Write(body)
	}))
	defer upstream.Close()

	logFile := filepath.Join(t.TempDir(), "test.jsonl")
	logger, _ := log.NewJSONLLogger(logFile)
	defer logger.Close()

	p := proxy.New("127.0.0.1:0", upstream.URL,
		proxy.WithLogger(logger),
		proxy.WithRecord(true),
	)

	go p.Serve()
	defer p.Shutdown(context.Background())
	time.Sleep(100 * time.Millisecond)

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Post("http://"+p.ListenAddr()+"/upload", "application/octet-stream", bytes.NewReader(largeBody))
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	time.Sleep(100 * time.Millisecond)

	recorded, _ := replay.LoadLogFile(logFile)
	if len(recorded) != 1 {
		t.Fatalf("expected 1 recorded request, got %d", len(recorded))
	}

	if !bytes.Equal(recorded[0].Request.Body, largeBody) {
		t.Errorf("request body mismatch, got %d bytes, want %d", len(recorded[0].Request.Body), len(largeBody))
	}

	if !bytes.Equal(recorded[0].Response.Body, largeBody) {
		t.Errorf("response body mismatch, got %d bytes, want %d", len(recorded[0].Response.Body), len(largeBody))
	}
}

func TestRecordingBinaryBody(t *testing.T) {
	binaryBody := []byte{0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD}

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
		w.Write(body)
	}))
	defer upstream.Close()

	logFile := filepath.Join(t.TempDir(), "test.jsonl")
	logger, _ := log.NewJSONLLogger(logFile)
	defer logger.Close()

	p := proxy.New("127.0.0.1:0", upstream.URL,
		proxy.WithLogger(logger),
		proxy.WithRecord(true),
	)

	go p.Serve()
	defer p.Shutdown(context.Background())
	time.Sleep(100 * time.Millisecond)

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Post("http://"+p.ListenAddr()+"/binary", "application/octet-stream", bytes.NewReader(binaryBody))
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	time.Sleep(100 * time.Millisecond)

	recorded, _ := replay.LoadLogFile(logFile)
	if len(recorded) != 1 {
		t.Fatalf("expected 1 recorded request, got %d", len(recorded))
	}

	if !bytes.Equal(recorded[0].Request.Body, binaryBody) {
		t.Errorf("request body mismatch")
	}
}

func TestReplay(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"replayed":true}`))
	}))
	defer upstream.Close()

	logFile := filepath.Join(t.TempDir(), "test.jsonl")
	logger, err := log.NewJSONLLogger(logFile)
	if err != nil {
		t.Fatalf("create logger: %v", err)
	}
	defer logger.Close()

	p := proxy.New("127.0.0.1:0", upstream.URL,
		proxy.WithLogger(logger),
		proxy.WithRecord(true),
	)

	go p.Serve()
	defer p.Shutdown(context.Background())
	time.Sleep(100 * time.Millisecond)

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get("http://" + p.ListenAddr() + "/test")
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	resp.Body.Close()

	time.Sleep(100 * time.Millisecond)

	engine, err := replay.New(logFile, replay.WithTarget(upstream.URL))
	if err != nil {
		t.Fatalf("create engine: %v", err)
	}
	results, err := engine.Run(context.Background())
	if err != nil {
		t.Fatalf("replay failed: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	if results[0].Error != "" {
		t.Errorf("replay error: %s", results[0].Error)
	}

	if results[0].StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", results[0].StatusCode)
	}
}

func TestReplayConcurrent(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	logFile := filepath.Join(t.TempDir(), "test.jsonl")
	logger, _ := log.NewJSONLLogger(logFile)
	defer logger.Close()

	p := proxy.New("127.0.0.1:0", upstream.URL,
		proxy.WithLogger(logger),
		proxy.WithRecord(true),
	)

	go p.Serve()
	defer p.Shutdown(context.Background())
	time.Sleep(100 * time.Millisecond)

	client := &http.Client{Timeout: 5 * time.Second}
	for i := 0; i < 5; i++ {
		resp, _ := client.Get("http://" + p.ListenAddr() + "/test")
		resp.Body.Close()
	}

	time.Sleep(100 * time.Millisecond)

	engine, _ := replay.New(logFile, replay.WithTarget(upstream.URL), replay.WithConcurrency(3))
	results, err := engine.Run(context.Background())
	if err != nil {
		t.Fatalf("replay failed: %v", err)
	}

	if len(results) != 5 {
		t.Fatalf("expected 5 results, got %d", len(results))
	}

	for _, r := range results {
		if r.Error != "" {
			t.Errorf("replay error: %s", r.Error)
		}
	}
}

func TestDiffJSON(t *testing.T) {
	recA := httpx.RecordedRequest{
		Request: httpx.Request{
			Method: "GET",
			URL:    "/test",
			Body:   []byte(`{"a":1}`),
		},
		Response: httpx.Response{
			StatusCode: 200,
			Body:       []byte(`{"status":"ok","count":1}`),
			Headers:    http.Header{"Content-Type": []string{"application/json"}},
		},
	}

	recB := httpx.RecordedRequest{
		Request: httpx.Request{
			Method: "GET",
			URL:    "/test",
			Body:   []byte(`{"a":1}`),
		},
		Response: httpx.Response{
			StatusCode: 200,
			Body:       []byte(`{"status":"ok","count":2}`),
			Headers:    http.Header{"Content-Type": []string{"application/json"}},
		},
	}

	result := diff.Compare(recA, recB, diff.DefaultOptions)

	if result.Equal {
		t.Error("expected diff, got equal")
	}

	if result.BodyDiff == nil {
		t.Fatal("expected body diff")
	}

	if result.BodyDiff.Match {
		t.Error("expected body diff to not match")
	}

	if len(result.BodyDiff.JSONDiffs) == 0 {
		t.Error("expected JSON diffs")
	}

	foundCountDiff := false
	for _, d := range result.BodyDiff.JSONDiffs {
		if d.Path == "count" {
			foundCountDiff = true
		}
	}
	if !foundCountDiff {
		t.Error("expected diff at path 'count'")
	}
}

func TestDiffStatusMismatch(t *testing.T) {
	recA := httpx.RecordedRequest{
		Request: httpx.Request{
			Method: "GET",
			URL:    "/test",
		},
		Response: httpx.Response{
			StatusCode: 200,
			Body:       []byte(`{}`),
		},
	}

	recB := httpx.RecordedRequest{
		Request: httpx.Request{
			Method: "GET",
			URL:    "/test",
		},
		Response: httpx.Response{
			StatusCode: 500,
			Body:       []byte(`{}`),
		},
	}

	result := diff.Compare(recA, recB, diff.DefaultOptions)

	if result.Equal {
		t.Error("expected diff, got equal")
	}

	if result.StatusMatch {
		t.Error("expected status mismatch")
	}
}

func TestDiffHeaders(t *testing.T) {
	recA := httpx.RecordedRequest{
		Request: httpx.Request{
			Method: "GET",
			URL:    "/test",
		},
		Response: httpx.Response{
			StatusCode: 200,
			Body:       []byte(`{}`),
			Headers:    http.Header{"X-Custom": []string{"value-a"}},
		},
	}

	recB := httpx.RecordedRequest{
		Request: httpx.Request{
			Method: "GET",
			URL:    "/test",
		},
		Response: httpx.Response{
			StatusCode: 200,
			Body:       []byte(`{}`),
			Headers:    http.Header{"X-Custom": []string{"value-b"}},
		},
	}

	result := diff.Compare(recA, recB, diff.DefaultOptions)

	if result.Equal {
		t.Error("expected diff, got equal")
	}

	if _, ok := result.HeaderDiffs["x-custom"]; !ok {
		t.Error("expected header diff for x-custom")
	}
}

func TestShadowNonBlocking(t *testing.T) {
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(50 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`primary`))
	}))
	defer primary.Close()

	shadow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(500 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`shadow`))
	}))
	defer shadow.Close()

	logFile := filepath.Join(t.TempDir(), "test.jsonl")
	logger, _ := log.NewJSONLLogger(logFile)
	defer logger.Close()

	p := proxy.New("127.0.0.1:0", primary.URL,
		proxy.WithLogger(logger),
		proxy.WithShadowMode(shadow.URL, 10*time.Second),
	)

	go p.Serve()
	defer p.Shutdown(context.Background())
	time.Sleep(100 * time.Millisecond)

	start := time.Now()

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get("http://" + p.ListenAddr() + "/test")
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	elapsed := time.Since(start)

	if string(body) != "primary" {
		t.Errorf("expected primary response, got %q", string(body))
	}

	if elapsed > 200*time.Millisecond {
		t.Errorf("primary response blocked by shadow, took %v", elapsed)
	}
}

func TestShadowDivergence(t *testing.T) {
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`primary`))
	}))
	defer primary.Close()

	shadow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`shadow`))
	}))
	defer shadow.Close()

	logFile := filepath.Join(t.TempDir(), "test.jsonl")
	logger, _ := log.NewJSONLLogger(logFile)
	defer logger.Close()

	p := proxy.New("127.0.0.1:0", primary.URL,
		proxy.WithLogger(logger),
		proxy.WithShadowMode(shadow.URL, 10*time.Second),
	)

	go p.Serve()
	defer p.Shutdown(context.Background())
	time.Sleep(100 * time.Millisecond)

	client := &http.Client{Timeout: 5 * time.Second}
	resp, _ := client.Get("http://" + p.ListenAddr() + "/test")
	resp.Body.Close()

	time.Sleep(200 * time.Millisecond)
}

func TestDiffBinary(t *testing.T) {
	recA := httpx.RecordedRequest{
		Request: httpx.Request{
			Method: "GET",
			URL:    "/test",
		},
		Response: httpx.Response{
			StatusCode: 200,
			Body:       []byte{0x00, 0x01, 0x02},
		},
	}

	recB := httpx.RecordedRequest{
		Request: httpx.Request{
			Method: "GET",
			URL:    "/test",
		},
		Response: httpx.Response{
			StatusCode: 200,
			Body:       []byte{0x00, 0x01, 0x03},
		},
	}

	result := diff.Compare(recA, recB, diff.DefaultOptions)

	if result.Equal {
		t.Error("expected diff, got equal")
	}

	if result.BodyDiff == nil {
		t.Fatal("expected body diff")
	}

	if result.BodyDiff.Type != "binary" {
		t.Errorf("expected binary diff type, got %s", result.BodyDiff.Type)
	}
}

func TestReplayWithRateLimit(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	logFile := filepath.Join(t.TempDir(), "test.jsonl")
	logger, _ := log.NewJSONLLogger(logFile)
	defer logger.Close()

	p := proxy.New("127.0.0.1:0", upstream.URL,
		proxy.WithLogger(logger),
		proxy.WithRecord(true),
	)

	go p.Serve()
	defer p.Shutdown(context.Background())
	time.Sleep(100 * time.Millisecond)

	client := &http.Client{Timeout: 5 * time.Second}
	for i := 0; i < 3; i++ {
		resp, _ := client.Get("http://" + p.ListenAddr() + "/test")
		resp.Body.Close()
	}

	time.Sleep(100 * time.Millisecond)

	engine, _ := replay.New(logFile, replay.WithTarget(upstream.URL), replay.WithRateLimit(2))
	start := time.Now()
	results, _ := engine.Run(context.Background())
	elapsed := time.Since(start)

	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	if elapsed < 1*time.Second {
		t.Errorf("expected rate limiting to slow down requests, took %v", elapsed)
	}
}

func TestRecordingTiming(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(50 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	logFile := filepath.Join(t.TempDir(), "test.jsonl")
	logger, _ := log.NewJSONLLogger(logFile)
	defer logger.Close()

	p := proxy.New("127.0.0.1:0", upstream.URL,
		proxy.WithLogger(logger),
		proxy.WithRecord(true),
	)

	go p.Serve()
	defer p.Shutdown(context.Background())
	time.Sleep(100 * time.Millisecond)

	client := &http.Client{Timeout: 5 * time.Second}
	resp, _ := client.Get("http://" + p.ListenAddr() + "/test")
	resp.Body.Close()

	time.Sleep(100 * time.Millisecond)

	recorded, _ := replay.LoadLogFile(logFile)
	if len(recorded) != 1 {
		t.Fatalf("expected 1 recorded request, got %d", len(recorded))
	}

	if recorded[0].Timing.Total < 40*time.Millisecond {
		t.Errorf("expected timing > 40ms, got %v", recorded[0].Timing.Total)
	}
}

func BenchmarkDiffJSON(b *testing.B) {
	recA := httpx.RecordedRequest{
		Request: httpx.Request{
			Method: "GET",
			URL:    "/test",
			Body:   []byte(`{"a":1}`),
		},
		Response: httpx.Response{
			StatusCode: 200,
			Body:       []byte(`{"status":"ok","count":1}`),
			Headers:    http.Header{"Content-Type": []string{"application/json"}},
		},
	}

	recB := httpx.RecordedRequest{
		Request: httpx.Request{
			Method: "GET",
			URL:    "/test",
			Body:   []byte(`{"a":1}`),
		},
		Response: httpx.Response{
			StatusCode: 200,
			Body:       []byte(`{"status":"ok","count":2}`),
			Headers:    http.Header{"Content-Type": []string{"application/json"}},
		},
	}

	for i := 0; i < b.N; i++ {
		diff.Compare(recA, recB, diff.DefaultOptions)
	}
}

func TestRaceConditions(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	logFile := filepath.Join(t.TempDir(), "test.jsonl")
	logger, _ := log.NewJSONLLogger(logFile)
	defer logger.Close()

	p := proxy.New("127.0.0.1:0", upstream.URL,
		proxy.WithLogger(logger),
		proxy.WithRecord(true),
	)

	go p.Serve()
	defer p.Shutdown(context.Background())
	time.Sleep(100 * time.Millisecond)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client := &http.Client{Timeout: 5 * time.Second}
			for j := 0; j < 10; j++ {
				resp, _ := client.Get("http://" + p.ListenAddr() + "/test")
				if resp != nil {
					resp.Body.Close()
				}
			}
		}()
	}

	wg.Wait()
}

func TestProxyStats(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	logFile := filepath.Join(t.TempDir(), "test.jsonl")
	logger, _ := log.NewJSONLLogger(logFile)
	defer logger.Close()

	p := proxy.New("127.0.0.1:0", upstream.URL,
		proxy.WithLogger(logger),
		proxy.WithRecord(true),
	)

	go p.Serve()
	defer p.Shutdown(context.Background())
	time.Sleep(100 * time.Millisecond)

	client := &http.Client{Timeout: 5 * time.Second}
	for i := 0; i < 5; i++ {
		resp, _ := client.Get("http://" + p.ListenAddr() + "/test")
		if resp != nil {
			resp.Body.Close()
		}
	}

	time.Sleep(100 * time.Millisecond)

	stats := p.Stats()
	if stats.Requests != 5 {
		t.Errorf("expected 5 requests, got %d", stats.Requests)
	}

	if stats.Responses != 5 {
		t.Errorf("expected 5 responses, got %d", stats.Responses)
	}
}

func TestRecordingJSONLFormat(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"test":true}`))
	}))
	defer upstream.Close()

	logFile := filepath.Join(t.TempDir(), "test.jsonl")
	logger, err := log.NewJSONLLogger(logFile)
	if err != nil {
		t.Fatalf("create logger: %v", err)
	}
	defer logger.Close()

	p := proxy.New("127.0.0.1:0", upstream.URL,
		proxy.WithLogger(logger),
		proxy.WithRecord(true),
	)

	go p.Serve()
	defer p.Shutdown(context.Background())
	time.Sleep(100 * time.Millisecond)

	client := &http.Client{Timeout: 5 * time.Second}
	resp, _ := client.Get("http://" + p.ListenAddr() + "/test")
	resp.Body.Close()

	time.Sleep(100 * time.Millisecond)

	data, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("read log: %v", err)
	}

	var rec httpx.RecordedRequest
	if err := json.Unmarshal(data, &rec); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if rec.Request.Method != "GET" {
		t.Errorf("expected GET, got %s", rec.Request.Method)
	}

	if rec.Response.StatusCode != 200 {
		t.Errorf("expected 200, got %d", rec.Response.StatusCode)
	}
}

var _ = fmt.Sprintf
var _ = sync.Mutex{}
