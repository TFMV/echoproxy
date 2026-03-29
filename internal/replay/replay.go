package replay

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/TFMV/echoproxy/internal/httpx"
)

type Engine struct {
	Source   string
	Target   string
	Client   *http.Client
	Recorder *Recorder
	Logger   io.Writer

	concurrency int
	rateLimit   int
	ordering    string
	timing      bool
	workers     int
}

type Recorder struct {
	mu      sync.Mutex
	results []Result
}

type Result struct {
	Index      int            `json:"index"`
	Request    httpx.Request  `json:"request"`
	Response   httpx.Response `json:"response"`
	StatusCode int            `json:"status_code"`
	Duration   time.Duration  `json:"duration"`
	Error      string         `json:"error,omitempty"`
	Timestamp  time.Time      `json:"timestamp"`
}

type ReplayOptions struct {
	Concurrency int
	RateLimit   int
	Ordering    string
	Timing      bool
	Workers     int
}

var DefaultOptions = ReplayOptions{
	Concurrency: 1,
	RateLimit:   0,
	Ordering:    "original",
	Timing:      false,
	Workers:     4,
}

type Option func(*Engine)

func WithConcurrency(c int) Option {
	return func(e *Engine) {
		e.concurrency = c
	}
}

func WithRateLimit(r int) Option {
	return func(e *Engine) {
		e.rateLimit = r
	}
}

func WithOrdering(o string) Option {
	return func(e *Engine) {
		e.ordering = o
	}
}

func WithTiming(t bool) Option {
	return func(e *Engine) {
		e.timing = t
	}
}

func WithWorkers(w int) Option {
	return func(e *Engine) {
		e.workers = w
	}
}

func WithTarget(target string) Option {
	return func(e *Engine) {
		e.Target = target
	}
}

func WithLogger(w io.Writer) Option {
	return func(e *Engine) {
		e.Logger = w
	}
}

func New(source string, opts ...Option) (*Engine, error) {
	e := &Engine{
		Source: source,
		Client: &http.Client{
			Timeout: 30 * time.Second,
		},
		Logger:      os.Stdout,
		concurrency: 1,
		rateLimit:   0,
		ordering:    "original",
		timing:      false,
		workers:     4,
	}

	for _, opt := range opts {
		opt(e)
	}

	if e.rateLimit > 0 {
		e.Client.Timeout = time.Duration(30+60/e.rateLimit) * time.Second
	}

	return e, nil
}

func (e *Engine) LoadRequests() ([]httpx.RecordedRequest, error) {
	f, err := os.Open(e.Source)
	if err != nil {
		return nil, fmt.Errorf("open source: %w", err)
	}
	defer f.Close()

	var requests []httpx.RecordedRequest
	dec := json.NewDecoder(f)

	for {
		var rec httpx.RecordedRequest
		if err := dec.Decode(&rec); err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("decode: %w", err)
		}
		requests = append(requests, rec)
	}

	return requests, nil
}

func (e *Engine) Run(ctx context.Context) ([]Result, error) {
	requests, err := e.LoadRequests()
	if err != nil {
		return nil, err
	}

	switch e.ordering {
	case "shuffle":
		shuffleRequests(requests)
	case "reverse":
		reverseRequests(requests)
	}

	e.Recorder = &Recorder{
		results: make([]Result, 0, len(requests)),
	}

	if e.concurrency > 1 {
		return e.runConcurrent(ctx, requests)
	}

	return e.runSequential(ctx, requests)
}

func (e *Engine) runSequential(ctx context.Context, requests []httpx.RecordedRequest) ([]Result, error) {
	results := make([]Result, len(requests))

	for i, req := range requests {
		if err := ctx.Err(); err != nil {
			return results, err
		}

		result := e.replayRequest(ctx, req, i)
		results[i] = result

		e.Recorder.mu.Lock()
		e.Recorder.results = append(e.Recorder.results, result)
		e.Recorder.mu.Unlock()

		if e.rateLimit > 0 {
			time.Sleep(time.Second / time.Duration(e.rateLimit))
		}

		fmt.Fprintf(e.Logger, "[%d/%d] %s %s -> %d (%s)\n",
			i+1, len(requests),
			req.Request.Method,
			req.Request.URL,
			result.StatusCode,
			result.Duration,
		)
	}

	return results, nil
}

func (e *Engine) runConcurrent(ctx context.Context, requests []httpx.RecordedRequest) ([]Result, error) {
	results := make([]Result, len(requests))

	var wg sync.WaitGroup
	sem := make(chan struct{}, e.concurrency)
	resultsCh := make(chan Result, len(requests))

	for i, req := range requests {
		wg.Add(1)
		go func(idx int, r httpx.RecordedRequest) {
			defer wg.Done()

			sem <- struct{}{}
			defer func() { <-sem }()

			result := e.replayRequest(ctx, r, idx)
			resultsCh <- result
		}(i, req)
	}

	go func() {
		wg.Wait()
		close(resultsCh)
	}()

	completed := 0
	for result := range resultsCh {
		results[result.Index] = result

		e.Recorder.mu.Lock()
		e.Recorder.results = append(e.Recorder.results, result)
		e.Recorder.mu.Unlock()

		completed++
		fmt.Fprintf(e.Logger, "[%d/%d] %s %s -> %d (%s)\n",
			completed, len(requests),
			requests[result.Index].Request.Method,
			requests[result.Index].Request.URL,
			result.StatusCode,
			result.Duration,
		)

		if e.rateLimit > 0 {
			time.Sleep(time.Second / time.Duration(e.rateLimit))
		}
	}

	return results, nil
}

func (e *Engine) replayRequest(ctx context.Context, rec httpx.RecordedRequest, index int) Result {
	start := time.Now()

	targetURL := e.Target + rec.Request.URL
	if e.Target == "" {
		targetURL = rec.Request.URL
	}

	req, err := http.NewRequestWithContext(ctx, rec.Request.Method, targetURL, bytes.NewReader(rec.Request.Body))
	if err != nil {
		return Result{
			Index:     index,
			Request:   rec.Request,
			Error:     err.Error(),
			Timestamp: start,
		}
	}

	for k, v := range rec.Request.Headers {
		req.Header[k] = v
	}

	resp, err := e.Client.Do(req)
	duration := time.Since(start)

	if err != nil {
		return Result{
			Index:     index,
			Request:   rec.Request,
			Response:  httpx.Response{},
			Error:     err.Error(),
			Duration:  duration,
			Timestamp: start,
		}
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	result := Result{
		Index:   index,
		Request: rec.Request,
		Response: httpx.Response{
			StatusCode: resp.StatusCode,
			Status:     resp.Status,
			Headers:    resp.Header.Clone(),
			Body:       body,
			BodyHash:   httpx.ComputeBodyHash(body),
			Size:       int64(len(body)),
		},
		StatusCode: resp.StatusCode,
		Duration:   duration,
		Timestamp:  start,
	}

	return result
}

func shuffleRequests(requests []httpx.RecordedRequest) {
	sort.Slice(requests, func(i, j int) bool {
		return time.Now().UnixNano()%2 == 0
	})
}

func reverseRequests(requests []httpx.RecordedRequest) {
	for i, j := 0, len(requests)-1; i < j; i, j = i+1, j-1 {
		requests[i], requests[j] = requests[j], requests[i]
	}
}

func (r *Recorder) Results() []Result {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.results
}

func LoadLogFile(path string) ([]httpx.RecordedRequest, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	defer f.Close()

	var requests []httpx.RecordedRequest
	dec := json.NewDecoder(bufio.NewReader(f))

	for {
		var rec httpx.RecordedRequest
		if err := dec.Decode(&rec); err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("decode: %w", err)
		}
		requests = append(requests, rec)
	}

	return requests, nil
}

func MergeLogs(paths []string) ([]httpx.RecordedRequest, error) {
	var all []httpx.RecordedRequest

	for _, path := range paths {
		requests, err := LoadLogFile(path)
		if err != nil {
			return nil, fmt.Errorf("load %s: %w", path, err)
		}
		all = append(all, requests...)
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].Request.Timestamp < all[j].Request.Timestamp
	})

	return all, nil
}

func FilterByMethod(requests []httpx.RecordedRequest, method string) []httpx.RecordedRequest {
	method = strings.ToUpper(method)
	var filtered []httpx.RecordedRequest
	for _, r := range requests {
		if strings.ToUpper(r.Request.Method) == method {
			filtered = append(filtered, r)
		}
	}
	return filtered
}

func FilterByPath(requests []httpx.RecordedRequest, path string) []httpx.RecordedRequest {
	var filtered []httpx.RecordedRequest
	for _, r := range requests {
		if strings.Contains(r.Request.URL, path) {
			filtered = append(filtered, r)
		}
	}
	return filtered
}

func FilterByStatus(requests []httpx.RecordedRequest, status int) []httpx.RecordedRequest {
	var filtered []httpx.RecordedRequest
	for _, r := range requests {
		if r.Response.StatusCode == status {
			filtered = append(filtered, r)
		}
	}
	return filtered
}

func ParseTarget(target string) (*url.URL, error) {
	if target == "" {
		return nil, nil
	}
	return url.Parse(target)
}

func ValidateTarget(target string) error {
	if target == "" {
		return nil
	}

	u, err := url.Parse(target)
	if err != nil {
		return fmt.Errorf("invalid URL: %w", err)
	}

	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("invalid scheme: %s", u.Scheme)
	}

	return nil
}
