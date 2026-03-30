package log

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/TFMV/echoproxy/internal/httpx"
)

type Level string

const (
	LevelDebug Level = "debug"
	LevelInfo  Level = "info"
	LevelWarn  Level = "warn"
	LevelError Level = "error"
)

type Entry struct {
	Timestamp time.Time `json:"timestamp"`
	Level     Level     `json:"level"`
	Message   string    `json:"message"`
	Fields    Fields    `json:"fields,omitempty"`
}

type Fields map[string]interface{}

type Logger struct {
	output  io.Writer
	mu      sync.Mutex
	bufSize int
	flushCh chan Entry
	stopCh  chan struct{}
	wg      sync.WaitGroup

	level Level
	ctx   context.Context
}

type LoggerOption func(*Logger)

func WithLevel(level Level) LoggerOption {
	return func(l *Logger) {
		l.level = level
	}
}

func WithContext(ctx context.Context) LoggerOption {
	return func(l *Logger) {
		l.ctx = ctx
	}
}

func WithBufferSize(size int) LoggerOption {
	return func(l *Logger) {
		l.bufSize = size
	}
}

func New(output io.Writer, opts ...LoggerOption) *Logger {
	l := &Logger{
		output:  output,
		bufSize: 4096,
		flushCh: make(chan Entry, 1024),
		stopCh:  make(chan struct{}),
		level:   LevelInfo,
		ctx:     context.Background(),
	}

	for _, opt := range opts {
		opt(l)
	}

	l.wg.Add(1)
	go l.writer()

	return l
}

func (l *Logger) writer() {
	defer l.wg.Done()

	encoder := json.NewEncoder(l.output)
	encoder.SetEscapeHTML(false)

	for {
		select {
		case <-l.stopCh:
			return
		case entry := <-l.flushCh:
			l.mu.Lock()
			encoder.Encode(entry)
			l.mu.Unlock()
		}
	}
}

func (l *Logger) Log(level Level, msg string, fields Fields) {
	if !l.shouldLog(level) {
		return
	}

	entry := Entry{
		Timestamp: time.Now().UTC(),
		Level:     level,
		Message:   msg,
		Fields:    fields,
	}

	select {
	case <-l.stopCh:
	case l.flushCh <- entry:
	default:
	}
}

func (l *Logger) shouldLog(level Level) bool {
	levels := map[Level]int{
		LevelDebug: 0,
		LevelInfo:  1,
		LevelWarn:  2,
		LevelError: 3,
	}
	return levels[level] >= levels[l.level]
}

func (l *Logger) Debug(msg string, fields Fields) {
	l.Log(LevelDebug, msg, fields)
}

func (l *Logger) Info(msg string, fields Fields) {
	l.Log(LevelInfo, msg, fields)
}

func (l *Logger) Warn(msg string, fields Fields) {
	l.Log(LevelWarn, msg, fields)
}

func (l *Logger) Error(msg string, fields Fields) {
	l.Log(LevelError, msg, fields)
}

func (l *Logger) Shutdown() error {
	close(l.stopCh)
	l.wg.Wait()
	return nil
}

type JSONLLogger struct {
	*Logger
	file      *os.File
	bufWriter *bufio.Writer
	encoder   *json.Encoder
}

func NewJSONLLogger(path string, opts ...LoggerOption) (*JSONLLogger, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create log dir: %w", err)
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("open log file: %w", err)
	}

	bw := bufio.NewWriter(f)
	logger := New(bw, opts...)

	return &JSONLLogger{
		Logger:    logger,
		file:      f,
		bufWriter: bw,
		encoder:   json.NewEncoder(bw),
	}, nil
}

func (l *JSONLLogger) WriteRecordedRequest(rec httpx.RecordedRequest) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.encoder.Encode(rec)
}

func (l *JSONLLogger) WriteShadowResult(req httpx.Request, primary, shadow httpx.Response, divergence *Divergence) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	fields := Fields{
		"request":        req,
		"primary":        primary,
		"shadow":         shadow,
		"has_divergence": divergence != nil,
	}
	if divergence != nil {
		fields["divergence"] = divergence
	}

	entry := Entry{
		Timestamp: time.Now().UTC(),
		Level:     LevelInfo,
		Message:   "shadow_result",
		Fields:    fields,
	}
	return l.encoder.Encode(entry)
}

func (l *JSONLLogger) Sync() error {
	if l.bufWriter != nil {
		return l.bufWriter.Flush()
	}
	return nil
}

func (l *JSONLLogger) Shutdown() error {
	l.Logger.Shutdown()
	return l.file.Close()
}

type Divergence struct {
	StatusMismatch bool              `json:"status_mismatch"`
	HeaderDiffs    map[string]string `json:"header_diffs"`
	BodyDiff       *BodyDiff         `json:"body_diff"`
	PrimaryHash    string            `json:"primary_hash"`
	ShadowHash     string            `json:"shadow_hash"`
}

type BodyDiff struct {
	Type     string `json:"type"`
	Match    bool   `json:"match"`
	JSONDiff []byte `json:"json_diff,omitempty"`
	Message  string `json:"message"`
}

func (l *JSONLLogger) Close() error {
	return l.Shutdown()
}

type RotatingLogger struct {
	*JSONLLogger
	MaxSize   int64
	MaxAge    int
	Compress  bool
	currentSz int64
	file      *os.File
}

func NewRotatingLogger(path string, maxSize int64, maxAge int, compress bool, opts ...LoggerOption) (*RotatingLogger, error) {
	jlog, err := NewJSONLLogger(path, opts...)
	if err != nil {
		return nil, err
	}

	return &RotatingLogger{
		JSONLLogger: jlog,
		MaxSize:     maxSize,
		MaxAge:      maxAge,
		Compress:    compress,
		currentSz:   0,
	}, nil
}

func (r *RotatingLogger) shouldRotate() bool {
	return r.currentSz >= r.MaxSize
}

func (r *RotatingLogger) rotate() error {
	ts := time.Now().Format("20060102_150405")
	oldPath := r.file.Name()
	newPath := fmt.Sprintf("%s.%s", oldPath, ts)

	if err := r.Sync(); err != nil {
		return err
	}

	if err := r.file.Close(); err != nil {
		return err
	}

	if err := os.Rename(oldPath, newPath); err != nil {
		return err
	}

	f, err := os.OpenFile(oldPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("open new file after rotation: %w", err)
	}

	r.file = f
	r.output = bufio.NewWriter(f)
	r.currentSz = 0

	go r.cleanupOldFiles(newPath)

	return nil
}

func (r *RotatingLogger) cleanupOldFiles(currentPath string) {
	if r.MaxAge <= 0 {
		return
	}

	dir := filepath.Dir(currentPath)
	pattern := filepath.Base(currentPath) + ".*"

	matches, _ := filepath.Glob(filepath.Join(dir, pattern))
	cutoff := time.Now().AddDate(0, 0, -r.MaxAge)

	for _, path := range matches {
		info, err := os.Stat(path)
		if err != nil {
			continue
		}
		if info.ModTime().Before(cutoff) {
			os.Remove(path)
		}
	}
}

func StdLogger() *Logger {
	return New(os.Stderr)
}

func DiscardLogger() *Logger {
	return New(io.Discard)
}
