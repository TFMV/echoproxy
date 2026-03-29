package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/TFMV/echoproxy/internal/diff"
	"github.com/TFMV/echoproxy/internal/log"
	"github.com/TFMV/echoproxy/internal/proxy"
	"github.com/TFMV/echoproxy/internal/replay"
)

var (
	listen      string
	upstream    string
	shadow      string
	logFile     string
	target      string
	verbose     bool
	workers     int
	concurrency int
	rateLimit   int
	ordering    string
	showTiming  bool
)

var rootCmd = &cobra.Command{
	Use:   "echoproxy",
	Short: "HTTP traffic recording, replay, diff, and shadow tool",
	Long: `echoproxy - Production-grade HTTP traffic testing tool

Record, replay, diff, and shadow HTTP traffic with:
- Accurate request/response capture (binary-safe, chunked encoding)
- Semantic JSON diffing
- Non-blocking shadow mode
- Structured JSONL logging`,
}

var recordCmd = &cobra.Command{
	Use:   "record",
	Short: "Start recording HTTP traffic",
	RunE:  runRecord,
}

var replayCmd = &cobra.Command{
	Use:   "replay",
	Short: "Replay recorded traffic",
	RunE:  runReplay,
}

var diffCmd = &cobra.Command{
	Use:   "diff",
	Short: "Compare two recorded logs",
	RunE:  runDiff,
}

var shadowCmd = &cobra.Command{
	Use:   "shadow",
	Short: "Run proxy with shadow traffic comparison",
	RunE:  runShadow,
}

func init() {
	rootCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "verbose output")

	recordCmd.Flags().StringVarP(&listen, "listen", "l", ":8080", "listen address")
	recordCmd.Flags().StringVarP(&upstream, "upstream", "u", "", "upstream URL (required)")
	recordCmd.Flags().StringVarP(&logFile, "log", "f", "echoproxy.jsonl", "log file")
	recordCmd.MarkFlagRequired("upstream")

	replayCmd.Flags().StringVarP(&target, "target", "t", "", "target URL (optional, uses original)")
	replayCmd.Flags().StringVarP(&logFile, "file", "f", "echoproxy.jsonl", "log file to replay")
	replayCmd.Flags().IntVarP(&concurrency, "concurrency", "c", 1, "number of concurrent requests")
	replayCmd.Flags().IntVarP(&rateLimit, "rate", "r", 0, "requests per second")
	replayCmd.Flags().StringVarP(&ordering, "order", "o", "original", "request order: original, shuffle, reverse")
	replayCmd.Flags().BoolVarP(&showTiming, "timing", "T", false, "show timing information")

	diffCmd.Flags().StringVarP(&logFile, "file-a", "a", "", "first log file")
	diffCmd.Flags().StringVar(&target, "file-b", "", "second log file")

	shadowCmd.Flags().StringVarP(&listen, "listen", "l", ":8080", "listen address")
	shadowCmd.Flags().StringVarP(&upstream, "upstream", "u", "", "primary upstream URL (required)")
	shadowCmd.Flags().StringVarP(&shadow, "shadow", "s", "", "shadow upstream URL (required)")
	shadowCmd.Flags().StringVarP(&logFile, "log", "f", "echoproxy.jsonl", "log file")
	shadowCmd.MarkFlagRequired("upstream")
	shadowCmd.MarkFlagRequired("shadow")

	rootCmd.AddCommand(recordCmd)
	rootCmd.AddCommand(replayCmd)
	rootCmd.AddCommand(diffCmd)
	rootCmd.AddCommand(shadowCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runRecord(cmd *cobra.Command, args []string) error {
	logger, err := log.NewJSONLLogger(logFile)
	if err != nil {
		return fmt.Errorf("create logger: %w", err)
	}
	defer logger.Close()

	proxyOpts := []proxy.Option{
		proxy.WithLogger(logger),
		proxy.WithRecord(true),
	}

	p := proxy.New(listen, upstream, proxyOpts...)

	fmt.Printf("Recording on %s -> %s\n", listen, upstream)
	fmt.Printf("Logging to %s\n", logFile)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		<-ctx.Done()
		fmt.Println("\nShutting down...")
		p.Shutdown(context.Background())
	}()

	return p.Serve()
}

func runReplay(cmd *cobra.Command, args []string) error {
	opts := []replay.Option{
		replay.WithLogger(os.Stdout),
	}

	if target != "" {
		opts = append(opts, replay.WithTarget(target))
	}

	if concurrency > 1 {
		opts = append(opts, replay.WithConcurrency(concurrency))
	}

	if rateLimit > 0 {
		opts = append(opts, replay.WithRateLimit(rateLimit))
	}

	if ordering != "" {
		opts = append(opts, replay.WithOrdering(ordering))
	}

	opts = append(opts, replay.WithTiming(showTiming))

	engine, err := replay.New(logFile, opts...)
	if err != nil {
		return fmt.Errorf("create engine: %w", err)
	}

	ctx := context.Background()

	fmt.Printf("Replaying %s\n", logFile)
	if target != "" {
		fmt.Printf("Target: %s\n", target)
	}
	fmt.Println()

	results, err := engine.Run(ctx)
	if err != nil {
		return fmt.Errorf("replay failed: %w", err)
	}

	printReplaySummary(results)

	return nil
}

func runDiff(cmd *cobra.Command, args []string) error {
	fileA := logFile
	fileB := target

	if fileA == "" || fileB == "" {
		return fmt.Errorf("both file-a and file-b are required")
	}

	requestsA, err := replay.LoadLogFile(fileA)
	if err != nil {
		return fmt.Errorf("load %s: %w", fileA, err)
	}

	requestsB, err := replay.LoadLogFile(fileB)
	if err != nil {
		return fmt.Errorf("load %s: %w", fileB, err)
	}

	fmt.Printf("Comparing %s (%d requests) vs %s (%d requests)\n\n",
		fileA, len(requestsA), fileB, len(requestsB))

	diffOpts := diff.DefaultOptions

	var diffCount int
	maxLen := len(requestsA)
	if len(requestsB) > maxLen {
		maxLen = len(requestsB)
	}

	for i := 0; i < maxLen; i++ {
		if i >= len(requestsA) {
			fmt.Printf("Extra request in %s at index %d\n", fileB, i)
			diffCount++
			continue
		}
		if i >= len(requestsB) {
			fmt.Printf("Extra request in %s at index %d\n", fileA, i)
			diffCount++
			continue
		}

		result := diff.Compare(requestsA[i], requestsB[i], diffOpts)

		if !result.Equal {
			diffCount++
			fmt.Printf("=== DIFF @ %s %s ===\n",
				requestsA[i].Request.Method,
				requestsA[i].Request.URL)
			fmt.Println(diff.HumanReadable(result))
		}
	}

	fmt.Printf("\nTotal differences: %d/%d\n", diffCount, maxLen)

	if diffCount > 0 {
		return fmt.Errorf("differences found")
	}

	return nil
}

func runShadow(cmd *cobra.Command, args []string) error {
	logger, err := log.NewJSONLLogger(logFile)
	if err != nil {
		return fmt.Errorf("create logger: %w", err)
	}
	defer logger.Close()

	proxyOpts := []proxy.Option{
		proxy.WithLogger(logger),
		proxy.WithRecord(true),
		proxy.WithShadowMode(shadow, 10*time.Second),
	}

	p := proxy.New(listen, upstream, proxyOpts...)

	fmt.Printf("Shadow mode on %s\n", listen)
	fmt.Printf("Primary: %s\n", upstream)
	fmt.Printf("Shadow:  %s\n", shadow)
	fmt.Printf("Logging to %s\n", logFile)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		<-ctx.Done()
		fmt.Println("\nShutting down...")
		p.Shutdown(context.Background())
	}()

	return p.Serve()
}

func printReplaySummary(results []replay.Result) {
	var success, failed int
	var totalDuration time.Duration

	for _, r := range results {
		if r.Error != "" {
			failed++
		} else {
			success++
		}
		totalDuration += r.Duration
	}

	fmt.Println()
	fmt.Printf("Total:   %d\n", len(results))
	fmt.Printf("Success: %d\n", success)
	fmt.Printf("Failed:  %d\n", failed)
	if len(results) > 0 {
		fmt.Printf("Avg:     %s\n", totalDuration/time.Duration(len(results)))
	}
}

var _ = io.Discard
