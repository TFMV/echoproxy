package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"
)

type RecordedRequest struct {
	Time    time.Time   `json:"time"`
	Method  string      `json:"method"`
	URL     string      `json:"url"`
	Headers http.Header `json:"headers"`
	Body    string      `json:"body"`

	StatusCode  int         `json:"status_code"`
	RespHeaders http.Header `json:"resp_headers"`
	RespBody    string      `json:"resp_body"`

	DurationMS int64 `json:"duration_ms"`
}

type Config struct {
	Listen   string
	Upstream string
	Shadow   string
	LogFile  string
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	switch os.Args[1] {

	case "record":
		recordCmd()

	case "replay":
		replayCmd()

	case "diff":
		diffCmd()

	case "shadow":
		shadowCmd()

	default:
		usage()
	}
}

func usage() {
	fmt.Println(`EchoProxy

Commands:
  record   Run proxy and log traffic
  replay   Print recorded traffic
  diff     Compare two logs
  shadow   Proxy + shadow upstream and compare responses`)
}

/* ---------------- RECORD ---------------- */

func recordCmd() {
	fs := flag.NewFlagSet("record", flag.ExitOnError)
	listen := fs.String("listen", ":8080", "listen addr")
	upstream := fs.String("upstream", "", "upstream URL")
	logfile := fs.String("log", "echoproxy.jsonl", "log file")
	fs.Parse(os.Args[2:])

	if *upstream == "" {
		log.Fatal("upstream required")
	}

	client := &http.Client{Timeout: 30 * time.Second}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		body, _ := io.ReadAll(r.Body)
		r.Body.Close()

		req, _ := http.NewRequest(r.Method, *upstream+r.RequestURI, bytes.NewReader(body))
		req.Header = r.Header.Clone()

		resp, err := client.Do(req)
		if err != nil {
			http.Error(w, err.Error(), 502)
			return
		}
		defer resp.Body.Close()

		respBody, _ := io.ReadAll(resp.Body)

		for k, v := range resp.Header {
			for _, vv := range v {
				w.Header().Add(k, vv)
			}
		}

		w.WriteHeader(resp.StatusCode)
		w.Write(respBody)

		rec := RecordedRequest{
			Time:        start,
			Method:      r.Method,
			URL:         r.URL.String(),
			Headers:     r.Header,
			Body:        string(body),
			StatusCode:  resp.StatusCode,
			RespHeaders: resp.Header,
			RespBody:    string(respBody),
			DurationMS:  time.Since(start).Milliseconds(),
		}

		appendLog(*logfile, rec)
	})

	log.Printf("recording on %s → %s", *listen, *upstream)
	log.Fatal(http.ListenAndServe(*listen, nil))
}

/* ---------------- REPLAY ---------------- */

func replayCmd() {
	fs := flag.NewFlagSet("replay", flag.ExitOnError)
	file := fs.String("file", "echoproxy.jsonl", "log file")
	fs.Parse(os.Args[2:])

	f, err := os.Open(*file)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	dec := json.NewDecoder(f)

	for {
		var rec RecordedRequest
		err := dec.Decode(&rec)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("[%s] %s %s → %d (%dms)\n",
			rec.Time.Format(time.RFC3339),
			rec.Method,
			rec.URL,
			rec.StatusCode,
			rec.DurationMS,
		)
	}
}

/* ---------------- DIFF ---------------- */

func diffCmd() {
	fs := flag.NewFlagSet("diff", flag.ExitOnError)
	aFile := fs.String("a", "", "file A")
	bFile := fs.String("b", "", "file B")
	fs.Parse(os.Args[2:])

	if *aFile == "" || *bFile == "" {
		log.Fatal("a and b required")
	}

	a := loadLogs(*aFile)
	b := loadLogs(*bFile)

	fmt.Println("Diff report:\n")

	max := len(a)
	if len(b) > max {
		max = len(b)
	}

	for i := 0; i < max; i++ {
		if i >= len(a) || i >= len(b) {
			fmt.Printf("Mismatch length at index %d\n", i)
			continue
		}

		if a[i].RespBody != b[i].RespBody ||
			a[i].StatusCode != b[i].StatusCode {

			fmt.Printf("DIFF @ %s %s\n", a[i].Method, a[i].URL)
			fmt.Printf("  A: %d %s\n", a[i].StatusCode, trim(a[i].RespBody))
			fmt.Printf("  B: %d %s\n\n", b[i].StatusCode, trim(b[i].RespBody))
		}
	}
}

/* ---------------- SHADOW ---------------- */

func shadowCmd() {
	fs := flag.NewFlagSet("shadow", flag.ExitOnError)
	listen := fs.String("listen", ":8080", "listen")
	upstream := fs.String("upstream", "", "primary")
	shadow := fs.String("shadow", "", "shadow target")
	fs.Parse(os.Args[2:])

	if *upstream == "" || *shadow == "" {
		log.Fatal("upstream and shadow required")
	}

	client := &http.Client{Timeout: 30 * time.Second}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		r.Body.Close()

		req1, _ := http.NewRequest(r.Method, *upstream+r.RequestURI, bytes.NewReader(body))
		req2, _ := http.NewRequest(r.Method, *shadow+r.RequestURI, bytes.NewReader(body))

		req1.Header = r.Header.Clone()
		req2.Header = r.Header.Clone()

		resp1, err := client.Do(req1)
		if err != nil {
			http.Error(w, err.Error(), 502)
			return
		}
		defer resp1.Body.Close()

		resp2, err := client.Do(req2)
		if err != nil {
			http.Error(w, err.Error(), 502)
			return
		}
		defer resp2.Body.Close()

		b1, _ := io.ReadAll(resp1.Body)
		b2, _ := io.ReadAll(resp2.Body)

		// return primary response
		for k, v := range resp1.Header {
			for _, vv := range v {
				w.Header().Add(k, vv)
			}
		}
		w.WriteHeader(resp1.StatusCode)
		w.Write(b1)

		if resp1.StatusCode != resp2.StatusCode || !bytes.Equal(b1, b2) {
			fmt.Println("⚠️ shadow divergence detected:")
			fmt.Println("PRIMARY:", resp1.StatusCode, string(b1))
			fmt.Println("SHADOW :", resp2.StatusCode, string(b2))
		}
	})

	log.Printf("shadow mode: %s → %s (shadow)", *upstream, *shadow)
	log.Fatal(http.ListenAndServe(*listen, nil))
}

/* ---------------- HELPERS ---------------- */

func appendLog(file string, rec RecordedRequest) {
	f, err := os.OpenFile(file, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Println(err)
		return
	}
	defer f.Close()

	b, _ := json.Marshal(rec)
	f.Write(append(b, '\n'))
}

func loadLogs(file string) []RecordedRequest {
	f, err := os.Open(file)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	var out []RecordedRequest
	dec := json.NewDecoder(f)

	for {
		var rec RecordedRequest
		err := dec.Decode(&rec)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		out = append(out, rec)
	}
	return out
}

func trim(s string) string {
	if len(s) > 120 {
		return s[:120] + "..."
	}
	return s
}
