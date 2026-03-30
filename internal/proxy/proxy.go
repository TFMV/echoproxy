package proxy

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/TFMV/echoproxy/internal/httpx"
	"github.com/TFMV/echoproxy/internal/log"
)

var _ = net.Listen

type Proxy struct {
	listen   string
	upstream string
	listenMu sync.RWMutex

	server       *http.Server
	logger       *log.JSONLLogger
	client       *http.Client
	transport    *http.Transport
	reverseProxy *httputil.ReverseProxy

	record     bool
	shadowMode bool
	shadowURL  string
	shadowTtl  time.Duration

	statsMu sync.RWMutex
	stats   Stats
}

func (p *Proxy) ListenAddr() string {
	p.listenMu.RLock()
	defer p.listenMu.RUnlock()
	return p.listen
}

func (p *Proxy) SetListen(addr string) {
	p.listenMu.Lock()
	defer p.listenMu.Unlock()
	p.listen = addr
}

type Stats struct {
	Requests  int64 `json:"requests"`
	Responses int64 `json:"responses"`
	Errors    int64 `json:"errors"`
	BytesIn   int64 `json:"bytes_in"`
	BytesOut  int64 `json:"bytes_out"`
}

type Option func(*Proxy)

func WithLogger(logger *log.JSONLLogger) Option {
	return func(p *Proxy) {
		p.logger = logger
	}
}

func WithTransport(t *http.Transport) Option {
	return func(p *Proxy) {
		p.transport = t
	}
}

func WithRecord(record bool) Option {
	return func(p *Proxy) {
		p.record = record
	}
}

func WithShadowMode(shadowURL string, ttl time.Duration) Option {
	return func(p *Proxy) {
		p.shadowMode = true
		p.shadowURL = shadowURL
		p.shadowTtl = ttl
	}
}

func New(listen, upstream string, opts ...Option) *Proxy {
	p := &Proxy{
		listen:   listen,
		upstream: upstream,
	}

	p.transport = &http.Transport{
		TLSClientConfig:       &tls.Config{InsecureSkipVerify: true},
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	p.client = &http.Client{
		Transport: p.transport,
		Timeout:   30 * time.Second,
	}

	for _, opt := range opts {
		opt(p)
	}

	p.server = &http.Server{
		Addr:         p.listen,
		Handler:      p,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	return p
}

func (p *Proxy) Serve() error {
	ln, err := net.Listen("tcp", p.listen)
	if err != nil {
		return err
	}
	p.SetListen(ln.Addr().String())
	return p.server.Serve(ln)
}

func (p *Proxy) ServeTLS(certFile, keyFile string) error {
	return p.server.ListenAndServeTLS(certFile, keyFile)
}

func (p *Proxy) Shutdown(ctx context.Context) error {
	return p.server.Shutdown(ctx)
}

func (p *Proxy) Close() error {
	p.client.CloseIdleConnections()
	return nil
}

func (p *Proxy) Stats() Stats {
	p.statsMu.RLock()
	defer p.statsMu.RUnlock()
	return p.stats
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	reqBody, err := io.ReadAll(r.Body)
	if err != nil {
		p.logger.Error("failed to read request body", log.Fields{"error": err.Error()})
		p.statsMu.Lock()
		p.stats.Errors++
		p.statsMu.Unlock()
		http.Error(w, "failed to read request", http.StatusBadRequest)
		return
	}
	r.Body = io.NopCloser(strings.NewReader(string(reqBody)))

	p.statsMu.Lock()
	p.stats.Requests++
	p.stats.BytesIn += int64(len(reqBody))
	p.statsMu.Unlock()

	upstreamURL := p.upstream + r.RequestURI

	req, err := http.NewRequestWithContext(r.Context(), r.Method, upstreamURL, strings.NewReader(string(reqBody)))
	if err != nil {
		p.logger.Error("failed to create upstream request", log.Fields{"error": err.Error()})
		p.statsMu.Lock()
		p.stats.Errors++
		p.statsMu.Unlock()
		http.Error(w, "upstream error", http.StatusBadGateway)
		return
	}

	for k, v := range r.Header {
		req.Header[k] = v
	}

	timing := httpx.Timing{
		Start:     start,
		NeedStart: true,
	}

	resp, err := p.client.Do(req)
	if err != nil {
		p.logger.Error("upstream request failed", log.Fields{"error": err.Error(), "upstream": upstreamURL})
		p.statsMu.Lock()
		p.stats.Errors++
		p.statsMu.Unlock()
		http.Error(w, "upstream error", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	timing.End = time.Now()
	timing.NeedEnd = true
	timing.Calculate()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		p.logger.Error("failed to read response body", log.Fields{"error": err.Error()})
		p.statsMu.Lock()
		p.stats.Errors++
		p.statsMu.Unlock()
		http.Error(w, "upstream error", http.StatusBadGateway)
		return
	}

	p.statsMu.Lock()
	p.stats.Responses++
	p.stats.BytesOut += int64(len(respBody))
	p.statsMu.Unlock()

	for k, v := range resp.Header {
		for _, vv := range v {
			w.Header().Add(k, vv)
		}
	}

	w.WriteHeader(resp.StatusCode)
	w.Write(respBody)

	if p.record && p.logger != nil {
		rec := httpx.RecordedRequest{
			Request: httpx.Request{
				Time:      start,
				Method:    r.Method,
				URL:       r.URL.String(),
				RawURL:    r.RequestURI,
				Headers:   r.Header.Clone(),
				Body:      reqBody,
				BodyHash:  httpx.ComputeBodyHash(reqBody),
				Size:      int64(len(reqBody)),
				Timestamp: start.Unix(),
			},
			Response: httpx.Response{
				StatusCode: resp.StatusCode,
				Status:     resp.Status,
				Headers:    resp.Header.Clone(),
				Body:       respBody,
				BodyHash:   httpx.ComputeBodyHash(respBody),
				Size:       int64(len(respBody)),
			},
			Timing: timing,
		}

		if err := p.logger.WriteRecordedRequest(rec); err != nil {
			p.logger.Error("failed to write log", log.Fields{"error": err.Error()})
		}
	}

	if p.shadowMode && p.shadowURL != "" {
		go p.doShadow(r, reqBody)
	}
}

func (p *Proxy) doShadow(r *http.Request, reqBody []byte) {
	ctx, cancel := context.WithTimeout(context.Background(), p.shadowTtl)
	defer cancel()

	shadowURL := p.shadowURL + r.RequestURI

	req, err := http.NewRequestWithContext(ctx, r.Method, shadowURL, strings.NewReader(string(reqBody)))
	if err != nil {
		p.logger.Warn("shadow request creation failed", log.Fields{"error": err.Error()})
		return
	}

	for k, v := range r.Header {
		req.Header[k] = v
	}

	resp, err := p.client.Do(req)
	if err != nil {
		p.logger.Warn("shadow request failed", log.Fields{"error": err.Error(), "shadow": shadowURL})
		return
	}
	defer resp.Body.Close()

	primaryBody := reqBody
	shadowBody, _ := io.ReadAll(resp.Body)

	p.logger.Info("shadow result", log.Fields{
		"method":        r.Method,
		"url":           r.RequestURI,
		"primary_body":  string(primaryBody),
		"shadow_body":   string(shadowBody),
		"shadow_status": resp.StatusCode,
	})
}

type ProxyHandler struct {
	proxy *Proxy
}

func NewHandler(listen, upstream string, opts ...Option) *ProxyHandler {
	return &ProxyHandler{
		proxy: New(listen, upstream, opts...),
	}
}

func (h *ProxyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.proxy.ServeHTTP(w, r)
}

func (h *ProxyHandler) Shutdown(ctx context.Context) error {
	return h.proxy.Shutdown(ctx)
}

func (h *ProxyHandler) Stats() Stats {
	return h.proxy.Stats()
}

func ProxyURL(target string) (*url.URL, error) {
	return url.Parse(target)
}

func SingleHostReverseProxy(target *url.URL) *httputil.ReverseProxy {
	return httputil.NewSingleHostReverseProxy(target)
}

type UpstreamDialer struct {
	net.Dialer
}

func (d *UpstreamDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	return d.DialContext(ctx, network, address)
}

func ReadRequest(r *http.Request) (httpx.Request, error) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return httpx.Request{}, fmt.Errorf("read body: %w", err)
	}

	normalizedHeaders := make(http.Header)
	for k, v := range r.Header {
		normalizedHeaders[strings.ToLower(k)] = v
	}

	return httpx.Request{
		Time:      time.Now(),
		Method:    r.Method,
		URL:       r.URL.String(),
		RawURL:    r.RequestURI,
		Headers:   normalizedHeaders,
		Body:      body,
		BodyHash:  httpx.ComputeBodyHash(body),
		Size:      int64(len(body)),
		Timestamp: time.Now().Unix(),
	}, nil
}

func ReadResponse(resp *http.Response) (httpx.Response, error) {
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return httpx.Response{}, fmt.Errorf("read body: %w", err)
	}

	normalizedHeaders := make(http.Header)
	for k, v := range resp.Header {
		normalizedHeaders[strings.ToLower(k)] = v
	}

	return httpx.Response{
		StatusCode: resp.StatusCode,
		Status:     resp.Status,
		Headers:    normalizedHeaders,
		Body:       body,
		BodyHash:   httpx.ComputeBodyHash(body),
		Size:       int64(len(body)),
	}, nil
}

func CopyHeaders(to http.Header, from http.Header) {
	for k, v := range from {
		for _, vv := range v {
			to.Add(k, vv)
		}
	}
}

func BufferRequestBody(r *http.Request) error {
	if r.Body == nil {
		return nil
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return fmt.Errorf("read body: %w", err)
	}
	r.Body = io.NopCloser(strings.NewReader(string(body)))
	return nil
}
