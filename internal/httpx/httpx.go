package httpx

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"
)

type Request struct {
	Time        time.Time   `json:"time"`
	Method      string      `json:"method"`
	URL         string      `json:"url"`
	RawURL      string      `json:"raw_url"`
	Headers     http.Header `json:"headers"`
	Body        []byte      `json:"body"`
	BodyEncoded string      `json:"body_encoding,omitempty"`
	BodyHash    string      `json:"body_hash"`

	Size      int64 `json:"size"`
	Timestamp int64 `json:"timestamp"`
}

type Response struct {
	StatusCode  int         `json:"status_code"`
	Status      string      `json:"status"`
	Headers     http.Header `json:"headers"`
	Body        []byte      `json:"body"`
	BodyEncoded string      `json:"body_encoding,omitempty"`
	BodyHash    string      `json:"body_hash"`

	Size int64 `json:"size"`
}

type Timing struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`

	DNS       time.Duration `json:"dns,omitempty"`
	Connect   time.Duration `json:"connect,omitempty"`
	TLS       time.Duration `json:"tls,omitempty"`
	FirstByte time.Duration `json:"first_byte,omitempty"`
	Total     time.Duration `json:"total"`

	NeedStart bool `json:"-"`
	NeedEnd   bool `json:"-"`
}

func (t *Timing) Calculate() {
	if t.NeedStart && t.NeedEnd {
		t.Total = t.End.Sub(t.Start)
	}
}

type RecordedRequest struct {
	Request  Request  `json:"request"`
	Response Response `json:"response"`
	Timing   Timing   `json:"timing"`
	Error    string   `json:"error,omitempty"`
}

func CloneRequest(r *http.Request) *http.Request {
	clone := r.Clone(r.Context())
	clone.Header = r.Header.Clone()
	if r.Body != nil {
		body, _ := io.ReadAll(r.Body)
		r.Body = io.NopCloser(bytes.NewReader(body))
		clone.Body = io.NopCloser(bytes.NewReader(body))
	}
	return clone
}

func NormalizeHeader(h http.Header) http.Header {
	normalized := make(http.Header)
	for k, v := range h {
		normalized[strings.ToLower(k)] = v
	}
	return normalized
}

func HeaderToMap(h http.Header) map[string]string {
	m := make(map[string]string)
	for k, v := range h {
		if len(v) > 0 {
			m[strings.ToLower(k)] = strings.Join(v, ", ")
		}
	}
	return m
}

func MapToHeader(m map[string]string) http.Header {
	h := make(http.Header)
	for k, v := range m {
		h[k] = []string{v}
	}
	return h
}

func ComputeBodyHash(body []byte) string {
	if len(body) == 0 {
		return ""
	}
	hash := sha256.Sum256(body)
	return base64.StdEncoding.EncodeToString(hash[:])
}

func EncodeBody(body []byte, encoding string) ([]byte, string, error) {
	if len(body) == 0 {
		return nil, "", nil
	}

	switch strings.ToLower(encoding) {
	case "gzip":
		return body, "gzip", nil
	case "deflate":
		return body, "deflate", nil
	case "base64":
		encoded := base64.StdEncoding.EncodeToString(body)
		return []byte(encoded), "base64", nil
	default:
		return body, "", nil
	}
}

func DecodeBody(body []byte, encoding string) ([]byte, error) {
	if len(body) == 0 || encoding == "" {
		return body, nil
	}

	switch strings.ToLower(encoding) {
	case "gzip":
		return DecompressGzip(bytes.NewReader(body))
	case "deflate":
		return DecompressDeflate(bytes.NewReader(body))
	case "base64":
		return base64.StdEncoding.DecodeString(string(body))
	default:
		return body, nil
	}
}

func DecompressGzip(r io.Reader) ([]byte, error) {
	gr, err := gzip.NewReader(r)
	if err != nil {
		return nil, err
	}
	defer gr.Close()
	return io.ReadAll(gr)
}

func DecompressDeflate(r io.Reader) ([]byte, error) {
	fr := flate.NewReader(r)
	defer fr.Close()
	return io.ReadAll(fr)
}

func ReadRequestBody(req *http.Request) ([]byte, error) {
	if req.Body == nil {
		return nil, nil
	}
	return io.ReadAll(req.Body)
}

func ReadResponseBody(resp *http.Response) ([]byte, error) {
	if resp.Body == nil {
		return nil, nil
	}
	return io.ReadAll(resp.Body)
}

func NormalizeJSON(body []byte) ([]byte, error) {
	var v interface{}
	if err := json.Unmarshal(body, &v); err != nil {
		return body, nil
	}
	return json.Marshal(v)
}

func IsJSONContentType(h http.Header) bool {
	ct := h.Get("Content-Type")
	return strings.Contains(ct, "application/json")
}

func IsTextContentType(h http.Header) bool {
	ct := h.Get("Content-Type")
	return strings.Contains(ct, "text/") ||
		strings.Contains(ct, "application/json") ||
		strings.Contains(ct, "application/xml")
}

type IgnoreHeaders map[string]bool

var DefaultIgnoreHeaders = IgnoreHeaders{
	"date":              true,
	"server":            true,
	"content-length":    true,
	"connection":        true,
	"keep-alive":        true,
	"transfer-encoding": true,
}

func (i IgnoreHeaders) IsIgnored(header string) bool {
	return i[strings.ToLower(header)]
}
