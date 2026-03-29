package diff

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/TFMV/echoproxy/internal/httpx"
	"github.com/TFMV/echoproxy/internal/log"
)

type DiffOptions struct {
	IgnoreHeaders     httpx.IgnoreHeaders
	JSONNormalization bool
	FloatTolerance    float64
	IgnoreWhitespace  bool
}

var DefaultOptions = DiffOptions{
	IgnoreHeaders:     httpx.DefaultIgnoreHeaders,
	JSONNormalization: true,
	FloatTolerance:    1e-6,
	IgnoreWhitespace:  true,
}

type Result struct {
	Equal            bool              `json:"equal"`
	StatusMatch      bool              `json:"status_match"`
	HeaderDiffs      map[string]string `json:"header_diffs"`
	BodyDiff         *BodyDiff         `json:"body_diff"`
	PrimaryRequest   httpx.Request     `json:"primary_request"`
	SecondaryRequest httpx.Request     `json:"secondary_request"`
}

type BodyDiff struct {
	Type      string     `json:"type"`
	Match     bool       `json:"match"`
	Human     string     `json:"human"`
	Machine   string     `json:"machine,omitempty"`
	JSONDiffs []JSONDiff `json:"json_diffs,omitempty"`
}

type JSONDiff struct {
	Path    string      `json:"path"`
	Primary interface{} `json:"primary"`
	Shadow  interface{} `json:"shadow"`
	Type    string      `json:"type"`
}

func Compare(primary, secondary httpx.RecordedRequest, opts DiffOptions) *Result {
	result := &Result{
		HeaderDiffs:      make(map[string]string),
		PrimaryRequest:   primary.Request,
		SecondaryRequest: secondary.Request,
	}

	result.StatusMatch = primary.Response.StatusCode == secondary.Response.StatusCode
	if !result.StatusMatch {
		result.Equal = false
	} else {
		result.Equal = true
	}

	compareHeaders(primary.Response.Headers, secondary.Response.Headers, opts.IgnoreHeaders, result.HeaderDiffs)
	compareBodies(primary.Response.Body, primary.Response.Headers, secondary.Response.Body, secondary.Response.Headers, opts, result)

	if !result.StatusMatch || len(result.HeaderDiffs) > 0 || (result.BodyDiff != nil && !result.BodyDiff.Match) {
		result.Equal = false
	}

	return result
}

func compareHeaders(primary, secondary http.Header, ignore httpx.IgnoreHeaders, diffs map[string]string) {
	normPrimary := httpx.HeaderToMap(primary)
	normSecondary := httpx.HeaderToMap(secondary)

	allKeys := make(map[string]bool)
	for k := range normPrimary {
		if !ignore.IsIgnored(k) {
			allKeys[k] = true
		}
	}
	for k := range normSecondary {
		if !ignore.IsIgnored(k) {
			allKeys[k] = true
		}
	}

	for k := range allKeys {
		p := normPrimary[k]
		s := normSecondary[k]
		if p != s {
			diffs[k] = fmt.Sprintf("primary: %q, secondary: %q", p, s)
		}
	}
}

func compareBodies(primaryBody []byte, primaryHeaders http.Header, secondaryBody []byte, secondaryHeaders http.Header, opts DiffOptions, result *Result) {
	primaryHash := httpx.ComputeBodyHash(primaryBody)
	secondaryHash := httpx.ComputeBodyHash(secondaryBody)

	if primaryHash == secondaryHash {
		result.BodyDiff = &BodyDiff{Match: true, Type: "hash"}
		return
	}

	primaryIsJSON := httpx.IsJSONContentType(primaryHeaders)
	secondaryIsJSON := httpx.IsJSONContentType(secondaryHeaders)

	if opts.IgnoreWhitespace {
		primaryBody = normalizeWhitespace(primaryBody)
		secondaryBody = normalizeWhitespace(secondaryBody)
	}

	if primaryIsJSON && secondaryIsJSON && opts.JSONNormalization {
		result.BodyDiff = compareJSON(primaryBody, secondaryBody, opts.FloatTolerance)
		return
	}

	result.BodyDiff = &BodyDiff{
		Type:    "binary",
		Match:   false,
		Human:   fmt.Sprintf("Body hash mismatch: primary=%s, secondary=%s", primaryHash[:8], secondaryHash[:8]),
		Machine: fmt.Sprintf(`{"primary_hash":"%s","secondary_hash":"%s"}`, primaryHash, secondaryHash),
	}
}

func compareJSON(primary, secondary []byte, tolerance float64) *BodyDiff {
	var pj, sj interface{}
	if err := json.Unmarshal(primary, &pj); err != nil {
		return &BodyDiff{Type: "json", Match: false, Human: fmt.Sprintf("Failed to parse primary JSON: %v", err)}
	}
	if err := json.Unmarshal(secondary, &sj); err != nil {
		return &BodyDiff{Type: "json", Match: false, Human: fmt.Sprintf("Failed to parse secondary JSON: %v", err)}
	}

	diffs := findJSONDiffs("", pj, sj, tolerance)

	if len(diffs) == 0 {
		return &BodyDiff{Type: "json", Match: true, Human: "Bodies are semantically equal"}
	}

	human := &strings.Builder{}
	human.WriteString("JSON semantic differences:\n")
	for _, d := range diffs {
		human.WriteString(fmt.Sprintf("  %s: %v != %v\n", d.Path, d.Primary, d.Shadow))
	}

	machine, _ := json.Marshal(diffs)

	return &BodyDiff{
		Type:      "json",
		Match:     false,
		Human:     human.String(),
		Machine:   string(machine),
		JSONDiffs: diffs,
	}
}

func findJSONDiffs(path string, primary, secondary interface{}, tolerance float64) []JSONDiff {
	var diffs []JSONDiff

	pmap, ok1 := primary.(map[string]interface{})
	smap, ok2 := secondary.(map[string]interface{})

	if ok1 && ok2 {
		allKeys := make(map[string]bool)
		for k := range pmap {
			allKeys[k] = true
		}
		for k := range smap {
			allKeys[k] = true
		}

		keys := make([]string, 0, len(allKeys))
		for k := range allKeys {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		for _, k := range keys {
			newPath := k
			if path != "" {
				newPath = path + "." + k
			}

			pv, pOK := pmap[k]
			sv, sOK := smap[k]

			if !pOK {
				diffs = append(diffs, JSONDiff{Path: newPath, Primary: nil, Shadow: sv, Type: "missing_in_primary"})
			} else if !sOK {
				diffs = append(diffs, JSONDiff{Path: newPath, Primary: pv, Shadow: nil, Type: "missing_in_secondary"})
			} else {
				subDiffs := findJSONDiffs(newPath, pv, sv, tolerance)
				diffs = append(diffs, subDiffs...)
			}
		}
		return diffs
	}

	parray, isPrimaryArray := primary.([]interface{})
	sarray, isSecondaryArray := secondary.([]interface{})

	if isPrimaryArray && isSecondaryArray {
		maxLen := len(parray)
		if len(sarray) > maxLen {
			maxLen = len(sarray)
		}

		for i := 0; i < maxLen; i++ {
			newPath := fmt.Sprintf("%s[%d]", path, i)

			var pv, sv interface{}
			pOK := i < len(parray)
			sOK := i < len(sarray)

			if pOK {
				pv = parray[i]
			}
			if sOK {
				sv = sarray[i]
			}

			if !pOK {
				diffs = append(diffs, JSONDiff{Path: newPath, Primary: nil, Shadow: sv, Type: "missing_in_primary"})
			} else if !sOK {
				diffs = append(diffs, JSONDiff{Path: newPath, Primary: pv, Shadow: nil, Type: "missing_in_secondary"})
			} else {
				subDiffs := findJSONDiffs(newPath, pv, sv, tolerance)
				diffs = append(diffs, subDiffs...)
			}
		}
		return diffs
	}

	if !deepEqual(primary, secondary, tolerance) {
		diffs = append(diffs, JSONDiff{
			Path:    path,
			Primary: primary,
			Shadow:  secondary,
			Type:    "value_mismatch",
		})
	}

	return diffs
}

func deepEqual(a, b interface{}, tolerance float64) bool {
	af, afOK := a.(float64)
	bf, bfOK := b.(float64)

	if afOK && bfOK {
		diff := af - bf
		if diff < 0 {
			diff = -diff
		}
		return diff <= tolerance
	}

	ai, aiOK := a.(int)
	bi, biOK := b.(int)

	if aiOK && biOK {
		return ai == bi
	}

	if afOK && biOK {
		return float64(bi) == af
	}

	if aiOK && bfOK {
		return float64(ai) == bf
	}

	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
}

func normalizeWhitespace(b []byte) []byte {
	var buf bytes.Buffer
	inString := false
	prevSpace := true

	for i := 0; i < len(b); i++ {
		c := b[i]

		if c == '"' && (i == 0 || b[i-1] != '\\') {
			inString = !inString
			buf.WriteByte(c)
			continue
		}

		if inString {
			buf.WriteByte(c)
			continue
		}

		if c == ' ' || c == '\n' || c == '\r' || c == '\t' {
			if !prevSpace {
				buf.WriteByte(' ')
				prevSpace = true
			}
			continue
		}

		buf.WriteByte(c)
		prevSpace = false
	}

	result := buf.Bytes()
	if len(result) > 0 && result[len(result)-1] == ' ' {
		result = result[:len(result)-1]
	}

	return result
}

func HumanReadable(result *Result) string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("Status: %v\n", result.StatusMatch))

	if len(result.HeaderDiffs) > 0 {
		sb.WriteString("Header Differences:\n")
		for k, v := range result.HeaderDiffs {
			sb.WriteString(fmt.Sprintf("  %s: %s\n", k, v))
		}
	}

	if result.BodyDiff != nil {
		sb.WriteString(fmt.Sprintf("Body (%s): %v\n", result.BodyDiff.Type, result.BodyDiff.Match))
		if !result.BodyDiff.Match {
			sb.WriteString(result.BodyDiff.Human)
		}
	}

	return sb.String()
}

func MachineReadable(result *Result) (string, error) {
	b, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func ToDivergence(result *Result) *log.Divergence {
	if result.Equal {
		return nil
	}

	div := &log.Divergence{
		StatusMismatch: !result.StatusMatch,
		HeaderDiffs:    result.HeaderDiffs,
		PrimaryHash:    httpx.ComputeBodyHash(result.PrimaryRequest.Body),
		ShadowHash:     httpx.ComputeBodyHash(result.SecondaryRequest.Body),
	}

	if result.BodyDiff != nil {
		div.BodyDiff = &log.BodyDiff{
			Type:    result.BodyDiff.Type,
			Match:   result.BodyDiff.Match,
			Message: result.BodyDiff.Human,
		}
		if len(result.BodyDiff.JSONDiffs) > 0 {
			b, _ := json.Marshal(result.BodyDiff.JSONDiffs)
			div.BodyDiff.JSONDiff = b
		}
	}

	return div
}
