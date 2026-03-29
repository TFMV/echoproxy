# EchoProxy 🪞

EchoProxy is a lightweight HTTP traffic capture, replay, and comparison tool.

It runs as a transparent proxy in front of any upstream service and records real requests as JSONL logs for later replay, diffing, and shadow testing.

---

## What it does

EchoProxy lets you:

* Record live HTTP traffic through a proxy
* Replay captured request logs as a timeline
* Diff two traffic runs for behavioral changes
* Run shadow mode to compare two live systems

It is designed as a minimal, single-binary control surface for observing and validating HTTP systems.

---

## Commands

### 1. record

Run a proxy that forwards requests to an upstream and logs all traffic.

```bash
./echoproxy record \
  -listen=:8080 \
  -upstream=http://localhost:9000 \
  -log=echoproxy.jsonl
```

Then send traffic through it:

```bash
curl http://localhost:8080/api/test
```

Output is written to:

```
echoproxy.jsonl
```

Each line is a recorded request + response pair.

---

### 2. replay

Replay a recorded log file as a timeline view.

```bash
./echoproxy replay -file=echoproxy.jsonl
```

This does not execute requests — it prints historical traffic.

---

### 3. diff

Compare two recorded runs and detect behavioral differences.

```bash
./echoproxy diff \
  -a=run1.jsonl \
  -b=run2.jsonl
```

Shows differences in:

* status codes
* response bodies
* request ordering (basic)

---

### 4. shadow

Proxy traffic to a primary upstream while also sending a copy to a shadow system.

Useful for:

* staging validation
* canary comparison
* migration testing

```bash
./echoproxy shadow \
  -listen=:8080 \
  -upstream=http://prod \
  -shadow=http://staging
```

If responses differ, EchoProxy prints a divergence warning.

---

## Data format

Logs are stored as JSONL (one JSON object per line).

Each record contains:

* request method + URL
* headers + body
* response status + headers + body
* latency

---

## Mental model

EchoProxy is not just a proxy.

It is a **runtime comparison layer for HTTP systems**.

Think of it as:

> a replayable memory of how your system behaved under real traffic

---

## Example workflow

1. Record traffic in front of a service
2. Run changes against a new version
3. Diff behavior
4. Shadow production safely before switching

---

## Next directions

Potential extensions:

* JSON-aware semantic diffing
* request correlation IDs
* streaming capture mode
* WASM-based request mutation plugins
* distributed log aggregation

---

## Status

Experimental but functional.
Single binary. No dependencies.
Built for systems that need to see themselves clearly.
