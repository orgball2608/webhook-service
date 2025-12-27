# Webhook Service (Shopee Policy Edition)

[![Go](https://img.shields.io/badge/Go-1.21+-blue.svg)](https://golang.org)
[![Temporal](https://img.shields.io/badge/Temporal-1.26+-orange.svg)](https://temporal.io)
[![Kafka](https://img.shields.io/badge/Kafka-2.8+-red.svg)](https://kafka.apache.org)
[![MySQL](https://img.shields.io/badge/MySQL-8.0+-blue.svg)](https://www.mysql.com)

A production-grade, self-healing webhook platform inspired by Shopee's SaaS architecture. Handles tens of millions of requests with advanced reliability, auto-governance, and real-time health audit.

---

## 🚀 Key Features (2025 Edition)

- **Triple Lane - Dual Phase Delivery**: Critical, Default, Backlog queues for optimal resource allocation and retry isolation.
- **Shopee Policy Health Audit**: Periodic Temporal workflow audits webhook health, disables or warns on low success rate (6h window, batch/pagination, anti-spam warning).
- **Bucketed Stats in Redis**: 10-minute buckets, blazing fast MGET for stats, minimal Redis load.
- **Distributed Circuit Breaker**: Per-webhook, per-partner, config-driven, protects against short-term spikes.
- **4xx/5xx Error Classification**: Smart error handling, separate 4xx warning logic for better UX.
- **Self-Governing**: Auto-disable, auto-warn, and self-recovery without human intervention.
- **Batch/Pagination**: Scalable audit for 100k+ webhooks, never OOM.

---

## 🏗️ Architecture Overview

![Webhook Notifier](./docs/diagram/webhook_notifier.png)

- **Kafka**: Ingests events.
- **App Layer**: Eager send, fallback, circuit breaker, stat recording.
- **Temporal**: Orchestrates retries, health audit, cron redrive.
- **Redis**: Fast bucketed stats, circuit breaker, anti-spam flags.
- **MySQL**: Durable storage.

---

## 🔥 Shopee Policy: How It Works

### 1. **Webhook Delivery (Triple Lane - Dual Phase)**
- **Phase 1 (Fresh):**
  - `sync_stock`/`payment_success` → Critical Queue (max resource)
  - Others → Default Queue
- **Phase 2 (Retry):**
  - Any failure (429/5xx) → Backlog Queue (slow, throttled)
  - Terminal errors (4xx except 429) → Mark failed, no retry

### 2. **Bucketed Stats (Redis)**
- **Key:** `webhook:stats:{id}:{success|fail}:{bucket}` (10-min bucket, TTL 7h)
- **MGET** for all buckets in 6h window (36 buckets) for both success/fail
- **Ultra-fast**: Only 2 Redis calls per webhook per audit

### 3. **Webhook Health Audit**
- **Runs every 30min** (Temporal cron workflow)
- **Batch/Pagination**: Fetches webhooks in pages (default 500/batch)
- **Logic:**
  - If total > 600 in 6h & success rate < 30% → Disable webhook, send email (anti-spam: only once/6h)
  - If total > 600 in 6h & success rate < 70% → Send warning email (anti-spam: only once/6h)
  - 4xx rate high → Optional: Send config warning

### 4. **Circuit Breaker**
- **Per-webhook config** (from metadata)
- **Short window (5min)**, disables only for transient spikes
- **No hardcoded threshold**: All dynamic from webhook config

---

## 📦 Project Structure (Key Folders)

```
internal/
  adapters/
    repository/webhook.go      # Redis/MySQL logic, bucketed stats, pagination
  app/                        # Business logic, circuit breaker, stat recording
  ports/temporal_workflow/    # Temporal workflows: delivery, audit, cron
  ...
```

---

## 🛠️ Quick Start

```bash
# Clone & start all services
make docker-compose-up

# Build & run locally
make build
./webhook_service --help
```

---

## ⚙️ Configuration

Edit `config/local.yaml` for DB, Kafka, Redis, Temporal, etc.

---

## 🧑‍💻 Usage

- **Start Kafka Consumer:** `./webhook_service kafka-consumer`
- **Start Temporal Worker:** `./webhook_service temporal-worker`
- **Start Health Audit Worker:** `./webhook_service temporal-cron-worker`

---

## 🧠 Advanced Logic

- **Pagination:** `GetActiveWebhooksPaginated(ctx, offset, limit)` for batch audit
- **MGET Stats:** `CalculateSuccessRate` uses MGET for all buckets
- **Anti-Spam Warning:** Audit workflow sets Redis flag to avoid duplicate emails
- **Per-Webhook CB:** `isCircuitOpen` uses config from webhook.Metadata

---

## 📊 Monitoring

- **Grafana:** [http://localhost:3000](http://localhost:3000)
- **Temporal UI:** [http://localhost:8080](http://localhost:8080)
- **Prometheus:** [http://localhost:9090](http://localhost:9090)

---

## 🧪 Load Testing

- See `testing/kafka_load_test.js` and `config/local.yaml` for loadtest config

---

## 🤝 Contributing

- Fork, branch, PR as usual
- Run `make generate_sqlc` after SQL changes
- Run `go test ./...` for all tests

---

## 📄 License

MIT License. See [LICENSE](LICENSE).

---

*Built for scale, reliability, and zero-ops webhook delivery. Inspired by Shopee, powered by Go + Temporal.*
