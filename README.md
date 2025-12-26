# Webhook Service

[![Go](https://img.shields.io/badge/Go-1.21+-blue.svg)](https://golang.org)
[![Temporal](https://img.shields.io/badge/Temporal-1.26+-orange.svg)](https://temporal.io)
[![Kafka](https://img.shields.io/badge/Kafka-2.8+-red.svg)](https://kafka.apache.org)
[![MySQL](https://img.shields.io/badge/MySQL-8.0+-blue.svg)](https://www.mysql.com)

A robust, scalable webhook service built with Go, designed to handle event notifications to partners with advanced reliability features. This repository is a fork from the original [webhook-service](https://github.com/minhvuongrbs/webhook-service) by minhvuongrbs, enhanced with production-ready improvements.

## 🚀 Features

### Core Features
- **Event-Driven Webhook Delivery**: Process subscriber events (created, subscribed, unsubscribed) and notify partners via HTTP.
- **Reliable Retry Mechanism**: Automatic retries with exponential backoff using Temporal workflows.
- **Idempotent Processing**: Ensures events are processed exactly once, preventing duplicates.

### Advanced Enhancements (Added in this Fork)
- **Distributed Circuit Breaker**: Redis-based circuit breaker to prevent cascading failures across multiple pods.
- **Auto-Disable Webhooks**: Automatically pause webhooks with high failure rates (>100 consecutive failures) to protect resources.
- **Priority Queues**: Separate Temporal task queues for high and low priority webhooks.
- **Cron Redrive**: Automated periodic retry for failed webhooks using Temporal Cron workflows.
- **Enhanced Monitoring**: Comprehensive metrics and logging for observability.

## 📋 Table of Contents

- [Architecture Overview](#architecture-overview)
- [Technical Stack](#technical-stack)
- [Why Temporal?](#why-temporal-over-traditional-message-queues-eg-rabbitmq)
- [Project Structure](#project-structure)
- [Installation](#installation)
- [Usage](#usage)
- [Characteristics](#characteristics)
- [Monitoring](#monitoring)
- [Load Testing](#load-testing)
- [Contributing](#contributing)
- [License](#license)

## 🏗️ Architecture Overview

![Webhook Notifier](./docs/diagram/webhook_notifier.png)

The system follows a clean architecture with layered design:
- **Kafka Consumer**: Ingests events from Kafka topics.
- **App Layer**: Contains business logic, including eager sending and fallback to Temporal.
- **Temporal Workflows**: Handle retries, circuit breakers, and cron redrive.
- **Adapters**: Interface with external systems (Redis, MySQL, HTTP clients).

## 🛠️ Technical Stack

- **Programming Language**: Golang
- **Database**: MySQL
- **Message Broker**: Kafka
- **Workflow Orchestration**: [Temporal](https://temporal.io) for durable, retryable activities
- **Caching & Circuit Breaker**: Redis
- **Monitoring**: Prometheus + Grafana
- **Containerization**: Docker + Docker Compose

### Why Temporal over Traditional Message Queues (e.g., RabbitMQ)?

While traditional message queues like RabbitMQ excel at simple pub/sub and task distribution, Temporal provides advanced orchestration for complex, long-running workflows:
- **Durable Execution**: Workflows persist state across failures, restarts, and deployments.
- **Built-in Retry & Backoff**: Automatic retry with exponential backoff, circuit breakers, and custom policies.
- **Visibility & Debugging**: Rich UI for real-time monitoring, workflow history, and failure debugging.
- **Time-Based Operations**: Native support for timers, delays, and cron schedules.
- **Scalability**: Handles millions of concurrent workflows with horizontal scaling.
- **Idempotency**: Ensures workflows run exactly once, preventing duplicate processing.

In this system, Kafka handles initial event ingestion, while Temporal manages complex retry and orchestration for webhook deliveries.

## 📁 Project Structure

```
webhook-service/
├── cmd/                    # CLI entry points
├── config/                 # Configuration files
├── docs/                   # Documentation and diagrams
├── internal/
│   ├── adapters/           # External system interfaces (Redis, HTTP, DB)
│   ├── app/                # Business logic and use cases
│   ├── common/             # Shared constants and utilities
│   ├── entities/           # Domain models
│   ├── ports/              # Input interfaces (Kafka, Temporal)
│   └── service/            # Dependency injection
├── pkg/                    # Reusable packages
├── testing/                # Test utilities
└── README.md
```

## 🚀 Installation

### Prerequisites
- Docker & Docker Compose
- Go 1.21+ (for development)

### Quick Start
```bash
# Clone the repository
git clone https://github.com/orgball2608/webhook-service.git
cd webhook-service

# Start all services
make docker-compose-up

# Or build and run locally
go mod tidy
go build -o webhook_service .
./webhook_service --help
```

### Configuration
Update `config/local.yaml` for your environment:
```yaml
database:
  address: localhost:3308
  user: your_user
  passwd: your_password

kafka:
  brokers: ["localhost:9092"]

temporal:
  host: localhost:7233

redis:
  addr: localhost:6379
```

## 📖 Usage

### Starting Services
```bash
# Start Kafka consumer
./webhook_service kafka-consumer

# Start Temporal worker
./webhook_service temporal-worker

# Start Cron worker for redrive
./webhook_service temporal-cron-worker
```

### API Endpoints
- **Health Check**: `GET /health`
- **Metrics**: `GET /metrics` (Prometheus format)

### Example Event Payload
```json
{
  "event_name": "subscriber.created",
  "event_time": "2025-12-26T10:00:00Z",
  "subscriber": {
    "id": "sub123",
    "email": "user@example.com",
    "first_name": "John",
    "last_name": "Doe"
  },
  "webhook_id": "wh456"
}
```

## ⚙️ Characteristics

This system is designed with enterprise-grade characteristics:
- **Reliability**: Temporal ensures recoverability from failures.
- **Scalability**: Horizontal scaling with multiple workers and pods.
- **Observability**: Comprehensive monitoring and logging.
- **Resilience**: Circuit breakers, auto-disable, and graceful degradation.

### Trade-offs and Demo Notes
In production, you might use a single Temporal task queue instead of priority queues for simplicity. The trade-off is reduced prioritization for critical traffic. This implementation showcases advanced scaling but can be simplified as needed.

## 📊 Monitoring

Prometheus and Grafana provide full-stack monitoring.

### Accessing Tools
- **Grafana Dashboard**: [http://localhost:3000](http://localhost:3000)
- **Temporal UI**: [http://localhost:8080](http://localhost:8080)
- **Prometheus Metrics**: [http://localhost:9090](http://localhost:9090)

### Key Metrics
- Webhook delivery success/failure rates
- Circuit breaker states
- Queue depths and processing times
- Temporal workflow executions

## 🧪 Load Testing

The system includes built-in load testing capabilities.

### Configuration
Update `config/local.yaml`:
```yaml
env: loadtest
loadtest:
  message_count: 1000
  error_rate: 0.05
  processing_delay: "0-10s"
```

### Running Tests
```bash
# Trigger load test via Docker Compose
docker-compose exec kafka produce_batch_kafka_messages
```

This generates 1000 Kafka messages with configurable error rates and delays.

## 🤝 Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Development Setup
```bash
# Run tests
go test ./...

# Generate mocks
go generate ./...

# Lint code
golangci-lint run
```

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Original project by [minhvuongrbs](https://github.com/minhvuongrbs/webhook-service)
- [Temporal](https://temporal.io) for powerful workflow orchestration
- [Go](https://golang.org) community for excellent tooling

---

*Built with ❤️ for reliable webhook delivery at scale.*
