package app

import (
	"context"
	"time"

	"github.com/minhvuongrbs/webhook-service/internal/entities/subscriber"
	"github.com/minhvuongrbs/webhook-service/internal/entities/webhook"
)

//go:generate mockgen --source=./interface.go --destination=./mocks.go --package=app
type PartnerAdapter interface {
	NotifyWebhookEvent(ctx context.Context, w *webhook.Webhook, subscriberEvent subscriber.Event) (*webhook.WebhookResponse, error)
}

type webhookRepository interface {
	GetWebhookById(ctx context.Context, webhookId string) (*webhook.Webhook, error)
	InsertWebhookLog(ctx context.Context, log *webhook.Log) error
	UpdateWebhookStatus(ctx context.Context, webhookId string, status webhook.Status) error
	UpdateWebhookLogRedriveStatus(ctx context.Context, id int64, isResolved bool, errorMsg string, nextRedriveAt *time.Time) error
	IncrFailRate(ctx context.Context, webhookID string) (int64, error)
	GetFailRate(ctx context.Context, webhookID string) (float64, error)
	ResetFailRate(ctx context.Context, webhookID string) error
	IncrSuccessRate(ctx context.Context, webhookID string) error
}

type temporalAdapter interface {
	RegisterWorkflowNotifyEvent(ctx context.Context, e subscriber.Event) error
}

type RateLimiter interface {
	ShouldBlock(ctx context.Context, partnerID string, rate int) (bool, error)
}

type CircuitBreaker interface {
	ShouldTrip(ctx context.Context, webhookID string, errorThreshold int, minRequests int, windowSeconds int) (bool, int64, int64, float64, error)
}

type NotifyEventHandler interface {
	Execute(ctx context.Context, e subscriber.Event) error
	ExecuteWithLogID(ctx context.Context, e subscriber.Event, logID int64, redriveCount int, currentTime time.Time) error
	GetWebhookById(ctx context.Context, webhookId string) (*webhook.Webhook, error)
	InsertWebhookLog(ctx context.Context, log *webhook.Log) error
}

type Application struct {
	RegisterNotifyEventHandler NotifyEventHandler
	NotifyEventHandler         NotifyEventHandler
	RedisClient                interface{}
	RateLimiter                RateLimiter
	PartnerAdapter             PartnerAdapter
	CircuitBreakerManager      CircuitBreaker
}
