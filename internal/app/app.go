package app

import (
	"context"

	webhookEntity "github.com/minhvuongrbs/webhook-service/internal/entities/webhook"
	"github.com/redis/go-redis/v9"
)

type App struct {
	NotifyEventHandler         NotifyEventHandler
	RegisterNotifyEventHandler RegisterNotifyEventHandler
	RedisClient                *redis.Client
	RateLimiter                RateLimiter
	PartnerAdapter             PartnerAdapter
	CircuitBreakerManager      CircuitBreaker
}

var _ Application = &App{}

func (a *App) GetActiveWebhooksPaginated(ctx context.Context, offset, limit int) ([]*webhookEntity.Webhook, error) {
	return a.NotifyEventHandler.GetActiveWebhooksPaginated(ctx, offset, limit)
}

func (a *App) CalculateSuccessRate(ctx context.Context, webhookID string) (float64, int64, error) {
	return a.NotifyEventHandler.CalculateSuccessRate(ctx, webhookID)
}

func (a *App) DisableWebhook(ctx context.Context, webhookID string) error {
	return a.NotifyEventHandler.DisableWebhook(ctx, webhookID)
}

func (a *App) FetchWebhooksByIDs(ctx context.Context, ids []string) ([]*webhookEntity.Webhook, error) {
	return a.NotifyEventHandler.FetchWebhooksByIDs(ctx, ids)
}
