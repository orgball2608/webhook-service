package service

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"time"

	"github.com/minhvuongrbs/webhook-service/config"
	"github.com/minhvuongrbs/webhook-service/internal/adapters/partner"
	redisrl "github.com/minhvuongrbs/webhook-service/internal/adapters/redis"
	"github.com/minhvuongrbs/webhook-service/internal/adapters/repository/webhook"
	"github.com/minhvuongrbs/webhook-service/internal/adapters/temporal"
	"github.com/minhvuongrbs/webhook-service/internal/app"
	"github.com/minhvuongrbs/webhook-service/internal/common/httpclient"
	"github.com/minhvuongrbs/webhook-service/internal/entities/subscriber"
	webhookEntity "github.com/minhvuongrbs/webhook-service/internal/entities/webhook"
	"github.com/minhvuongrbs/webhook-service/pkg/database"
	"github.com/minhvuongrbs/webhook-service/pkg/logging"
	"github.com/minhvuongrbs/webhook-service/pkg/redis"
	pkgtemporal "github.com/minhvuongrbs/webhook-service/pkg/temporal"
	goredis "github.com/redis/go-redis/v9"
	"go.temporal.io/sdk/client"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

// ProvideApplication returns fx options for dependency injection
func ProvideApplication(conf config.Config) fx.Option {
	return fx.Options(
		fx.Provide(func() config.Config { return conf }),
		fx.Provide(database.NewMysqlDatabaseConn),
		fx.Provide(redis.NewRedisClient),
		fx.Provide(func(db *sql.DB, redisClient *goredis.Client) webhook.Repository {
			return webhook.NewRepository(db, redisClient)
		}),
		fx.Provide(func() *http.Client {
			httpClientTP := httpclient.NewRoundTripper()
			return &http.Client{Timeout: conf.HttpClient.Timeout, Transport: httpClientTP}
		}),
		fx.Provide(func() *simpleLogger { return &simpleLogger{} }),
		fx.Provide(pkgtemporal.NewTemporalClient),
		fx.Provide(func(temporalClient client.Client, conf config.Config) temporal.Adapter {
			return temporal.NewAdapter(temporalClient, conf.Temporal.TaskQueue)
		}),
		fx.Provide(func(redisClient *goredis.Client) app.RateLimiter {
			return redisrl.NewRedisRateLimiter(redisClient)
		}),
		fx.Provide(func(redisClient *goredis.Client) app.CircuitBreaker {
			return redisrl.NewCircuitBreakerManagerAdapter(redisrl.NewCircuitBreakerManager(redisClient))
		}),
		fx.Provide(func(httpClient *http.Client, rateLimiter app.RateLimiter, logger *simpleLogger, conf config.Config) app.PartnerAdapter {
			if config.IsLoadTestEnv(conf.Env) {
				fmt.Println("system is running under loadtest environment")
				return partner.NewLoadTestAdapter()
			}
			return partner.NewAdapter(*httpClient, rateLimiter, logger, 3)
		}),
		fx.Provide(func(temporalAdapter temporal.Adapter, webhookRepo webhook.Repository) app.RegisterNotifyEventHandler {
			return app.NewRegisterNotifyEventHandler(&temporalAdapter, webhookRepo)
		}),
		fx.Provide(func(webhookRepo webhook.Repository, partnerAdapter app.PartnerAdapter, circuitBreaker app.CircuitBreaker, rateLimiter app.RateLimiter, registerHandler app.RegisterNotifyEventHandler) app.NotifyEventHandler {
			registerHandlerWrapper := registerHandlerWrapper{handler: registerHandler, webhookRepo: webhookRepo}
			return app.NewNotifyEventHandler(webhookRepo, partnerAdapter, circuitBreaker, rateLimiter, registerHandlerWrapper)
		}),
		fx.Provide(func(registerHandler app.RegisterNotifyEventHandler, notifyHandler app.NotifyEventHandler, redisClient *goredis.Client, rateLimiter app.RateLimiter, partnerAdapter app.PartnerAdapter, circuitBreaker app.CircuitBreaker) app.App {
			return app.App{
				RegisterNotifyEventHandler: registerHandler,
				NotifyEventHandler:         notifyHandler,
				RedisClient:                redisClient,
				RateLimiter:                rateLimiter,
				PartnerAdapter:             partnerAdapter,
				CircuitBreakerManager:      circuitBreaker,
			}
		}),
		fx.Provide(func(a app.App) app.Application { return &a }),
		fx.Provide(func(cfg config.Config) logging.Config {
			return cfg.Logger
		}),
		fx.Provide(func(cfg config.Config) redis.Config {
			return cfg.RedisConnection
		}),
		fx.Provide(func(cfg config.Config) pkgtemporal.Config {
			return cfg.Temporal
		}),
		fx.Invoke(logging.InitLogger), // Initialize logger on startup
		fx.Invoke(func(lc fx.Lifecycle, db *sql.DB, redisClient *goredis.Client, temporalClient client.Client) {
			lc.Append(fx.Hook{
				OnStop: func(ctx context.Context) error {
					if err := db.Close(); err != nil {
						return err
					}
					if err := redisClient.Close(); err != nil {
						return err
					}
					temporalClient.Close()
					return nil
				},
			})
		}),
	)
}

type simpleLogger struct{}

func (l *simpleLogger) Info(msg string, keysAndValues ...interface{}) {
	zap.L().Info(msg, zap.Any("args", keysAndValues))
}

func (l *simpleLogger) Error(msg string, keysAndValues ...interface{}) {
	zap.L().Error(msg, zap.Any("args", keysAndValues))
}

func (l *simpleLogger) Debug(msg string, keysAndValues ...interface{}) {
	zap.L().Debug(msg, zap.Any("args", keysAndValues))
}

func (l *simpleLogger) Warn(msg string, keysAndValues ...interface{}) {
	zap.L().Warn(msg, zap.Any("args", keysAndValues))
}

type registerHandlerWrapper struct {
	handler     app.RegisterNotifyEventHandler
	webhookRepo webhook.Repository
}

func (w registerHandlerWrapper) FetchWebhooksByIDs(ctx context.Context, ids []string) ([]*webhookEntity.Webhook, error) {
	return w.webhookRepo.FetchWebhooksByIDs(ctx, ids)
}

func (w registerHandlerWrapper) GetActiveWebhooksPaginated(ctx context.Context, offset, limit int) ([]*webhookEntity.Webhook, error) {
	return w.webhookRepo.GetActiveWebhooksPaginated(ctx, offset, limit)
}

func (w registerHandlerWrapper) Execute(ctx context.Context, e subscriber.Event) error {
	return w.handler.Execute(ctx, e)
}

func (w registerHandlerWrapper) ExecuteWithLogID(ctx context.Context, e subscriber.Event, logID int64, redriveCount int, currentTime time.Time) error {
	return w.handler.ExecuteWithLogID(ctx, e, logID, redriveCount, currentTime)
}

func (w registerHandlerWrapper) GetWebhookById(ctx context.Context, webhookId string) (*webhookEntity.Webhook, error) {
	return w.webhookRepo.GetWebhookById(ctx, webhookId)
}

func (w registerHandlerWrapper) InsertWebhookLog(ctx context.Context, log *webhookEntity.Log) error {
	return w.webhookRepo.InsertWebhookLog(ctx, log)
}

func (w registerHandlerWrapper) GetActiveWebhooks(ctx context.Context) ([]*webhookEntity.Webhook, error) {
	return w.webhookRepo.GetActiveWebhooks(ctx)
}

func (w registerHandlerWrapper) CalculateSuccessRate(ctx context.Context, webhookID string) (float64, int64, error) {
	return w.webhookRepo.CalculateSuccessRate(ctx, webhookID)
}

func (w registerHandlerWrapper) DisableWebhook(ctx context.Context, webhookID string) error {
	return w.webhookRepo.UpdateWebhookStatus(ctx, webhookID, webhookEntity.StatusInactive)
}
