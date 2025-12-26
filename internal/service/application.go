package service

import (
	"context"
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
	"go.uber.org/zap"
)

func NewApplication(conf config.Config) (app.App, error) {
	db, err := database.NewMysqlDatabaseConn(conf.Database)
	if err != nil {
		return app.App{}, fmt.Errorf("failed to connect database: %w", err)
	}
	redisClient, err := redis.NewRedisClient(conf.RedisConnection)
	if err != nil {
		return app.App{}, fmt.Errorf("failed to connect redis: %w", err)
	}
	webhookRepo := webhook.NewRepository(db, redisClient)

	httpClientTP := httpclient.NewRoundTripper()
	httpClient := http.Client{Timeout: conf.HttpClient.Timeout, Transport: httpClientTP}

	// Initialize logger
	err = logging.InitLogger(conf.Logger)
	if err != nil {
		return app.App{}, fmt.Errorf("failed to initialize logger: %w", err)
	}
	logger := &simpleLogger{}

	temporalClient, err := pkgtemporal.NewTemporalClient(conf.Temporal)
	if err != nil {
		return app.App{}, fmt.Errorf("failed to create temporal client: %w", err)
	}
	temporalAdapter := temporal.NewAdapter(temporalClient, conf.Temporal.TaskQueue)
	redisRateLimiter := redisrl.NewRedisRateLimiter(redisClient)
	circuitBreakerManager := redisrl.NewCircuitBreakerManager(redisClient)
	circuitBreakerAdapter := redisrl.NewCircuitBreakerManagerAdapter(circuitBreakerManager)

	var partnerAdapter app.PartnerAdapter
	if config.IsLoadTestEnv(conf.Env) {
		fmt.Println("system is running under loadtest environment")
		partnerAdapter = partner.NewLoadTestAdapter()
	} else {
		partnerAdapter = partner.NewAdapter(httpClient, redisRateLimiter, logger, 3)
	}

	registerHandler := app.NewRegisterNotifyEventHandler(temporalAdapter, webhookRepo)
	registerHandlerWrapper := registerHandlerWrapper{handler: registerHandler, webhookRepo: webhookRepo}

	return app.App{
		RegisterNotifyEventHandler: registerHandler,
		NotifyEventHandler:         app.NewNotifyEventHandler(webhookRepo, partnerAdapter, circuitBreakerAdapter, redisRateLimiter, registerHandlerWrapper),
		RedisClient:                redisClient,
		RateLimiter:                redisRateLimiter,
		PartnerAdapter:             partnerAdapter,
		CircuitBreakerManager:      circuitBreakerAdapter,
	}, nil
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
