package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/minhvuongrbs/webhook-service/config"
	"github.com/minhvuongrbs/webhook-service/internal/app"
	"github.com/minhvuongrbs/webhook-service/internal/ports/kafka_consumer"
	"github.com/minhvuongrbs/webhook-service/internal/service"
	"github.com/minhvuongrbs/webhook-service/pkg/database"
	"github.com/minhvuongrbs/webhook-service/pkg/logging"
	"github.com/minhvuongrbs/webhook-service/pkg/metric_server"
	"github.com/minhvuongrbs/webhook-service/pkg/pubsub"
	"github.com/urfave/cli/v2"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

const (
	defaultMaxRetries = 3
)

func startKafkaConsumer(cmdCLI *cli.Context) error {
	confPath := cmdCLI.String("config")
	conf, err := config.LoadConfig(confPath)
	if err != nil {
		return fmt.Errorf("cannot load config")
	}

	err = logging.InitLogger(conf.Logger)
	if err != nil {
		return err
	}

	l := zap.S()
	l.Infow("start kafka consumer", "config", conf)

	l.Infow("CONFIG & PORT MAPPING",
		"DB_HOST", conf.Database.Address,
		"DB_USER", conf.Database.User,
		"DB_PASS", conf.Database.Passwd,
		"KAFKA_BROKERS", conf.KafkaSubscriberEvent.Brokers,
		"CONFIG_PATH", confPath,
		"PORT", conf.Monitoring.KafkaConsumerPrometheusPort,
	)
	l.Infow("ENV & PORT MAPPING",
		"DB_HOST", os.Getenv("DB_HOST"),
		"DB_PORT", os.Getenv("DB_PORT"),
		"DB_USER", os.Getenv("DB_USER"),
		"DB_PASS", os.Getenv("DB_PASS"),
		"KAFKA_BROKERS", os.Getenv("KAFKA_BROKERS"),
		"CONFIG_PATH", os.Getenv("CONFIG_PATH"),
		"PORT", os.Getenv("PORT"),
	)
	l.Infow("ALL ENV", "envs", os.Environ())

	var consumeApp *pubsub.KafkaConsumeApp
	l.Info("Before fx.New().Run() for DI")
	fx.New(
		service.ProvideApplication(conf),
		fx.Provide(func(conf config.Config) pubsub.KafkaSubscriberConfig {
			return pubsub.KafkaSubscriberConfig{
				SubscribeConfig:          conf.KafkaSubscriberEvent,
				DeadLetterProducerConfig: conf.DeadLetterProducer,
				MaxRetries:               defaultMaxRetries,
			}
		}),
		fx.Provide(func(appInstance app.App, partnerAdapter app.PartnerAdapter, rateLimiter app.RateLimiter, kafkaConf pubsub.KafkaSubscriberConfig) (*pubsub.KafkaConsumer, error) {
			consumeSubscriberEventHandler := kafka_consumer.NewConsumeSubscriberEvent(appInstance, partnerAdapter, rateLimiter)
			return pubsub.NewKafkaConsumer(kafkaConf, consumeSubscriberEventHandler.Handle)
		}),
		fx.Provide(func(consumer *pubsub.KafkaConsumer) (*pubsub.KafkaConsumeApp, error) {
			return pubsub.NewKafkaConsumeApp(consumer)
		}),
		fx.Provide(func(cfg config.Config) database.Config {
			return cfg.Database
		}),
		fx.Invoke(func(a *pubsub.KafkaConsumeApp) {
			consumeApp = a
		}),
		fx.Invoke(func() {
			metric_server.StartPromAndHealthHTTPServerNoLocking(conf.Monitoring.KafkaConsumerPrometheusPort)
		}),
	).Run()
	l.Info("After fx.New().Run() for DI")

	ctx := context.Background()
	if err = consumeApp.StartConsume(ctx); err != nil {
		l.Errorw("cannot start kafka consumer", "error", err)
		return err
	}

	fmt.Println("Kafka consumer started successfully")

	return nil
}
