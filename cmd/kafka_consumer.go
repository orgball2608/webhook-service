package cmd

import (
	"context"
	"fmt"

	"github.com/minhvuongrbs/webhook-service/config"
	"github.com/minhvuongrbs/webhook-service/internal/app"
	"github.com/minhvuongrbs/webhook-service/internal/ports/kafka_consumer"
	"github.com/minhvuongrbs/webhook-service/internal/service"
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

	var consumeApp *pubsub.KafkaConsumeApp
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
		fx.Invoke(func(a *pubsub.KafkaConsumeApp) {
			consumeApp = a
		}),
		fx.Invoke(func() {
			metric_server.StartPromAndHealthHTTPServerNoLocking(conf.Monitoring.KafkaConsumerPrometheusPort)
		}),
	).Run()

	ctx := context.Background()
	if err = consumeApp.StartConsume(ctx); err != nil {
		l.Errorw("cannot start kafka consumer", "error", err)
		return err
	}
	return nil
}
